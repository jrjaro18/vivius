package cluster

import (
	"testing"
	"time"

	"vivius/node"
)

// countLeaders returns how many nodes currently believe they're Leader.
func countLeaders(c *Cluster) int {
    count := 0
    for _, n := range c.Nodes {
        if n.GetRole() == node.Leader {
            count++
        }
    }
    return count
}

// waitForExactlyOneLeader polls the cluster until exactly one leader is
// found, or the timeout expires. Polling instead of a fixed sleep avoids
// tests being slower than necessary on a fast run, and avoids flakiness
// on a slow one.
func waitForExactlyOneLeader(t *testing.T, c *Cluster, timeout time.Duration) {
    t.Helper()
    deadline := time.Now().Add(timeout)
    for time.Now().Before(deadline) {
        if countLeaders(c) == 1 {
            return
        }
        time.Sleep(10 * time.Millisecond)
    }
    t.Fatalf("no single leader elected within %v (found %d)", timeout, countLeaders(c))
}


func findLeader(c *Cluster) *node.Node {
    for _, n := range c.Nodes {
        if n.GetRole() == node.Leader {
            return n
        }
    }
    return nil
}

func findLeaderAmong(c *Cluster, ids []int) *node.Node {
    for _, id := range ids {
        if n := c.Nodes[id]; n.GetRole() == node.Leader {
            return n
        }
    }
    return nil
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool) bool {
    t.Helper()
    deadline := time.Now().Add(timeout)
    for time.Now().Before(deadline) {
        if cond() {
            return true
        }
        time.Sleep(10 * time.Millisecond)
    }
    return false
}

func TestThreeNodeClusterElectsExactlyOneLeader(t *testing.T) {
    c := NewCluster([]int{1, 2, 3})
    c.StartAll()

    waitForExactlyOneLeader(t, c, 2*time.Second)

    // give it a bit longer and check AGAIN - catches the case where an
    // election "succeeds" briefly but then a second node also becomes
    // leader shortly after (a real safety violation, not just slow startup)
    time.Sleep(500 * time.Millisecond)
    if got := countLeaders(c); got != 1 {
        t.Fatalf("expected exactly 1 leader after stabilizing, got %d", got)
    }
}

func TestFiveNodeClusterElectsExactlyOneLeader(t *testing.T) {
    c := NewCluster([]int{1, 2, 3, 4, 5})
    c.StartAll()

    waitForExactlyOneLeader(t, c, 2*time.Second)
}

func TestSingleNodeClusterElectsItselfLeader(t *testing.T) {
    // edge case: a "cluster" of 1 node should trivially elect itself,
    // since a majority of 1 is 1 - worth checking the majority math
    // doesn't break at this boundary
    c := NewCluster([]int{1})
    c.StartAll()

    waitForExactlyOneLeader(t, c, 1*time.Second)
}

func TestLeaderRemainsStableWithNoFailures(t *testing.T) {
    // with no partitions/crashes, the SAME node should stay leader -
    // if leadership keeps flipping between nodes with everything healthy,
    // that points to a heartbeat/timeout timing bug, not a real failure scenario
    c := NewCluster([]int{1, 2, 3})
    c.StartAll()

    waitForExactlyOneLeader(t, c, 2*time.Second)

    var firstLeaderID int
    for id, n := range c.Nodes {
        if n.GetRole() == node.Leader {
            firstLeaderID = id
            break
        }
    }

    time.Sleep(1 * time.Second)

    if c.Nodes[firstLeaderID].GetRole() != node.Leader {
        t.Fatalf("node %d was leader but lost leadership with no failures injected", firstLeaderID)
    }
    if got := countLeaders(c); got != 1 {
        t.Fatalf("expected exactly 1 leader after stability window, got %d", got)
    }
}

func TestClientWriteReplicatesToAllNodes(t *testing.T) {
    c := NewCluster([]int{1, 2, 3})
    c.StartAll()
    waitForExactlyOneLeader(t, c, 2*time.Second)

    var leader *node.Node
    for _, n := range c.Nodes {
        if n.GetRole() == node.Leader {
            leader = n
        }
    }

    err := leader.ReceiveWriteRequestFromClient(node.Command{Op: "Put", Key: "x", Value: "1"})
    if err != nil {
        t.Fatalf("write failed: %v", err)
    }

    time.Sleep(300 * time.Millisecond) // let commitIndex propagate to followers via heartbeat

    for id, n := range c.Nodes {
        val, ok := n.Get("x") // needs a small exported getter, see below
        if !ok || val != "1" {
            t.Fatalf("node %d does not have expected value: got %q, ok=%v", id, val, ok)
        }
    }
}

// --- 1. Core safety: a leader stuck in the minority partition can never commit a write ---

func TestMinorityPartitionLeaderCannotCommit(t *testing.T) {
    ids := []int{1, 2, 3, 4, 5}
    c := NewCluster(ids)
    c.StartAll()

    if !waitUntil(t, 2*time.Second, func() bool { return findLeader(c) != nil }) {
        t.Fatal("no leader elected initially")
    }
    leader := findLeader(c)
    leaderID := leader.GetId()

    // pick one healthy follower to trap in the minority alongside the leader
    var partnerID int
    for _, id := range ids {
        if id != leaderID {
            partnerID = id
            break
        }
    }

    // partition leader + 1 follower away from the other 3 (minority = 2, majority = 3)
    minority := []int{leaderID, partnerID}
    majority := []int{}
    for _, id := range ids {
        if id != leaderID && id != partnerID {
            majority = append(majority, id)
        }
    }
    c.Partition(minority...)

    // majority side must elect a NEW leader
    if !waitUntil(t, 2*time.Second, func() bool { return findLeaderAmong(c, majority) != nil }) {
        t.Fatal("majority partition failed to elect a new leader")
    }

    // old leader's write must never succeed - it can't reach majority
    err := c.Nodes[leaderID].ReceiveWriteRequestFromClient(node.Command{Op: "Put", Key: "x", Value: "stale"})
    t.Logf("write result: err=%v", err) // add this before the if err == nil check
    if err == nil {
        t.Fatal("expected write on minority-partitioned leader to fail, but it succeeded")
    }

    c.Heal(minority...)
}

// --- 2. Majority side keeps making progress while a minority is cut off ---

func TestMajorityPartitionElectsNewLeaderAndProgresses(t *testing.T) {
    ids := []int{1, 2, 3, 4, 5}
    c := NewCluster(ids)
    c.StartAll()

    if !waitUntil(t, 2*time.Second, func() bool { return findLeader(c) != nil }) {
        t.Fatal("no leader elected initially")
    }
    leader := findLeader(c)
    leaderID := leader.GetId()

    minority := []int{leaderID}
    for _, id := range ids {
        if id != leaderID {
            if len(minority) < 2 {
                minority = append(minority, id)
            }
        }
    }
    c.Partition(minority...)

    var majority []int
    for _, id := range ids {
        isMinority := false
        for _, m := range minority {
            if id == m {
                isMinority = true
            }
        }
        if !isMinority {
            majority = append(majority, id)
        }
    }

    if !waitUntil(t, 2*time.Second, func() bool { return findLeaderAmong(c, majority) != nil }) {
        t.Fatal("majority failed to elect a new leader")
    }

    newLeader := findLeaderAmong(c, majority)
    err := newLeader.ReceiveWriteRequestFromClient(node.Command{Op: "Put", Key: "y", Value: "1"})
    if err != nil {
        t.Fatalf("write on majority-side leader should succeed, got: %v", err)
    }

    ok := waitUntil(t, 2*time.Second, func() bool {
        for _, id := range majority {
            val, found := c.Nodes[id].Get("y")
            if !found || val != "1" {
                return false
            }
        }
        return true
    })
    if !ok {
        for _, id := range majority {
            val, found := c.Nodes[id].Get("y")
            t.Logf("node %d: val=%q found=%v", id, val, found)
        }
        t.Fatal("majority did not converge on committed write")
    }

    c.Heal(minority...)
}

// --- 3. After healing, every node converges to the same store state ---

func TestPartitionHealsAndStoreConverges(t *testing.T) {
    ids := []int{1, 2, 3, 4, 5}
    c := NewCluster(ids)
    c.StartAll()

    if !waitUntil(t, 2*time.Second, func() bool { return findLeader(c) != nil }) {
        t.Fatal("no leader elected initially")
    }
    leader := findLeader(c)
    leaderID := leader.GetId()

    minority := []int{leaderID}
    for _, id := range ids {
        if id != leaderID && len(minority) < 2 {
            minority = append(minority, id)
        }
    }
    c.Partition(minority...)

    var majority []int
    for _, id := range ids {
        m := false
        for _, mm := range minority {
            if id == mm {
                m = true
            }
        }
        if !m {
            majority = append(majority, id)
        }
    }

    if !waitUntil(t, 2*time.Second, func() bool { return findLeaderAmong(c, majority) != nil }) {
        t.Fatal("majority failed to elect during partition")
    }
    newLeader := findLeaderAmong(c, majority)
    if err := newLeader.ReceiveWriteRequestFromClient(node.Command{Op: "Put", Key: "z", Value: "42"}); err != nil {
        t.Fatalf("write during partition failed: %v", err)
    }

    c.Heal(minority...)

    // give heartbeats time to reconcile the rejoined minority nodes
    ok := waitUntil(t, 3*time.Second, func() bool {
        for _, id := range ids {
            val, found := c.Nodes[id].Get("z")
            if !found || val != "42" {
                return false
            }
        }
        return true
    })
    if !ok {
        for _, id := range ids {
            val, found := c.Nodes[id].Get("z")
            t.Logf("node %d: val=%q found=%v", id, val, found)
        }
        t.Fatal("cluster did not converge on committed write after healing")
    }
}

// --- 4. Stale leader's uncommitted entry gets discarded on rejoin, never observable ---

func TestStaleLeaderRejoinDiscardsUncommittedEntry(t *testing.T) {
    ids := []int{1, 2, 3, 4, 5}
    c := NewCluster(ids)
    c.StartAll()

    if !waitUntil(t, 2*time.Second, func() bool { return findLeader(c) != nil }) {
        t.Fatal("no leader elected initially")
    }
    leader := findLeader(c)
    leaderID := leader.GetId()

    // partner stays with the old leader in the minority
    var partnerID int
    for _, id := range ids {
        if id != leaderID {
            partnerID = id
            break
        }
    }
    minority := []int{leaderID, partnerID}
    c.Partition(minority...)

    var majority []int
    for _, id := range ids {
        if id != leaderID && id != partnerID {
            majority = append(majority, id)
        }
    }

    // old leader tries a write - will be appended locally but never committed
    _ = leader.ReceiveWriteRequestFromClient(node.Command{Op: "Put", Key: "ghost", Value: "should-not-survive"})

    if !waitUntil(t, 2*time.Second, func() bool { return findLeaderAmong(c, majority) != nil }) {
        t.Fatal("majority failed to elect during partition")
    }
    newLeader := findLeaderAmong(c, majority)
    if err := newLeader.ReceiveWriteRequestFromClient(node.Command{Op: "Put", Key: "real", Value: "committed"}); err != nil {
        t.Fatalf("majority write failed: %v", err)
    }

    c.Heal(minority...)

    ok := waitUntil(t, 3*time.Second, func() bool {
        _, ghostFound := c.Nodes[leaderID].Get("ghost")
        val, realFound := c.Nodes[leaderID].Get("real")
        return !ghostFound && realFound && val == "committed"
    })
    if !ok {
        ghostVal, ghostFound := c.Nodes[leaderID].Get("ghost")
        realVal, realFound := c.Nodes[leaderID].Get("real")
        t.Fatalf("old leader did not converge correctly: ghost(found=%v,val=%q) real(found=%v,val=%q)",
            ghostFound, ghostVal, realFound, realVal)
    }
}

// --- 5. Old leader resurfacing mid-election does not disrupt candidates or already-cast votes ---

func TestOldLeaderCannotDisruptOngoingElection(t *testing.T) {
    ids := []int{1, 2, 3, 4, 5}
    c := NewCluster(ids)
    c.StartAll()

    if !waitUntil(t, 2*time.Second, func() bool { return findLeader(c) != nil }) {
        t.Fatal("no leader elected initially")
    }
    leader := findLeader(c)
    leaderID := leader.GetId()

    // isolate old leader alone - rest of cluster must elect a new leader
    c.Partition(leaderID)

    var rest []int
    for _, id := range ids {
        if id != leaderID {
            rest = append(rest, id)
        }
    }
    if !waitUntil(t, 2*time.Second, func() bool {
        n := findLeaderAmong(c, rest)
        return n != nil
    }) {
        t.Fatal("remaining 4 nodes failed to elect a new leader")
    }

    c.Heal(leaderID)

    // old leader must step down and NOT remain/become leader again on its own term
    ok := waitUntil(t, 2*time.Second, func() bool {
        return c.Nodes[leaderID].GetRole() != node.Leader
    })
    if !ok {
        t.Fatalf("old leader (node %d) incorrectly remained Leader after rejoining", leaderID)
    }

    // cluster should settle back to exactly one leader overall
    if !waitUntil(t, 2*time.Second, func() bool {
        count := 0
        for _, n := range c.Nodes {
            if n.GetRole() == node.Leader {
                count++
            }
        }
        return count == 1
    }) {
        t.Fatal("cluster did not settle back to exactly one leader after full heal")
    }
}