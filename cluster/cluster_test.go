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