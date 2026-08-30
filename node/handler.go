package node

import (
    "log"
    "math/rand"
    "time"
)

func (n *Node) handleFollower() {
	// log.Printf("Node %d: handling follower state", n.id)

    timeout := 150*time.Millisecond + time.Duration(rand.Intn(150))*time.Millisecond
    timer := time.NewTimer(timeout)
    defer timer.Stop()

    select {
    case <-timer.C:
        n.mutex.Lock()
        log.Printf("Node %d: election timeout elapsed (term %d) - becoming Candidate", n.id, n.currentTerm)
        n.role = Candidate
        n.mutex.Unlock()
    case <-n.receivedInput:
        // valid heartbeat/entry arrived - stay follower, loop will call
        // handleFollower again fresh next iteration of Start()
        // log.Printf("Node %d: received heartbeat/entry, resetting election timer", n.id)
    }
}

func (n *Node) handleCandidate() {
	log.Printf("Node %d: handling candidate state", n.id)
    n.mutex.Lock()
    n.currentTerm++
    n.votedFor = n.id
    n.persistentStore.SaveTermAndVote(n.currentTerm, n.votedFor)
    term := n.currentTerm
    n.mutex.Unlock()

    // log.Printf("Node %d: starting election for term %d", n.id, term)

    n.askForVote() // blocks until vote-counting completes (askForVote already does wg.Wait())

    n.mutex.Lock()
    stillCandidate := n.role == Candidate
    role := n.role
    n.mutex.Unlock()

    if stillCandidate {
        log.Printf("Node %d: election for term %d inconclusive, backing off before retry", n.id, term)
        timeout := 150*time.Millisecond + time.Duration(rand.Intn(150))*time.Millisecond
        time.Sleep(timeout)
    } else {
        log.Printf("Node %d: exiting candidate state for term %d, new role=%v", n.id, term, role)
    }
    // if role changed (became Leader, or got demoted to Follower via a
    // higher-term reply inside askForVote), just return - Start()'s loop
    // will pick up the new role on its next iteration
}

func (n *Node) handleLeader() {
	// log.Printf("Node %d: handling leader state", n.id)

    n.mutex.Lock()
    term := n.currentTerm
    n.mutex.Unlock()

    for _, peerID := range n.peers() {
        go func(id int) {
            success, err := n.propagateWriteRequest(id)
            if err != nil {
                log.Printf("Node %d (leader, term %d): heartbeat to %d failed: %v", n.id, term, id, err)
                return
            }
            if !success {
                log.Printf("Node %d (leader, term %d): heartbeat to %d rejected", n.id, term, id)
            }
        }(peerID)
    }
    time.Sleep(50 * time.Millisecond) // heartbeat interval - must be well under the election timeout (150-300ms)
}