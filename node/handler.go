package node

import (
	"math/rand"
	"time"
)

func (n *Node) handleFollower() {
    timeout := 150*time.Millisecond + time.Duration(rand.Intn(150))*time.Millisecond
    timer := time.NewTimer(timeout)
    defer timer.Stop()

    select {
    case <-timer.C:
        n.mutex.Lock()
        n.role = Candidate
        n.mutex.Unlock()
    case <-n.receivedInput:
        // valid heartbeat/entry arrived - stay follower, loop will call
        // handleFollower again fresh next iteration of Start()
    }
}

func (n *Node) handleCandidate() {
    n.mutex.Lock()
    n.currentTerm++
    n.votedFor = n.id
    n.persistentStore.SaveTermAndVote(n.currentTerm, n.votedFor)
    n.mutex.Unlock()

    n.askForVote() // blocks until vote-counting completes (askForVote already does wg.Wait())

    n.mutex.Lock()
    stillCandidate := n.role == Candidate
    n.mutex.Unlock()

    if stillCandidate {
        // didn't win (split vote or not enough reachable peers) - back off
        // randomly before retrying, same reasoning as the follower timeout
        timeout := 150*time.Millisecond + time.Duration(rand.Intn(150))*time.Millisecond
        time.Sleep(timeout)
    }
    // if role changed (became Leader, or got demoted to Follower via a
    // higher-term reply inside askForVote), just return - Start()'s loop
    // will pick up the new role on its next iteration
}

func (n *Node) handleLeader() {
    for _, peerID := range n.peers() {
        go func(id int) {
            n.propagateWriteRequest(id) // entry param now unused, per earlier note - sends whatever's in nextIndex..latest, or nothing (pure heartbeat) if caught up
        }(peerID)
    }
    time.Sleep(50 * time.Millisecond) // heartbeat interval - must be well under the election timeout (150-300ms)
}