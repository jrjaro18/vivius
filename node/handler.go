package node

import "time"

func (n *Node) handleFollower() {
	timer := time.NewTimer(150 * time.Millisecond)
	select {
	case <-timer.C:
		n.mutex.Lock()
		defer n.mutex.Unlock()
		n.setRole(Candidate)
	case <-n.receivedInput:
		timer.Stop()
	}
}

func (n *Node) handleCandidate() {
	// Implement the candidate loop logic here
}

func (n *Node) handleLeader() {
	// Implement the leader loop logic here
}