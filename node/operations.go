package node

import (
	"log"
	"sync"
	"sync/atomic"
)

func (n *Node) Set(key, value string) {
	n.dataStore.Set(key, value)
}

func (n *Node) Get(key string) (string, bool) {
	return n.dataStore.Get(key)
}

func (n *Node) Delete(key string) {
	n.dataStore.Delete(key)
}

func (n *Node) setRole(role Role) {
	n.role = role
}

func (n *Node) askForVote() {
	var totalVotes atomic.Int32 = atomic.Int32{}
	electionTerm := n.currentTerm
	totalVotes.Store(1)

	wg := sync.WaitGroup{}
	
	for _, peerID := range n.transport.GetPeers() {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			args := RequestVoteArgs{
				currentTerm: n.currentTerm,
				candidateID: n.id,
				lastLogTerm: n.persistentStore.LastTerm(),
				lastLogIndex: n.persistentStore.LastIndex(),
			}
			reply, err := n.transport.SendRequestVote(id, args)
			if err != nil {
				log.Printf("Error %v", err.Error())
			}
			
			n.mutex.Lock()
			if reply.term > n.currentTerm {
				log.Printf("Peer %v term: %v, candidate %v currentTerm: %v", id, reply.term, n.id, n.currentTerm)
				n.currentTerm = reply.term
				n.role = Follower
				n.votedFor	 = -1
				n.persistentStore.SaveTermAndVote(n.currentTerm, n.votedFor)
			}
			n.mutex.Unlock()

			if reply.voteGranted {
				totalVotes.Add(1)
			}
		}(peerID)
	}
	wg.Wait()
	n.mutex.Lock()
	defer n.mutex.Unlock()
	if n.role != Candidate || n.currentTerm != electionTerm {
    	return
	}
	if totalVotes.Load() > int32(len(n.transport.GetPeers())/2) {
		log.Printf("Node %d received majority votes, becoming leader", n.id)
		n.setRole(Leader)
		n.currentTerm+=1
	}
}

func (n *Node) giveVote() {
	// Implementation for giving a vote to a candidate node
}

func (n *Node) receiveWriteRequest() {
	// Implementation for receiving write requests from clients
}

func (n *Node) sendWriteRequest() {
	// Implementation for sending write requests to other nodes
}