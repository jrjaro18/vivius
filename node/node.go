package node

import (
	"log"
	"sync"
)

type Node struct {
	id   int
	role Role
	currentTerm int
	votedFor    int
	// commitIndex    	int
	// lastApplied		int
	// nextIndex      	map[int]int
	// matchIndex     	map[int]int
	dataStore       DataStore
	persistentStore PersistentStore
	transport       Transport
	receivedInput   chan struct{}
	mutex           sync.Mutex
}

func NewNode(id int, dataStore DataStore, persistentStore PersistentStore, transport Transport) *Node {
	return &Node{
		id:   id,
		role: Follower,
		currentTerm: 0,
		// votedFor:    -1,
		dataStore:       dataStore,
		persistentStore: persistentStore,
		transport:       transport,
		receivedInput:   make(chan struct{}),
	}
}

func (n *Node) Start() {
	// TODO: check to pass the persistent store and data store
	for {
		switch n.role {
		case Follower:
			n.handleFollower()
		case Candidate:
			n.handleCandidate()
		case Leader:
			n.handleLeader()
		default:
			log.Fatal("Unknown role: ", n.role)
		}
	}
}
