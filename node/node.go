package node

import (
	"log"
	"sync"
)

type Node struct {
    id              int
    role            Role
    currentTerm     int
    votedFor        int
    leaderId        int
    commitIndex     int
    lastApplied     int
    nextIndex       map[int]int
    matchIndex      map[int]int
    dataStore       DataStore
    persistentStore PersistentStore
    transport       Transport
    receivedInput   chan struct{}
    mutex           sync.Mutex
}

func NewNode(id int, dataStore DataStore, persistentStore PersistentStore, transport Transport) *Node {
    n := &Node{
        id:              id,
        role:            Follower, // explicit, not relying on zero-value alone
        votedFor:        -1,
        leaderId:        -1,
        dataStore:       dataStore,
        persistentStore: persistentStore,
        transport:       transport,
        nextIndex:       make(map[int]int),
        matchIndex:      make(map[int]int),
        receivedInput:   make(chan struct{}, 1),
    }
    // recover persisted state on startup - commitIndex/lastApplied stay 0,
    // they're volatile and get re-derived from the leader's next heartbeat
    if term, votedFor, err := persistentStore.LoadTermAndVote(); err == nil {
        n.currentTerm = term
        n.votedFor = votedFor
    }
    return n
}

func (n *Node) Start() {
    for {
        n.mutex.Lock()
        role := n.role
        n.mutex.Unlock()

        switch role {
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