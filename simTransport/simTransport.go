package simtransport

import (
    "errors"
    "sync"

    "vivius/node"
)

type SimTransport struct {
    mu           sync.Mutex
    nodes        map[int]*node.Node
    peers        []int
    partitioned  map[int]bool // nodes currently cut off from everyone
}

func NewSimTransport() *SimTransport {
    return &SimTransport{
        nodes:        make(map[int]*node.Node),
        partitioned:  make(map[int]bool),
    }
}

func (t *SimTransport) RegisterNode(id int, n *node.Node) {
    t.mu.Lock()
    defer t.mu.Unlock()
    t.nodes[id] = n
    t.peers = append(t.peers, id)
}

func (t *SimTransport) GetAllNodeIds() []int {
    t.mu.Lock()
    defer t.mu.Unlock()
    result := make([]int, len(t.peers))
    copy(result, t.peers)
    return result
}

func (t *SimTransport) isReachable(target int) bool {
    t.mu.Lock()
    defer t.mu.Unlock()
    return !t.partitioned[target]
}

func (t *SimTransport) SendRequestVote(target int, args node.RequestVoteArgs) (node.RequestVoteReply, error) {
    if !t.isReachable(target) {
        return node.RequestVoteReply{}, errors.New("target unreachable")
    }
    t.mu.Lock()
    targetNode, ok := t.nodes[target]
    t.mu.Unlock()
    if !ok {
        return node.RequestVoteReply{}, errors.New("unknown node")
    }
    return targetNode.GiveVote(args), nil // exported wrapper - see note below
}

func (t *SimTransport) SendAppendEntries(target int, args node.AppendEntriesArgs) (node.AppendEntriesReply, error) {
    if !t.isReachable(target) {
        return node.AppendEntriesReply{}, errors.New("target unreachable")
    }
    t.mu.Lock()
    targetNode, ok := t.nodes[target]
    t.mu.Unlock()
    if !ok {
        return node.AppendEntriesReply{}, errors.New("unknown node")
    }
    return targetNode.ReceiveWriteRequestFromLeader(args), nil
}

// --- fault injection, for your test harness ---

func (t *SimTransport) PartitionNode(id int) {
    t.mu.Lock()
    defer t.mu.Unlock()
    t.partitioned[id] = true
}

func (t *SimTransport) HealPartition(id int) {
    t.mu.Lock()
    defer t.mu.Unlock()
    delete(t.partitioned, id)
}