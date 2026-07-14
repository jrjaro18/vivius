package node

import (
	"vivius/kvStore"
)

type Role int

const (
	RoleLeader Role = iota
	RoleCandidate
	RoleFollower
)

type Node struct {
	id    int
	role  Role
	store kvStore.Store
}

func NewNode(id int, store kvStore.Store) *Node {
	return &Node{
		id:    id,
		store: store,
	}
}

func (n *Node) Set(key, value string)  {
	n.store.Set(key, value)
}

func (n *Node) Get(key string) (string, bool) {
	return n.store.Get(key)
}