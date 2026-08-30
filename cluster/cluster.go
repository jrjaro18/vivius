package cluster

import (
	"log"
	"vivius/memStore"
	"vivius/node"
	"vivius/simTransport"
	"vivius/store"
)

type Cluster struct {
    Nodes     map[int]*node.Node
    Transport *simTransport.SimTransport
}

func NewCluster(nodeIDs []int) *Cluster {
    transport := simTransport.NewSimTransport()

    c := &Cluster{
        Nodes:     make(map[int]*node.Node),
        Transport: transport,
    }

    for _, id := range nodeIDs {
        n := node.NewNode(
            id,
            store.NewKvStore(),
            memStore.NewMemPersistentStore(),
            transport, // shared directly - no per-node wrapper needed anymore
        )
        transport.RegisterNode(id, n)
        c.Nodes[id] = n
    }

    return c
}

func (c *Cluster) StartAll() {
    for _, n := range c.Nodes {
		log.Println("Starting node")
        go n.Start()
    }
}

func (c *Cluster) Partition(ids ...int) {
    for _, id := range ids {
        c.Transport.PartitionNode(id)
    }
}

func (c *Cluster) Heal(ids ...int) {
    for _, id := range ids {
        c.Transport.HealPartition(id)
    }
}