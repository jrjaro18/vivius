package cluster

import (
    "vivius/memstore"
	"vivius/store"
    "vivius/node"
    "vivius/simtransport"
)

type Cluster struct {
    Nodes     map[int]*node.Node
    Transport *simtransport.SimTransport
}

func NewCluster(nodeIDs []int) *Cluster {
    transport := simtransport.NewSimTransport()

    c := &Cluster{
        Nodes:     make(map[int]*node.Node),
        Transport: transport,
    }

    for _, id := range nodeIDs {
        n := node.NewNode(
            id,
            store.NewKvStore(),
            memstore.NewMemPersistentStore(),
            transport, // shared directly - no per-node wrapper needed anymore
        )
        transport.RegisterNode(id, n)
        c.Nodes[id] = n
    }

    return c
}

func (c *Cluster) StartAll() {
    for _, n := range c.Nodes {
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