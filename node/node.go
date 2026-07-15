package node

type Node struct {
	id              int
	role            Role
	currentTerm     int
	votedFor        int
	commitIndex    	int
	lastApplied		int
	nextIndex      	map[int]int
	matchIndex     	map[int]int
	dataStore       DataStore
	persistentStore PersistentStore
}

func NewNode(id int, dataStore DataStore) *Node {
	return &Node{
		id:          id,
		role:        Follower,
		currentTerm: 0,
		votedFor:    -1,
		dataStore:   dataStore,
	}
}

func (n *Node) Set(key, value string) {
	n.dataStore.Set(key, value)
}

func (n *Node) Get(key string) (string, bool) {
	return n.dataStore.Get(key)
}

func (n *Node) Delete(key string) {
	n.dataStore.Delete(key)
}
