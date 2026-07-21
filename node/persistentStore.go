package node

type PersistentStore interface {
	Append(entries []LogEntry) error
	GetEntry(index int) (LogEntry, error)
	GetEntriesFrom(index int) ([]LogEntry, error)
	LastIndex() int
	LastTerm() int
	TruncateFrom(index int) error
	Len() int
	SaveTermAndVote(term int, votedFor int) error
	LoadTermAndVote() (term int, votedFor int, err error)
}

type LogEntry struct {
	Index   int     `json:"index"`
	Term    int     `json:"term"`
	Command Command `json:"command"`
}

type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
}