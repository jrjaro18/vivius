package node

type Transport interface {
	SendRequestVote(target int, args RequestVoteArgs) (RequestVoteReply, error)
    SendAppendEntries(target int, args AppendEntriesArgs) (AppendEntriesReply, error)
	GetAllNodeIds() []int
}

type RequestVoteArgs struct {
	currentTerm int
	candidateID  int
	lastLogTerm  int
	lastLogIndex int
}

type RequestVoteReply struct {
	voteGranted bool
	term 	  	int
}

type AppendEntriesArgs struct {
    term         int
    leaderID     int
    prevLogIndex int
    prevLogTerm  int
    entries      []LogEntry
    leaderCommit int
}

type AppendEntriesReply struct {
    term    int
    success bool
}