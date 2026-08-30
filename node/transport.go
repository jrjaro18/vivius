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

func (args RequestVoteArgs) CandidateID() int {
	return args.candidateID
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

func (args AppendEntriesArgs) LeaderID() int {
	return args.leaderID
}

type AppendEntriesReply struct {
    term    int
    success bool
}