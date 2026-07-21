package node

type Transport interface {
	SendRequestVote(target int, args RequestVoteArgs) (RequestVoteReply, error)
    SendAppendEntries(target int, args AppendEntriesArgs) (AppendEntriesReply, error)
	GetPeers() []int
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
}

type AppendEntriesReply struct {
}