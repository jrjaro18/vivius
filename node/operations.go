package node

import (
	"log"
	"sync"
	"sync/atomic"
	"vivius/errors"
)

func (n *Node) Set(key, value string) {
	n.dataStore.Set(key, value)
}

func (n *Node) Get(key string) (string, bool) {
	return n.dataStore.Get(key)
}

func (n *Node) Delete(key string) {
	n.dataStore.Delete(key)
}

func (n *Node) askForVote() {
    // FIX: read currentTerm/build args under the lock, not unguarded
    n.mutex.Lock()
    electionTerm := n.currentTerm
    args := RequestVoteArgs{
        currentTerm:  n.currentTerm,
        candidateID:  n.id,
        lastLogTerm:  n.persistentStore.LastTerm(),
        lastLogIndex: n.persistentStore.LastIndex(),
    }
    n.mutex.Unlock()

    var totalVotes atomic.Int32
    totalVotes.Store(1) // FIX: count self-vote

    wg := sync.WaitGroup{}
    for _, peerID := range n.transport.GetPeers() {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()
            reply, err := n.transport.SendRequestVote(id, args)
            if err != nil {
                log.Printf("Error requesting vote from %v: %v", id, err.Error())
                return
            }

            n.mutex.Lock()
            if reply.term > n.currentTerm {
                log.Printf("Peer %v term: %v, candidate %v currentTerm: %v", id, reply.term, n.id, n.currentTerm)
                n.currentTerm = reply.term
                n.role = Follower
                n.votedFor = -1
                n.persistentStore.SaveTermAndVote(n.currentTerm, n.votedFor)
                n.mutex.Unlock()
                return // FIX: don't count vote after stepping down
            }
            n.mutex.Unlock()

            if reply.voteGranted {
                totalVotes.Add(1)
            }
        }(peerID)
    }
    wg.Wait()

    n.mutex.Lock()
    defer n.mutex.Unlock()
    if n.role != Candidate || n.currentTerm != electionTerm {
        return // stale election attempt - already changed underneath us
    }

    totalNodes := len(n.transport.GetPeers()) + 1
    if int(totalVotes.Load()) > totalNodes/2 {
        log.Printf("Node %d received majority votes, becoming leader", n.id)
        n.role = Leader
        n.leaderId = n.id
        // FIX: removed `n.currentTerm += 1` - leadership is valid for the
        // term just campaigned on, bumping again would invalidate the votes

        // FIX: initialize nextIndex/matchIndex fresh on election, per §5.3
        lastIndex := n.persistentStore.LastIndex()
        n.nextIndex = make(map[int]int)
        n.matchIndex = make(map[int]int)
        for _, peerID := range n.transport.GetPeers() {
            n.nextIndex[peerID] = lastIndex + 1
            n.matchIndex[peerID] = 0
        }
    }
}

func (n *Node) giveVote(args RequestVoteArgs) RequestVoteReply {
    n.mutex.Lock()
    defer n.mutex.Unlock()

    if args.currentTerm < n.currentTerm {
        return RequestVoteReply{voteGranted: false, term: n.currentTerm}
    }

    if args.currentTerm > n.currentTerm {
        n.currentTerm = args.currentTerm
        n.votedFor = -1
        n.role = Follower
    }

    lastTerm := n.persistentStore.LastTerm()
    lastIndex := n.persistentStore.LastIndex()
    logOK := args.lastLogTerm > lastTerm ||
        (args.lastLogTerm == lastTerm && args.lastLogIndex >= lastIndex)

    canVote := n.votedFor == -1 || n.votedFor == args.candidateID

    if !canVote || !logOK {
        n.persistentStore.SaveTermAndVote(n.currentTerm, n.votedFor) // term may have changed above
        return RequestVoteReply{voteGranted: false, term: n.currentTerm}
    }

    n.votedFor = args.candidateID
    n.persistentStore.SaveTermAndVote(n.currentTerm, n.votedFor)
    return RequestVoteReply{voteGranted: true, term: n.currentTerm}
}

func (n *Node) receiveWriteRequestFromClient(c Command) error {
    n.mutex.Lock()
    if n.role != Leader {
        hint := n.leaderId
        n.mutex.Unlock()
        return &errors.ErrNotLeader{Node: n.id, Hint: hint}
    }

    logEntry := LogEntry{
        Command: c,
        Term:    n.currentTerm,
        Index:   n.persistentStore.LastIndex() + 1, // FIX: +1, was off-by-one before
    }
    n.mutex.Unlock() // FIX: release before append/fan-out - don't hold across I/O or wg.Wait()

    if err := n.persistentStore.Append([]LogEntry{logEntry}); err != nil {
        return &errors.ErrAppendFail{Node: n.id, Term: logEntry.Term, Index: logEntry.Index, Err: err}
    }

    var successCount atomic.Int32
    successCount.Store(1) // FIX: leader counts its own append

    wg := sync.WaitGroup{}
    for _, peerId := range n.transport.GetPeers() {
        wg.Add(1)
        go func(nodeId int) {
            defer wg.Done()
            // FIX: pass nodeId - original called propagateWriteRequest with
            // no target, it had no way to know who to send to
            success, err := n.propagateWriteRequest(nodeId)
            if err != nil {
                log.Printf("Couldn't propagate command to %v with error: %v", nodeId, err.Error())
                return
            }
            if success {
                successCount.Add(1)
            }
        }(peerId)
    }
    wg.Wait()

    n.mutex.Lock()
    defer n.mutex.Unlock()

    // FIX: re-check we're still leader for this term - could have stepped
    // down mid-replication if a peer's reply carried a higher term
    if n.role != Leader || n.currentTerm != logEntry.Term {
        return &errors.ErrNotLeader{Node: n.id, Hint: n.leaderId}
    }

    totalNodes := len(n.transport.GetPeers()) + 1
    if int(successCount.Load()) <= totalNodes/2 { // FIX: threshold now counts leader correctly
        return &errors.ErrNoMajority{Node: n.id, VotesReceived: successCount.Load(), TotalPeers: len(n.transport.GetPeers())}
    }

    if logEntry.Index > n.commitIndex {
        n.commitIndex = logEntry.Index
        n.applyEntries() // FIX: was missing entirely - commit alone doesn't touch the store
    }

    return nil
}

func (n *Node) propagateWriteRequest(peerID int) (bool, error) {
    for {
        n.mutex.Lock()

        // if we've stepped down or moved to a different term since starting,
        // abandon this replication attempt entirely - it's stale
        if n.role != Leader {
            n.mutex.Unlock()
            return false, &errors.ErrNotLeader{Node: n.id, Hint: n.leaderId}
        }

        nextIdx := n.nextIndex[peerID]
        prevLogIndex := nextIdx - 1
        prevLogTerm := n.getTermAtIndex(prevLogIndex)

        // send everything from nextIdx up through the leader's latest entry -
        // not just the single `entry` passed in - since backing off may mean
        // the follower is missing more than just this one entry
        entriesToSend, err := n.persistentStore.GetEntriesFrom(nextIdx)
        if err != nil {
            n.mutex.Unlock()
            return false, err
        }

        args := AppendEntriesArgs{
            term:         n.currentTerm,
            leaderID:     n.id,
            prevLogIndex: prevLogIndex,
            prevLogTerm:  prevLogTerm,
            entries:      entriesToSend,
            leaderCommit: n.commitIndex,
        }
        n.mutex.Unlock()

        reply, err := n.transport.SendAppendEntries(peerID, args)
        if err != nil {
            return false, err // peer unreachable - give up, don't spin forever on a dead node
        }

        n.mutex.Lock()

        if reply.term > n.currentTerm {
            n.currentTerm = reply.term
            n.role = Follower
            n.votedFor = -1
            n.persistentStore.SaveTermAndVote(n.currentTerm, n.votedFor)
            n.mutex.Unlock()
            return false, nil // stepped down - stop retrying, this leadership is over
        }

        if reply.success {
            lastSentIndex := prevLogIndex + len(entriesToSend)
            n.matchIndex[peerID] = lastSentIndex
            n.nextIndex[peerID] = lastSentIndex + 1
            n.mutex.Unlock()
            return true, nil // caught up, done
        }

        // rejected due to log mismatch - back off one and loop again
        if n.nextIndex[peerID] > 1 {
            n.nextIndex[peerID]--
        }
        n.mutex.Unlock()
        // loop continues, retries immediately with the decremented nextIndex
    }
}

func (n *Node) receiveWriteRequestFromLeader(args AppendEntriesArgs) AppendEntriesReply {
    n.mutex.Lock()
    defer n.mutex.Unlock()

    if args.term < n.currentTerm {
        return AppendEntriesReply{term: n.currentTerm, success: false}
    }

    if args.term > n.currentTerm {
        n.currentTerm = args.term
        n.votedFor = -1
    }
    n.role = Follower
    n.leaderId = args.leaderID
    n.persistentStore.SaveTermAndVote(n.currentTerm, n.votedFor)

    // signal the election timer to reset - non-blocking send
    select {
    case n.receivedInput <- struct{}{}:
    default:
    }

    if args.prevLogIndex > 0 {
        if n.getTermAtIndex(args.prevLogIndex) != args.prevLogTerm {
            return AppendEntriesReply{term: n.currentTerm, success: false}
        }
    }

    if len(args.entries) > 0 {
        if err := n.persistentStore.TruncateFrom(args.prevLogIndex + 1); err != nil {
            return AppendEntriesReply{term: n.currentTerm, success: false}
        }
        if err := n.persistentStore.Append(args.entries); err != nil {
            return AppendEntriesReply{term: n.currentTerm, success: false}
        }
    }

    if args.leaderCommit > n.commitIndex {
        lastNewIndex := args.prevLogIndex + len(args.entries)
        if args.leaderCommit < lastNewIndex {
            n.commitIndex = args.leaderCommit
        } else {
            n.commitIndex = lastNewIndex
        }
        n.applyEntries()
    }

    return AppendEntriesReply{term: n.currentTerm, success: true}
}

// getTermAtIndex must only be called while n.mutex is already held.
func (n *Node) getTermAtIndex(index int) int {
    if index <= 0 {
        return 0
    }
    entry, err := n.persistentStore.GetEntry(index)
    if err != nil {
        return 0
    }
    return entry.Term
}

// applyEntries must only be called while n.mutex is already held -
// it never locks itself, to avoid deadlocking its callers (see earlier
// discussion on reentrant locking).
func (n *Node) applyEntries() {
    for n.lastApplied < n.commitIndex {
        n.lastApplied++
        entry, err := n.persistentStore.GetEntry(n.lastApplied)
        if err != nil {
            log.Printf("Node %d: failed to get entry %d to apply: %v", n.id, n.lastApplied, err)
            return
        }
        switch entry.Command.Op {
        case "Put":
            n.dataStore.Set(entry.Command.Key, entry.Command.Value)
        case "Delete":
            n.dataStore.Delete(entry.Command.Key)
        }
    }
}