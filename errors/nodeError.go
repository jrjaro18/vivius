package errors

import (
	"fmt"
)

type ErrNotLeader struct {
	Node int
	Hint int
}

func (err *ErrNotLeader) Error() string {
	return fmt.Sprintf("Given node, %v, is not a leader, node %v might be a leader", err.Node, err.Hint)
}

type ErrNoMajority struct {
	Node int
	VotesReceived int32
	TotalPeers int
}

func (err *ErrNoMajority) Error() string {
	return fmt.Sprintf("Given node, %v, couldn't receive majority of the votes required to perform a successful write operation, received %v votes from %v nodes", err.Node, err.VotesReceived, err.TotalPeers)
}