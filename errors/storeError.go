package errors

import "fmt"

type ErrAppendFail struct {
	Node int
	Term int
	Index int
	Err error
}

func (e *ErrAppendFail) Error() string {
	return fmt.Sprintf("Node %v failed to append entry with term %v and index %v, more details (if any): %v", e.Node, e.Term, e.Index, e.Err.Error())
}