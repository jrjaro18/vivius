package errors

type TransportError struct {

}

func (t *TransportError) Error() string {
	return ""
}