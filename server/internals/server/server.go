package server

import "vivius/store/internals/store"

type Server[K comparable, V any] struct {
	*store.Store[K, V]
}

func NewServer[K comparable, V any]() *Server[K, V]{
	return &Server[K, V]{
		Store: store.NewStore[K, V](),
	}
}