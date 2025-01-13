package store

import (
	"fmt"
	"sync"
)

type Store[K comparable, V any] struct {
	mutex sync.RWMutex
	data map[K]V
}

func NewStore[K comparable, V any]() *Store[K, V] {
	return &Store[K, V]{
		data: make(map[K]V), 
	}
}

// Add inserts a key-value pair into the store.
func (s *Store[K, V]) Add(k K, v V) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.data[k] = v
}

// Contains checks if a key is present in the store.
// It returns true if the key is found, otherwise false.
func (s *Store[K, V]) Contains(k K) bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	_, present := s.data[k]
	return present
}

// It returns the value and a boolean indicating whether the key was found.
func (s *Store[K, V]) Get(k K) (V, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	v, present := s.data[k]
	return v, present
}

// Remove deletes a key-value pair from the store.
// It returns an error if the key is not found.
func (s *Store[K, V]) Remove(k K) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, present := s.data[k]; !present {
		return fmt.Errorf("key not found")
	}
	delete(s.data, k)
	return nil
}