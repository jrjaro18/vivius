package store

import (
	"fmt"
	"sync"
)

type Store struct {
	mutex sync.RWMutex
	data map[string]any
}

func NewStore() *Store {
	return &Store{
		data: make(map[string]any), 
	}
}

// Add inserts a key-value pair into the store.
func (s *Store) Add(k string, v any) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.data[k] = v
}

// Contains checks if a key is present in the store.
// It returns true if the key is found, otherwise false.
func (s *Store) Contains(k string) bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	_, present := s.data[k]
	return present
}

// It returns the value and a boolean indicating whether the key was found.
func (s *Store) Get(k string) (any, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	v, present := s.data[k]
	return v, present
}

// Remove deletes a key-value pair from the store.
// It returns an error if the key is not found.
func (s *Store) Remove(k string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, present := s.data[k]; !present {
		return fmt.Errorf("key not found")
	}
	delete(s.data, k)
	return nil
}