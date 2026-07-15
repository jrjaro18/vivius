package store

import (
	"sync"
)

type KvStore struct {
	mutex  sync.Mutex
	data map[string]string
}

func NewKvStore() *KvStore {
	return &KvStore{
		data: make(map[string]string),
	}
}

func (s *KvStore) Set(key, value string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.data[key] = value
}

func (s *KvStore) Get(key string) (string, bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	value, exists := s.data[key]
	return value, exists
}

func (s *KvStore) Delete(key string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.data, key)
}