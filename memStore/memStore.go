package memStore

import (
    "errors"
    "sync"

    "vivius/node"
)

type MemPersistentStore struct {
    mu       sync.Mutex
    term     int
    votedFor int
    entries  []node.LogEntry // index 0 unused - entries[i] corresponds to log index i
}

func NewMemPersistentStore() *MemPersistentStore {
    return &MemPersistentStore{
        votedFor: -1,
        entries:  make([]node.LogEntry, 1), // placeholder at index 0
    }
}

func (m *MemPersistentStore) Append(entries []node.LogEntry) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    m.entries = append(m.entries, entries...)
    return nil
}

func (m *MemPersistentStore) GetEntry(index int) (node.LogEntry, error) {
    m.mu.Lock()
    defer m.mu.Unlock()
    if index <= 0 || index >= len(m.entries) {
        return node.LogEntry{}, errors.New("index out of range")
    }
    return m.entries[index], nil
}

func (m *MemPersistentStore) GetEntriesFrom(index int) ([]node.LogEntry, error) {
    m.mu.Lock()
    defer m.mu.Unlock()
    if index <= 0 {
        index = 1
    }
    if index >= len(m.entries) {
        return []node.LogEntry{}, nil // nothing to send - caller is fully caught up
    }
    result := make([]node.LogEntry, len(m.entries)-index)
    copy(result, m.entries[index:])
    return result, nil
}

func (m *MemPersistentStore) LastIndex() int {
    m.mu.Lock()
    defer m.mu.Unlock()
    return len(m.entries) - 1
}

func (m *MemPersistentStore) LastTerm() int {
    m.mu.Lock()
    defer m.mu.Unlock()
    if len(m.entries) <= 1 {
        return 0
    }
    return m.entries[len(m.entries)-1].Term
}

func (m *MemPersistentStore) TruncateFrom(index int) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    if index <= 0 || index >= len(m.entries) {
        return nil // nothing to truncate
    }
    m.entries = m.entries[:index]
    return nil
}

func (m *MemPersistentStore) Len() int {
    m.mu.Lock()
    defer m.mu.Unlock()
    return len(m.entries) - 1
}

func (m *MemPersistentStore) SaveTermAndVote(term int, votedFor int) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    m.term = term
    m.votedFor = votedFor
    return nil
}

func (m *MemPersistentStore) LoadTermAndVote() (int, int, error) {
    m.mu.Lock()
    defer m.mu.Unlock()
    return m.term, m.votedFor, nil
}