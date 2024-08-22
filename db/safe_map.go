package db

import "sync"

type SafeMap[T any] struct {
	mu sync.RWMutex
	m  map[string]T
}

func NewSafeMap[T any]() *SafeMap[T] {
	return &SafeMap[T]{
		m: make(map[string]T),
	}
}

func (s *SafeMap[T]) Store(key string, value T) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key] = value
}

func (s *SafeMap[T]) Load(key string) (T, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.m[key]
	return value, ok
}

func (s *SafeMap[T]) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, key)
}

func (s *SafeMap[T]) Range(cb func(key string, value T)) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for k, v := range s.m {
		cb(k, v)
	}
}
