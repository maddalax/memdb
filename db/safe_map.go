package db

import (
	"iter"
	"sync"
)

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

func (s *SafeMap[T]) StoreMany(items map[string]T) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, v := range items {
		s.m[k] = v
	}
}

func (s *SafeMap[T]) Load(key string) (T, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.m[key]
	return value, ok
}

func (s *SafeMap[T]) LoadMany(keys map[string]bool) iter.Seq2[string, T] {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return func(yield func(string, T) bool) {
		s.mu.RLock()
		defer s.mu.RUnlock()
		for k := range keys {
			s.mu.RUnlock()
			record := s.m[k]
			y := yield(k, record)
			s.mu.RLock()
			if !y {
				return
			}
		}
	}
}

func (s *SafeMap[T]) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, key)
}

func (s *SafeMap[T]) Range() iter.Seq2[string, T] {
	return func(yield func(string, T) bool) {
		s.mu.RLock()
		defer s.mu.RUnlock()
		for k, v := range s.m {
			s.mu.RUnlock()
			y := yield(k, v)
			s.mu.RLock()
			if !y {
				return
			}
		}
	}
}
