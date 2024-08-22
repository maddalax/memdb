package db

import (
	"iter"
	"memdb/util"
	"runtime"
	"sync"
)

type Entities[T Entity[T]] struct {
	items       *TrackedMap[T]
	persistence *Persistence[T]
	file        string
	mu          sync.Mutex
}

func CreateEntities[T Entity[T]](path string) *Entities[T] {
	items := NewTrackedMap[T]()
	entities := &Entities[T]{file: path, items: items, persistence: NewPersistence[T](path, items)}
	entities.Initialize()
	return entities
}

type Entity[T any] interface {
	Key() string
}

func (e *Entities[T]) Initialize() {
	e.persistence.Start()
	e.Load()
}

func (e *Entities[T]) Load() {
	util.TracePerf("Loading entities from disk bulk", func() {
		toLoad := make([]KeyValue[T], 0)
		e.persistence.Load(func(key string, item *T) {
			toLoad = append(toLoad, KeyValue[T]{Key: key, Value: *item})
		})
		e.items.LoadMany(toLoad)
		toLoad = nil
	})
	runtime.GC()
}

func (e *Entities[T]) Add(item T) {
	e.items.Set(item.Key(), item)
}

func (e *Entities[T]) AddMany(items []T) {
	for _, item := range items {
		e.items.Set(item.Key(), item)
	}
}

func (e *Entities[T]) Remove(item T) {
	for id, _ := range e.items.Items() {
		if id == item.Key() {
			e.items.Remove(id)
			break
		}
	}
}

func (e *Entities[T]) RemoveBy(fn func(T) bool) {
	for id, v := range e.items.Items() {
		if fn(v) {
			e.items.Remove(id)
		}
	}
}

func (e *Entities[T]) Find(fn func(T) bool) *T {
	for v := range e.items.Values() {
		if fn(v) {
			return &v
		}
	}
	return nil
}

func (e *Entities[T]) Each() iter.Seq[T] {
	return func(yield func(T) bool) {
		for v := range e.items.Values() {
			if !yield(v) {
				return
			}
		}
	}
}

func (e *Entities[T]) Range(start int, end int) iter.Seq[T] {
	return e.RangeFilter(start, end, func(T) bool { return true })
}

func (e *Entities[T]) RangeFilter(start int, end int, filter func(T) bool) iter.Seq[T] {
	return func(yield func(T) bool) {
		count := 0
		for v := range e.items.Values() {
			if count < start {
				continue
			}
			if count >= end {
				break
			}
			count++
			if filter(v) {
				if !yield(v) {
					return
				}
			}
		}
	}
}
