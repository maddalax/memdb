package db

import (
	"fmt"
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

type Index[T Entity[T]] struct {
	items     map[string]map[string]bool
	entity    *Entities[T]
	transform func(T) string
}

func CreateIndex[T Entity[T]](entity *Entities[T], transform func(T) string) *Index[T] {
	items := make(map[string]map[string]bool)
	index := &Index[T]{items: items, entity: entity}

	entity.items.hooks = append(entity.items.hooks, Hooks[T]{
		OnSet: func(key string, value T) {
			fieldKey := transform(value)
			if _, ok := index.items[fieldKey]; !ok {
				index.items[fieldKey] = make(map[string]bool)
			}
			index.items[fieldKey][key] = true
		},
		OnRemove: func(key string, value T) {
			fieldKey := transform(value)
			if _, ok := index.items[fieldKey]; !ok {
				index.items[fieldKey] = make(map[string]bool)
			}
			delete(index.items[fieldKey], key)
		},
	})

	return index
}

func (e *Index[T]) Each(key string) iter.Seq2[string, T] {
	results := e.items[key]
	if results == nil {
		return func(yield func(string, T) bool) {}
	}
	return e.entity.items.GetMany(results)
}

func CreateEntities[T Entity[T]](path string) *Entities[T] {
	items := NewTrackedMap[T]()
	entities := &Entities[T]{file: path, items: items, persistence: NewPersistence[T](path, items)}
	entities.Initialize()
	return entities
}

func CreateEntitiesWithHooks[T Entity[T]](path string, hooks Hooks[T]) *Entities[T] {
	items := NewTrackedMap[T]()
	items.hooks = append(items.hooks, hooks)
	entities := &Entities[T]{file: path, items: items, persistence: NewPersistence[T](path, items)}
	entities.Initialize()
	return entities
}

type Entity[T any] interface {
	Key() string
}

func (e *Entities[T]) Initialize() {
	e.persistence.Start()
	AddLoad()
	go e.Load()
}

func (e *Entities[T]) PrintMetrics() {
	metrics := e.items.GetMetrics()
	fmt.Printf("Path: %s, Entities: %d, ToPersist: %d, ToDelete: %d, Persisted: %d, Deleted: %d\n",
		e.file, metrics.keyCount, metrics.toPersistCount, metrics.toDeleteCount, metrics.totalPersisted, metrics.totalDeleted)
}

func (e *Entities[T]) Load() {
	util.TracePerf("Loading entities from disk bulk", func() {
		toLoad := make(map[string]T)
		total := 0
		batch := 0
		e.persistence.Load(func(event *Event[T]) {
			if event.Type == "delete" {
				e.items.values.Delete(event.Key)
				delete(toLoad, event.Key)
				return
			}

			key := event.Key
			item := &event.Value
			toLoad[key] = *item
			total++
			batch++
			if batch == 10_00 {
				e.items.LoadMany(toLoad)
				toLoad = make(map[string]T)
				batch = 0
			}
			if total%10_000 == 0 {
				fmt.Printf("Loaded %d entities from %s\n", total, e.file)
			}
		})
		e.items.LoadMany(toLoad)
		fmt.Printf("Loaded %d entities from %s\n", total, e.file)
		toLoad = nil
		DoneLoad()
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

func (e *Entities[T]) Filter(filter func(T) bool) iter.Seq[T] {
	return func(yield func(T) bool) {
		for v := range e.items.Values() {
			if !filter(v) {
				continue
			}
			if !yield(v) {
				return
			}
		}
	}
}

func (e *Entities[T]) FilterLimit(limit int, filter func(T) bool) iter.Seq[T] {
	return func(yield func(T) bool) {
		count := 0
		for v := range e.items.Values() {
			if !filter(v) {
				continue
			}
			count++
			if !yield(v) {
				return
			}
			if count >= limit {
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
