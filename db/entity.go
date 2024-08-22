package db

import (
	"bytes"
	"encoding/json"
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

func Serialize[T any](item T) bytes.Buffer {
	var buf bytes.Buffer
	result, err := json.Marshal(item)
	if err != nil {
		return bytes.Buffer{}
	}
	buf.Write(result)
	buf.WriteString("\n")
	return buf
}

func Deserialize[T any](line []byte) (*T, error) {
	// Create a new Gob decoder
	// Remove the newline character if present
	line = bytes.TrimSuffix(line, []byte("\n"))
	if len(line) == 0 {
		return nil, nil
	}

	// Decode into a User struct
	var entity = new(T)
	err := json.Unmarshal(line, &entity)
	if err != nil {
		return nil, err
	}
	return entity, nil
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

//func (e *Entities[T]) Range(start int, end int) []T {
//	return e.RangeFilter(start, end, func(T) bool { return true })
//}
//
//func (e *Entities[T]) RangeFilter(start int, end int, filter func(T) bool) []T {
//	values := e.items.Values()
//	if end > len(values) {
//		end = len(values)
//	}
//	if start > len(values) {
//		return []T{}
//	}
//	items := make([]T, 0)
//	count := 0
//	for _, v := range values {
//		if filter(v) {
//			items = append(items, v)
//			count++
//			if count >= end {
//				break
//			}
//		}
//	}
//	if end > len(items) {
//		end = len(items)
//	}
//	if start > len(items) {
//		return []T{}
//	}
//	return items[start:end]
//}
