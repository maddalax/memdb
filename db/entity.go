package db

import (
	"bytes"
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
)

type Entities[T Entity[T]] struct {
	items       *OrderedMap[T]
	persistence *Persistence[T]
	file        string
	mu          sync.Mutex
}

func CreateEntities[T Entity[T]](path string) *Entities[T] {
	items := NewOrderedMap[T]()
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
	e.persistence.Load(func(key string, item *T) {
		e.items.Set(key, *item)
		e.items.MarkPersisted(key)
	})
	fmt.Printf("Loaded %d items from %s\n", e.items.Length(), e.file)
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
	for _, v := range e.items.Items() {
		if v.Value.Key() == item.Key() {
			e.items.Remove(v.Key)
			break
		}
	}
}

func (e *Entities[T]) RemoveBy(fn func(T) bool) {
	for _, v := range e.items.Items() {
		if fn(v.Value) {
			e.items.Remove(v.Key)
		}
	}
}

func (e *Entities[T]) Find(fn func(T) bool) *T {
	for _, v := range e.items.Values() {
		if fn(v) {
			return &v
		}
	}
	return nil
}

func (e *Entities[T]) Filter(fn func(T) bool) []T {
	var items []T
	for _, v := range e.items.Values() {
		if fn(v) {
			items = append(items, v)
		}
	}
	return items
}

func (e *Entities[T]) Range(start int, end int) []T {
	return e.RangeFilter(start, end, func(T) bool { return true })
}

func (e *Entities[T]) RangeFilter(start int, end int, filter func(T) bool) []T {
	values := e.items.Values()
	if end > len(values) {
		end = len(values)
	}
	if start > len(values) {
		return []T{}
	}
	result := values[start:end]
	items := make([]T, 0, end-start)
	for _, v := range result {
		if filter(v) {
			items = append(items, v)
		}
	}
	return items
}
