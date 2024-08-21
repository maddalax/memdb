package db

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"
)

type Item[T any] struct {
	Value     T
	Persisted bool
}

type Entities[T Entity[T]] struct {
	Items []Item[T]
	File  string
	mu    sync.Mutex
}

func CreateEntities[T Entity[T]](path string) *Entities[T] {
	entities := &Entities[T]{File: path, Items: make([]Item[T], 0)}
	entities.Initialize()
	return entities
}

type Entity[T any] interface {
	Eq(T) bool
}

func (e *Entities[T]) Initialize() {
	e.StartPersistence()
	e.Load()
}

func (e *Entities[T]) Load() {
	loadFile[T](e.File, func(item *T) {
		e.Items = append(e.Items, Item[T]{Value: *item, Persisted: true})
	})
	fmt.Printf("Loaded %d items from %s\n", len(e.Items), e.File)
	runtime.GC()
}

func (e *Entities[T]) StartPersistence() {
	timer := time.NewTicker(100 * time.Millisecond)
	f, err := os.OpenFile(e.File, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)

	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}

	go func() {
		for range timer.C {
			e.mu.Lock()
			writer := bufio.NewWriter(f)
			count := 0
			for i, v := range e.Items {
				if v.Persisted {
					continue
				}
				count++
				buf := Serialize(v.Value)
				if _, err := writer.Write(buf.Bytes()); err != nil {
					fmt.Println("Error writing to file:", err)
				}
				e.Items[i].Persisted = true
			}
			err = writer.Flush()
			if err != nil {
				fmt.Println("Error flushing to file:", err)
			}
			if count > 0 {
				fmt.Printf("finished persisting %d items to %s\n", count, e.File)
			}
			e.mu.Unlock()
		}
	}()
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

func loadFile[T any](path string, cb func(*T)) {
	// Open the file for reading
	file, err := os.Open(path)
	if err != nil {
		file, _ = os.Create(path)
	}
	defer file.Close()
	// Create a scanner to read the file line by line
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		b := scanner.Bytes()

		if len(b) == 0 {
			continue
		}

		// Deserialize the line into a User struct
		entity, err := Deserialize[T](b)

		if err != nil {
			fmt.Println("Error deserializing:", err)
			continue
		}
		cb(entity)
	}
}

func (e *Entities[T]) Add(item T) {
	e.Items = append(e.Items, Item[T]{
		Value: item,
	})
}

func (e *Entities[T]) AddMany(items []T) {
	c := make([]Item[T], len(items))
	for i, v := range items {
		c[i] = Item[T]{Value: v, Persisted: false}
	}
	e.Items = append(e.Items, c...)
}

func (e *Entities[T]) Remove(item T) {
	for i, v := range e.Items {
		if v.Value.Eq(item) {
			e.Items = append(e.Items[:i], e.Items[i+1:]...)
			break
		}
	}
}

func (e *Entities[T]) RemoveBy(fn func(T) bool) {
	for i, v := range e.Items {
		if fn(v.Value) {
			e.Items = append(e.Items[:i], e.Items[i+1:]...)
			break
		}
	}
}

func (e *Entities[T]) Find(fn func(T) bool) *T {
	for _, v := range e.Items {
		if fn(v.Value) {
			return &v.Value
		}
	}
	return nil
}

func (e *Entities[T]) Filter(fn func(T) bool) []T {
	var items []T
	for _, v := range e.Items {
		if fn(v.Value) {
			items = append(items, v.Value)
		}
	}
	return items
}

func (e *Entities[T]) Range(start int, end int) []T {
	return e.RangeFilter(start, end, func(T) bool { return true })
}

func (e *Entities[T]) RangeFilter(start int, end int, filter func(T) bool) []T {
	if end > len(e.Items) {
		end = len(e.Items)
	}
	if start > len(e.Items) {
		return []T{}
	}
	result := e.Items[start:end]
	items := make([]T, 0, end-start)
	for _, v := range result {
		if filter(v.Value) {
			items = append(items, v.Value)
		}
	}
	return items
}
