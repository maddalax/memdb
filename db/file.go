package db

import (
	"bufio"
	"bytes"
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"
)

type Event[T any] struct {
	Type  string
	Key   string
	Time  int64
	Value T
}

type Persistence[T any] struct {
	path  string
	items *TrackedMap[T]
	mu    sync.Mutex
	disk  *os.File
}

func NewPersistence[T any](path string, items *TrackedMap[T]) *Persistence[T] {
	disk, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		log.Fatal(err)
	}

	return &Persistence[T]{path: path, disk: disk, mu: sync.Mutex{}, items: items}
}

func (p *Persistence[T]) Start() {
	timer := time.NewTicker(100 * time.Millisecond)
	persistedCount := 0

	go func() {
		for range timer.C {
			p.mu.Lock()
			setCount := 0
			deleteCount := 0
			items := ToSliceChunk(p.items.GetPendingPersist(), 100*1000)

			if len(items) == 0 {
				p.mu.Unlock()
				continue
			}

			writer := bufio.NewWriter(p.disk)

			for _, key := range items {
				v, _ := p.items.Get(key)
				if p.items.isPendingDelete(key) {
					deleteCount++
					persistedCount++
					event := Event[T]{Type: "delete", Key: key, Time: time.Now().Unix()}
					serialized := Serialize(event)
					_, err := writer.Write(serialized.Bytes())
					if err != nil {
						log.Fatal(err)
					}
				} else {
					setCount++
					persistedCount++
					event := Event[T]{Type: "set", Key: key, Value: *v, Time: time.Now().Unix()}
					serialized := Serialize(event)
					_, err := writer.Write(serialized.Bytes())
					if err != nil {
						log.Fatal(err)
					}
				}
				p.items.MarkPersisted(key)
			}

			err := writer.Flush()

			if err != nil {
				log.Fatal(err)
			}

			newPendingPersistCount := 0
			newPendingDeleteCount := 0
			for _ = range p.items.GetPendingPersist() {
				newPendingPersistCount++
			}
			for _ = range p.items.GetPendingDelete() {
				newPendingDeleteCount++
			}
			p.items.toPersistCount = newPendingPersistCount
			p.items.toDeleteCount = newPendingDeleteCount

			p.mu.Unlock()
		}
	}()
}

func (p *Persistence[T]) Load(cb func(event *Event[T])) {
	disk, err := os.Open(p.path)
	defer disk.Close()

	if err != nil {
		log.Fatal(err)
	}

	scanner := bufio.NewScanner(disk)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		event, err := Deserialize[Event[T]](line)
		if err != nil {
			log.Fatal(err)
		}
		cb(event)
	}
}

func Serialize[T any](item T) bytes.Buffer {
	var buffer bytes.Buffer

	// Create a new encoder that writes to the buffer
	encoder := json.NewEncoder(&buffer)
	// Encode the struct into the buffer
	err := encoder.Encode(item)
	if err != nil {
		log.Fatal("Encode error:", err)
	}
	return buffer
}

func Deserialize[T any](line []byte) (*T, error) {

	buffer := bytes.NewBuffer(line)
	decoder := json.NewDecoder(buffer)

	// Variable to hold the decoded data
	decoded := new(T)

	// Decode the data into the variable
	err := decoder.Decode(&decoded)

	if err != nil {
		log.Fatal("Decode error:", err)
	}

	return decoded, nil
}
