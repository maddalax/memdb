package db

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/cockroachdb/pebble"
	"log"
	"sync"
	"time"
)

type Persistence[T any] struct {
	path  string
	items *TrackedMap[T]
	mu    sync.Mutex
	disk  *pebble.DB
}

func NewPersistence[T any](path string, items *TrackedMap[T]) *Persistence[T] {
	db, err := pebble.Open("./data", &pebble.Options{})

	if err != nil {
		log.Fatal(err)
	}

	return &Persistence[T]{path: path, disk: db, mu: sync.Mutex{}, items: items}
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

			batch := p.disk.NewBatch()

			for _, key := range items {
				v, _ := p.items.Get(key)
				if p.items.isPendingDelete(key) {
					deleteCount++
					persistedCount++
					err := batch.Delete([]byte(key), nil)
					if err != nil {
						log.Fatal(err)
					}
				} else {
					setCount++
					persistedCount++
					serialized := Serialize(*v)
					err := batch.Set([]byte(key), serialized.Bytes(), nil)
					if err != nil {
						log.Fatal(err)
					}
				}
				p.items.MarkPersisted(key)
			}

			err := batch.Commit(nil)

			if err != nil {
				log.Fatal(err)
			}

			if setCount > 0 || deleteCount > 0 {
				fmt.Printf("finished persistence. set: %d, delete: %d, total: %d\n", setCount, deleteCount, persistedCount)
			}
			p.mu.Unlock()
		}
	}()
}

func (p *Persistence[T]) Load(cb func(string, *T)) {
	iter, err := p.disk.NewIter(nil)

	if err != nil {
		log.Fatal(err)
	}

	for iter.First(); iter.Valid(); iter.Next() {
		v, err := iter.ValueAndErr()
		if err != nil {
			log.Fatal(err)
		}
		item, err := Deserialize[T](v)
		if err != nil {
			log.Fatal(err)
		}
		cb(string(iter.Key()), item)
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
