package db

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"
)

import bolt "go.etcd.io/bbolt"

type Persistence[T any] struct {
	path  string
	items *TrackedMap[T]
	mu    sync.Mutex
	disk  *bolt.DB
}

func NewPersistence[T any](path string, items *TrackedMap[T]) *Persistence[T] {
	db, err := bolt.Open(path, 0600, nil)

	if err != nil {
		log.Fatal(err)
	}

	// Create or open a bucket (a namespace within the database)
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("default"))
		return err
	})
	if err != nil {
		log.Fatal(err)
	}

	return &Persistence[T]{path: path, disk: db, mu: sync.Mutex{}, items: items}
}

func (p *Persistence[T]) Start() {
	timer := time.NewTicker(100 * time.Millisecond)

	go func() {
		for range timer.C {
			p.mu.Lock()
			setCount := 0
			deleteCount := 0
			err := p.disk.Update(func(tx *bolt.Tx) error {
				bucket := tx.Bucket([]byte("default"))
				for key := range p.items.GetPendingPersist() {
					v, _ := p.items.Get(key)
					if p.items.isPendingDelete(key) {
						deleteCount++
						err := bucket.Delete([]byte(key))
						if err != nil {
							return err
						}
					} else {
						setCount++
						serialized := Serialize(*v)
						err := bucket.Put([]byte(key), serialized.Bytes())
						if err != nil {
							return err
						}
					}
					p.items.MarkPersisted(key)
				}
				return nil
			})

			if err != nil {
				log.Fatal(err)
			}

			if setCount > 0 || deleteCount > 0 {
				fmt.Printf("finished persistence. set: %d, delete: %d\n", setCount, deleteCount)
			}
			p.mu.Unlock()
		}
	}()
}

func (p *Persistence[T]) Load(cb func(string, *T)) {
	err := p.disk.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket([]byte("default"))

		err := b.ForEach(func(k, v []byte) error {
			item, err := Deserialize[T](v)
			if err != nil {
				return err
			}
			cb(string(k), item)
			return nil
		})
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
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
