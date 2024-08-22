package db

import (
	"fmt"
	"log"
	"sync"
	"time"
)

import bolt "go.etcd.io/bbolt"

type Persistence[T any] struct {
	path  string
	items *OrderedMap[T]
	mu    sync.Mutex
	disk  *bolt.DB
}

func NewPersistence[T any](path string, items *OrderedMap[T]) *Persistence[T] {
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
			count := 0
			for _, key := range p.items.GetPendingPersist() {
				v, _ := p.items.Get(key)
				count++
				if p.items.isPendingDelete(key) {
					p.remove(key)
				} else {
					p.set(key, v)
				}
				p.items.MarkPersisted(key)
			}
			if count > 0 {
				fmt.Printf("finished persisting %d items to %s\n", count, p.path)
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

func (p *Persistence[T]) set(key string, value *T) {
	serialized := Serialize(*value)
	// Add some key/value pairs to the bucket
	err := p.disk.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("default"))
		err := bucket.Put([]byte(key), serialized.Bytes())
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}

func (p *Persistence[T]) remove(key string) {
	err := p.disk.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("default"))
		err := bucket.Delete([]byte(key))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
