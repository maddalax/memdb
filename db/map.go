package db

import (
	"iter"
)

type Hooks[T any] struct {
	OnSet    func(key string, value T)
	OnRemove func(key string, value T)
}

type TrackedMap[T any] struct {
	keyLength      int
	hooks          []Hooks[T]
	values         *SafeMap[T]
	toPersist      *SafeMap[bool]
	toDelete       *SafeMap[bool]
	toPersistCount int
	toDeleteCount  int
	totalPersisted int
	totalDeleted   int
}

type TrackedMapMetrics struct {
	keyCount       int
	toPersistCount int
	toDeleteCount  int
	totalPersisted int
	totalDeleted   int
}

type KeyValue[T any] struct {
	Key   string
	Value T
}

func NewTrackedMap[T any]() *TrackedMap[T] {
	return &TrackedMap[T]{
		keyLength:      0,
		hooks:          make([]Hooks[T], 0),
		values:         NewSafeMap[T](),
		toPersist:      NewSafeMap[bool](),
		toDelete:       NewSafeMap[bool](),
		toPersistCount: 0,
		toDeleteCount:  0,
		totalPersisted: 0,
		totalDeleted:   0,
	}
}

func (o *TrackedMap[T]) GetMetrics() TrackedMapMetrics {
	return TrackedMapMetrics{
		keyCount:       o.keyLength,
		toPersistCount: o.toPersistCount,
		toDeleteCount:  o.toDeleteCount,
		totalPersisted: o.totalPersisted,
		totalDeleted:   o.totalDeleted,
	}
}

func (o *TrackedMap[T]) LoadMany(items map[string]T) {
	o.values.StoreMany(items)
	for k, v := range items {
		for _, hook := range o.hooks {
			hook.OnSet(k, v)
		}
		o.keyLength++
	}
}

func (o *TrackedMap[T]) Set(key string, value T) {
	if _, exists := o.values.Load(key); !exists {
		o.keyLength++
	}
	o.values.Store(key, value)
	o.markToPersist(key)
	for _, hook := range o.hooks {
		hook.OnSet(key, value)
	}
}

func (o *TrackedMap[T]) GetPendingPersist() iter.Seq[string] {
	return func(yield func(string) bool) {
		for key, _ := range o.toPersist.Range() {
			if !yield(key) {
				return
			}
		}
	}
}

func (o *TrackedMap[T]) MarkPersisted(key string) {
	o.toPersist.Delete(key)
	o.toPersistCount--
	o.totalPersisted++
	if o.isPendingDelete(key) {
		o.toDelete.Delete(key)
		o.toDeleteCount--
		o.totalDeleted++
	}
	if o.toPersistCount < 0 {
		o.toPersistCount = 0
	}
	if o.toDeleteCount < 0 {
		o.toDeleteCount = 0
	}
}

func (o *TrackedMap[T]) markToPersist(key string) {
	o.toPersist.Store(key, true)
	o.toPersistCount++
}

func (o *TrackedMap[T]) markToDelete(key string) {
	o.toDelete.Store(key, true)
	o.toDeleteCount++
}

func (o *TrackedMap[T]) isPendingDelete(key string) bool {
	_, exists := o.toDelete.Load(key)
	return exists
}

func (o *TrackedMap[T]) GetPendingDelete() iter.Seq[string] {
	return func(yield func(string) bool) {
		for key, _ := range o.toDelete.Range() {
			if !yield(key) {
				return
			}
		}
	}
}

func (o *TrackedMap[T]) Remove(key string) {
	entry, exists := o.values.Load(key)
	if exists {
		o.markToDelete(key)
		o.markToPersist(key)
		o.values.Delete(key)
		o.keyLength--
		for _, hook := range o.hooks {
			hook.OnRemove(key, entry)
		}
	}
}

func (o *TrackedMap[T]) GetMany(keys map[string]bool) iter.Seq2[string, T] {
	return func(yield func(string, T) bool) {
		for k, v := range o.values.LoadMany(keys) {
			if o.isPendingDelete(k) {
				continue
			}
			y := yield(k, v)
			if !y {
				return
			}
		}
	}
}

func (o *TrackedMap[T]) Get(key string) (*T, bool) {
	if o.isPendingDelete(key) {
		return nil, false
	}
	val, exists := o.values.Load(key)
	if !exists {
		return nil, false
	}
	return &val, exists
}

func (o *TrackedMap[T]) Keys() iter.Seq[string] {
	return func(yield func(string) bool) {
		for key, _ := range o.values.Range() {
			if !yield(key) {
				return
			}
		}
	}
}

func (o *TrackedMap[T]) Length() int {
	return o.keyLength
}

func (o *TrackedMap[T]) Items() iter.Seq2[string, T] {
	return o.values.Range()
}

func (o *TrackedMap[T]) Values() iter.Seq[T] {
	return func(yield func(T) bool) {
		for key, value := range o.values.Range() {
			if o.isPendingDelete(key) {
				continue
			}
			if !yield(value) {
				return
			}
		}
	}
}
