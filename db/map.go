package db

type OrderedMap[T any] struct {
	keys      *SafeMap[bool]
	keyLength int
	values    *SafeMap[T]
	toPersist *SafeMap[bool]
	toDelete  *SafeMap[bool]
}

type KeyValue[T any] struct {
	Key   string
	Value T
}

func NewOrderedMap[T any]() *OrderedMap[T] {
	return &OrderedMap[T]{
		keys:      NewSafeMap[bool](),
		values:    NewSafeMap[T](),
		toPersist: NewSafeMap[bool](),
		toDelete:  NewSafeMap[bool](),
	}
}

func (o *OrderedMap[T]) Set(key string, value T) {
	if _, exists := o.values.Load(key); !exists {
		o.keys.Store(key, true)
		o.keyLength++
	}
	o.values.Store(key, value)
	o.markToPersist(key)

}

func (o *OrderedMap[T]) GetPendingPersist() []string {
	keys := make([]string, 0)
	o.toPersist.Range(func(key string, value bool) {
		keys = append(keys, key)
	})
	return keys
}

func (o *OrderedMap[T]) MarkPersisted(key string) {
	o.toPersist.Delete(key)
	if o.isPendingDelete(key) {
		o.delete(key)
	}

}

func (o *OrderedMap[T]) markToPersist(key string) {
	o.toPersist.Store(key, true)
}

func (o *OrderedMap[T]) markToDelete(key string) {
	o.toDelete.Store(key, true)
	o.removeActiveKey(key)
}

func (o *OrderedMap[T]) isPendingDelete(key string) bool {
	_, exists := o.toDelete.Load(key)
	return exists
}

func (o *OrderedMap[T]) GetPendingDelete() []string {
	keys := make([]string, 0)
	o.toDelete.Range(func(key string, value bool) {
		keys = append(keys, key)
	})
	return keys
}

func (o *OrderedMap[T]) delete(key string) {
	o.removeActiveKey(key)
	o.values.Delete(key)
	o.toDelete.Delete(key)
}

func (o *OrderedMap[T]) Remove(key string) {
	o.markToDelete(key)
	o.markToPersist(key)
}

func (o *OrderedMap[T]) removeActiveKey(toRemove string) {
	_, exists := o.keys.Load(toRemove)
	if exists {
		o.keys.Delete(toRemove)
		o.keyLength--
	}
}

func (o *OrderedMap[T]) Get(key string) (*T, bool) {
	if o.isPendingDelete(key) {
		return nil, false
	}
	val, exists := o.values.Load(key)
	if !exists {
		return nil, false
	}
	return &val, exists
}

func (o *OrderedMap[T]) Keys() []string {
	keys := make([]string, 0, o.keyLength)
	o.keys.Range(func(key string, value bool) {
		keys = append(keys, key)
	})
	return keys
}

func (o *OrderedMap[T]) Length() int {
	return o.keyLength
}

func (o *OrderedMap[T]) Items() []KeyValue[T] {
	items := make([]KeyValue[T], o.keyLength)
	index := 0

	o.keys.Range(func(key string, value bool) {
		val, _ := o.values.Load(key)
		items[index] = KeyValue[T]{Key: key, Value: val}
		index++
	})

	return items
}

func (o *OrderedMap[T]) Values() []T {
	values := make([]T, 0, o.keyLength)
	o.keys.Range(func(key string, value bool) {
		val, _ := o.values.Load(key)
		values = append(values, val)
	})
	return values
}
