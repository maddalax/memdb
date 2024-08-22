package db

type TrackedMap[T any] struct {
	keyLength int
	values    *SafeMap[T]
	toPersist *SafeMap[bool]
	toDelete  *SafeMap[bool]
}

type KeyValue[T any] struct {
	Key   string
	Value T
}

func NewTrackedMap[T any]() *TrackedMap[T] {
	return &TrackedMap[T]{
		keyLength: 0,
		values:    NewSafeMap[T](),
		toPersist: NewSafeMap[bool](),
		toDelete:  NewSafeMap[bool](),
	}
}

func (o *TrackedMap[T]) LoadMany(items []KeyValue[T]) {
	o.values.StoreMany(items)
	o.keyLength = len(items)
}

func (o *TrackedMap[T]) Set(key string, value T) {
	if _, exists := o.values.Load(key); !exists {
		o.keyLength++
	}
	o.values.Store(key, value)
	o.markToPersist(key)
}

func (o *TrackedMap[T]) GetPendingPersist() []string {
	keys := make([]string, 0)
	o.toPersist.Range(func(key string, value bool) {
		keys = append(keys, key)
	})
	return keys
}

func (o *TrackedMap[T]) MarkPersisted(key string) {
	o.toPersist.Delete(key)
	if o.isPendingDelete(key) {
		o.toDelete.Delete(key)
	}
}

func (o *TrackedMap[T]) markToPersist(key string) {
	o.toPersist.Store(key, true)
}

func (o *TrackedMap[T]) markToDelete(key string) {
	o.toDelete.Store(key, true)
}

func (o *TrackedMap[T]) isPendingDelete(key string) bool {
	_, exists := o.toDelete.Load(key)
	return exists
}

func (o *TrackedMap[T]) GetPendingDelete() []string {
	keys := make([]string, 0)
	o.toDelete.Range(func(key string, value bool) {
		keys = append(keys, key)
	})
	return keys
}

func (o *TrackedMap[T]) Remove(key string) {
	_, exists := o.values.Load(key)
	if exists {
		o.markToDelete(key)
		o.markToPersist(key)
		o.values.Delete(key)
		o.keyLength--
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

func (o *TrackedMap[T]) Keys() []string {
	keys := make([]string, 0, o.keyLength)
	o.values.Range(func(key string, value T) {
		keys = append(keys, key)
	})
	return keys
}

func (o *TrackedMap[T]) Length() int {
	return o.keyLength
}

func (o *TrackedMap[T]) Items() []KeyValue[T] {
	items := make([]KeyValue[T], o.keyLength)
	index := 0
	o.values.Range(func(key string, value T) {
		items[index] = KeyValue[T]{Key: key, Value: value}
		index++
	})
	return items
}

func (o *TrackedMap[T]) Values() []T {
	values := make([]T, o.keyLength)
	index := 0
	o.values.Range(func(key string, value T) {
		values[index] = value
		index++
	})
	return values
}
