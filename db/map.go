package db

type OrderedMap[T any] struct {
	keys      []string
	keyLength int
	values    map[string]T
	toPersist map[string]bool
	toDelete  map[string]bool
}

type KeyValue[T any] struct {
	Key   string
	Value T
}

func NewOrderedMap[T any]() *OrderedMap[T] {
	return &OrderedMap[T]{
		keys:      []string{},
		values:    make(map[string]T),
		toPersist: make(map[string]bool),
		toDelete:  make(map[string]bool),
	}
}

func (o *OrderedMap[T]) Set(key string, value T) {
	if _, exists := o.values[key]; !exists {
		o.keys = append(o.keys, key)
		o.keyLength++
	}
	o.values[key] = value
	o.markToPersist(key)
}

func (o *OrderedMap[T]) GetPendingPersist() []string {
	keys := make([]string, 0, len(o.toPersist))
	for key := range o.toPersist {
		keys = append(keys, key)
	}
	return keys
}

func (o *OrderedMap[T]) MarkPersisted(key string) {
	delete(o.toPersist, key)
	if o.isPendingDelete(key) {
		o.delete(key)
	}
}

func (o *OrderedMap[T]) markToPersist(key string) {
	o.toPersist[key] = true
}

func (o *OrderedMap[T]) markToDelete(key string) {
	o.toDelete[key] = true
	o.removeActiveKey(key)
}

func (o *OrderedMap[T]) isPendingDelete(key string) bool {
	_, exists := o.toDelete[key]
	return exists
}

func (o *OrderedMap[T]) GetPendingDelete() []string {
	keys := make([]string, 0, len(o.toDelete))
	for key := range o.toDelete {
		keys = append(keys, key)
	}
	return keys
}

func (o *OrderedMap[T]) delete(key string) {
	o.removeActiveKey(key)
	delete(o.values, key)
	delete(o.toDelete, key)
}

func (o *OrderedMap[T]) Remove(key string) {
	o.markToDelete(key)
	o.markToPersist(key)
}

func (o *OrderedMap[T]) removeActiveKey(toRemove string) {
	for i, k := range o.keys {
		if k == toRemove {
			o.keys = append(o.keys[:i], o.keys[i+1:]...)
			o.keyLength--
			break
		}
	}
}

func (o *OrderedMap[T]) Get(key string) (*T, bool) {
	if o.isPendingDelete(key) {
		return nil, false
	}
	val, exists := o.values[key]
	if !exists {
		return nil, false
	}
	return &val, exists
}

func (o *OrderedMap[T]) Keys() []string {
	return o.keys
}

func (o *OrderedMap[T]) Length() int {
	return o.keyLength
}

func (o *OrderedMap[T]) Items() []KeyValue[T] {
	items := make([]KeyValue[T], o.keyLength)

	for i, key := range o.keys {
		items[i] = KeyValue[T]{Key: key, Value: o.values[key]}
	}

	return items
}

func (o *OrderedMap[T]) Values() []T {
	values := make([]T, 0, len(o.keys))
	for _, key := range o.keys {
		values = append(values, o.values[key])
	}
	return values
}

func (o *OrderedMap[T]) GetByIndex(index int) (string, *T, bool) {
	if index >= 0 && index < len(o.keys) {
		key := o.keys[index]
		val := o.values[key]
		return key, &val, true
	}
	return "", nil, false
}
