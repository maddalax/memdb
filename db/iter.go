package db

import "iter"

func ToSlice[T any](iter iter.Seq[T]) []T {
	var slice = make([]T, 0)
	for item := range iter {
		slice = append(slice, item)
	}
	return slice
}
