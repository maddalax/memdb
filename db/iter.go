package db

import "iter"

func ToSlice[T any](iter iter.Seq[T]) []T {
	var slice = make([]T, 0)
	for item := range iter {
		slice = append(slice, item)
	}
	return slice
}

func ToSliceChunk[T any](iter iter.Seq[T], max int) []T {
	var slice = make([]T, 0, max)
	count := 0
	for item := range iter {
		if count >= max {
			break
		}
		slice = append(slice, item)
		count++
	}
	return slice
}
