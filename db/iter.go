package db

import "iter"

func IterMapValues[Map ~map[K]V, K comparable, V any](m Map, predicate func(key K, item V) bool) iter.Seq[V] {
	return func(yield func(V) bool) {
		for k, v := range m {
			if !predicate(k, v) {
				continue
			}
			if !yield(v) {
				return
			}
		}
	}
}
