package db

import "sync"

var wg = sync.WaitGroup{}

func WaitForLoad() {
	wg.Wait()
}

func AddLoad() {
	wg.Add(1)
}

func DoneLoad() {
	wg.Done()
}
