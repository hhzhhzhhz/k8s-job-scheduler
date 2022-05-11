package utils

import "sync"

// WaitGroupWrapper wrapper of sync WaitGroup
type WaitGroupWrapper struct {
	sync.WaitGroup
}

// Wrap calls the callback, and incr waitgroup
func (w *WaitGroupWrapper) Wrap(cb func()) {
	w.Add(1)
	go func() {
		cb()
		w.Done()
	}()
}

func Wrap(f func()) {
	go func() {
		f()
	}()
}
