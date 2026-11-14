package main

import "sync"

// flight 是进程内的 singleflight。同一个 key 上，进行中的函数只有一个，后来的等它的结果。
type flight struct {
	mu sync.Mutex
	m  map[string]*flightCall
}

type flightCall struct {
	wg  sync.WaitGroup
	val string
	err error
}

func (f *flight) Do(key string, fn func() (string, error)) (string, error) {
	f.mu.Lock()
	if f.m == nil {
		f.m = make(map[string]*flightCall)
	}
	if c, ok := f.m[key]; ok {
		f.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err
	}
	c := &flightCall{}
	c.wg.Add(1)
	f.m[key] = c
	f.mu.Unlock()

	c.val, c.err = fn()
	c.wg.Done()

	f.mu.Lock()
	delete(f.m, key)
	f.mu.Unlock()
	return c.val, c.err
}
