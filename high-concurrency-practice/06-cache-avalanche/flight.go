package main

import "sync"

// group 是进程内的 singleflight。不跨进程。
type group struct {
	mu sync.Mutex
	m  map[int]*call
}

type call struct {
	done chan struct{}
	body string
	err  error
}

func newGroup() *group {
	return &group{m: make(map[int]*call)}
}

func (g *group) Do(key int, fn func() (string, error)) (string, error) {
	g.mu.Lock()
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		<-c.done
		return c.body, c.err
	}
	c := &call{done: make(chan struct{})}
	g.m[key] = c
	g.mu.Unlock()
	c.body, c.err = fn()
	close(c.done)
	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()
	return c.body, c.err
}
