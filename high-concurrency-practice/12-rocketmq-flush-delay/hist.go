package main

import "sync/atomic"

// hist 的桶上界用来写「p99 不超过该值」。
type hist struct {
	bounds []int64
	bins   []int64
	n      int64
	sum    int64
	max    int64
}

func newHist() *hist {
	bounds := []int64{
		200, 500, 1_000, 2_000, 5_000, 10_000,
		20_000, 50_000, 100_000, 200_000, 500_000,
		1_000_000, 2_000_000, 5_000_000,
		10_000_000, 20_000_000, 50_000_000,
		100_000_000, 200_000_000, 500_000_000,
		1_000_000_000, 2_000_000_000, 5_000_000_000,
	}
	return &hist{
		bounds: bounds,
		bins:   make([]int64, len(bounds)+1),
	}
}

func (h *hist) add(ns int64) {
	atomic.AddInt64(&h.n, 1)
	atomic.AddInt64(&h.sum, ns)
	for {
		old := atomic.LoadInt64(&h.max)
		if ns <= old || atomic.CompareAndSwapInt64(&h.max, old, ns) {
			break
		}
	}
	i := 0
	for i < len(h.bounds) && ns > h.bounds[i] {
		i++
	}
	atomic.AddInt64(&h.bins[i], 1)
}

func (h *hist) le(p float64) int64 {
	n := atomic.LoadInt64(&h.n)
	if n == 0 {
		return 0
	}
	need := int64(float64(n)*p + 0.999999)
	if need < 1 {
		need = 1
	}
	var seen int64
	for i := range h.bins {
		seen += atomic.LoadInt64(&h.bins[i])
		if seen >= need {
			if i >= len(h.bounds) {
				return atomic.LoadInt64(&h.max)
			}
			return h.bounds[i]
		}
	}
	return atomic.LoadInt64(&h.max)
}

func (h *hist) count() int64 { return atomic.LoadInt64(&h.n) }
