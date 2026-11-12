package main

// hist 按 worker 本地累计，结束后再合并。桶上界用来写「p99 不超过该值」。
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
		1_000_000_000, 1_500_000_000, 2_000_000_000,
		2_500_000_000, 3_000_000_000, 5_000_000_000,
	}
	return &hist{bounds: bounds, bins: make([]int64, len(bounds)+1)}
}

func (h *hist) add(ns int64) {
	if ns < 0 {
		ns = 0
	}
	h.n++
	h.sum += ns
	if ns > h.max {
		h.max = ns
	}
	i := 0
	for i < len(h.bounds) && ns > h.bounds[i] {
		i++
	}
	h.bins[i]++
}

func (h *hist) merge(o *hist) {
	if o == nil || o.n == 0 {
		return
	}
	h.n += o.n
	h.sum += o.sum
	if o.max > h.max {
		h.max = o.max
	}
	if len(o.bins) != len(h.bins) {
		return
	}
	for i := range h.bins {
		h.bins[i] += o.bins[i]
	}
}

func (h *hist) addSnap(bins []int64, n, sum, max int64) {
	if n == 0 || len(bins) != len(h.bins) {
		return
	}
	h.n += n
	h.sum += sum
	if max > h.max {
		h.max = max
	}
	for i := range h.bins {
		h.bins[i] += bins[i]
	}
}

func (h *hist) le(p float64) int64 {
	if h.n == 0 {
		return 0
	}
	need := int64(float64(h.n)*p + 0.999999)
	if need < 1 {
		need = 1
	}
	var seen int64
	for i, b := range h.bins {
		seen += b
		if seen >= need {
			if i >= len(h.bounds) {
				return h.max
			}
			return h.bounds[i]
		}
	}
	return h.max
}
