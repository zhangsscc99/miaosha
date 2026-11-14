package main

// hist 按 worker 本地累计，结束后再合并，避免延迟统计本身变成全局锁。
// 桶上界用于报告「p99 不超过该值」，比插值略保守。
type hist struct {
	bounds []int64
	bins   []int64
	n      int64
	sum    int64
	max    int64
}

func newHist() *hist {
	// 单位纳秒。本地原子读在 1 微秒以下，Redis 往返在几十到几百微秒。
	bounds := []int64{
		200, 500, 1_000, 2_000, 5_000, 10_000,
		20_000, 50_000, 100_000, 200_000, 500_000,
		1_000_000, 2_000_000, 5_000_000,
		10_000_000, 20_000_000, 50_000_000,
		100_000_000, 200_000_000, 500_000_000,
	}
	return &hist{
		bounds: bounds,
		bins:   make([]int64, len(bounds)+1),
	}
}

func (h *hist) add(ns int64) {
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
	if o == nil {
		return
	}
	h.n += o.n
	h.sum += o.sum
	if o.max > h.max {
		h.max = o.max
	}
	for i := range h.bins {
		h.bins[i] += o.bins[i]
	}
}

type histSnap struct {
	Bins []int64 `json:"bins,omitempty"`
	N    int64   `json:"n"`
	Sum  int64   `json:"sum"`
	Max  int64   `json:"max"`
}

func (h *hist) snapshot() histSnap {
	return histSnap{
		Bins: append([]int64(nil), h.bins...),
		N:    h.n,
		Sum:  h.sum,
		Max:  h.max,
	}
}

func (h *hist) addSnap(s histSnap) {
	if len(s.Bins) != len(h.bins) {
		return
	}
	h.n += s.N
	h.sum += s.Sum
	if s.Max > h.max {
		h.max = s.Max
	}
	for i := range h.bins {
		h.bins[i] += s.Bins[i]
	}
}

// le 返回 p 分位的桶上界，单位纳秒。p 取 0.50、0.99、0.999。
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
