package main

import "math"

// slotOf 是正数上的整数除法，和 Lua 里修正后的窗口号对齐。
func slotOf(now, window int64) int64 {
	if window <= 0 || now < 0 {
		return 0
	}
	return now / window
}

// slideAllows 是 slide.lua 的整数公式。curr 是已经放行的次数，不含这一次。
func slideAllows(prev, curr, elapsed, window, limit int64) bool {
	numer := prev*(window-elapsed) + (curr+1)*window
	return numer <= limit*window
}

// maxInWindow 统计任意一段长度小于 window 的区间里最多有多少个时间戳。
// 和滑动日志一致：now-ts >= window 的成员已经被删掉，所以差值 >= window 不算在内。
func maxInWindow(ts []int64, window int64) int64 {
	if len(ts) == 0 {
		return 0
	}
	best := int64(1)
	j := 0
	for i := 0; i < len(ts); i++ {
		for j < i && ts[i]-ts[j] >= window {
			j++
		}
		n := int64(i - j + 1)
		if n > best {
			best = n
		}
	}
	return best
}

// memBucket 按 bucket.lua 的毫令牌规则在进程里走一遍，用来和 Redis 对答案。
type memBucket struct {
	rate   int64
	window int64
	capN   int64
	tokens int64
	ts     int64
	init   bool
}

func (b *memBucket) allow(now int64) bool {
	const scale int64 = 1000
	capN := b.capN
	if capN <= 0 {
		capN = b.rate
	}
	cap := capN * scale
	if !b.init {
		b.tokens = cap
		b.ts = now
		b.init = true
	} else {
		elapsed := now - b.ts
		if elapsed < 0 {
			elapsed = 0
		}
		if elapsed > b.window {
			elapsed = b.window
		}
		if elapsed > 0 {
			refill := int64(math.Floor(float64(elapsed) * float64(b.rate) * float64(scale) / float64(b.window)))
			b.tokens += refill
			if b.tokens > cap {
				b.tokens = cap
			}
			b.ts = now
		}
	}
	if b.tokens < scale {
		return false
	}
	b.tokens -= scale
	return true
}

type memFixed struct {
	limit  int64
	window int64
	slot   int64
	n      int64
	have   bool
}

func (f *memFixed) allow(now int64) bool {
	s := slotOf(now, f.window)
	if !f.have || s != f.slot {
		f.slot = s
		f.n = 0
		f.have = true
	}
	if f.n+1 > f.limit {
		return false
	}
	f.n++
	return true
}

type memSlide struct {
	limit  int64
	window int64
	slot   int64
	prev   int64
	curr   int64
	have   bool
}

func (s *memSlide) allow(now int64) bool {
	slot := slotOf(now, s.window)
	elapsed := now - slot*s.window
	if !s.have || slot != s.slot {
		if s.have && slot == s.slot+1 {
			s.prev = s.curr
		} else {
			s.prev = 0
		}
		s.curr = 0
		s.slot = slot
		s.have = true
	}
	if !slideAllows(s.prev, s.curr, elapsed, s.window, s.limit) {
		return false
	}
	s.curr++
	return true
}

type memLog struct {
	limit  int64
	window int64
	ts     []int64
	lo     int
}

func (m *memLog) allow(now int64) bool {
	cutoff := now - m.window
	for m.lo < len(m.ts) && m.ts[m.lo] <= cutoff {
		m.lo++
	}
	if int64(len(m.ts)-m.lo) >= m.limit {
		return false
	}
	m.ts = append(m.ts, now)
	if m.lo > 4096 && m.lo*2 > len(m.ts) {
		m.ts = append([]int64(nil), m.ts[m.lo:]...)
		m.lo = 0
	}
	return true
}

func countAllows(now int64, n int, allow func(int64) bool) (int64, []int64) {
	var got int64
	ts := make([]int64, 0, n)
	for i := 0; i < n; i++ {
		if allow(now) {
			got++
			ts = append(ts, now)
		}
	}
	return got, ts
}
