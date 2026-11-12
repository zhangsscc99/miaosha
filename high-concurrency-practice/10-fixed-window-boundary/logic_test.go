package main

import "testing"

func TestSlideTail(t *testing.T) {
	const limit, window int64 = 100, 1000
	prev := int64(0)
	curr := int64(0)
	for i := 0; i < int(limit); i++ {
		if !slideAllows(prev, curr, 999, window, limit) {
			t.Fatalf("first window rejected at %d", i)
		}
		curr++
	}
	if slideAllows(prev, curr, 999, window, limit) {
		t.Fatal("first window admitted one extra")
	}
	// 这些放行落在 0 号窗。下一窗刚开始时，上一窗按满额计。
	if slideAllows(curr, 0, 0, window, limit) {
		t.Fatal("elapsed 0 should reject")
	}
	var second int64
	prev = curr
	curr = 0
	for i := 0; i < 200; i++ {
		if slideAllows(prev, curr, 998, window, limit) {
			second++
			curr++
		}
	}
	if second != 99 {
		t.Fatalf("elapsed 998 admitted %d, want 99", second)
	}
}

func TestMaxInWindow(t *testing.T) {
	ts := make([]int64, 0, 200)
	for i := 0; i < 100; i++ {
		ts = append(ts, 999)
	}
	for i := 0; i < 100; i++ {
		ts = append(ts, 1000)
	}
	if got := maxInWindow(ts, 1000); got != 200 {
		t.Fatalf("gap 1ms = %d", got)
	}
	ts = ts[:100]
	for i := 0; i < 99; i++ {
		ts = append(ts, 1998)
	}
	if got := maxInWindow(ts, 1000); got != 199 {
		t.Fatalf("gap 999ms = %d", got)
	}
	ts = []int64{999}
	for i := 0; i < 100; i++ {
		ts = append(ts, 1999)
	}
	if got := maxInWindow(ts, 1000); got != 100 {
		t.Fatalf("exactly one window = %d", got)
	}
}

func TestBucketShapes(t *testing.T) {
	b := memBucket{rate: 100, window: 1000, capN: 100}
	at0, _ := countAllows(0, 150, b.allow)
	at1, _ := countAllows(1, 100, b.allow)
	var spread int64
	var ts []int64
	for now := int64(10); now <= 1000; now += 10 {
		if b.allow(now) {
			spread++
			ts = append(ts, now)
		}
	}
	if at0 != 100 || at1 != 0 || spread != 100 {
		t.Fatalf("cap=rate got %d %d %d", at0, at1, spread)
	}
	all := append([]int64{}, ts...)
	// 初始 100 次在 0，加上铺开的 100 次，整段跨度 1000ms，两端差值刚好等于窗口，不进同一个窗。
	front := make([]int64, 100)
	all = append(front, all...)
	// t=0 的 100 次和 t=10..990 的 99 次落在同一个长度小于 1000ms 的区间里。t=1000 和 t=0 刚好差一个窗口，不在里面。
	if got := maxInWindow(all, 1000); got != 199 {
		t.Fatalf("bucket spread max in window %d", got)
	}

	small := memBucket{rate: 100, window: 1000, capN: 1}
	first, _ := countAllows(0, 100, small.allow)
	again, _ := countAllows(0, 100, small.allow)
	var paced int64
	var pacedTs []int64
	pacedTs = append(pacedTs, 0)
	for now := int64(10); now <= 1000; now += 10 {
		if small.allow(now) {
			paced++
			pacedTs = append(pacedTs, now)
		}
	}
	if first != 1 || again != 0 || paced != 100 {
		t.Fatalf("cap=1 got %d %d %d", first, again, paced)
	}
	// 一共放行 101 次，但 t=0 和 t=1000 相差刚好一个窗口，任意一段里最多 100 次。
	if got := maxInWindow(pacedTs, 1000); got != 100 {
		t.Fatalf("cap=1 max in window %d", got)
	}
}

func TestBucketOneMillisecond(t *testing.T) {
	for _, rate := range []int64{100, 1000, 5000} {
		b := memBucket{rate: rate, window: 1000, capN: rate}
		a1, _ := countAllows(999, int(rate), b.allow)
		a2, _ := countAllows(1000, int(rate), b.allow)
		if a1 != rate || a2 != rate/1000 {
			t.Fatalf("rate %d got %d + %d", rate, a1, a2)
		}
	}
}

func TestBucketGap999(t *testing.T) {
	b := memBucket{rate: 100, window: 1000, capN: 100}
	at0, _ := countAllows(999, 100, b.allow)
	at1, _ := countAllows(1998, 200, b.allow)
	if at0 != 100 || at1 != 99 {
		t.Fatalf("gap 999ms bucket %d %d", at0, at1)
	}
}

func TestSlot(t *testing.T) {
	window := int64(1000)
	samples := []int64{0, 1, 999, 1000, 1001, 1998, 1999, 2000}
	for _, now := range samples {
		if slotOf(now, window) != now/window {
			t.Fatalf("slot %d", now)
		}
	}
}
