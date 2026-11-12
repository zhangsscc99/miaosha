package main

import "testing"

func TestTTLSpread(t *testing.T) {
	buckets := map[int]int{}
	min, max := 1<<30, 0
	for k := 0; k < 200; k++ {
		ttl := ttlFor("warm-jitter", k)
		if ttl < min {
			min = ttl
		}
		if ttl > max {
			max = ttl
		}
		buckets[ttl/100]++
		if ttlFor("warm-fixed", k) != 700 {
			t.Fatalf("fixed %d", k)
		}
	}
	if min != 350 || max != 1047 {
		t.Fatalf("range %d..%d", min, max)
	}
	for _, n := range buckets {
		if n > 40 {
			t.Fatalf("bucket clump %d", n)
		}
	}
}

func TestJobsGroupRepeats(t *testing.T) {
	jobs := buildJobs(3, 4)
	if len(jobs) != 12 {
		t.Fatalf("len %d", len(jobs))
	}
	for i := 0; i < 4; i++ {
		if jobs[i] != 0 || jobs[4+i] != 1 {
			t.Fatalf("jobs %v", jobs)
		}
	}
}

func TestSlideMax(t *testing.T) {
	b := []int64{10, 20, 30, 5}
	if slideMax(b, 1) != 30 || slideMax(b, 2) != 50 {
		t.Fatalf("slide %d %d", slideMax(b, 1), slideMax(b, 2))
	}
}

func TestLocalOffset(t *testing.T) {
	if localOffset("local-fixed", 3, 9) != 0 {
		t.Fatal("fixed")
	}
	if localOffset("local-proc", 3, 9) != 600 {
		t.Fatal("proc")
	}
	if localOffset("local-key", 3, 1) != localOffset("local-key", 0, 1) {
		t.Fatal("key offset must be the same on every process")
	}
	seen := map[int64]int{}
	for k := 0; k < 1000; k++ {
		seen[localOffset("local-key", 0, k)/100]++
	}
	for _, n := range seen {
		if n < 200 || n > 300 {
			t.Fatalf("key jitter bucket %d", n)
		}
	}
}
