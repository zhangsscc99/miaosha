package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const localBucketsN = 40

type childResult struct {
	Instance    int     `json:"instance"`
	Ops         int64   `json:"ops"`
	Hits        int64   `json:"hits"`
	Misses      int64   `json:"misses"`
	Errors      int64   `json:"errors"`
	Polls       int64   `json:"polls"`
	Loads       int64   `json:"loads"`
	RedisCmds   int64   `json:"redisCmds"`
	FirstMissMs int64   `json:"firstMissMs"`
	ElapsedSec  float64 `json:"elapsedSec"`
	P50us       int64   `json:"p50us"`
	P99us       int64   `json:"p99us"`
	Maxus       int64   `json:"maxus"`
	MissP50us   int64   `json:"missP50us"`
	MissP99us   int64   `json:"missP99us"`
	MissMaxus   int64   `json:"missMaxus"`
	HitP99us    int64   `json:"hitP99us"`
	MissBuckets []int64 `json:"missBuckets,omitempty"`
	HistBins    []int64 `json:"histBins,omitempty"`
	HistN       int64   `json:"histN,omitempty"`
	HistSum     int64   `json:"histSum,omitempty"`
	HistMax     int64   `json:"histMax,omitempty"`
	MissBins    []int64 `json:"missBins,omitempty"`
	MissN       int64   `json:"missN,omitempty"`
	MissSum     int64   `json:"missSum,omitempty"`
	MissMax     int64   `json:"missMax,omitempty"`
	HitBins     []int64 `json:"hitBins,omitempty"`
	HitN        int64   `json:"hitN,omitempty"`
	HitSum      int64   `json:"hitSum,omitempty"`
	HitMax      int64   `json:"hitMax,omitempty"`
}

type tally struct {
	ops         int64
	hits        int64
	misses      int64
	errors      int64
	polls       int64
	loads       int64
	redisCmds   int64
	firstMiss   int64
	h           *hist
	missH       *hist
	hitH        *hist
	missBuckets []int64
}

func newTally() *tally {
	return &tally{
		h:           newHist(),
		missH:       newHist(),
		hitH:        newHist(),
		firstMiss:   -1,
		missBuckets: make([]int64, localBucketsN),
	}
}

func (t *tally) merge(o *tally) {
	t.ops += o.ops
	t.hits += o.hits
	t.misses += o.misses
	t.errors += o.errors
	t.polls += o.polls
	t.loads += o.loads
	t.redisCmds += o.redisCmds
	if o.firstMiss >= 0 && (t.firstMiss < 0 || o.firstMiss < t.firstMiss) {
		t.firstMiss = o.firstMiss
	}
	t.h.merge(o.h)
	t.missH.merge(o.missH)
	t.hitH.merge(o.hitH)
	for i := range t.missBuckets {
		t.missBuckets[i] += o.missBuckets[i]
	}
}

func (t *tally) result(instance int, elapsed time.Duration) childResult {
	first := t.firstMiss
	if first < 0 {
		first = -1
	}
	return childResult{
		Instance: instance, Ops: t.ops, Hits: t.hits, Misses: t.misses, Errors: t.errors,
		Polls: t.polls, Loads: t.loads, RedisCmds: t.redisCmds, FirstMissMs: first,
		ElapsedSec: elapsed.Seconds(),
		P50us:      t.h.le(0.50) / 1000, P99us: t.h.le(0.99) / 1000, Maxus: t.h.max / 1000,
		MissP50us: t.missH.le(0.50) / 1000, MissP99us: t.missH.le(0.99) / 1000, MissMaxus: t.missH.max / 1000,
		HitP99us:    t.hitH.le(0.99) / 1000,
		MissBuckets: append([]int64(nil), t.missBuckets...),
		HistBins:    append([]int64(nil), t.h.bins...), HistN: t.h.n, HistSum: t.h.sum, HistMax: t.h.max,
		MissBins: append([]int64(nil), t.missH.bins...), MissN: t.missH.n, MissSum: t.missH.sum, MissMax: t.missH.max,
		HitBins: append([]int64(nil), t.hitH.bins...), HitN: t.hitH.n, HitSum: t.hitH.sum, HitMax: t.hitH.max,
	}
}

func runChild(cfg config) error {
	payload := goodsPayload()
	conns := make([]*Conn, cfg.Workers)
	var origins []*originClient
	if needsOrigin(cfg.Mode) {
		origins = make([]*originClient, cfg.Workers)
	}
	for i := 0; i < cfg.Workers; i++ {
		c, err := dial(cfg.Addr)
		if err != nil {
			return err
		}
		defer c.Close()
		conns[i] = c
		if needsOrigin(cfg.Mode) {
			oc, err := dialOrigin(cfg.Origin)
			if err != nil {
				return err
			}
			defer oc.Close()
			origins[i] = oc
		}
	}
	if stringsHasPrefixLocal(cfg.Mode) {
		if err := preloadLocal(conns[0], cfg.Keys, payload); err != nil {
			return err
		}
	}
	if err := waitBarrier(cfg.Barrier); err != nil {
		return err
	}
	t0 := time.Now()
	var total tally
	switch {
	case stringsHasPrefixLocal(cfg.Mode):
		total = runLocal(cfg, conns, payload, t0)
	case len(cfg.Mode) >= 5 && cfg.Mode[:5] == "gate-":
		total = runGate(cfg, conns, origins, payload, t0)
	case cfg.Duration > 0:
		total = runWave(cfg, conns, origins, payload, t0)
	default:
		total = runBurst(cfg, conns, origins, payload, t0)
	}
	res := total.result(cfg.Instance, time.Since(t0))
	return json.NewEncoder(os.Stdout).Encode(res)
}

func stringsHasPrefixLocal(mode string) bool {
	return len(mode) >= 6 && mode[:6] == "local-"
}

func preloadLocal(c *Conn, keys int, payload string) error {
	for k := 0; k < keys; k++ {
		rep, err := c.get(keyName(k))
		if err != nil {
			return err
		}
		if rep.Nil || rep.Str != payload {
			return fmt.Errorf("preload g:%d missing", k)
		}
	}
	return nil
}

func waitBarrier(addr string) error {
	c, err := net.DialTimeout("tcp", addr, 30*time.Second)
	if err != nil {
		return err
	}
	defer c.Close()
	var b [1]byte
	if _, err := io.ReadFull(c, b[:]); err != nil {
		return err
	}
	if b[0] != 'G' {
		return fmt.Errorf("barrier byte %q", b[0])
	}
	return nil
}

type worker struct {
	cfg     config
	redis   *Conn
	origin  *originClient
	payload string
	flight  *group
	t       *tally
	t0      time.Time
}

func runBurst(cfg config, conns []*Conn, origins []*originClient, payload string, t0 time.Time) tally {
	jobs := buildJobs(cfg.Keys, cfg.Repeat)
	var next atomic.Int64
	flight := newGroup()
	tallies := make([]*tally, cfg.Workers)
	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		i := i
		tallies[i] = newTally()
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := worker{cfg: cfg, redis: conns[i], origin: origins[i], payload: payload, flight: flight, t: tallies[i], t0: t0}
			for {
				n := int(next.Add(1)) - 1
				if n >= len(jobs) {
					return
				}
				w.one(jobs[n], true)
			}
		}()
	}
	wg.Wait()
	var total tally
	total = *newTally()
	for _, t := range tallies {
		total.merge(t)
	}
	return total
}

func runGate(cfg config, conns []*Conn, origins []*originClient, payload string, t0 time.Time) tally {
	jobs := buildJobs(cfg.Keys, cfg.Repeat)
	var next atomic.Int64
	var phase1 sync.WaitGroup
	phase1.Add(cfg.Workers)
	start := make(chan struct{})
	errc := make(chan error, 1)
	flight := newGroup()
	tallies := make([]*tally, cfg.Workers)
	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		i := i
		tallies[i] = newTally()
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := worker{cfg: cfg, redis: conns[i], origin: origins[i], payload: payload, flight: flight, t: tallies[i], t0: t0}
			for {
				n := int(next.Add(1)) - 1
				if n >= len(jobs) {
					break
				}
				rep, err := w.redis.get(keyName(jobs[n]))
				w.t.redisCmds++
				if err != nil || !rep.Nil {
					select {
					case errc <- fmt.Errorf("gate phase1 g:%d err=%v nil=%v", jobs[n], err, rep.Nil):
					default:
					}
					w.t.errors++
				}
			}
			phase1.Done()
			if i == 0 {
				phase1.Wait()
				if err := waitGate(conns[0], cfg.Instances); err != nil {
					select {
					case errc <- err:
					default:
					}
					w.t.errors++
				}
				close(start)
			} else {
				<-start
			}
			for _, key := range jobsForWorker(jobs, i, cfg.Workers) {
				w.forceMiss(key)
			}
		}()
	}
	wg.Wait()
	select {
	case err := <-errc:
		fmt.Fprintf(os.Stderr, "gate: %v\n", err)
	default:
	}
	total := *newTally()
	for _, t := range tallies {
		total.merge(t)
	}
	return total
}

// jobsForWorker 把同一条 key 的重复请求尽量分给不同 worker，方便 plain 同时回源。
func jobsForWorker(jobs []int, worker, workers int) []int {
	if workers < 1 {
		return jobs
	}
	out := make([]int, 0, len(jobs)/workers+1)
	for i, key := range jobs {
		if i%workers == worker {
			out = append(out, key)
		}
	}
	return out
}

func waitGate(c *Conn, instances int) error {
	rep, err := c.cmd("INCR", "avalanche:gate")
	if err != nil {
		return err
	}
	if rep.Int >= int64(instances) {
		return nil
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
		got, err := c.cmd("GET", "avalanche:gate")
		if err != nil {
			return err
		}
		if got.Nil {
			continue
		}
		n, err := strconv.ParseInt(got.Str, 10, 64)
		if err != nil {
			return err
		}
		if n >= int64(instances) {
			return nil
		}
	}
	return fmt.Errorf("gate timeout")
}

func (w *worker) forceMiss(key int) {
	t0 := time.Now()
	useFlight := w.cfg.Mode != "gate-plain" && w.cfg.Mode != "gate-onekey-plain"
	if w.cfg.Mode == "gate-flight" {
		// 不二次 GET。每个进程对每个 key 回源一次，用来和 3200、4、200 对齐。
		w.noteMiss()
		body, err := w.flight.Do(key, func() (string, error) {
			return w.loadAndSet(key)
		})
		if err != nil || body != w.payload {
			w.fail(t0)
			return
		}
		w.doneMiss(t0)
		return
	}
	if !useFlight {
		// 阶段 1 已经确认未命中。不合并的设计里，每个请求都回源，即使别人刚刚写好。
		w.noteMiss()
		body, err := w.loadAndSet(key)
		if err != nil || body != w.payload {
			w.fail(t0)
			return
		}
		w.doneMiss(t0)
		return
	}
	rep, err := w.redis.get(keyName(key))
	w.t.redisCmds++
	if err != nil {
		w.fail(t0)
		return
	}
	if !rep.Nil {
		if rep.Str != w.payload {
			w.fail(t0)
			return
		}
		w.hit(t0)
		return
	}
	w.noteMiss()
	if w.cfg.Mode == "gate-rebuild" && w.cfg.Instance != 0 {
		w.poll(key, t0)
		return
	}
	body, err := w.flight.Do(key, func() (string, error) {
		again, err := w.redis.get(keyName(key))
		w.t.redisCmds++
		if err != nil {
			return "", err
		}
		if !again.Nil {
			if again.Str != w.payload {
				return "", fmt.Errorf("payload mismatch")
			}
			return again.Str, nil
		}
		return w.loadAndSet(key)
	})
	if err != nil || body != w.payload {
		w.fail(t0)
		return
	}
	w.doneMiss(t0)
}

func runWave(cfg config, conns []*Conn, origins []*originClient, payload string, t0 time.Time) tally {
	var next atomic.Int64
	flight := newGroup()
	end := t0.Add(cfg.Duration)
	tallies := make([]*tally, cfg.Workers)
	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		i := i
		tallies[i] = newTally()
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := worker{cfg: cfg, redis: conns[i], origin: origins[i], payload: payload, flight: flight, t: tallies[i], t0: t0}
			for time.Now().Before(end) {
				k := int(next.Add(1)-1) % cfg.Keys
				w.one(k, true)
			}
		}()
	}
	wg.Wait()
	total := *newTally()
	for _, t := range tallies {
		total.merge(t)
	}
	return total
}

func (w *worker) one(key int, allowOrigin bool) {
	name := keyName(key)
	t0 := time.Now()
	rep, err := w.redis.get(name)
	w.t.redisCmds++
	if err != nil {
		w.fail(t0)
		return
	}
	if !rep.Nil {
		if rep.Str != w.payload {
			w.fail(t0)
			return
		}
		w.hit(t0)
		return
	}
	w.noteMiss()
	if !allowOrigin || (w.cfg.Mode == "burst-rebuild" && w.cfg.Instance != 0) {
		w.poll(key, t0)
		return
	}
	body, err := w.load(key)
	if err != nil || body != w.payload {
		w.fail(t0)
		return
	}
	w.doneMiss(t0)
}

func (w *worker) load(key int) (string, error) {
	useFlight := w.cfg.Mode != "burst-plain" && w.cfg.Mode != "burst-plain-p4" && w.cfg.Mode != "burst-onekey-plain"
	if !useFlight {
		return w.loadAndSet(key)
	}
	return w.flight.Do(key, func() (string, error) {
		rep, err := w.redis.get(keyName(key))
		w.t.redisCmds++
		if err != nil {
			return "", err
		}
		if !rep.Nil {
			if rep.Str != w.payload {
				return "", fmt.Errorf("payload mismatch")
			}
			return rep.Str, nil
		}
		return w.loadAndSet(key)
	})
}

func (w *worker) loadAndSet(key int) (string, error) {
	body, err := w.origin.load()
	if err != nil {
		return "", err
	}
	w.t.loads++
	if string(body) != w.payload {
		return "", fmt.Errorf("origin payload")
	}
	if err := w.redis.setPX(keyName(key), w.payload, ttlFor(w.cfg.Mode, key)); err != nil {
		return "", err
	}
	w.t.redisCmds++
	return w.payload, nil
}

func (w *worker) poll(key int, t0 time.Time) {
	body, err := w.flight.Do(key, func() (string, error) {
		deadline := time.Now().Add(8 * time.Second)
		for {
			time.Sleep(time.Millisecond)
			w.t.polls++
			rep, err := w.redis.get(keyName(key))
			w.t.redisCmds++
			if err != nil {
				return "", err
			}
			if !rep.Nil {
				if rep.Str != w.payload {
					return "", fmt.Errorf("payload mismatch")
				}
				return rep.Str, nil
			}
			if time.Now().After(deadline) {
				return "", fmt.Errorf("poll timeout g:%d", key)
			}
		}
	})
	if err != nil || body != w.payload {
		w.fail(t0)
		return
	}
	w.doneMiss(t0)
}

func (w *worker) noteMiss() {
	w.t.misses++
	if w.t.firstMiss < 0 {
		ms := time.Since(w.t0).Milliseconds()
		w.t.firstMiss = ms
	}
}

func (w *worker) doneMiss(t0 time.Time) {
	el := time.Since(t0).Nanoseconds()
	w.t.h.add(el)
	w.t.missH.add(el)
	w.t.ops++
	idx := int(time.Since(w.t0) / (50 * time.Millisecond))
	if idx >= 0 && idx < len(w.t.missBuckets) {
		w.t.missBuckets[idx]++
	}
}

func (w *worker) hit(t0 time.Time) {
	el := time.Since(t0).Nanoseconds()
	w.t.h.add(el)
	w.t.hitH.add(el)
	w.t.hits++
	w.t.ops++
}

func (w *worker) fail(t0 time.Time) {
	w.t.errors++
	w.t.ops++
	w.t.h.add(time.Since(t0).Nanoseconds())
}

func runLocal(cfg config, conns []*Conn, payload string, t0 time.Time) tally {
	dead := make([]atomic.Int64, cfg.Keys)
	base := time.Now().Add(400 * time.Millisecond).UnixNano()
	for k := 0; k < cfg.Keys; k++ {
		dead[k].Store(base + localOffset(cfg.Mode, cfg.Instance, k)*int64(time.Millisecond))
	}
	end := t0.Add(cfg.Duration)
	tallies := make([]*tally, cfg.Workers)
	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		i := i
		tallies[i] = newTally()
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := worker{cfg: cfg, redis: conns[i], payload: payload, t: tallies[i], t0: t0}
			for time.Now().Before(end) {
				round := time.Now()
				for k := i; k < cfg.Keys; k += cfg.Workers {
					now := time.Now().UnixNano()
					if now < dead[k].Load() {
						w.t.hits++
						w.t.ops++
						continue
					}
					tGet := time.Now()
					rep, err := w.redis.get(keyName(k))
					w.t.redisCmds++
					w.noteMiss()
					if err != nil || rep.Nil || rep.Str != payload {
						w.t.errors++
						w.t.ops++
						continue
					}
					dead[k].Store(time.Now().Add(30 * time.Second).UnixNano())
					el := time.Since(tGet).Nanoseconds()
					w.t.h.add(el)
					w.t.missH.add(el)
					w.t.ops++
					idx := int(time.Since(w.t0) / (50 * time.Millisecond))
					if idx >= 0 && idx < len(w.t.missBuckets) {
						w.t.missBuckets[idx]++
					}
				}
				if d := time.Since(round); d < 10*time.Millisecond {
					time.Sleep(10*time.Millisecond - d)
				}
			}
		}()
	}
	wg.Wait()
	total := *newTally()
	for _, t := range tallies {
		total.merge(t)
	}
	return total
}
