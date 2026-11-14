package main

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type benchSpec struct {
	name    string
	algo    string
	workers int
	limit   int64
	window  int64
	extra   int64
	pin     int64
	reject  bool
	warmup  time.Duration
	dur     time.Duration
}

func (r *runner) bench(spec benchSpec) result {
	res := result{
		Scenario: spec.name,
		Algo:     spec.algo,
		Limit:    spec.limit,
		WindowMs: spec.window,
		Workers:  spec.workers,
		Note:     "4 秒测量窗外加 1 秒预热。拒绝组先把额度用完，再计拒绝。QPS 只含测量窗。CPU 百分比只在测量窗长于 1 秒时有意义。",
	}
	if err := r.flush(); err != nil {
		return done(res, false, err.Error())
	}
	key := "bench:" + spec.name
	if spec.reject {
		n, err := r.serial(spec.algo, key, spec.limit, spec.window, spec.pin, spec.extra, spec.limit)
		if err != nil {
			return done(res, false, "prefill: "+err.Error())
		}
		if n != spec.limit {
			return done(res, false, fmt.Sprintf("prefill admits %d want %d", n, spec.limit))
		}
	}
	if _, _, err := r.drive(spec, key, spec.warmup, false); err != nil {
		return done(res, false, "warmup: "+err.Error())
	}
	beforeCmds, cmdErr := r.c.commands()
	st, wall, err := r.drive(spec, key, spec.dur, true)
	afterCmds, cmdErr2 := r.c.commands()
	if err != nil && st.errors == 0 {
		return done(res, false, err.Error())
	}
	elapsed := wall.Seconds()
	ops := st.admits + st.rejects
	res.Admits = st.admits
	res.Rejects = st.rejects
	res.Errors = st.errors
	res.ElapsedSec = elapsed
	res.Attempts = ops
	if elapsed > 0 && ops > 0 {
		res.QPS = float64(ops) / elapsed
	}
	if cmdErr == nil && cmdErr2 == nil {
		// after 那次 INFO 算进了 after，减掉。测量窗里的命令都是 worker 的 EVALSHA 和脚本里的 redis.call。
		res.Commands = afterCmds - beforeCmds - 1
		if ops > 0 && res.Commands > 0 {
			res.CommandsPerOp = float64(res.Commands) / float64(ops)
		}
	}
	res.RedisCPUSec = st.redisCPU
	res.ClientCPUSec = st.clientCPU
	if elapsed >= 1 && r.clk > 0 {
		res.CPUSampleValid = true
		res.RedisCPUPercent = st.redisCPU / r.clk / elapsed * 100
		res.ClientCPUPercent = st.clientCPU / r.clk / elapsed * 100
	}
	res.RedisRSSKB = rssKB(r.pid)
	res.P50us = us(st.h.le(0.50))
	res.P99us = us(st.h.le(0.99))
	res.P999us = us(st.h.le(0.999))
	if st.h.max > 0 {
		res.Maxus = st.h.max / 1000
	}
	if mem, err := r.c.usedMemory(); err == nil {
		res.UsedMemory = mem
	}
	if spec.algo == "log" {
		if rep, err := r.c.cmd("ZCARD", key); err == nil {
			res.Members = rep.Int
		}
	}
	if spec.pin >= 0 && (spec.algo == "fixed" || spec.algo == "fixedhot" || spec.algo == "slide") {
		slotKey := fmt.Sprintf("%s:%d", key, slotOf(spec.pin, spec.window))
		if n, ok, err := r.c.getInt(slotKey); err == nil && ok {
			res.FinalCount = n
		}
	}
	pass := err == nil && st.errors == 0 && ops > 0 && elapsed >= spec.dur.Seconds()*0.95 && elapsed < spec.dur.Seconds()+2
	if spec.reject && st.admits != 0 {
		pass = false
	}
	if !spec.reject && st.rejects != 0 {
		pass = false
	}
	errText := ""
	if err != nil {
		errText = " err=" + err.Error()
	}
	res = done(res, pass, fmt.Sprintf("qps=%.0f admits=%d rejects=%d errors=%d p99us<=%d maxus=%d redisCpu=%.1f%% clientCpu=%.1f%% elapsed=%.3fs final=%d%s",
		res.QPS, st.admits, st.rejects, st.errors, res.P99us, res.Maxus, res.RedisCPUPercent, res.ClientCPUPercent, elapsed, res.FinalCount, errText))
	return res
}

type benchStat struct {
	admits    int64
	rejects   int64
	errors    int64
	redisCPU  float64
	clientCPU float64
	h         *hist
	firstErr  string
}

func (r *runner) drive(spec benchSpec, key string, dur time.Duration, sample bool) (benchStat, time.Duration, error) {
	var st benchStat
	st.h = newHist()
	var admits, rejects, errors atomic.Int64
	var errOnce sync.Once
	var firstErr atomic.Value
	histMu := sync.Mutex{}
	workers := spec.workers
	var wg sync.WaitGroup
	ready := sync.WaitGroup{}
	start := make(chan struct{})
	conns := make([]*Conn, workers)
	for i := 0; i < workers; i++ {
		c, err := dial(r.addr)
		if err != nil {
			for _, cc := range conns {
				if cc != nil {
					cc.Close()
				}
			}
			return st, 0, err
		}
		c.extend(dur + 30*time.Second)
		conns[i] = c
	}
	sha := r.s.sha[spec.algo]
	for i := 0; i < workers; i++ {
		wg.Add(1)
		ready.Add(1)
		go func(c *Conn) {
			defer wg.Done()
			h := newHist()
			ready.Done()
			<-start
			deadline := time.Now().Add(dur)
			for time.Now().Before(deadline) {
				now := spec.pin
				if now < 0 {
					now = time.Now().UnixMilli()
				}
				t0 := time.Now()
				v, err := c.evalInt(sha, key, spec.limit, spec.window, now, spec.extra)
				h.add(time.Since(t0).Nanoseconds())
				if err != nil {
					errors.Add(1)
					errOnce.Do(func() { firstErr.Store(err.Error()) })
					return
				}
				if v == 1 {
					admits.Add(1)
				} else {
					rejects.Add(1)
				}
			}
			histMu.Lock()
			st.h.merge(h)
			histMu.Unlock()
		}(conns[i])
	}
	ready.Wait()
	var c0, p0 float64
	if sample {
		c0 = cpuTicks(r.pid)
		p0 = cpuTicks(os.Getpid())
	}
	t0 := time.Now()
	close(start)
	wg.Wait()
	wall := time.Since(t0)
	if sample {
		st.redisCPU = cpuTicks(r.pid) - c0
		st.clientCPU = cpuTicks(os.Getpid()) - p0
	}
	for _, c := range conns {
		c.Close()
	}
	st.admits = admits.Load()
	st.rejects = rejects.Load()
	st.errors = errors.Load()
	if v := firstErr.Load(); v != nil {
		st.firstErr = v.(string)
		if st.errors > 0 {
			return st, wall, fmt.Errorf("%s", st.firstErr)
		}
	}
	return st, wall, nil
}
