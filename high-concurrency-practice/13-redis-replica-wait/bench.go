package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func scenarioBench() (map[string]any, error) {
	return withBox(func(b *box) (map[string]any, error) {
		if err := b.flush(); err != nil {
			return nil, err
		}
		var proxy *delayProxy
		if C.delay > 0 {
			var err error
			proxy, err = startDelayProxy("127.0.0.1:16379", C.master, C.delay)
			if err != nil {
				return nil, err
			}
			defer proxy.Close()
			if err := b.setReplicaCount(1); err != nil {
				return nil, err
			}
			if err := replicaof(b.r, "127.0.0.1", "16379"); err != nil {
				return nil, err
			}
			if err := waitCaughtUp(b.m, b.r, 15*time.Second); err != nil {
				return nil, err
			}
		} else if err := b.setReplicaCount(C.replicas); err != nil {
			return nil, err
		}
		payload := strings.Repeat("x", C.payload)
		if C.payload < 1 {
			payload = "x"
		}
		if _, err := b.m.cmd("SET", "benchkey", payload); err != nil {
			return nil, err
		}
		if C.replicas > 0 || C.delay > 0 {
			if _, err := b.waitReplicas(1, 2000); err != nil {
				return nil, err
			}
		}
		kind := C.from
		if kind == "" {
			kind = "set"
		}
		op := makeOp(kind, payload)
		type side struct {
			name  string
			addr  string
			conns []*Conn
		}
		var sides []side
		add := func(name, addr string, n int) error {
			cs, err := dialN(addr, n)
			if err != nil {
				return err
			}
			sides = append(sides, side{name: name, addr: addr, conns: cs})
			return nil
		}
		n := C.workers
		if n < 1 {
			n = 1
		}
		var err error
		switch kind {
		case "replica":
			err = add("replica", C.replica, n)
		case "both":
			if err = add("master", C.master, n); err == nil {
				err = add("replica", C.replica, n)
			}
		default:
			err = add("master", C.master, n)
		}
		if err != nil {
			return nil, err
		}
		defer func() {
			for _, s := range sides {
				for _, c := range s.conns {
					c.Close()
				}
			}
		}()
		for _, s := range sides {
			runPhase(s.conns, C.warmup, op)
		}
		mpid, _ := pidOf(b.m)
		rpid, _ := pidOf(b.r)
		r2pid := 0
		if b.r2 != nil {
			r2pid, _ = pidOf(b.r2)
		}
		beforeCmd := commands(b.m)
		m0, r0, r20, c0 := cpuTicks(mpid), cpuTicks(rpid), cpuTicks(r2pid), cpuTicks(os.Getpid())
		t0 := time.Now()
		type part struct {
			name         string
			ops, errs    int64
			h            *hist
			first        string
			p50, p99, mx int64
		}
		parts := make([]part, len(sides))
		var wg sync.WaitGroup
		for i := range sides {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				ops, errs, h, first := runPhase(sides[i].conns, C.duration, op)
				parts[i] = part{name: sides[i].name, ops: ops, errs: errs, h: h, first: first}
			}(i)
		}
		wg.Wait()
		elapsed := time.Since(t0).Seconds()
		mCPU := cpuPercent(m0, cpuTicks(mpid), elapsed)
		rCPU := cpuPercent(r0, cpuTicks(rpid), elapsed)
		r2CPU := cpuPercent(r20, cpuTicks(r2pid), elapsed)
		cCPU := cpuPercent(c0, cpuTicks(os.Getpid()), elapsed)
		cmdDelta := commands(b.m) - beforeCmd
		var ops, errs int64
		merged := newHist()
		sideOut := make([]map[string]any, 0, len(parts))
		first := ""
		for _, p := range parts {
			ops += p.ops
			errs += p.errs
			if p.h != nil {
				merged.merge(p.h)
			}
			if first == "" && p.first != "" {
				first = p.first
			}
			qps := 0.0
			if elapsed > 0 {
				qps = float64(p.ops) / elapsed
			}
			sideOut = append(sideOut, map[string]any{
				"name": p.name, "ops": p.ops, "errors": p.errs, "qps": qps,
				"p50us": us(p.h, 0.50), "p99us": us(p.h, 0.99), "maxus": maxus(p.h),
			})
		}
		qps := 0.0
		if elapsed > 0 {
			qps = float64(ops) / elapsed
		}
		f := map[string]any{
			"scenario":           "bench-" + kind,
			"note":               benchNote(kind),
			"from":               kind,
			"workers":            n,
			"payload":            len(payload),
			"replicas":           C.replicas,
			"waitFor":            C.waitFor,
			"waitMs":             C.waitMs,
			"delayMs":            C.delay.Milliseconds(),
			"elapsedSec":         elapsed,
			"ops":                ops,
			"errors":             errs,
			"qps":                qps,
			"p50us":              us(merged, 0.50),
			"p99us":              us(merged, 0.99),
			"p999us":             us(merged, 0.999),
			"maxus":              maxus(merged),
			"masterCpuPercent":   mCPU,
			"replicaCpuPercent":  rCPU,
			"replica2CpuPercent": r2CPU,
			"clientCpuPercent":   cCPU,
			"masterRssKB":        rssKB(mpid),
			"replicaRssKB":       rssKB(rpid),
			"commandDelta":       cmdDelta,
			"firstError":         first,
			"sides":              sideOut,
		}
		pass := elapsed > 0 && ops > 0 && (errs == 0 || C.delay > 0)
		return finish(f, pass, fmt.Sprintf("kind=%s qps=%.1f ops=%d err=%d p99us=%d masterCPU=%.1f replicaCPU=%.1f clientCPU=%.1f delayMs=%d", kind, qps, ops, errs, us(merged, 0.99), mCPU, rCPU, cCPU, C.delay.Milliseconds())), nil
	})
}

func benchNote(kind string) string {
	switch kind {
	case "set-wait":
		return "一次操作是 SET 再 WAIT，QPS 按这一对算，不是按 Redis 命令数。没有用管线。"
	case "replica":
		return "只读从库上的同一个键。主库开着，复制连接闲着。"
	case "both":
		return "主库和从库同时读同一个键。workers 是每一侧的连接数。QPS 是两侧相加。"
	case "master":
		return "只读主库上的同一个键。从库按 replicas 参数挂着或摘掉。"
	default:
		return "反复 SET 同一个键。没有用管线。replicas 是挂在主库上的从库个数，摘掉的进程还在，只是不再复制。"
	}
}

func makeOp(kind, payload string) func(*Conn) error {
	switch kind {
	case "set-wait":
		wf := strconv.Itoa(C.waitFor)
		wm := strconv.Itoa(C.waitMs)
		return func(c *Conn) error {
			if _, err := c.cmd("SET", "benchkey", payload); err != nil {
				return err
			}
			rep, err := c.cmd("WAIT", wf, wm)
			if err != nil {
				return err
			}
			if rep.Int < int64(C.waitFor) {
				return fmt.Errorf("wait %d", rep.Int)
			}
			return nil
		}
	case "set":
		return func(c *Conn) error {
			_, err := c.cmd("SET", "benchkey", payload)
			return err
		}
	default:
		return func(c *Conn) error {
			_, err := c.cmd("GET", "benchkey")
			return err
		}
	}
}

func dialN(addr string, n int) ([]*Conn, error) {
	out := make([]*Conn, 0, n)
	for i := 0; i < n; i++ {
		c, err := dial(addr)
		if err != nil {
			for _, x := range out {
				x.Close()
			}
			return nil, err
		}
		out = append(out, c)
	}
	return out, nil
}

func runPhase(conns []*Conn, d time.Duration, op func(*Conn) error) (ops, errs int64, h *hist, first string) {
	if d <= 0 {
		return 0, 0, newHist(), ""
	}
	var wg sync.WaitGroup
	hists := make([]*hist, len(conns))
	opN := make([]int64, len(conns))
	errN := make([]int64, len(conns))
	firsts := make([]string, len(conns))
	start := make(chan struct{})
	for i, c := range conns {
		wg.Add(1)
		go func(i int, c *Conn) {
			defer wg.Done()
			local := newHist()
			var n, e int64
			var fst string
			<-start
			end := time.Now().Add(d)
			consec := 0
			for time.Now().Before(end) {
				t0 := time.Now()
				err := op(c)
				if err != nil {
					e++
					consec++
					if fst == "" {
						fst = err.Error()
					}
					if consec > 50 {
						break
					}
					continue
				}
				consec = 0
				local.add(time.Since(t0).Nanoseconds())
				n++
			}
			hists[i] = local
			opN[i] = n
			errN[i] = e
			firsts[i] = fst
		}(i, c)
	}
	close(start)
	wg.Wait()
	h = newHist()
	for i := range conns {
		ops += opN[i]
		errs += errN[i]
		h.merge(hists[i])
		if first == "" {
			first = firsts[i]
		}
	}
	return ops, errs, h, first
}

func us(h *hist, p float64) int64 {
	if h == nil || h.n == 0 {
		return 0
	}
	return h.le(p) / 1000
}

func maxus(h *hist) int64 {
	if h == nil {
		return 0
	}
	return h.max / 1000
}

func commands(c *Conn) int64 {
	info, err := c.info("stats")
	if err != nil {
		return 0
	}
	return infoInt(info, "total_commands_processed")
}

func scenarioWaitTimeout() (map[string]any, error) {
	return withBox(func(b *box) (map[string]any, error) {
		if err := b.flush(); err != nil {
			return nil, err
		}
		p, err := startDelayProxy("127.0.0.1:16379", C.master, 20*time.Millisecond)
		if err != nil {
			return nil, err
		}
		defer p.Close()
		if err := b.setReplicaCount(1); err != nil {
			return nil, err
		}
		if err := replicaof(b.r, "127.0.0.1", "16379"); err != nil {
			return nil, err
		}
		if err := waitCaughtUp(b.m, b.r, 15*time.Second); err != nil {
			return nil, err
		}
		const n = 30
		var wait0, wait1, saw2, saw1 int64
		for i := 0; i < n; i++ {
			if _, err := b.m.cmd("SET", "n", "0"); err != nil {
				return nil, err
			}
			if _, err := b.m.cmd("INCR", "n"); err != nil {
				return nil, err
			}
			rep, err := b.m.cmd("WAIT", "1", "5")
			if err != nil {
				return nil, err
			}
			if rep.Int == 0 {
				wait0++
				if _, err := b.m.cmd("INCR", "n"); err != nil {
					return nil, err
				}
			} else {
				wait1++
			}
			time.Sleep(100 * time.Millisecond)
			s, ok, err := getStr(b.r, "n")
			if err != nil {
				return nil, err
			}
			if ok && s == "2" {
				saw2++
			} else if ok && s == "1" {
				saw1++
			}
		}
		f := map[string]any{
			"scenario":   "wait-timeout",
			"note":       "代理每方向 20 毫秒，WAIT 超时 5 毫秒。超时返回 0 之后又 INCR 一次。WAIT 超时不会把已经写进主库的 INCR 撤掉。",
			"iterations": n, "wait0": wait0, "wait1": wait1, "replicaSaw2": saw2, "replicaSaw1": saw1,
			"delayMs": int64(20), "waitMs": 5,
		}
		pass := wait0 > 0 && saw2 > 0
		return finish(f, pass, fmt.Sprintf("wait0=%d wait1=%d saw2=%d saw1=%d", wait0, wait1, saw2, saw1)), nil
	})
}
