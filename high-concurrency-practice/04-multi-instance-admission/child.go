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

type childResult struct {
	Instance    int       `json:"instance"`
	Phase       int       `json:"phase"`
	Gate        string    `json:"gate"`
	QuotaPerSec float64   `json:"quotaPerSec"`
	Evals       int64     `json:"evals"`
	StockEvals  int64     `json:"stockEvals"`
	LocalReject int64     `json:"localReject"`
	QuotaReject int64     `json:"quotaReject"`
	Probes      int64     `json:"probes"`
	Success     int64     `json:"success"`
	SoldOut     int64     `json:"soldOut"`
	Duplicate   int64     `json:"duplicate"`
	Errors      int64     `json:"errors"`
	Ops         int64     `json:"ops"`
	Cleared     int64     `json:"cleared"`
	ElapsedSec  float64   `json:"elapsedSec"`
	FlagAtMs    float64   `json:"flagAtMs"`
	P50us       int64     `json:"p50us"`
	P99us       int64     `json:"p99us"`
	P999us      int64     `json:"p999us"`
	Maxus       int64     `json:"maxus"`
	Hist        *histSnap `json:"hist,omitempty"`
}

type tally struct {
	evals       int64
	stockEvals  int64
	localReject int64
	quotaReject int64
	probes      int64
	success     int64
	soldOut     int64
	dup         int64
	errs        int64
	ops         int64
	cleared     int64
}

func (t *tally) add(o tally) {
	t.evals += o.evals
	t.stockEvals += o.stockEvals
	t.localReject += o.localReject
	t.quotaReject += o.quotaReject
	t.probes += o.probes
	t.success += o.success
	t.soldOut += o.soldOut
	t.dup += o.dup
	t.errs += o.errs
	t.ops += o.ops
	t.cleared += o.cleared
}

// bucket 是进程内的令牌桶。rate<=0 表示不限制。桶一开始是满的，所以长度为 T 的窗口大约放行 burst+rate*T。
type bucket struct {
	mu     sync.Mutex
	tokens float64
	last   time.Time
	rate   float64
	burst  float64
	init   bool
}

func (b *bucket) allow(now time.Time) bool {
	if b == nil || b.rate <= 0 {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.init {
		b.tokens = b.burst
		b.last = now
		b.init = true
		if b.tokens > b.burst {
			b.tokens = b.burst
		}
	} else if dt := now.Sub(b.last).Seconds(); dt > 0 {
		b.tokens += dt * b.rate
		if b.tokens > b.burst {
			b.tokens = b.burst
		}
		b.last = now
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

type prober struct {
	every time.Duration
	next  atomic.Int64
}

func (p *prober) ready(now time.Time) bool {
	if p.every <= 0 {
		return false
	}
	return now.UnixNano() >= p.next.Load()
}

func (p *prober) take(now time.Time) bool {
	if p.every <= 0 {
		return false
	}
	for {
		cur := p.next.Load()
		if now.UnixNano() < cur {
			return false
		}
		if p.next.CompareAndSwap(cur, now.Add(p.every).UnixNano()) {
			return true
		}
	}
}

type state struct {
	cfg      config
	sold     atomic.Bool
	flagAt   atomic.Int64
	seq      atomic.Int64
	start    time.Time
	bucket   bucket
	probe    prober
	stockSHA string
	admitSHA string
}

func runChild(cfg config) error {
	if cfg.Workers < 1 {
		return fmt.Errorf("workers must be >= 1")
	}
	if cfg.StockSHA == "" {
		return fmt.Errorf("missing stock sha")
	}
	conns := make([]*Conn, cfg.Workers)
	for i := 0; i < cfg.Workers; i++ {
		c, err := dial(cfg.Addr)
		if err != nil {
			for _, prev := range conns[:i] {
				prev.Close()
			}
			return err
		}
		conns[i] = c
	}
	defer func() {
		for _, c := range conns {
			if c != nil {
				c.Close()
			}
		}
	}()
	if err := waitBarrier(cfg.Barrier1); err != nil {
		return err
	}
	st := &state{
		cfg:      cfg,
		bucket:   bucket{rate: cfg.Quota, burst: cfg.Burst},
		probe:    prober{every: cfg.Probe},
		stockSHA: cfg.StockSHA,
		admitSHA: cfg.AdmitSHA,
	}
	if err := writeChild(st.runPhase(conns, cfg.Attempts, cfg.Duration, 1)); err != nil {
		return err
	}
	if cfg.Phases < 2 {
		return nil
	}
	if err := waitBarrier(cfg.Barrier2); err != nil {
		return err
	}
	return writeChild(st.runPhase(conns, 0, cfg.Phase2, 2))
}

func (s *state) runPhase(conns []*Conn, attempts int64, duration time.Duration, phase int) childResult {
	if attempts <= 0 && duration <= 0 {
		return childResult{Instance: s.cfg.Instance, Phase: phase, Errors: 1, FlagAtMs: -1}
	}
	s.flagAt.Store(0)
	s.start = time.Now()
	end := s.start.Add(duration)
	var wg sync.WaitGroup
	tallys := make([]tally, len(conns))
	hists := make([]*hist, len(conns))
	wg.Add(len(conns))
	for w := range conns {
		go func(w int) {
			defer wg.Done()
			h := newHist()
			hists[w] = h
			c := conns[w]
			var t tally
			if attempts > 0 {
				for {
					id := s.seq.Add(1)
					if id > attempts {
						break
					}
					s.one(c, h, &t, id)
				}
			} else {
				for time.Now().Before(end) {
					id := s.seq.Add(1)
					s.one(c, h, &t, id)
				}
			}
			tallys[w] = t
		}(w)
	}
	wg.Wait()
	merged := newHist()
	var sum tally
	for i := range tallys {
		sum.add(tallys[i])
		if hists[i] != nil {
			merged.merge(hists[i])
		}
	}
	snap := merged.snapshot()
	flagMs := -1.0
	if ns := s.flagAt.Load(); ns > 0 {
		flagMs = float64(ns) / 1e6
	}
	return childResult{
		Instance:    s.cfg.Instance,
		Phase:       phase,
		Gate:        s.cfg.Gate,
		QuotaPerSec: s.cfg.Quota,
		Evals:       sum.evals,
		StockEvals:  sum.stockEvals,
		LocalReject: sum.localReject,
		QuotaReject: sum.quotaReject,
		Probes:      sum.probes,
		Success:     sum.success,
		SoldOut:     sum.soldOut,
		Duplicate:   sum.dup,
		Errors:      sum.errs,
		Ops:         sum.ops,
		Cleared:     sum.cleared,
		ElapsedSec:  time.Since(s.start).Seconds(),
		FlagAtMs:    flagMs,
		P50us:       merged.le(0.50) / 1000,
		P99us:       merged.le(0.99) / 1000,
		P999us:      merged.le(0.999) / 1000,
		Maxus:       merged.max / 1000,
		Hist:        &snap,
	}
}

func (s *state) one(c *Conn, h *hist, t *tally, id int64) {
	t.ops++
	now := time.Now()
	// probed 才允许把标记清掉。已经在途的成功请求和售罄响应会交错返回，
	// 如果成功就清标记，标记会被自己的在途请求打掉，补货之后挡不住。
	probed := false
	if s.cfg.UseFlag && s.sold.Load() {
		if !s.probe.ready(now) {
			t.localReject++
			return
		}
		if s.cfg.Gate == "local" && !s.bucket.allow(now) {
			t.quotaReject++
			return
		}
		if !s.probe.take(now) {
			t.localReject++
			return
		}
		probed = true
		t.probes++
	} else if s.cfg.Gate == "local" && !s.bucket.allow(now) {
		t.quotaReject++
		return
	}
	if s.cfg.Gate == "redis" {
		t0 := time.Now()
		code, err := c.evalSHA(s.admitSHA, 1, "sec:admit", strconv.Itoa(s.cfg.Limit), strconv.Itoa(s.cfg.WindowMs))
		if err != nil {
			t.errs++
			return
		}
		h.add(time.Since(t0).Nanoseconds())
		t.evals++
		if code == 0 {
			t.quotaReject++
			return
		}
	}
	user := strconv.FormatInt(int64(s.cfg.Instance)*1_000_000_000+id, 10)
	t0 := time.Now()
	code, err := c.evalSHA(s.stockSHA, 2, "sec:stock:1", "sec:win:1", user)
	if err != nil {
		t.errs++
		return
	}
	h.add(time.Since(t0).Nanoseconds())
	t.evals++
	t.stockEvals++
	switch code {
	case codeOK:
		t.success++
		s.openAfterProbe(probed, t)
	case codeSoldOut:
		t.soldOut++
		if s.cfg.UseFlag {
			s.sold.Store(true)
			s.markSold(time.Now())
		}
	case codeDup:
		t.dup++
		s.openAfterProbe(probed, t)
	default:
		t.errs++
	}
}

// openAfterProbe 只在这次请求是探测时清标记。探测到的成功就是这个用户买到的那一件。
func (s *state) openAfterProbe(probed bool, t *tally) {
	if probed && s.sold.CompareAndSwap(true, false) {
		t.cleared++
	}
}

func (s *state) markSold(now time.Time) {
	elapsed := now.Sub(s.start).Nanoseconds()
	if elapsed < 1 {
		elapsed = 1
	}
	s.flagAt.CompareAndSwap(0, elapsed)
}

func writeChild(r childResult) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	_, err = os.Stdout.Write(b)
	return err
}

func waitBarrier(addr string) error {
	if addr == "" {
		return fmt.Errorf("empty barrier")
	}
	var conn net.Conn
	var err error
	for i := 0; i < 100; i++ {
		conn, err = net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	buf := make([]byte, 1)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return err
	}
	if buf[0] != 'G' {
		return fmt.Errorf("barrier byte %q", buf[0])
	}
	return nil
}
