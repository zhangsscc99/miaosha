package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	detailKey = "gd:1"
	lockKey   = "gd:1:lock"
	missKey   = "gd:404"
	nullMark  = "__NULL__"
	cacheTTL  = 60
)

type childResult struct {
	Instance    int     `json:"instance"`
	Ops         int64   `json:"ops"`
	Hits        int64   `json:"hits"`
	Misses      int64   `json:"misses"`
	Errors      int64   `json:"errors"`
	Polls       int64   `json:"polls"`
	Stale       int64   `json:"stale"`
	Fresh       int64   `json:"fresh"`
	NullHits    int64   `json:"nullHits"`
	Loads       int64   `json:"loads"`
	FollowLoads int64   `json:"followLoads"`
	FollowOps   int64   `json:"followOps"`
	Refreshed   int64   `json:"refreshed"`
	RedisCmds   int64   `json:"redisCmds"`
	ElapsedSec  float64 `json:"elapsedSec"`
	P50us       int64   `json:"p50us"`
	P99us       int64   `json:"p99us"`
	P999us      int64   `json:"p999us"`
	Maxus       int64   `json:"maxus"`
	HistBins    []int64 `json:"histBins,omitempty"`
	HistN       int64   `json:"histN,omitempty"`
	HistSum     int64   `json:"histSum,omitempty"`
	HistMax     int64   `json:"histMax,omitempty"`
}

type tally struct {
	ops         int64
	hits        int64
	misses      int64
	errors      int64
	polls       int64
	stale       int64
	fresh       int64
	nullHits    int64
	loads       int64
	followLoads int64
	followOps   int64
	refreshed   int64
	redisCmds   int64
	h           *hist
}

func (t *tally) add(o tally) {
	t.ops += o.ops
	t.hits += o.hits
	t.misses += o.misses
	t.errors += o.errors
	t.polls += o.polls
	t.stale += o.stale
	t.fresh += o.fresh
	t.nullHits += o.nullHits
	t.loads += o.loads
	t.followLoads += o.followLoads
	t.followOps += o.followOps
	t.refreshed += o.refreshed
	t.redisCmds += o.redisCmds
	if t.h == nil {
		t.h = newHist()
	}
	t.h.merge(o.h)
}

var (
	pageSink   atomic.Uint64
	tokenSeq   atomic.Int64
	localPage  atomic.Value
	refreshing atomic.Bool
	loadGroup  flight
)

func runChild(cfg config) error {
	fix, err := os.ReadFile(cfg.Fixture)
	if err != nil {
		return err
	}
	fixture := string(fix)
	needRedis := cfg.Mode != "sql-fast" && cfg.Mode != "local-hit" && cfg.Mode != "local-logical"
	needOrigin := cfg.Mode != "redis-get" && cfg.Mode != "redis-get-touch" && cfg.Mode != "local-hit" && cfg.Mode != "lock-stuck"

	var sha string
	var redisConns []*Conn
	if needRedis {
		redisConns = make([]*Conn, cfg.Workers)
		for i := 0; i < cfg.Workers; i++ {
			c, err := dial(cfg.Addr)
			if err != nil {
				return err
			}
			defer c.Close()
			redisConns[i] = c
		}
		if cfg.Mode == "distlock" || cfg.Mode == "logical" || cfg.Mode == "lock-short" || cfg.Mode == "lock-stuck" {
			body, err := os.ReadFile(cfg.LuaDir + "/unlock.lua")
			if err != nil {
				return err
			}
			sha, err = redisConns[0].scriptLoad(string(body))
			if err != nil {
				return err
			}
		}
	}
	var origins []*originClient
	if needOrigin {
		origins = make([]*originClient, cfg.Workers)
		for i := 0; i < cfg.Workers; i++ {
			oc, err := dialOrigin(cfg.OriginAddr)
			if err != nil {
				return err
			}
			defer oc.Close()
			origins[i] = oc
		}
	}
	if cfg.Mode == "local-logical" {
		localPage.Store(&cachedPage{exp: 0, body: fixture})
	}

	bufs := make([][]byte, cfg.Workers)
	for i := range bufs {
		bufs[i] = make([]byte, len(fixture))
	}
	if cfg.Duration > 0 {
		if err := warm(cfg, redisConns, bufs[0], fixture); err != nil {
			return err
		}
	}
	if err := waitBarrier(cfg.Barrier); err != nil {
		return err
	}

	start := time.Now()
	var seq atomic.Int64
	tallies := make([]tally, cfg.Workers)
	var wg sync.WaitGroup
	wg.Add(cfg.Workers)
	for w := 0; w < cfg.Workers; w++ {
		go func(w int) {
			defer wg.Done()
			var c *Conn
			if needRedis {
				c = redisConns[w]
			}
			var oc *originClient
			if needOrigin {
				oc = origins[w]
			}
			h := newHist()
			t := tally{h: h}
			if cfg.Duration > 0 {
				end := time.Now().Add(cfg.Duration)
				for time.Now().Before(end) {
					one(cfg, c, oc, bufs[w], fixture, sha, w, &t)
				}
			} else {
				for {
					id := seq.Add(1)
					if id > cfg.Attempts {
						break
					}
					one(cfg, c, oc, bufs[w], fixture, sha, w, &t)
				}
				if cfg.Follow > 0 && (cfg.Mode == "negative" || cfg.Mode == "negative-race") {
					for i := 0; i < cfg.Follow; i++ {
						followNull(c, &t)
					}
				}
			}
			tallies[w] = t
		}(w)
	}
	wg.Wait()
	sum := tally{h: newHist()}
	for i := range tallies {
		sum.add(tallies[i])
	}
	out := childResult{
		Instance:    cfg.Instance,
		Ops:         sum.ops,
		Hits:        sum.hits,
		Misses:      sum.misses,
		Errors:      sum.errors,
		Polls:       sum.polls,
		Stale:       sum.stale,
		Fresh:       sum.fresh,
		NullHits:    sum.nullHits,
		Loads:       sum.loads,
		FollowLoads: sum.followLoads,
		FollowOps:   sum.followOps,
		Refreshed:   sum.refreshed,
		RedisCmds:   sum.redisCmds,
		ElapsedSec:  time.Since(start).Seconds(),
		P50us:       sum.h.le(0.50) / 1000,
		P99us:       sum.h.le(0.99) / 1000,
		P999us:      sum.h.le(0.999) / 1000,
		Maxus:       sum.h.max / 1000,
		HistBins:    append([]int64(nil), sum.h.bins...),
		HistN:       sum.h.n,
		HistSum:     sum.h.sum,
		HistMax:     sum.h.max,
	}
	return json.NewEncoder(os.Stdout).Encode(out)
}

func warm(cfg config, conns []*Conn, buf []byte, fixture string) error {
	end := time.Now().Add(500 * time.Millisecond)
	var t tally
	t.h = newHist()
	for time.Now().Before(end) {
		switch cfg.Mode {
		case "local-hit":
			absorb(buf, fixture)
		default:
			if len(conns) == 0 {
				return nil
			}
			rep, err := conns[0].get(detailKey)
			if err != nil {
				return err
			}
			if rep.Nil || rep.Str != fixture {
				return fmt.Errorf("warmup miss")
			}
			if cfg.Mode == "redis-get-touch" {
				absorb(buf, rep.Str)
			}
		}
		_ = t
	}
	return nil
}

func absorb(dst []byte, src string) {
	n := copy(dst[:len(src)], src)
	if n == 0 {
		return
	}
	pageSink.Add(uint64(dst[0]) + uint64(dst[n-1]) + uint64(n))
}

func one(cfg config, c *Conn, oc *originClient, buf []byte, fixture, sha string, worker int, t *tally) {
	switch cfg.Mode {
	case "redis-get":
		redisRead(c, buf, fixture, t, false)
	case "redis-get-touch":
		redisRead(c, buf, fixture, t, true)
	case "local-hit":
		t0 := time.Now()
		absorb(buf, fixture)
		t.h.add(time.Since(t0).Nanoseconds())
		t.ops++
		t.hits++
	case "sql-fast":
		sqlRead(oc, fixture, t)
	case "stampede-fast":
		stampede(c, oc, buf, fixture, t, 'F')
	case "stampede-slow":
		stampede(c, oc, buf, fixture, t, 'L')
	case "singleflight":
		single(c, oc, buf, fixture, t)
	case "distlock":
		locked(c, oc, buf, fixture, sha, t, cfg.LockMs, false)
	case "lock-short":
		locked(c, oc, buf, fixture, sha, t, cfg.LockMs, true)
	case "logical":
		logicalRedis(c, oc, buf, fixture, sha, t, cfg.LockMs)
	case "local-logical":
		logicalLocal(oc, buf, fixture, t)
	case "lock-stuck":
		stuck(c, t, cfg.Instance, worker)
	case "penetrate":
		penetrate(c, oc, t)
	case "negative":
		negative(c, oc, t, true)
	case "negative-race":
		negative(c, oc, t, false)
	default:
		t.errors++
		t.ops++
	}
}

func redisRead(c *Conn, buf []byte, fixture string, t *tally, touch bool) {
	t0 := time.Now()
	rep, err := c.get(detailKey)
	t.redisCmds++
	if err != nil || rep.Nil || rep.Str != fixture {
		t.errors++
		t.ops++
		return
	}
	if touch {
		absorb(buf, rep.Str)
	} else {
		pageSink.Add(uint64(len(rep.Str)))
	}
	t.h.add(time.Since(t0).Nanoseconds())
	t.ops++
	t.hits++
}

func sqlRead(oc *originClient, fixture string, t *tally) {
	t0 := time.Now()
	st, body, err := oc.call('F')
	noteLoad(t, err)
	if err != nil || st != 'B' || string(body) != fixture {
		t.errors++
		t.ops++
		return
	}
	t.h.add(time.Since(t0).Nanoseconds())
	t.ops++
	t.hits++
}

func stampede(c *Conn, oc *originClient, buf []byte, fixture string, t *tally, op byte) {
	t0 := time.Now()
	rep, err := c.get(detailKey)
	t.redisCmds++
	if err != nil {
		t.errors++
		t.ops++
		return
	}
	var body string
	if rep.Nil {
		t.misses++
		st, b, err := oc.call(op)
		noteLoad(t, err)
		if err != nil || st != 'B' || string(b) != fixture {
			t.errors++
			t.ops++
			return
		}
		body = string(b)
		if err := c.setEX(detailKey, body, cacheTTL); err != nil {
			t.errors++
			t.ops++
			return
		}
		t.redisCmds++
	} else {
		t.hits++
		if rep.Str != fixture {
			t.errors++
			t.ops++
			return
		}
		body = rep.Str
	}
	absorb(buf, body)
	t.h.add(time.Since(t0).Nanoseconds())
	t.ops++
}

func single(c *Conn, oc *originClient, buf []byte, fixture string, t *tally) {
	t0 := time.Now()
	rep, err := c.get(detailKey)
	t.redisCmds++
	if err != nil {
		t.errors++
		t.ops++
		return
	}
	body := rep.Str
	if rep.Nil {
		t.misses++
		body, err = loadGroup.Do(detailKey, func() (string, error) {
			again, err := c.get(detailKey)
			t.redisCmds++
			if err != nil {
				return "", err
			}
			if !again.Nil {
				return again.Str, nil
			}
			st, b, err := oc.call('L')
			noteLoad(t, err)
			if err != nil {
				return "", err
			}
			if st != 'B' || string(b) != fixture {
				return "", fmt.Errorf("bad origin body")
			}
			if err := c.setEX(detailKey, string(b), cacheTTL); err != nil {
				return "", err
			}
			t.redisCmds++
			return string(b), nil
		})
		if err != nil || body != fixture {
			t.errors++
			t.ops++
			return
		}
	} else {
		t.hits++
		if body != fixture {
			t.errors++
			t.ops++
			return
		}
	}
	absorb(buf, body)
	t.h.add(time.Since(t0).Nanoseconds())
	t.ops++
}

func locked(c *Conn, oc *originClient, buf []byte, fixture, sha string, t *tally, px int, retryLock bool) {
	t0 := time.Now()
	deadline := t0.Add(5 * time.Second)
	for time.Now().Before(deadline) {
		rep, err := c.get(detailKey)
		t.redisCmds++
		if err != nil {
			t.errors++
			t.ops++
			return
		}
		if !rep.Nil {
			if rep.Str != fixture {
				t.errors++
				t.ops++
				return
			}
			absorb(buf, rep.Str)
			t.hits++
			t.h.add(time.Since(t0).Nanoseconds())
			t.ops++
			return
		}
		t.misses++
		tok := nextToken()
		ok, err := c.setNX(lockKey, tok, px)
		t.redisCmds++
		if err != nil {
			t.errors++
			t.ops++
			return
		}
		if ok {
			st, b, err := oc.call('L')
			noteLoad(t, err)
			if err != nil || st != 'B' || string(b) != fixture {
				_, _ = c.unlock(sha, lockKey, tok)
				t.redisCmds++
				t.errors++
				t.ops++
				return
			}
			if err := c.setEX(detailKey, string(b), cacheTTL); err != nil {
				_, _ = c.unlock(sha, lockKey, tok)
				t.redisCmds++
				t.errors++
				t.ops++
				return
			}
			t.redisCmds++
			if _, err := c.unlock(sha, lockKey, tok); err != nil {
				t.errors++
				t.ops++
				return
			}
			t.redisCmds++
			t.refreshed++
			absorb(buf, string(b))
			t.h.add(time.Since(t0).Nanoseconds())
			t.ops++
			return
		}
		if retryLock {
			time.Sleep(time.Millisecond)
			t.polls++
			continue
		}
		for time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
			t.polls++
			rep, err = c.get(detailKey)
			t.redisCmds++
			if err != nil {
				t.errors++
				t.ops++
				return
			}
			if rep.Nil {
				continue
			}
			if rep.Str != fixture {
				t.errors++
				t.ops++
				return
			}
			absorb(buf, rep.Str)
			t.hits++
			t.h.add(time.Since(t0).Nanoseconds())
			t.ops++
			return
		}
		t.errors++
		t.ops++
		return
	}
	t.errors++
	t.ops++
}

func logicalRedis(c *Conn, oc *originClient, buf []byte, fixture, sha string, t *tally, px int) {
	t0 := time.Now()
	rep, err := c.get(detailKey)
	t.redisCmds++
	if err != nil || rep.Nil {
		t.errors++
		t.ops++
		return
	}
	exp, body, ok := splitMeta(rep.Str)
	if !ok || body != fixture {
		t.errors++
		t.ops++
		return
	}
	absorb(buf, body)
	now := time.Now().UnixMilli()
	if exp > now {
		t.fresh++
		t.h.add(time.Since(t0).Nanoseconds())
		t.ops++
		return
	}
	t.stale++
	tok := nextToken()
	held, err := c.setNX(lockKey, tok, px)
	t.redisCmds++
	if err != nil {
		t.errors++
		t.ops++
		return
	}
	if !held {
		t.h.add(time.Since(t0).Nanoseconds())
		t.ops++
		return
	}
	t.refreshed++
	st, b, err := oc.call('L')
	noteLoad(t, err)
	if err != nil || st != 'B' || string(b) != fixture {
		_, _ = c.unlock(sha, lockKey, tok)
		t.redisCmds++
		t.errors++
		t.ops++
		return
	}
	next := strconv.FormatInt(time.Now().UnixMilli()+60_000, 10) + "\n" + string(b)
	if err := c.setPlain(detailKey, next); err != nil {
		_, _ = c.unlock(sha, lockKey, tok)
		t.redisCmds++
		t.errors++
		t.ops++
		return
	}
	t.redisCmds++
	if _, err := c.unlock(sha, lockKey, tok); err != nil {
		t.errors++
		t.ops++
		return
	}
	t.redisCmds++
	t.h.add(time.Since(t0).Nanoseconds())
	t.ops++
}

type cachedPage struct {
	exp  int64
	body string
}

func logicalLocal(oc *originClient, buf []byte, fixture string, t *tally) {
	t0 := time.Now()
	page := localPage.Load().(*cachedPage)
	if page.body != fixture {
		t.errors++
		t.ops++
		return
	}
	absorb(buf, page.body)
	if page.exp > time.Now().UnixMilli() {
		t.fresh++
		t.h.add(time.Since(t0).Nanoseconds())
		t.ops++
		return
	}
	t.stale++
	if !refreshing.CompareAndSwap(false, true) {
		t.h.add(time.Since(t0).Nanoseconds())
		t.ops++
		return
	}
	t.refreshed++
	st, b, err := oc.call('L')
	noteLoad(t, err)
	if err != nil || st != 'B' || string(b) != fixture {
		refreshing.Store(false)
		t.errors++
		t.ops++
		return
	}
	localPage.Store(&cachedPage{exp: time.Now().UnixMilli() + 60_000, body: string(b)})
	refreshing.Store(false)
	t.h.add(time.Since(t0).Nanoseconds())
	t.ops++
}

func stuck(c *Conn, t *tally, instance, worker int) {
	t0 := time.Now()
	if instance == 0 && worker == 0 {
		ok, err := c.setNX(lockKey, nextToken(), 0)
		t.redisCmds++
		if err != nil || !ok {
			t.errors++
			t.ops++
			return
		}
		// 拿到锁之后进程退出：不回源，不 DEL，锁没有过期时间。
		t.errors++
		t.ops++
		t.h.add(time.Since(t0).Nanoseconds())
		return
	}
	deadline := time.Now().Add(300 * time.Millisecond)
	sawLock := false
	for time.Now().Before(deadline) {
		rep, err := c.get(lockKey)
		t.redisCmds++
		t.polls++
		if err != nil {
			t.errors++
			t.ops++
			return
		}
		if !rep.Nil {
			sawLock = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !sawLock {
		t.errors++
		t.ops++
		return
	}
	deadline = time.Now().Add(150 * time.Millisecond)
	for time.Now().Before(deadline) {
		rep, err := c.get(detailKey)
		t.redisCmds++
		t.polls++
		if err != nil || !rep.Nil {
			t.errors++
			t.ops++
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.errors++
	t.ops++
	t.h.add(time.Since(t0).Nanoseconds())
}

func penetrate(c *Conn, oc *originClient, t *tally) {
	t0 := time.Now()
	rep, err := c.get(missKey)
	t.redisCmds++
	if err != nil || !rep.Nil {
		t.errors++
		t.ops++
		return
	}
	t.misses++
	st, _, err := oc.call('N')
	noteLoad(t, err)
	if err != nil || st != 'M' {
		t.errors++
		t.ops++
		return
	}
	t.h.add(time.Since(t0).Nanoseconds())
	t.ops++
}

func negative(c *Conn, oc *originClient, t *tally, coalesce bool) {
	t0 := time.Now()
	rep, err := c.get(missKey)
	t.redisCmds++
	if err != nil {
		t.errors++
		t.ops++
		return
	}
	if !rep.Nil {
		if rep.Str != nullMark {
			t.errors++
			t.ops++
			return
		}
		t.nullHits++
		t.h.add(time.Since(t0).Nanoseconds())
		t.ops++
		return
	}
	t.misses++
	var loadErr error
	if coalesce {
		_, loadErr = loadGroup.Do(missKey, func() (string, error) {
			return storeNull(c, oc, t)
		})
	} else {
		_, loadErr = storeNull(c, oc, t)
	}
	if loadErr != nil {
		t.errors++
		t.ops++
		return
	}
	t.h.add(time.Since(t0).Nanoseconds())
	t.ops++
}

func storeNull(c *Conn, oc *originClient, t *tally) (string, error) {
	again, err := c.get(missKey)
	t.redisCmds++
	if err != nil {
		return "", err
	}
	if !again.Nil {
		if again.Str != nullMark {
			return "", fmt.Errorf("unexpected %q", again.Str)
		}
		return nullMark, nil
	}
	st, _, err := oc.call('N')
	noteLoad(t, err)
	if err != nil {
		return "", err
	}
	if st != 'M' {
		return "", fmt.Errorf("status %q", st)
	}
	if err := c.setEX(missKey, nullMark, cacheTTL); err != nil {
		return "", err
	}
	t.redisCmds++
	return nullMark, nil
}

func followNull(c *Conn, t *tally) {
	rep, err := c.get(missKey)
	t.redisCmds++
	t.followOps++
	if err != nil || rep.Nil || rep.Str != nullMark {
		t.followLoads++
		t.errors++
		return
	}
	t.nullHits++
}

func splitMeta(v string) (int64, string, bool) {
	i := strings.IndexByte(v, '\n')
	if i <= 0 {
		return 0, "", false
	}
	n, err := strconv.ParseInt(v[:i], 10, 64)
	if err != nil {
		return 0, "", false
	}
	return n, v[i+1:], true
}

func noteLoad(t *tally, err error) {
	if err == nil {
		t.loads++
	}
}

func nextToken() string {
	return strconv.FormatInt(int64(os.Getpid()), 10) + "-" + strconv.FormatInt(tokenSeq.Add(1), 10)
}

func waitBarrier(addr string) error {
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return err
	}
	defer c.Close()
	buf := make([]byte, 1)
	_, err = io.ReadFull(c, buf)
	if err != nil {
		return err
	}
	if buf[0] != 'G' {
		return fmt.Errorf("barrier byte %q", buf[0])
	}
	return nil
}
