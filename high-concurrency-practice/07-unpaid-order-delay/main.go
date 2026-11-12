package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	dueKey    = "due"
	statusKey = "status"
	stockKey  = "stock"
	procKey   = "processing"
)

type result struct {
	Scenario        string  `json:"scenario"`
	Note            string  `json:"note"`
	Mode            string  `json:"mode"`
	Pass            bool    `json:"pass"`
	Detail          string  `json:"detail,omitempty"`
	Workers         int     `json:"workers"`
	Orders          int     `json:"orders"`
	ElapsedSec      float64 `json:"elapsedSec"`
	Ops             int64   `json:"ops"`
	QPS             float64 `json:"qps"`
	Errors          int64   `json:"errors"`
	Closed          int64   `json:"closed"`
	Paid            int64   `json:"paid"`
	Unpaid          int64   `json:"unpaid"`
	Stock           int64   `json:"stock"`
	ZCard           int64   `json:"zcard"`
	HLen            int64   `json:"hlen"`
	Processing      int64   `json:"processing"`
	Early           int64   `json:"early"`
	Misses          int64   `json:"misses"`
	RedisCommands   int64   `json:"redisCommands"`
	RedisCPUSec     float64 `json:"redisCpuSec"`
	RedisCPUpct     float64 `json:"redisCpuPercent"`
	ClientCPUpct    float64 `json:"clientCpuPercent"`
	UsedMemory      int64   `json:"usedMemory"`
	MemoryDelta     int64   `json:"memoryDelta"`
	BytesPerMember  float64 `json:"bytesPerMember"`
	HashBytes       float64 `json:"hashBytesPerField"`
	P50us           int64   `json:"p50us"`
	P99us           int64   `json:"p99us"`
	Maxus           int64   `json:"maxus"`
	RSSKB           int64   `json:"redisRssKB"`
	ExpiredKeys     int64   `json:"expiredKeys"`
	DBSize          int64   `json:"dbSize"`
	AOF             bool    `json:"aof"`
	SQL             *sqlRun `json:"sql,omitempty"`
	NoIndexMedianUs int64   `json:"noIndexMedianUs"`
	IndexMedianUs   int64   `json:"indexMedianUs"`
}

func main() {
	mode := flag.String("mode", "", "unsafe, lua-byid, pop, pay-race, replay, not-yet, ttl, recover, drain, recycle, zadd, memory, late-poll, late-smart, sql")
	scenario := flag.String("scenario", "", "written into the json")
	note := flag.String("note", "", "written into the json")
	out := flag.String("out", "", "result json path")
	addr := flag.String("addr", "127.0.0.1:6379", "redis address")
	workers := flag.Int("workers", 16, "worker goroutines")
	orders := flag.Int("orders", 1000, "orders")
	card := flag.Int("card", 10000, "zset cardinality for recycle")
	rows := flag.Int("rows", 200000, "sqlite rows")
	due := flag.Int("due", 5000, "sqlite due rows")
	duration := flag.Duration("duration", 3*time.Second, "measure window")
	warmup := flag.Duration("warmup", time.Second, "warmup before measure")
	poll := flag.Duration("poll", 20*time.Millisecond, "blind poll interval")
	step := flag.Duration("step", time.Millisecond, "gap between order deadlines")
	aof := flag.Bool("aof", false, "turn on AOF everysec before loading")
	flag.Parse()
	if *mode == "" {
		fatal(fmt.Errorf("missing -mode"))
	}

	res := result{Scenario: *scenario, Note: *note, Mode: *mode, Workers: *workers, Orders: *orders, AOF: *aof}
	if res.Scenario == "" {
		res.Scenario = *mode
	}

	var err error
	switch *mode {
	case "sql":
		err = runSQLMode(&res, *rows, *due)
	default:
		err = runRedis(*mode, &res, *addr, *workers, *orders, *card, *duration, *warmup, *poll, *step, *aof)
	}
	if err != nil {
		fatal(err)
	}
	b, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		fatal(err)
	}
	b = append(b, '\n')
	if *out != "" {
		if err := os.WriteFile(*out, b, 0o644); err != nil {
			fatal(err)
		}
	}
	fmt.Printf("%s pass=%v closed=%d paid=%d stock=%d qps=%.0f %s\n", res.Scenario, res.Pass, res.Closed, res.Paid, res.Stock, res.QPS, res.Detail)
	if !res.Pass {
		os.Exit(1)
	}
}

func runSQLMode(res *result, rows, due int) error {
	out, err := runSQL(rows, due)
	if err != nil {
		return err
	}
	res.SQL = &out
	res.Orders = rows
	res.NoIndexMedianUs = median(out.NoIndex)
	res.IndexMedianUs = median(out.Index)
	res.Pass = out.Changed == due && strings.Contains(out.Plan, "idx_due")
	res.Detail = fmt.Sprintf("rows=%d due=%d noIndexMedianUs=%d indexMedianUs=%d updateUs=%d plan=%s",
		rows, due, res.NoIndexMedianUs, res.IndexMedianUs, out.UpdateUs, out.Plan)
	return nil
}

func runRedis(mode string, res *result, addr string, workers, orders, card int, duration, warmup, poll, step time.Duration, aof bool) error {
	c, err := dial(addr)
	if err != nil {
		return err
	}
	defer c.Close()
	if aof {
		if _, err := c.cmd("CONFIG", "SET", "appendonly", "yes"); err != nil {
			return err
		}
		if _, err := c.cmd("CONFIG", "SET", "appendfsync", "everysec"); err != nil {
			return err
		}
	}
	pid, err := redisPID(c)
	if err != nil {
		return err
	}
	switch mode {
	case "unsafe":
		return runUnsafe(res, c, addr, pid, workers, orders)
	case "lua-byid":
		return runLuaByID(res, c, addr, pid, workers, orders)
	case "pop":
		return runPop(res, c, addr, pid, workers, orders)
	case "pay-race":
		return runPayRace(res, c, addr, pid, workers, orders)
	case "replay":
		return runReplay(res, c, addr, pid)
	case "not-yet":
		return runNotYet(res, c, addr, pid, orders)
	case "ttl":
		return runTTL(res, c, addr, pid, orders)
	case "recover":
		return runRecover(res, c, addr, pid, orders)
	case "drain":
		return runDrain(res, c, addr, pid, workers, orders)
	case "recycle":
		return runRecycle(res, c, addr, pid, workers, card, duration, warmup)
	case "zadd":
		return runZAdd(res, c, addr, pid, workers, orders)
	case "memory":
		return runMemory(res, c, pid, card)
	case "late-poll":
		return runLate(res, c, addr, pid, orders, poll, step, false)
	case "late-smart":
		return runLate(res, c, addr, pid, orders, poll, step, true)
	default:
		return fmt.Errorf("unknown mode %s", mode)
	}
}

func runUnsafe(res *result, c *Conn, addr string, pid, workers, orders int) error {
	if err := loadOrders(c, orders, func(int) string { return "0" }); err != nil {
		return err
	}
	cs, err := dialAll(addr, workers)
	if err != nil {
		return err
	}
	defer closeAll(cs)
	before := commands(c)
	cpu0, client0 := cpuTicks(pid), cpuTicks(os.Getpid())
	start := time.Now()
	var wg sync.WaitGroup
	var errs atomic.Int64
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(w *Conn) {
			defer wg.Done()
			for id := 1; id <= orders; id++ {
				s := strconv.Itoa(id)
				rep, err := w.cmd("HGET", statusKey, s)
				if err != nil {
					errs.Add(1)
					return
				}
				if rep.Nil || rep.Str != "UNPAID" {
					continue
				}
				if _, err := w.cmd("HSET", statusKey, s, "CLOSED"); err != nil {
					errs.Add(1)
					return
				}
				if _, err := w.cmd("INCR", stockKey); err != nil {
					errs.Add(1)
					return
				}
			}
		}(cs[i])
	}
	wg.Wait()
	fillWall(res, start, pid, cpu0, client0, commands(c)-before)
	if err := fillStock(res, c, orders); err != nil {
		return err
	}
	res.Errors = errs.Load()
	res.Ops = int64(workers * orders)
	if res.ElapsedSec > 0 {
		res.QPS = float64(res.Ops) / res.ElapsedSec
	}
	res.Pass = res.Errors == 0 && res.Closed == int64(orders) && res.Stock > int64(orders)
	res.Detail = fmt.Sprintf("workers=%d each tried all %d orders; stock %d closed %d extra %d",
		workers, orders, res.Stock, res.Closed, res.Stock-res.Closed)
	return nil
}

func runLuaByID(res *result, c *Conn, addr string, pid, workers, orders int) error {
	if err := loadOrders(c, orders, func(int) string { return "0" }); err != nil {
		return err
	}
	sha, err := loadSHA(c, "lua/close_id.lua")
	if err != nil {
		return err
	}
	cs, err := dialAll(addr, workers)
	if err != nil {
		return err
	}
	defer closeAll(cs)
	before := commands(c)
	cpu0, client0 := cpuTicks(pid), cpuTicks(os.Getpid())
	start := time.Now()
	var wg sync.WaitGroup
	var errs atomic.Int64
	var closed atomic.Int64
	hists := make([]*hist, workers)
	for i := 0; i < workers; i++ {
		hists[i] = newHist()
		wg.Add(1)
		go func(w *Conn, h *hist) {
			defer wg.Done()
			for id := 1; id <= orders; id++ {
				t0 := time.Now()
				rep, err := w.evalSHA(sha, 3, dueKey, statusKey, stockKey, strconv.Itoa(id))
				h.add(time.Since(t0).Nanoseconds())
				if err != nil || rep.Kind != ':' {
					errs.Add(1)
					return
				}
				if rep.Int == 1 {
					closed.Add(1)
				}
			}
		}(cs[i], hists[i])
	}
	wg.Wait()
	fillWall(res, start, pid, cpu0, client0, commands(c)-before)
	fillHist(res, hists)
	if err := fillStock(res, c, orders); err != nil {
		return err
	}
	res.Errors = errs.Load()
	got := closed.Load()
	res.Ops = int64(workers * orders)
	if res.ElapsedSec > 0 {
		res.QPS = float64(res.Ops) / res.ElapsedSec
	}
	res.Pass = res.Errors == 0 && res.Stock == int64(orders) && res.Closed == int64(orders) && got == res.Closed && res.ZCard == 0
	res.Detail = fmt.Sprintf("same %d workers as unsafe; stock %d returns %d", workers, res.Stock, got)
	return nil
}

func runPop(res *result, c *Conn, addr string, pid, workers, orders int) error {
	if err := loadOrders(c, orders, func(int) string { return "0" }); err != nil {
		return err
	}
	return popAll(res, c, addr, pid, workers, orders, "9000000000000")
}

func runDrain(res *result, c *Conn, addr string, pid, workers, orders int) error {
	if err := loadOrders(c, orders, func(int) string { return "0" }); err != nil {
		return err
	}
	return popAll(res, c, addr, pid, workers, orders, "9000000000000")
}

func popAll(res *result, c *Conn, addr string, pid, workers, orders int, now string) error {
	sha, err := loadSHA(c, "lua/close_due.lua")
	if err != nil {
		return err
	}
	cs, err := dialAll(addr, workers)
	if err != nil {
		return err
	}
	defer closeAll(cs)
	before := commands(c)
	cpu0, client0 := cpuTicks(pid), cpuTicks(os.Getpid())
	var wg sync.WaitGroup
	var errs atomic.Int64
	var closed atomic.Int64
	hists := make([]*hist, workers)
	start := time.Now()
	for i := 0; i < workers; i++ {
		hists[i] = newHist()
		wg.Add(1)
		go func(w *Conn, h *hist) {
			defer wg.Done()
			for {
				t0 := time.Now()
				rep, err := w.evalSHA(sha, 3, dueKey, statusKey, stockKey, now)
				if err != nil || rep.Kind != ':' {
					errs.Add(1)
					return
				}
				h.add(time.Since(t0).Nanoseconds())
				if rep.Int == 0 {
					return
				}
				if rep.Int > 0 {
					closed.Add(1)
				}
			}
		}(cs[i], hists[i])
	}
	wg.Wait()
	fillWall(res, start, pid, cpu0, client0, commands(c)-before)
	fillHist(res, hists)
	if err := fillStock(res, c, orders); err != nil {
		return err
	}
	res.Errors = errs.Load()
	res.Closed = closed.Load()
	res.Ops = closed.Load()
	if res.ElapsedSec > 0 {
		res.QPS = float64(res.Ops) / res.ElapsedSec
	}
	res.Pass = res.Errors == 0 && res.Closed == int64(orders) && res.Stock == int64(orders) && res.ZCard == 0 && res.Unpaid == 0
	res.Detail = fmt.Sprintf("pop workers=%d closed=%d stock=%d", workers, res.Closed, res.Stock)
	return nil
}

func runPayRace(res *result, c *Conn, addr string, pid, workers, orders int) error {
	if err := loadOrders(c, orders, func(int) string { return "0" }); err != nil {
		return err
	}
	paySHA, err := loadSHA(c, "lua/pay.lua")
	if err != nil {
		return err
	}
	closeSHA, err := loadSHA(c, "lua/close_id.lua")
	if err != nil {
		return err
	}
	cs, err := dialAll(addr, workers*2)
	if err != nil {
		return err
	}
	defer closeAll(cs)
	before := commands(c)
	cpu0, client0 := cpuTicks(pid), cpuTicks(os.Getpid())
	var wg sync.WaitGroup
	var errs atomic.Int64
	var paid, closed atomic.Int64
	start := time.Now()
	for i := 0; i < workers; i++ {
		wg.Add(2)
		go func(w *Conn, mine int) {
			defer wg.Done()
			for id := mine; id <= orders; id += workers {
				rep, err := w.evalSHA(paySHA, 2, dueKey, statusKey, strconv.Itoa(id))
				if err != nil || rep.Kind != ':' {
					errs.Add(1)
					return
				}
				if rep.Int == 1 {
					paid.Add(1)
				}
			}
		}(cs[i], i+1)
		go func(w *Conn, mine int) {
			defer wg.Done()
			for id := mine; id <= orders; id += workers {
				rep, err := w.evalSHA(closeSHA, 3, dueKey, statusKey, stockKey, strconv.Itoa(id))
				if err != nil || rep.Kind != ':' {
					errs.Add(1)
					return
				}
				if rep.Int == 1 {
					closed.Add(1)
				}
			}
		}(cs[workers+i], i+1)
	}
	wg.Wait()
	fillWall(res, start, pid, cpu0, client0, commands(c)-before)
	if err := fillStock(res, c, orders); err != nil {
		return err
	}
	res.Errors = errs.Load()
	gotPaid, gotClosed := paid.Load(), closed.Load()
	if res.ElapsedSec > 0 {
		res.QPS = float64(res.Paid+res.Closed) / res.ElapsedSec
	}
	res.Pass = res.Errors == 0 && gotPaid == res.Paid && gotClosed == res.Closed &&
		res.Paid+res.Closed == int64(orders) && res.Stock == res.Closed && res.Unpaid == 0 && res.ZCard == 0
	res.Detail = fmt.Sprintf("paid %d closed %d stock %d", res.Paid, res.Closed, res.Stock)
	return nil
}

func runReplay(res *result, c *Conn, addr string, pid int) error {
	if err := loadOrders(c, 1, func(int) string { return "0" }); err != nil {
		return err
	}
	sha, err := loadSHA(c, "lua/close_id.lua")
	if err != nil {
		return err
	}
	w, err := dial(addr)
	if err != nil {
		return err
	}
	defer w.Close()
	a, err := w.evalSHA(sha, 3, dueKey, statusKey, stockKey, "1")
	if err != nil {
		return err
	}
	b, err := w.evalSHA(sha, 3, dueKey, statusKey, stockKey, "1")
	if err != nil {
		return err
	}
	if err := fillStock(res, c, 1); err != nil {
		return err
	}
	res.Ops = 2
	res.Pass = a.Int == 1 && b.Int == 3 && res.Stock == 1 && res.Closed == 1
	res.Detail = fmt.Sprintf("first=%d second=%d stock=%d", a.Int, b.Int, res.Stock)
	_ = pid
	return nil
}

func runNotYet(res *result, c *Conn, addr string, pid, orders int) error {
	future := time.Now().Add(60 * time.Second).UnixMilli()
	score := strconv.FormatInt(future, 10)
	if err := loadOrders(c, orders, func(int) string { return score }); err != nil {
		return err
	}
	sha, err := loadSHA(c, "lua/close_due.lua")
	if err != nil {
		return err
	}
	w, err := dial(addr)
	if err != nil {
		return err
	}
	defer w.Close()
	now := strconv.FormatInt(time.Now().UnixMilli(), 10)
	rep, err := w.evalSHA(sha, 3, dueKey, statusKey, stockKey, now)
	if err != nil {
		return err
	}
	z, err := w.cmd("ZCARD", dueKey)
	if err != nil {
		return err
	}
	earlyStock, err := w.cmd("GET", stockKey)
	if err != nil {
		return err
	}
	later := strconv.FormatInt(future+1000, 10)
	var closed int64
	for {
		r, err := w.evalSHA(sha, 3, dueKey, statusKey, stockKey, later)
		if err != nil {
			return err
		}
		if r.Int == 0 {
			break
		}
		if r.Int > 0 {
			closed++
		}
	}
	if err := fillStock(res, c, orders); err != nil {
		return err
	}
	res.Closed = closed
	early := earlyStock.Str
	res.Pass = rep.Int == 0 && z.Int == int64(orders) && early == "0" && res.Closed == int64(orders) && res.Stock == int64(orders) && res.ZCard == 0
	res.Detail = fmt.Sprintf("before due pop=%d zcard=%d stock=%s; after %d", rep.Int, z.Int, early, res.Closed)
	_ = pid
	return nil
}

func runTTL(res *result, c *Conn, addr string, pid, orders int) error {
	if _, err := c.cmd("FLUSHDB"); err != nil {
		return err
	}
	if _, err := c.cmd("SET", stockKey, "0"); err != nil {
		return err
	}
	batch := make([][]string, 0, 64)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		err := c.pipeline(batch)
		batch = batch[:0]
		return err
	}
	for i := 1; i <= orders; i++ {
		batch = append(batch, []string{"SET", "hold:" + strconv.Itoa(i), "x", "PX", "400"})
		if len(batch) >= 64 {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}
	info0, err := c.info("stats")
	if err != nil {
		return err
	}
	base, _ := strconv.ParseInt(infoValue(info0, "expired_keys"), 10, 64)
	deadline := time.Now().Add(3 * time.Second)
	var db int64
	for time.Now().Before(deadline) {
		rep, err := c.cmd("DBSIZE")
		if err != nil {
			return err
		}
		db = rep.Int
		if db <= 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	info1, err := c.info("stats")
	if err != nil {
		return err
	}
	exp, _ := strconv.ParseInt(infoValue(info1, "expired_keys"), 10, 64)
	stock, err := c.cmd("GET", stockKey)
	if err != nil {
		return err
	}
	res.ExpiredKeys = exp - base
	res.DBSize = db
	res.Stock, _ = strconv.ParseInt(stock.Str, 10, 64)
	res.Orders = orders
	res.Pass = res.Stock == 0 && res.DBSize == 1 && res.ExpiredKeys >= int64(orders)
	res.Detail = fmt.Sprintf("px=400ms expired=%d dbsize=%d stock=%d; no callback ran", res.ExpiredKeys, res.DBSize, res.Stock)
	_ = pid
	_ = addr
	return nil
}

func runRecover(res *result, c *Conn, addr string, pid, orders int) error {
	if err := loadOrders(c, orders, func(int) string { return "0" }); err != nil {
		return err
	}
	claimSHA, err := loadSHA(c, "lua/claim.lua")
	if err != nil {
		return err
	}
	finishSHA, err := loadSHA(c, "lua/finish.lua")
	if err != nil {
		return err
	}
	w, err := dial(addr)
	if err != nil {
		return err
	}
	defer w.Close()
	now := "9000000000000"
	var skipped int64
	for {
		rep, err := w.evalSHA(claimSHA, 2, dueKey, procKey, now)
		if err != nil || rep.Kind != ':' {
			return fmt.Errorf("claim: %v", err)
		}
		if rep.Int == 0 {
			break
		}
		if rep.Int%10 == 0 {
			skipped++
			continue
		}
		fin, err := w.evalSHA(finishSHA, 3, statusKey, procKey, stockKey, strconv.FormatInt(rep.Int, 10))
		if err != nil || fin.Int != 1 {
			return fmt.Errorf("finish %d -> %v %d", rep.Int, err, fin.Int)
		}
	}
	midStock, err := c.getInt(stockKey)
	if err != nil {
		return err
	}
	midLen, err := c.cmd("LLEN", procKey)
	if err != nil {
		return err
	}
	for {
		rep, err := w.cmd("LINDEX", procKey, "0")
		if err != nil {
			return err
		}
		if rep.Nil {
			break
		}
		fin, err := w.evalSHA(finishSHA, 3, statusKey, procKey, stockKey, rep.Str)
		if err != nil || fin.Int != 1 {
			return fmt.Errorf("recover %s -> %v %d", rep.Str, err, fin.Int)
		}
	}
	again, err := w.evalSHA(finishSHA, 3, statusKey, procKey, stockKey, "1")
	if err != nil {
		return err
	}
	if err := fillStock(res, c, orders); err != nil {
		return err
	}
	res.Processing = 0
	res.Pass = skipped == midLen.Int && midStock == int64(orders)-skipped && again.Int == 2 && res.Stock == int64(orders) && res.Closed == int64(orders)
	llen, _ := c.cmd("LLEN", procKey)
	res.Processing = llen.Int
	res.Pass = res.Pass && res.Processing == 0
	res.Detail = fmt.Sprintf("skipped %d midStock %d midLen %d replay=%d finalStock %d", skipped, midStock, midLen.Int, again.Int, res.Stock)
	_ = pid
	return nil
}

func runRecycle(res *result, c *Conn, addr string, pid, workers, card int, duration, warmup time.Duration) error {
	if err := loadOrders(c, card, func(int) string { return "0" }); err != nil {
		return err
	}
	sha, err := loadSHA(c, "lua/recycle.lua")
	if err != nil {
		return err
	}
	cs, err := dialAll(addr, workers)
	if err != nil {
		return err
	}
	defer closeAll(cs)
	var phase atomic.Int32
	var errs atomic.Int64
	var misses atomic.Int64
	hists := make([]*hist, workers)
	var ops []atomic.Int64
	ops = make([]atomic.Int64, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		hists[i] = newHist()
		wg.Add(1)
		go func(w *Conn, h *hist, counter *atomic.Int64) {
			defer wg.Done()
			for phase.Load() < 2 {
				t0 := time.Now()
				rep, err := w.evalSHA(sha, 3, dueKey, statusKey, stockKey, "9000000000000")
				el := time.Since(t0).Nanoseconds()
				if err != nil || rep.Kind != ':' {
					errs.Add(1)
					return
				}
				if phase.Load() == 1 {
					h.add(el)
					counter.Add(1)
					if rep.Int == 0 {
						misses.Add(1)
					}
				}
			}
		}(cs[i], hists[i], &ops[i])
	}
	time.Sleep(warmup)
	before := commands(c)
	cpu0, client0 := cpuTicks(pid), cpuTicks(os.Getpid())
	phase.Store(1)
	start := time.Now()
	time.Sleep(duration)
	fillWall(res, start, pid, cpu0, client0, 0)
	phase.Store(2)
	wg.Wait()
	res.RedisCommands = commands(c) - before
	var n int64
	for i := range ops {
		n += ops[i].Load()
	}
	res.Ops = n
	res.Errors = errs.Load()
	res.Misses = misses.Load()
	if res.ElapsedSec > 0 {
		res.QPS = float64(n) / res.ElapsedSec
	}
	fillHist(res, hists)
	z, err := c.cmd("ZCARD", dueKey)
	if err != nil {
		return err
	}
	res.ZCard = z.Int
	res.Orders = card
	mem, _ := usedMemory(c)
	res.UsedMemory = mem
	res.RSSKB = rssKB(pid)
	res.Pass = res.Errors == 0 && res.Misses == 0 && res.ZCard == int64(card) && res.Ops > 0
	res.Detail = fmt.Sprintf("card=%d workers=%d ops=%d misses=%d", card, workers, res.Ops, res.Misses)
	return nil
}

func runZAdd(res *result, c *Conn, addr string, pid, workers, orders int) error {
	if _, err := c.cmd("FLUSHDB"); err != nil {
		return err
	}
	mem0, err := usedMemory(c)
	if err != nil {
		return err
	}
	cs, err := dialAll(addr, workers)
	if err != nil {
		return err
	}
	defer closeAll(cs)
	var seq atomic.Int64
	var errs atomic.Int64
	hists := make([]*hist, workers)
	var wg sync.WaitGroup
	startCh := make(chan struct{})
	for i := 0; i < workers; i++ {
		hists[i] = newHist()
		wg.Add(1)
		go func(w *Conn, h *hist) {
			defer wg.Done()
			<-startCh
			for {
				id := seq.Add(1)
				if id > int64(orders) {
					return
				}
				t0 := time.Now()
				if _, err := w.cmd("ZADD", dueKey, "0", strconv.FormatInt(id, 10)); err != nil {
					errs.Add(1)
					return
				}
				h.add(time.Since(t0).Nanoseconds())
			}
		}(cs[i], hists[i])
	}
	before := commands(c)
	cpu0, client0 := cpuTicks(pid), cpuTicks(os.Getpid())
	start := time.Now()
	close(startCh)
	wg.Wait()
	fillWall(res, start, pid, cpu0, client0, commands(c)-before)
	fillHist(res, hists)
	z, err := c.cmd("ZCARD", dueKey)
	if err != nil {
		return err
	}
	mem1, err := usedMemory(c)
	if err != nil {
		return err
	}
	res.Errors = errs.Load()
	res.ZCard = z.Int
	res.Ops = z.Int
	res.UsedMemory = mem1
	res.MemoryDelta = mem1 - mem0
	if z.Int > 0 {
		res.BytesPerMember = float64(res.MemoryDelta) / float64(z.Int)
	}
	if res.ElapsedSec > 0 {
		res.QPS = float64(res.Ops) / res.ElapsedSec
	}
	res.Pass = res.Errors == 0 && res.ZCard == int64(orders)
	res.Detail = fmt.Sprintf("zadd %d bytes/member %.1f", res.ZCard, res.BytesPerMember)
	return nil
}

func runMemory(res *result, c *Conn, pid, card int) error {
	if _, err := c.cmd("FLUSHDB"); err != nil {
		return err
	}
	base, err := usedMemory(c)
	if err != nil {
		return err
	}
	if err := zaddOnly(c, card); err != nil {
		return err
	}
	afterZ, err := usedMemory(c)
	if err != nil {
		return err
	}
	if err := hsetOnly(c, card); err != nil {
		return err
	}
	afterH, err := usedMemory(c)
	if err != nil {
		return err
	}
	z, err := c.cmd("ZCARD", dueKey)
	if err != nil {
		return err
	}
	h, err := c.cmd("HLEN", statusKey)
	if err != nil {
		return err
	}
	res.ZCard = z.Int
	res.HLen = h.Int
	res.Orders = card
	res.UsedMemory = afterH
	res.MemoryDelta = afterZ - base
	if z.Int > 0 {
		res.BytesPerMember = float64(afterZ-base) / float64(z.Int)
		res.HashBytes = float64(afterH-afterZ) / float64(h.Int)
	}
	res.RSSKB = rssKB(pid)
	res.Pass = z.Int == int64(card) && h.Int == int64(card) && res.BytesPerMember > 0
	res.Detail = fmt.Sprintf("base=%d zset=%d hash=%d bytes/zset=%.1f bytes/field=%.1f rssKB=%d",
		base, afterZ-base, afterH-afterZ, res.BytesPerMember, res.HashBytes, res.RSSKB)
	return nil
}

func runLate(res *result, c *Conn, addr string, pid, orders int, poll, step time.Duration, smart bool) error {
	if step <= 0 {
		return fmt.Errorf("step must be positive")
	}
	// 留出灌入时间。截止时间从灌完之前就算起，灌得太慢会把前几单的迟到算进调度。
	base := time.Now().Add(250 * time.Millisecond).UnixMilli()
	deadlines := make([]int64, orders+1)
	if err := loadOrders(c, orders, func(i int) string {
		ms := base + int64(i-1)*step.Milliseconds()
		deadlines[i] = ms
		return strconv.FormatInt(ms, 10)
	}); err != nil {
		return err
	}
	sha, err := loadSHA(c, "lua/close_due.lua")
	if err != nil {
		return err
	}
	w, err := dial(addr)
	if err != nil {
		return err
	}
	defer w.Close()
	h := newHist()
	var closed int64
	var early int64
	var errs int64
	guard := time.UnixMilli(deadlines[orders]).Add(5 * time.Second)
	cpu0, client0 := cpuTicks(pid), cpuTicks(os.Getpid())
	before := commands(c)
	start := time.Now()
	for closed < int64(orders) {
		if time.Now().After(guard) {
			break
		}
		if smart {
			rep, err := w.cmd("ZRANGE", dueKey, "0", "0", "WITHSCORES")
			if err != nil {
				return err
			}
			if len(rep.Elems) < 2 {
				break
			}
			score, err := strconv.ParseFloat(rep.Elems[1].Str, 64)
			if err != nil {
				return err
			}
			dueAt := time.UnixMilli(int64(score))
			if wait := time.Until(dueAt); wait > 0 {
				time.Sleep(wait)
			}
		}
		progress := false
		for {
			now := strconv.FormatInt(time.Now().UnixMilli(), 10)
			t0 := time.Now()
			rep, err := w.evalSHA(sha, 3, dueKey, statusKey, stockKey, now)
			if err != nil || rep.Kind != ':' {
				errs++
				break
			}
			if rep.Int == 0 {
				break
			}
			progress = true
			if rep.Int < 0 {
				errs++
				continue
			}
			lat := time.Since(time.UnixMilli(deadlines[rep.Int]))
			if lat < 0 {
				early++
			}
			h.add(lat.Nanoseconds())
			closed++
			_ = t0
		}
		if errs > 0 {
			break
		}
		if !smart {
			time.Sleep(poll)
		} else if !progress {
			time.Sleep(time.Millisecond)
		}
	}
	fillWall(res, start, pid, cpu0, client0, commands(c)-before)
	fillHist(res, []*hist{h})
	if err := fillStock(res, c, orders); err != nil {
		return err
	}
	res.Closed = closed
	res.Early = early
	res.Errors = errs
	res.Ops = closed
	res.Workers = 1
	res.Pass = errs == 0 && early == 0 && closed == int64(orders) && res.Stock == int64(orders) && res.ZCard == 0
	kind := "poll " + poll.String()
	if smart {
		kind = "smart"
	}
	res.Detail = fmt.Sprintf("%s step=%s closed=%d p50us=%d p99us=%d maxus=%d", kind, step, closed, res.P50us, res.P99us, res.Maxus)
	return nil
}

func loadOrders(c *Conn, n int, score func(i int) string) error {
	if _, err := c.cmd("FLUSHDB"); err != nil {
		return err
	}
	if _, err := c.cmd("SET", stockKey, "0"); err != nil {
		return err
	}
	batch := make([][]string, 0, 64)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		err := c.pipeline(batch)
		batch = batch[:0]
		return err
	}
	for i := 1; i <= n; i++ {
		id := strconv.Itoa(i)
		batch = append(batch, []string{"ZADD", dueKey, score(i), id})
		batch = append(batch, []string{"HSET", statusKey, id, "UNPAID"})
		if len(batch) >= 64 {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

func zaddOnly(c *Conn, n int) error {
	batch := make([][]string, 0, 64)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		err := c.pipeline(batch)
		batch = batch[:0]
		return err
	}
	for i := 1; i <= n; i++ {
		batch = append(batch, []string{"ZADD", dueKey, "0", strconv.Itoa(i)})
		if len(batch) >= 64 {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

func hsetOnly(c *Conn, n int) error {
	batch := make([][]string, 0, 64)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		err := c.pipeline(batch)
		batch = batch[:0]
		return err
	}
	for i := 1; i <= n; i++ {
		batch = append(batch, []string{"HSET", statusKey, strconv.Itoa(i), "UNPAID"})
		if len(batch) >= 64 {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

func loadSHA(c *Conn, path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return c.scriptLoad(string(b))
}

func dialAll(addr string, n int) ([]*Conn, error) {
	cs := make([]*Conn, n)
	for i := 0; i < n; i++ {
		c, err := dial(addr)
		if err != nil {
			closeAll(cs[:i])
			return nil, err
		}
		cs[i] = c
	}
	return cs, nil
}

func closeAll(cs []*Conn) {
	for _, c := range cs {
		if c != nil {
			c.Close()
		}
	}
}

func fillStock(res *result, c *Conn, orders int) error {
	unpaid, paid, closed, missing, err := countStatus(c, orders)
	if err != nil {
		return err
	}
	stock, err := c.getInt(stockKey)
	if err != nil {
		return err
	}
	z, err := c.cmd("ZCARD", dueKey)
	if err != nil {
		return err
	}
	h, err := c.cmd("HLEN", statusKey)
	if err != nil {
		return err
	}
	res.Unpaid = unpaid
	res.Paid = paid
	res.Closed = closed
	res.Stock = stock
	res.ZCard = z.Int
	res.HLen = h.Int
	if missing != 0 {
		res.Errors += missing
	}
	return nil
}

func (c *Conn) getInt(key string) (int64, error) {
	rep, err := c.cmd("GET", key)
	if err != nil {
		return 0, err
	}
	if rep.Nil || rep.Str == "" {
		return 0, nil
	}
	return strconv.ParseInt(rep.Str, 10, 64)
}

func countStatus(c *Conn, n int) (unpaid, paid, closed, missing int64, err error) {
	// 正确性场景的订单数是几千。逐个 HGET，不拿来当吞吐。
	for i := 1; i <= n; i++ {
		rep, e := c.cmd("HGET", statusKey, strconv.Itoa(i))
		if e != nil {
			return 0, 0, 0, 0, e
		}
		switch {
		case rep.Nil:
			missing++
		case rep.Str == "UNPAID":
			unpaid++
		case rep.Str == "PAID":
			paid++
		case rep.Str == "CLOSED":
			closed++
		default:
			missing++
		}
	}
	return unpaid, paid, closed, missing, nil
}

func fillWall(res *result, start time.Time, pid int, cpu0, client0 float64, cmds int64) {
	elapsed := time.Since(start)
	res.ElapsedSec = elapsed.Seconds()
	res.RedisCommands = cmds
	clk := 100.0
	res.RedisCPUSec = (cpuTicks(pid) - cpu0) / clk
	res.ClientCPUpct = ((cpuTicks(os.Getpid()) - client0) / clk) / elapsed.Seconds() * 100
	if elapsed > 0 {
		res.RedisCPUpct = res.RedisCPUSec / elapsed.Seconds() * 100
	}
	res.RSSKB = rssKB(pid)
}

func fillHist(res *result, hs []*hist) {
	m := newHist()
	for _, h := range hs {
		m.merge(h)
	}
	res.P50us = m.le(0.50) / 1000
	res.P99us = m.le(0.99) / 1000
	res.Maxus = m.max / 1000
}

func redisPID(c *Conn) (int, error) {
	info, err := c.info("server")
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(infoValue(info, "process_id"))
}

func commands(c *Conn) int64 {
	info, err := c.info("stats")
	if err != nil {
		return 0
	}
	n, _ := strconv.ParseInt(infoValue(info, "total_commands_processed"), 10, 64)
	return n
}

func usedMemory(c *Conn) (int64, error) {
	info, err := c.info("memory")
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(infoValue(info, "used_memory"), 10, 64)
}

func rssKB(pid int) int64 {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				v, _ := strconv.ParseInt(fields[1], 10, 64)
				return v
			}
		}
	}
	return 0
}

func cpuTicks(pid int) float64 {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0
	}
	s := string(b)
	i := strings.LastIndex(s, ")")
	if i < 0 || i+2 >= len(s) {
		return 0
	}
	fields := strings.Fields(s[i+2:])
	if len(fields) < 13 {
		return 0
	}
	ut, _ := strconv.ParseFloat(fields[11], 64)
	st, _ := strconv.ParseFloat(fields[12], 64)
	return ut + st
}

func median(xs []int64) int64 {
	if len(xs) == 0 {
		return 0
	}
	cp := append([]int64(nil), xs...)
	for i := 1; i < len(cp); i++ {
		j := i
		for j > 0 && cp[j] < cp[j-1] {
			cp[j], cp[j-1] = cp[j-1], cp[j]
			j--
		}
	}
	return cp[len(cp)/2]
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "error: %v\n", err)
	os.Exit(1)
}
