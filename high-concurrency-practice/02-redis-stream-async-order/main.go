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
	keyStock  = "ms:stock"
	keyWin    = "ms:win"
	keyStream = "ms:order"
	keyDLQ    = "ms:dlq"
	group     = "g"
	consumer  = "c1"
	goodsID   = "1"
	dbPath    = "/tmp/miaosha-stream-orders.db"

	codeSoldOut = 0
	codeOK      = 1
	codeDup     = 2
)

type msg struct {
	ID    string
	User  string
	Goods string
}

type result struct {
	Scenario        string  `json:"scenario"`
	Note            string  `json:"note"`
	Pass            bool    `json:"pass"`
	Detail          string  `json:"detail,omitempty"`
	Workers         int     `json:"workers,omitempty"`
	ElapsedSec      float64 `json:"elapsedSec,omitempty"`
	QPS             float64 `json:"qps,omitempty"`
	Ops             int64   `json:"ops,omitempty"`
	Success         int64   `json:"success,omitempty"`
	SoldOut         int64   `json:"soldOut,omitempty"`
	Duplicate       int64   `json:"duplicate,omitempty"`
	Errors          int64   `json:"errors,omitempty"`
	InitialStock    int64   `json:"initialStock,omitempty"`
	FinalStock      int64   `json:"finalStock,omitempty"`
	Winners         int64   `json:"winners,omitempty"`
	StreamLen       int64   `json:"streamLen,omitempty"`
	Pending         int64   `json:"pending,omitempty"`
	Orders          int64   `json:"orders,omitempty"`
	Deliveries      int64   `json:"deliveries,omitempty"`
	Redeliveries    int64   `json:"redeliveries,omitempty"`
	FailedAttempts  int64   `json:"failedAttempts,omitempty"`
	DlqLen          int64   `json:"dlqLen,omitempty"`
	Deleted         int64   `json:"deleted,omitempty"`
	RedisCPUSec     float64 `json:"redisCpuSec,omitempty"`
	RedisCPUPercent float64 `json:"redisCpuPercent,omitempty"`
	RedisRSSKB      int64   `json:"redisRssKB,omitempty"`
	UsedMemory      int64   `json:"usedMemory,omitempty"`
	MemBase         int64   `json:"memBase,omitempty"`
	BytesPerEntry   float64 `json:"bytesPerEntry,omitempty"`
	Entries         int64   `json:"entries,omitempty"`
	StoppedOn       string  `json:"stoppedOn,omitempty"`
	P50us           int64   `json:"p50us,omitempty"`
	P99us           int64   `json:"p99us,omitempty"`
	P999us          int64   `json:"p999us,omitempty"`
	Maxus           int64   `json:"maxus,omitempty"`
	SQLiteSync      string  `json:"sqliteSync,omitempty"`
	Batch           int     `json:"batch,omitempty"`
	UserFacingSec   float64 `json:"userFacingSec,omitempty"`
	OrderVisibleSec float64 `json:"orderVisibleSec,omitempty"`
	ProducerQPS     float64 `json:"producerQps,omitempty"`
	DrainQPS        float64 `json:"drainQps,omitempty"`
	MaxStreamLen    int64   `json:"maxStreamLen,omitempty"`
	Maxlen          int     `json:"maxlen,omitempty"`
}

func main() {
	scenario := flag.String("scenario", "", "atomic-enqueue|split-loss|retry|poison|maxlen-loss|enqueue-tps|xadd-tps|consume|sync-path|async-path|backlog")
	out := flag.String("out", "", "json path")
	addr := flag.String("addr", "127.0.0.1:6379", "redis")
	workers := flag.Int("workers", 16, "producer workers")
	consumers := flag.Int("consumers", 1, "sqlite consumers")
	duration := flag.Duration("duration", 3*time.Second, "measure window")
	warmup := flag.Duration("warmup", 500*time.Millisecond, "warmup")
	stock := flag.Int64("stock", 100, "initial stock")
	users := flag.Int("users", 400, "distinct users")
	repeat := flag.Int("repeat", 10, "attempts per user")
	syncMode := flag.String("sync", "FULL", "sqlite synchronous")
	batch := flag.Int("batch", 1, "inserts per commit")
	messages := flag.Int("messages", 2000, "prefill or backlog cap")
	maxlen := flag.Int("maxlen", 0, "stream maxlen, 0 keeps every entry")
	fat := flag.Int("fat", 0, "extra payload bytes")
	flag.Parse()
	if *scenario == "" || *out == "" {
		fmt.Fprintln(os.Stderr, "need -scenario and -out")
		os.Exit(2)
	}

	body, err := os.ReadFile("lua/enqueue.lua")
	if err != nil {
		fatal(err)
	}
	var res result
	switch *scenario {
	case "atomic-enqueue":
		res, err = runAtomic(*addr, string(body), *workers, *stock, *users, *repeat, *syncMode)
	case "split-loss":
		res, err = runSplit(*addr, *workers, *stock, *users, *syncMode)
	case "retry":
		res, err = runRetry(*addr, *syncMode, false)
	case "poison":
		res, err = runRetry(*addr, *syncMode, true)
	case "maxlen-loss":
		res, err = runMaxlenLoss(*addr)
	case "enqueue-tps":
		res, err = runEnqueueTPS(*addr, string(body), *workers, *duration, *warmup, *stock, *maxlen)
	case "xadd-tps":
		res, err = runXaddTPS(*addr, *workers, *duration, *warmup, *maxlen, *fat)
	case "consume":
		res, err = runConsume(*addr, *messages, *consumers, *syncMode, *batch)
	case "sync-path":
		res, err = runSyncPath(*addr, string(body), *workers, *stock, *syncMode)
	case "async-path":
		res, err = runAsyncPath(*addr, string(body), *workers, *stock, *syncMode, *batch)
	case "backlog":
		res, err = runBacklog(*addr, *messages, *fat)
	default:
		fatal(fmt.Errorf("unknown scenario %s", *scenario))
	}
	if err != nil {
		fatal(err)
	}
	if err := writeOut(*out, res); err != nil {
		fatal(err)
	}
	fmt.Fprintf(os.Stderr, "%s pass=%v qps=%.0f orders=%d entries=%d detail=%s\n",
		res.Scenario, res.Pass, firstNonZero(res.QPS, res.ProducerQPS, res.DrainQPS), res.Orders, res.Entries, res.Detail)
	if !res.Pass {
		os.Exit(1)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func firstNonZero(vs ...float64) float64 {
	for _, v := range vs {
		if v != 0 {
			return v
		}
	}
	return 0
}

func writeOut(path string, res result) error {
	b, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

func flush(c *Conn) error {
	if _, err := c.cmd("FLUSHDB"); err != nil {
		return err
	}
	_, err := c.cmd("SCRIPT", "FLUSH")
	return err
}

func ensureGroup(c *Conn, stream, id string) error {
	_, err := c.cmd("XGROUP", "CREATE", stream, group, id, "MKSTREAM")
	if err != nil && strings.Contains(err.Error(), "BUSYGROUP") {
		return nil
	}
	return err
}

func xlen(c *Conn, key string) (int64, error) {
	rep, err := c.cmd("XLEN", key)
	if err != nil {
		return 0, err
	}
	return rep.Int, nil
}

func pending(c *Conn, stream string) (int64, error) {
	rep, err := c.cmd("XPENDING", stream, group)
	if err != nil {
		return 0, err
	}
	if len(rep.Elems) == 0 {
		return 0, fmt.Errorf("empty xpending")
	}
	return rep.Elems[0].Int, nil
}

func scard(c *Conn, key string) (int64, error) {
	rep, err := c.cmd("SCARD", key)
	if err != nil {
		return 0, err
	}
	return rep.Int, nil
}

func usedMemory(c *Conn) (int64, error) {
	info, err := c.info("memory")
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(infoValue(info, "used_memory"), 10, 64)
}

func redisPID(c *Conn) (int, error) {
	info, err := c.info("server")
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(infoValue(info, "process_id"))
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

func fillHist(res *result, h *hist) {
	if h == nil || h.n == 0 {
		return
	}
	res.P50us = h.le(0.50) / 1000
	res.P99us = h.le(0.99) / 1000
	res.P999us = h.le(0.999) / 1000
	res.Maxus = h.max / 1000
}

func parseEntries(entries Reply) ([]msg, error) {
	out := make([]msg, 0, len(entries.Elems))
	for _, ent := range entries.Elems {
		if len(ent.Elems) < 2 {
			return nil, fmt.Errorf("bad stream entry")
		}
		m := msg{ID: ent.Elems[0].Str}
		fields := ent.Elems[1].Elems
		for i := 0; i+1 < len(fields); i += 2 {
			switch fields[i].Str {
			case "u":
				m.User = fields[i+1].Str
			case "g":
				m.Goods = fields[i+1].Str
			}
		}
		if m.ID == "" || m.User == "" {
			return nil, fmt.Errorf("entry missing id or user")
		}
		out = append(out, m)
	}
	return out, nil
}

func parseRead(rep Reply) ([]msg, error) {
	if rep.Nil || len(rep.Elems) == 0 {
		return nil, nil
	}
	stream := rep.Elems[0]
	if len(stream.Elems) < 2 {
		return nil, fmt.Errorf("bad xread")
	}
	return parseEntries(stream.Elems[1])
}

func parseClaim(rep Reply) ([]msg, int, error) {
	if rep.Nil || len(rep.Elems) < 2 {
		return nil, 0, fmt.Errorf("bad xautoclaim")
	}
	msgs, err := parseEntries(rep.Elems[1])
	if err != nil {
		return nil, 0, err
	}
	deleted := 0
	if len(rep.Elems) >= 3 {
		deleted = len(rep.Elems[2].Elems)
	}
	return msgs, deleted, nil
}

func readNew(c *Conn, count int) ([]msg, error) {
	rep, err := c.cmd("XREADGROUP", "GROUP", group, consumer, "COUNT", strconv.Itoa(count), "STREAMS", keyStream, ">")
	if err != nil {
		return nil, err
	}
	return parseRead(rep)
}

func claimIdle(c *Conn, count int) ([]msg, int, error) {
	rep, err := c.cmd("XAUTOCLAIM", keyStream, group, consumer, "0", "0-0", "COUNT", strconv.Itoa(count))
	if err != nil {
		return nil, 0, err
	}
	return parseClaim(rep)
}

func ack(c *Conn, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	parts := make([]string, 0, 3+len(ids))
	parts = append(parts, "XACK", keyStream, group)
	parts = append(parts, ids...)
	_, err := c.cmd(parts...)
	return err
}

func applyUs(h *hist, start time.Time) {
	h.add(time.Since(start).Nanoseconds())
}

func runAtomic(addr, script string, workers int, stock int64, users, repeat int, syncMode string) (result, error) {
	c0, err := dial(addr)
	if err != nil {
		return result{}, err
	}
	defer c0.Close()
	if err := flush(c0); err != nil {
		return result{}, err
	}
	sha, err := c0.scriptLoad(script)
	if err != nil {
		return result{}, err
	}
	if _, err := c0.cmd("SET", keyStock, strconv.FormatInt(stock, 10)); err != nil {
		return result{}, err
	}
	conns := []*Conn{c0}
	for i := 1; i < workers; i++ {
		c, err := dial(addr)
		if err != nil {
			return result{}, err
		}
		defer c.Close()
		conns = append(conns, c)
	}
	total := users * repeat
	var next int64
	var mu sync.Mutex
	res := result{
		Scenario:     "atomic-enqueue",
		Note:         "Lua 里同时扣库存和 XADD。消费者先写库再 ACK；不 ACK 就重投，主键冲突挡住第二张订单。",
		Workers:      workers,
		InitialStock: stock,
		SQLiteSync:   syncMode,
	}
	var wg sync.WaitGroup
	start := time.Now()
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			defer wg.Done()
			local := result{}
			c := conns[w]
			for {
				i := atomic.AddInt64(&next, 1) - 1
				if i >= int64(total) {
					break
				}
				user := strconv.Itoa(int(i) % users)
				code, opErr := c.evalSHA(sha, 3, keyStock, keyWin, keyStream, user, goodsID, "0")
				local.Ops++
				if opErr != nil {
					local.Errors++
					continue
				}
				switch code {
				case codeOK:
					local.Success++
				case codeSoldOut:
					local.SoldOut++
				case codeDup:
					local.Duplicate++
				default:
					local.Errors++
				}
			}
			mu.Lock()
			res.Ops += local.Ops
			res.Success += local.Success
			res.SoldOut += local.SoldOut
			res.Duplicate += local.Duplicate
			res.Errors += local.Errors
			mu.Unlock()
		}(w)
	}
	wg.Wait()
	res.ElapsedSec = time.Since(start).Seconds()
	res.QPS = float64(res.Ops) / res.ElapsedSec
	res.FinalStock, err = c0.getInt(keyStock)
	if err != nil {
		return result{}, err
	}
	res.Winners, err = scard(c0, keyWin)
	if err != nil {
		return result{}, err
	}
	res.StreamLen, err = xlen(c0, keyStream)
	if err != nil {
		return result{}, err
	}
	if err := ensureGroup(c0, keyStream, "0"); err != nil {
		return result{}, err
	}
	db, err := openDB(dbPath, syncMode, true)
	if err != nil {
		return result{}, err
	}
	defer db.Close()
	for {
		msgs, err := readNew(c0, 200)
		if err != nil {
			return result{}, err
		}
		if len(msgs) == 0 {
			break
		}
		if err := db.begin(); err != nil {
			return result{}, err
		}
		for _, m := range msgs {
			res.Deliveries++
			uid, err := strconv.ParseInt(m.User, 10, 64)
			if err != nil {
				return result{}, err
			}
			ok, err := db.insert(uid, 1)
			if err != nil {
				return result{}, err
			}
			if ok {
				res.Orders++
			} else {
				res.Duplicate++
			}
		}
		if err := db.commit(); err != nil {
			return result{}, err
		}
	}
	res.Pending, err = pending(c0, keyStream)
	if err != nil {
		return result{}, err
	}
	for {
		msgs, _, err := claimIdle(c0, 200)
		if err != nil {
			return result{}, err
		}
		if len(msgs) == 0 {
			break
		}
		ids := make([]string, 0, len(msgs))
		if err := db.begin(); err != nil {
			return result{}, err
		}
		for _, m := range msgs {
			res.Redeliveries++
			uid, err := strconv.ParseInt(m.User, 10, 64)
			if err != nil {
				return result{}, err
			}
			ok, err := db.insert(uid, 1)
			if err != nil {
				return result{}, err
			}
			if ok {
				res.Orders++
			}
			ids = append(ids, m.ID)
		}
		if err := db.commit(); err != nil {
			return result{}, err
		}
		if err := ack(c0, ids); err != nil {
			return result{}, err
		}
	}
	orders, err := db.count()
	if err != nil {
		return result{}, err
	}
	res.Orders = orders
	res.Pending, err = pending(c0, keyStream)
	if err != nil {
		return result{}, err
	}
	wantDup := stock * int64(repeat-1)
	wantSold := int64(users)*int64(repeat) - stock - wantDup
	res.Pass = res.Errors == 0 && res.Success == stock && res.Duplicate == wantDup && res.SoldOut == wantSold &&
		res.FinalStock == 0 && res.Winners == stock && res.StreamLen == stock &&
		orders == stock && res.Redeliveries == stock && res.Pending == 0
	res.Detail = fmt.Sprintf("wantSuccess=%d wantDup=%d wantSoldOut=%d orders=%d redeliveries=%d pending=%d",
		stock, wantDup, wantSold, orders, res.Redeliveries, res.Pending)
	return res, nil
}

func runSplit(addr string, workers int, stock int64, users int, syncMode string) (result, error) {
	c0, err := dial(addr)
	if err != nil {
		return result{}, err
	}
	defer c0.Close()
	if err := flush(c0); err != nil {
		return result{}, err
	}
	if _, err := c0.cmd("SET", keyStock, strconv.FormatInt(stock, 10)); err != nil {
		return result{}, err
	}
	conns := []*Conn{c0}
	for i := 1; i < workers; i++ {
		c, err := dial(addr)
		if err != nil {
			return result{}, err
		}
		defer c.Close()
		conns = append(conns, c)
	}
	var next int64
	var mu sync.Mutex
	res := result{
		Scenario:     "split-loss",
		Note:         "先 DECR 再 XADD。用户 id 能被 5 整除的胜出者在两次调用之间被丢掉，用来模拟进程在发消息前崩溃。现有控制器就是先 decr 再 sendMiaoshaMessage。",
		Workers:      workers,
		InitialStock: stock,
		SQLiteSync:   syncMode,
	}
	var lost atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			defer wg.Done()
			local := result{}
			c := conns[w]
			for {
				i := atomic.AddInt64(&next, 1) - 1
				if i >= int64(users) {
					break
				}
				user := strconv.FormatInt(i, 10)
				rep, opErr := c.cmd("DECR", keyStock)
				local.Ops++
				if opErr != nil {
					local.Errors++
					continue
				}
				if rep.Int < 0 {
					if _, opErr = c.cmd("INCR", keyStock); opErr != nil {
						local.Errors++
						continue
					}
					local.SoldOut++
					continue
				}
				local.Success++
				if i%5 == 0 {
					lost.Add(1)
					continue
				}
				if _, opErr = c.cmd("XADD", keyStream, "*", "u", user, "g", goodsID); opErr != nil {
					local.Errors++
					continue
				}
			}
			mu.Lock()
			res.Ops += local.Ops
			res.Success += local.Success
			res.SoldOut += local.SoldOut
			res.Errors += local.Errors
			mu.Unlock()
		}(w)
	}
	wg.Wait()
	res.ElapsedSec = time.Since(start).Seconds()
	res.FinalStock, err = c0.getInt(keyStock)
	if err != nil {
		return result{}, err
	}
	res.StreamLen, err = xlen(c0, keyStream)
	if err != nil {
		return result{}, err
	}
	if err := ensureGroup(c0, keyStream, "0"); err != nil {
		return result{}, err
	}
	db, err := openDB(dbPath, syncMode, true)
	if err != nil {
		return result{}, err
	}
	defer db.Close()
	ids := make([]string, 0, 128)
	for {
		msgs, err := readNew(c0, 200)
		if err != nil {
			return result{}, err
		}
		if len(msgs) == 0 {
			break
		}
		if err := db.begin(); err != nil {
			return result{}, err
		}
		ids = ids[:0]
		for _, m := range msgs {
			uid, err := strconv.ParseInt(m.User, 10, 64)
			if err != nil {
				return result{}, err
			}
			if _, err := db.insert(uid, 1); err != nil {
				return result{}, err
			}
			ids = append(ids, m.ID)
		}
		if err := db.commit(); err != nil {
			return result{}, err
		}
		if err := ack(c0, ids); err != nil {
			return result{}, err
		}
	}
	res.Orders, err = db.count()
	if err != nil {
		return result{}, err
	}
	res.Pending, err = pending(c0, keyStream)
	if err != nil {
		return result{}, err
	}
	lostN := lost.Load()
	res.Pass = res.Errors == 0 && res.Success == stock && res.FinalStock == 0 && lostN > 0 &&
		res.StreamLen == res.Success-lostN && res.Orders == res.StreamLen && res.Pending == 0
	res.Detail = fmt.Sprintf("decrOk=%d lostBeforeXadd=%d stream=%d orders=%d finalStock=%d",
		res.Success, lostN, res.StreamLen, res.Orders, res.FinalStock)
	return res, nil
}

func prefill(c *Conn, n int) error {
	for i := 0; i < n; i++ {
		if _, err := c.cmd("XADD", keyStream, "*", "u", strconv.Itoa(i+1), "g", goodsID); err != nil {
			return err
		}
	}
	return nil
}

func runRetry(addr, syncMode string, poison bool) (result, error) {
	const n = 20
	c, err := dial(addr)
	if err != nil {
		return result{}, err
	}
	defer c.Close()
	if err := flush(c); err != nil {
		return result{}, err
	}
	if err := prefill(c, n); err != nil {
		return result{}, err
	}
	if err := ensureGroup(c, keyStream, "0"); err != nil {
		return result{}, err
	}
	db, err := openDB(dbPath, syncMode, true)
	if err != nil {
		return result{}, err
	}
	defer db.Close()
	name := "retry"
	note := "前两次投递在写库前失败并且不 ACK。消息留在 PEL，第三次 XAUTOCLAIM 后写库并 ACK。"
	if poison {
		name = "poison"
		note = "一直失败的消息在第 3 次之后写入死信流并 ACK，避免占着 PEL。"
	}
	res := result{Scenario: name, Note: note, SQLiteSync: syncMode, InitialStock: n}
	attempts := map[string]int{}
	for spin := 0; spin < 20; spin++ {
		msgs, err := readNew(c, 100)
		if err != nil {
			return result{}, err
		}
		fromClaim := false
		deleted := 0
		if len(msgs) == 0 {
			msgs, deleted, err = claimIdle(c, 100)
			if err != nil {
				return result{}, err
			}
			fromClaim = true
			res.Deleted += int64(deleted)
		}
		if len(msgs) == 0 {
			break
		}
		_ = fromClaim
		var acks []string
		if err := db.begin(); err != nil {
			return result{}, err
		}
		wrote := false
		for _, m := range msgs {
			res.Deliveries++
			attempts[m.ID]++
			if attempts[m.ID] < 3 {
				res.FailedAttempts++
				continue
			}
			uid, err := strconv.ParseInt(m.User, 10, 64)
			if err != nil {
				return result{}, err
			}
			if poison {
				if _, err := c.cmd("XADD", keyDLQ, "*", "u", m.User, "g", goodsID, "id", m.ID); err != nil {
					return result{}, err
				}
				acks = append(acks, m.ID)
				continue
			}
			ok, err := db.insert(uid, 1)
			if err != nil {
				return result{}, err
			}
			wrote = true
			if !ok {
				res.Duplicate++
			}
			acks = append(acks, m.ID)
		}
		if wrote {
			if err := db.commit(); err != nil {
				return result{}, err
			}
		} else if err := db.exec("ROLLBACK"); err != nil {
			return result{}, err
		}
		if err := ack(c, acks); err != nil {
			return result{}, err
		}
	}
	res.Orders, err = db.count()
	if err != nil {
		return result{}, err
	}
	res.Pending, err = pending(c, keyStream)
	if err != nil {
		return result{}, err
	}
	res.DlqLen, err = xlen(c, keyDLQ)
	if err != nil {
		return result{}, err
	}
	res.StreamLen, err = xlen(c, keyStream)
	if err != nil {
		return result{}, err
	}
	maxAttempt := 0
	for _, n := range attempts {
		if n > maxAttempt {
			maxAttempt = n
		}
	}
	if poison {
		res.Pass = res.Orders == 0 && res.DlqLen == n && res.Pending == 0 && res.FailedAttempts == int64(n*2) && maxAttempt == 3
	} else {
		res.Pass = res.Orders == n && res.DlqLen == 0 && res.Pending == 0 && res.FailedAttempts == int64(n*2) && maxAttempt == 3
	}
	res.Detail = fmt.Sprintf("orders=%d dlq=%d pending=%d failedAttempts=%d maxAttempt=%d deliveries=%d",
		res.Orders, res.DlqLen, res.Pending, res.FailedAttempts, maxAttempt, res.Deliveries)
	return res, nil
}

func runMaxlenLoss(addr string) (result, error) {
	c, err := dial(addr)
	if err != nil {
		return result{}, err
	}
	defer c.Close()
	if err := flush(c); err != nil {
		return result{}, err
	}
	const n = 200
	if err := prefill(c, n); err != nil {
		return result{}, err
	}
	if err := ensureGroup(c, keyStream, "0"); err != nil {
		return result{}, err
	}
	if _, err := readNew(c, 500); err != nil {
		return result{}, err
	}
	pend, err := pending(c, keyStream)
	if err != nil {
		return result{}, err
	}
	for i := 0; i < 30; i++ {
		if _, err := c.cmd("XADD", keyStream, "MAXLEN", "10", "*", "u", strconv.Itoa(10000+i), "g", goodsID); err != nil {
			return result{}, err
		}
	}
	_, deleted, err := claimIdle(c, 500)
	if err != nil {
		return result{}, err
	}
	res := result{
		Scenario: "maxlen-loss",
		Note:     "精确 MAXLEN 会裁掉还没 ACK 的旧消息。XAUTOCLAIM 把它们放在删除列表里，订单就找不回来了。近似裁剪只适合压测限内存，不能当消费进度。",
		Pending:  pend,
		Deleted:  int64(deleted),
	}
	res.StreamLen, err = xlen(c, keyStream)
	if err != nil {
		return result{}, err
	}
	res.Pass = pend == n && res.Deleted >= 100 && res.StreamLen <= 40
	res.Detail = fmt.Sprintf("readPending=%d deletedOnClaim=%d xlenAfterTrim=%d", pend, res.Deleted, res.StreamLen)
	return res, nil
}
