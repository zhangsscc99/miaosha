package main

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

func readNewNamed(c *Conn, name string, count int) ([]msg, error) {
	rep, err := c.cmd("XREADGROUP", "GROUP", group, name, "COUNT", strconv.Itoa(count), "STREAMS", keyStream, ">")
	if err != nil {
		return nil, err
	}
	return parseRead(rep)
}

type redisOp func(*Conn, int64) (int64, error)

func runEnqueueTPS(addr, script string, workers int, duration, warmup time.Duration, stock int64, maxlen int) (result, error) {
	return driveRedis(addr, "enqueue-tps",
		"成功路径是一次 Lua：判断、扣库存、XADD。MAXLEN ~ 只为了让 64MB 装得下压测。生产上胜出人数等于库存，不靠裁剪丢消息。",
		workers, duration, warmup,
		func(c0 *Conn) (redisOp, error) {
			if err := flush(c0); err != nil {
				return nil, err
			}
			sha, err := c0.scriptLoad(script)
			if err != nil {
				return nil, err
			}
			if _, err := c0.cmd("SET", keyStock, strconv.FormatInt(stock, 10)); err != nil {
				return nil, err
			}
			capArg := strconv.Itoa(maxlen)
			return func(c *Conn, id int64) (int64, error) {
				user := strconv.FormatInt(id, 10)
				return c.evalSHA(sha, 3, keyStock, keyWin, keyStream, user, goodsID, capArg)
			}, nil
		},
		func(c0 *Conn, res *result) error {
			var err error
			res.InitialStock = stock
			res.Maxlen = maxlen
			res.FinalStock, err = c0.getInt(keyStock)
			if err != nil {
				return err
			}
			res.StreamLen, err = xlen(c0, keyStream)
			if err != nil {
				return err
			}
			res.Winners, err = scard(c0, keyWin)
			return err
		},
		func(res *result) {
			res.Pass = res.Errors == 0 && res.Success > 0 && res.SoldOut == 0 && res.QPS > 1000
			res.Detail = fmt.Sprintf("success=%d qps=%.0f xlen=%d winners=%d mem=%d p99us<=%d cpu=%.1f%%",
				res.Success, res.QPS, res.StreamLen, res.Winners, res.UsedMemory, res.P99us, res.RedisCPUPercent)
		},
	)
}

func runXaddTPS(addr string, workers int, duration, warmup time.Duration, maxlen, fat int) (result, error) {
	return driveRedis(addr, "xadd-tps",
		"只有 XADD，没有扣库存。用来看这条流本身的追加上限，和 Lua 成功路径分开。fat 是额外的正文字节，用来接近一条真正的订单消息。",
		workers, duration, warmup,
		func(c0 *Conn) (redisOp, error) {
			if err := flush(c0); err != nil {
				return nil, err
			}
			body := ""
			if fat > 0 {
				body = stringsRepeat(fat)
			}
			return func(c *Conn, id int64) (int64, error) {
				user := strconv.FormatInt(id, 10)
				var err error
				if fat > 0 {
					_, err = c.cmd("XADD", keyStream, "MAXLEN", "~", strconv.Itoa(maxlen), "*", "u", user, "g", goodsID, "b", body)
				} else {
					_, err = c.cmd("XADD", keyStream, "MAXLEN", "~", strconv.Itoa(maxlen), "*", "u", user, "g", goodsID)
				}
				if err != nil {
					return 0, err
				}
				return codeOK, nil
			}, nil
		},
		func(c0 *Conn, res *result) error {
			var err error
			res.Maxlen = maxlen
			res.StreamLen, err = xlen(c0, keyStream)
			return err
		},
		func(res *result) {
			res.Pass = res.Errors == 0 && res.Success > 0 && res.QPS > 1000
			res.Detail = fmt.Sprintf("qps=%.0f xlen=%d mem=%d p99us<=%d cpu=%.1f%%",
				res.QPS, res.StreamLen, res.UsedMemory, res.P99us, res.RedisCPUPercent)
		},
	)
}

func driveRedis(addr, name, note string, workers int, duration, warmup time.Duration,
	setup func(*Conn) (redisOp, error),
	finish func(*Conn, *result) error,
	judge func(*result),
) (result, error) {
	c0, err := dial(addr)
	if err != nil {
		return result{}, err
	}
	defer c0.Close()
	op, err := setup(c0)
	if err != nil {
		return result{}, err
	}
	pid, err := redisPID(c0)
	if err != nil {
		return result{}, err
	}
	conns := make([]*Conn, workers)
	conns[0] = c0
	for i := 1; i < workers; i++ {
		c, err := dial(addr)
		if err != nil {
			return result{}, err
		}
		defer c.Close()
		conns[i] = c
	}

	var phase atomic.Int32 // 0 warmup, 1 measure, 2 stop
	var seq int64
	locals := make([]result, workers)
	hs := make([]*hist, workers)
	for i := range hs {
		hs[i] = newHist()
	}
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(i int) {
			defer wg.Done()
			c := conns[i]
			for {
				p := phase.Load()
				if p == 2 {
					return
				}
				id := atomic.AddInt64(&seq, 1)
				t0 := time.Now()
				code, opErr := op(c, id)
				dt := time.Since(t0).Nanoseconds()
				if p != 1 {
					continue
				}
				locals[i].Ops++
				hs[i].add(dt)
				if opErr != nil {
					locals[i].Errors++
					continue
				}
				switch code {
				case codeOK:
					locals[i].Success++
				case codeSoldOut:
					locals[i].SoldOut++
				case codeDup:
					locals[i].Duplicate++
				default:
					locals[i].Errors++
				}
			}
		}(i)
	}

	time.Sleep(warmup)
	cpu0 := cpuTicks(pid)
	phase.Store(1)
	time.Sleep(duration)
	cpu1 := cpuTicks(pid)
	phase.Store(2)
	wg.Wait()

	res := result{Scenario: name, Note: note, Workers: workers, ElapsedSec: duration.Seconds()}
	merged := newHist()
	for i := 0; i < workers; i++ {
		res.Ops += locals[i].Ops
		res.Success += locals[i].Success
		res.SoldOut += locals[i].SoldOut
		res.Duplicate += locals[i].Duplicate
		res.Errors += locals[i].Errors
		merged.merge(hs[i])
	}
	res.QPS = float64(res.Ops) / res.ElapsedSec
	res.RedisCPUSec = (cpu1 - cpu0) / 100
	if res.ElapsedSec > 0 {
		res.RedisCPUPercent = res.RedisCPUSec / res.ElapsedSec * 100
	}
	res.RedisRSSKB = rssKB(pid)
	res.UsedMemory, err = usedMemory(c0)
	if err != nil {
		return result{}, err
	}
	fillHist(&res, merged)
	if err := finish(c0, &res); err != nil {
		return result{}, err
	}
	judge(&res)
	return res, nil
}

func runConsume(addr string, messages, consumers int, syncMode string, batch int) (result, error) {
	if batch < 1 {
		batch = 1
	}
	if consumers < 1 {
		consumers = 1
	}
	c0, err := dial(addr)
	if err != nil {
		return result{}, err
	}
	defer c0.Close()
	if err := flush(c0); err != nil {
		return result{}, err
	}
	if err := prefill(c0, messages); err != nil {
		return result{}, err
	}
	if err := ensureGroup(c0, keyStream, "0"); err != nil {
		return result{}, err
	}
	db0, err := openDB(dbPath, syncMode, true)
	if err != nil {
		return result{}, err
	}
	db0.Close()

	conns := make([]*Conn, consumers)
	for i := 0; i < consumers; i++ {
		c, err := dial(addr)
		if err != nil {
			return result{}, err
		}
		defer c.Close()
		conns[i] = c
	}
	dbs := make([]*DB, consumers)
	for i := 0; i < consumers; i++ {
		db, err := openDB(dbPath, syncMode, false)
		if err != nil {
			return result{}, err
		}
		defer db.Close()
		dbs[i] = db
	}

	pid, _ := redisPID(c0)
	cpu0 := cpuTicks(pid)
	var orders atomic.Int64
	var errs atomic.Int64
	hs := make([]*hist, consumers)
	for i := range hs {
		hs[i] = newHist()
	}
	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(consumers)
	for i := 0; i < consumers; i++ {
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("c%d", i)
			c := conns[i]
			db := dbs[i]
			empty := 0
			for orders.Load() < int64(messages) && empty < 1000 {
				msgs, err := readNewNamed(c, name, batch)
				if err != nil {
					errs.Add(1)
					return
				}
				if len(msgs) == 0 {
					empty++
					continue
				}
				empty = 0
				t0 := time.Now()
				if err := db.begin(); err != nil {
					errs.Add(1)
					return
				}
				ids := make([]string, 0, len(msgs))
				var wrote int64
				for _, m := range msgs {
					uid, err := strconv.ParseInt(m.User, 10, 64)
					if err != nil {
						errs.Add(1)
						return
					}
					ok, err := db.insert(uid, 1)
					if err != nil {
						errs.Add(1)
						return
					}
					if ok {
						wrote++
					}
					ids = append(ids, m.ID)
				}
				if err := db.commit(); err != nil {
					errs.Add(1)
					return
				}
				if err := ack(c, ids); err != nil {
					errs.Add(1)
					return
				}
				hs[i].add(time.Since(t0).Nanoseconds())
				orders.Add(wrote)
			}
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)
	cpu1 := cpuTicks(pid)
	res := result{
		Scenario:   "consume",
		Note:       "先把胜出消息放进流里，再计时把它们写进 SQLite。这是落库速度，不是点击速度。一批一提交时，崩溃最多重放这一批，主键让重放不会多出订单。",
		Workers:    consumers,
		SQLiteSync: syncMode,
		Batch:      batch,
		ElapsedSec: elapsed.Seconds(),
		Errors:     errs.Load(),
	}
	res.Orders, err = dbs[0].count()
	if err != nil {
		return result{}, err
	}
	res.Pending, err = pending(c0, keyStream)
	if err != nil {
		return result{}, err
	}
	res.QPS = float64(res.Orders) / res.ElapsedSec
	res.DrainQPS = res.QPS
	res.RedisCPUSec = (cpu1 - cpu0) / 100
	res.RedisCPUPercent = res.RedisCPUSec / res.ElapsedSec * 100
	res.RedisRSSKB = rssKB(pid)
	merged := newHist()
	for _, h := range hs {
		merged.merge(h)
	}
	fillHist(&res, merged)
	res.Pass = res.Errors == 0 && res.Orders == int64(messages) && res.Pending == 0
	res.Detail = fmt.Sprintf("orders=%d/%d pending=%d qps=%.0f sync=%s batch=%d consumers=%d p99us<=%d",
		res.Orders, messages, res.Pending, res.QPS, syncMode, batch, consumers, res.P99us)
	return res, nil
}

func runSyncPath(addr, script string, workers int, stock int64, syncMode string) (result, error) {
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
	if _, err := openAndClose(syncMode, true); err != nil {
		return result{}, err
	}
	conns := make([]*Conn, workers)
	dbs := make([]*DB, workers)
	conns[0] = c0
	for i := 0; i < workers; i++ {
		if i > 0 {
			c, err := dial(addr)
			if err != nil {
				return result{}, err
			}
			defer c.Close()
			conns[i] = c
		}
		db, err := openDB(dbPath, syncMode, false)
		if err != nil {
			return result{}, err
		}
		defer db.Close()
		dbs[i] = db
	}
	var seq int64
	var committed atomic.Int64
	var errs atomic.Int64
	var ops atomic.Int64
	var userFacing atomic.Int64
	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(i int) {
			defer wg.Done()
			c := conns[i]
			db := dbs[i]
			for committed.Load() < stock && errs.Load() == 0 && ops.Load() < stock*20 {
				id := atomic.AddInt64(&seq, 1)
				ops.Add(1)
				code, opErr := c.evalSHA(sha, 3, keyStock, keyWin, keyStream, strconv.FormatInt(id, 10), goodsID, "0")
				if opErr != nil {
					errs.Add(1)
					return
				}
				if code != codeOK {
					if committed.Load() >= stock || errs.Load() > 0 {
						return
					}
					continue
				}
				uid := id
				if err := db.begin(); err != nil {
					errs.Add(1)
					return
				}
				if _, err := db.insert(uid, 1); err != nil {
					errs.Add(1)
					return
				}
				if err := db.commit(); err != nil {
					errs.Add(1)
					return
				}
				n := committed.Add(1)
				if n == stock {
					userFacing.Store(time.Since(start).Nanoseconds())
				}
			}
		}(i)
	}
	wg.Wait()
	res := result{
		Scenario:     "sync-path",
		Note:         "胜出的请求在返回前等待 SQLite 提交。用户看到结果的时间和订单落库的时间是同一个数。",
		Workers:      workers,
		InitialStock: stock,
		SQLiteSync:   syncMode,
		Batch:        1,
		Errors:       errs.Load(),
		Ops:          ops.Load(),
	}
	res.Orders, err = dbs[0].count()
	if err != nil {
		return result{}, err
	}
	res.FinalStock, err = c0.getInt(keyStock)
	if err != nil {
		return result{}, err
	}
	res.StreamLen, err = xlen(c0, keyStream)
	if err != nil {
		return result{}, err
	}
	ns := userFacing.Load()
	res.UserFacingSec = float64(ns) / 1e9
	res.OrderVisibleSec = res.UserFacingSec
	res.ElapsedSec = res.UserFacingSec
	if res.UserFacingSec > 0 {
		res.QPS = float64(stock) / res.UserFacingSec
	}
	res.Pass = res.Errors == 0 && res.Orders == stock && res.FinalStock == 0 && res.StreamLen == stock && ns > 0
	res.Detail = fmt.Sprintf("stock=%d orders=%d userFacingSec=%.4f qps=%.0f", stock, res.Orders, res.UserFacingSec, res.QPS)
	return res, nil
}

func openAndClose(syncMode string, create bool) (int, error) {
	db, err := openDB(dbPath, syncMode, create)
	if err != nil {
		return 0, err
	}
	db.Close()
	return 0, nil
}

func runAsyncPath(addr, script string, workers int, stock int64, syncMode string, batch int) (result, error) {
	if batch < 1 {
		batch = 1
	}
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
	if err := ensureGroup(c0, keyStream, "$"); err != nil {
		return result{}, err
	}
	if _, err := openAndClose(syncMode, true); err != nil {
		return result{}, err
	}
	db, err := openDB(dbPath, syncMode, false)
	if err != nil {
		return result{}, err
	}
	defer db.Close()
	cc, err := dial(addr)
	if err != nil {
		return result{}, err
	}
	defer cc.Close()

	conns := make([]*Conn, workers)
	conns[0] = c0
	for i := 1; i < workers; i++ {
		c, err := dial(addr)
		if err != nil {
			return result{}, err
		}
		defer c.Close()
		conns[i] = c
	}

	var seq int64
	var produced atomic.Int64
	var errs atomic.Int64
	var ops atomic.Int64
	var userFacing atomic.Int64
	var maxLen atomic.Int64
	prodDone := make(chan struct{})
	consDone := make(chan struct{})
	start := time.Now()
	deadline := start.Add(45 * time.Second)

	go func() {
		defer close(consDone)
		for {
			if time.Now().After(deadline) {
				errs.Add(1)
				return
			}
			msgs, err := readNewNamed(cc, "async", batch)
			if err != nil {
				errs.Add(1)
				return
			}
			if len(msgs) == 0 {
				select {
				case <-prodDone:
					p, err := pending(cc, keyStream)
					if err != nil {
						errs.Add(1)
						return
					}
					n, err := db.count()
					if err != nil {
						errs.Add(1)
						return
					}
					if p == 0 && n >= stock {
						return
					}
					claimed, _, err := claimIdle(cc, batch)
					if err != nil {
						errs.Add(1)
						return
					}
					if len(claimed) == 0 {
						time.Sleep(200 * time.Microsecond)
						continue
					}
					msgs = claimed
				default:
					time.Sleep(200 * time.Microsecond)
					continue
				}
			}
			if ln, err := xlen(cc, keyStream); err == nil {
				for {
					old := maxLen.Load()
					if ln <= old || maxLen.CompareAndSwap(old, ln) {
						break
					}
				}
			}
			if err := db.begin(); err != nil {
				errs.Add(1)
				return
			}
			ids := make([]string, 0, len(msgs))
			for _, m := range msgs {
				uid, err := strconv.ParseInt(m.User, 10, 64)
				if err != nil {
					errs.Add(1)
					return
				}
				if _, err := db.insert(uid, 1); err != nil {
					errs.Add(1)
					return
				}
				ids = append(ids, m.ID)
			}
			if err := db.commit(); err != nil {
				errs.Add(1)
				return
			}
			if err := ack(cc, ids); err != nil {
				errs.Add(1)
				return
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(i int) {
			defer wg.Done()
			c := conns[i]
			for produced.Load() < stock && errs.Load() == 0 && ops.Load() < stock*20 {
				id := atomic.AddInt64(&seq, 1)
				ops.Add(1)
				code, opErr := c.evalSHA(sha, 3, keyStock, keyWin, keyStream, strconv.FormatInt(id, 10), goodsID, "0")
				if opErr != nil {
					errs.Add(1)
					return
				}
				if code != codeOK {
					if produced.Load() >= stock || errs.Load() > 0 {
						return
					}
					continue
				}
				n := produced.Add(1)
				if n == stock {
					userFacing.Store(time.Since(start).Nanoseconds())
				}
			}
		}(i)
	}
	wg.Wait()
	close(prodDone)
	<-consDone
	visible := time.Since(start)
	res := result{
		Scenario:     "async-path",
		Note:         "请求只等 Lua 入队就返回。SQLite 在旁边的消费者里提交。用户面对的时间只包括 Redis，订单可见的时间还包括把队列写完。",
		Workers:      workers,
		InitialStock: stock,
		SQLiteSync:   syncMode,
		Batch:        batch,
		Errors:       errs.Load(),
		Ops:          ops.Load(),
		MaxStreamLen: maxLen.Load(),
	}
	res.Orders, err = db.count()
	if err != nil {
		return result{}, err
	}
	res.FinalStock, err = c0.getInt(keyStock)
	if err != nil {
		return result{}, err
	}
	res.StreamLen, err = xlen(c0, keyStream)
	if err != nil {
		return result{}, err
	}
	res.Pending, err = pending(c0, keyStream)
	if err != nil {
		return result{}, err
	}
	res.UserFacingSec = float64(userFacing.Load()) / 1e9
	res.OrderVisibleSec = visible.Seconds()
	res.ElapsedSec = res.OrderVisibleSec
	if res.UserFacingSec > 0 {
		res.ProducerQPS = float64(stock) / res.UserFacingSec
		res.QPS = res.ProducerQPS
	}
	if res.OrderVisibleSec > 0 {
		res.DrainQPS = float64(res.Orders) / res.OrderVisibleSec
	}
	res.Pass = res.Errors == 0 && res.Orders == stock && res.FinalStock == 0 && res.Pending == 0 &&
		res.StreamLen == stock && userFacing.Load() > 0 && res.OrderVisibleSec+0.001 >= res.UserFacingSec
	res.Detail = fmt.Sprintf("stock=%d orders=%d userFacingSec=%.4f orderVisibleSec=%.4f producerQps=%.0f maxStream=%d",
		stock, res.Orders, res.UserFacingSec, res.OrderVisibleSec, res.ProducerQPS, res.MaxStreamLen)
	return res, nil
}

func runBacklog(addr string, cap int, fat int) (result, error) {
	if cap <= 0 {
		cap = 2_000_000
	}
	c, err := dial(addr)
	if err != nil {
		return result{}, err
	}
	defer c.Close()
	if err := flush(c); err != nil {
		return result{}, err
	}
	base, err := usedMemory(c)
	if err != nil {
		return result{}, err
	}
	pid, err := redisPID(c)
	if err != nil {
		return result{}, err
	}
	body := ""
	if fat > 0 {
		body = stringsRepeat(fat)
	}
	const limit = 56 * 1024 * 1024
	var n int64
	stopped := "cap"
	start := time.Now()
	for n < int64(cap) {
		user := strconv.FormatInt(n+1, 10)
		var opErr error
		if fat > 0 {
			_, opErr = c.cmd("XADD", keyStream, "*", "u", user, "g", goodsID, "b", body)
		} else {
			_, opErr = c.cmd("XADD", keyStream, "*", "u", user, "g", goodsID)
		}
		if opErr != nil {
			if stringsContainsOOM(opErr) {
				stopped = "oom"
				break
			}
			return result{}, opErr
		}
		n++
		if n%2000 == 0 {
			mem, err := usedMemory(c)
			if err != nil {
				return result{}, err
			}
			if mem >= limit {
				stopped = "memory"
				break
			}
		}
	}
	mem, err := usedMemory(c)
	if err != nil {
		return result{}, err
	}
	length, err := xlen(c, keyStream)
	if err != nil {
		return result{}, err
	}
	res := result{
		Scenario:     "backlog",
		Note:         "一条不裁剪的订单流能在 64MB 上限里堆多少条。胜出人数等于库存时这个数字很大；如果每个点击都入队，几秒就会写满。",
		ElapsedSec:   time.Since(start).Seconds(),
		Entries:      length,
		UsedMemory:   mem,
		MemBase:      base,
		StoppedOn:    stopped,
		RedisRSSKB:   rssKB(pid),
		InitialStock: int64(fat),
	}
	if length > 0 {
		res.BytesPerEntry = float64(mem-base) / float64(length)
	}
	res.Pass = length > 1000 && res.BytesPerEntry > 0 && (stopped == "memory" || stopped == "oom" || stopped == "cap")
	res.Detail = fmt.Sprintf("entries=%d bytesPerEntry=%.1f used=%d base=%d stopped=%s fat=%d elapsed=%.2fs",
		length, res.BytesPerEntry, mem, base, stopped, fat, res.ElapsedSec)
	return res, nil
}

func stringsRepeat(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = 'x'
	}
	return string(b)
}

func stringsContainsOOM(err error) bool {
	return err != nil && (contains(err.Error(), "OOM") || contains(err.Error(), "maxmemory"))
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
