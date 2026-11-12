package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	codeSoldOut = 0
	codeOK      = 1
	codeDup     = 2
	codeBusy    = 3
	codeLimited = 4
)

type result struct {
	Scenario      string  `json:"scenario"`
	Note          string  `json:"note"`
	Addrs         string  `json:"addrs"`
	Workers       int     `json:"workers"`
	ElapsedSec    float64 `json:"elapsedSec"`
	Ops           int64   `json:"ops"`
	QPS           float64 `json:"qps"`
	Success       int64   `json:"success"`
	SoldOut       int64   `json:"soldOut"`
	Duplicate     int64   `json:"duplicate"`
	Busy          int64   `json:"busy"`
	RateLimited   int64   `json:"rateLimited"`
	Errors        int64   `json:"errors"`
	InitialStock  int64   `json:"initialStock"`
	FinalStock    int64   `json:"finalStock"`
	Winners       int64   `json:"winners"`
	RedisCommands int64   `json:"redisCommands"`
	RedisCPUSec   float64 `json:"redisCpuSec"`
	RedisCPUpct   float64 `json:"redisCpuPercent"`
	RedisRSSKB    int64   `json:"redisRssKB"`
	UsedMemory    int64   `json:"usedMemory"`
	P50us         int64   `json:"p50us"`
	P99us         int64   `json:"p99us"`
	P999us        int64   `json:"p999us"`
	Maxus         int64   `json:"maxus"`
	Pass          bool    `json:"pass"`
	Detail        string  `json:"detail"`
}

func main() {
	scenario := flag.String("scenario", "lua-correct", "see run.sh")
	addrs := flag.String("addrs", "127.0.0.1:6379", "comma separated redis addrs")
	workers := flag.Int("workers", 32, "goroutines, one connection each")
	duration := flag.Duration("duration", 5*time.Second, "throughput window, after warmup")
	warmup := flag.Duration("warmup", 1*time.Second, "throughput warmup")
	stock := flag.Int64("stock", 100, "initial stock per node or total for single key")
	attempts := flag.Int64("attempts", 4000, "fixed request count for correctness")
	users := flag.Int("users", 50, "distinct users in correctness runs")
	repeat := flag.Int("repeat", 20, "requests per user in correctness runs")
	limit := flag.Int("limit", 100, "fixed-window rate limit")
	luaDir := flag.String("lua", "lua", "directory of lua scripts")
	out := flag.String("out", "", "write this result json here")
	flag.Parse()

	runtime.GOMAXPROCS(4)
	body := map[string]string{}
	for _, name := range []string{"stock.lua", "soldout.lua", "unlock.lua", "ratelimit.lua"} {
		b, err := os.ReadFile(*luaDir + "/" + name)
		if err != nil {
			fatal(err)
		}
		body[name] = string(b)
	}
	nodes := strings.Split(*addrs, ",")
	var res result
	var err error
	switch *scenario {
	case "unsafe-correct":
		res, err = runCorrect(*scenario, nodes, body, *workers, *stock, *users, *repeat, false, true)
	case "lua-correct":
		res, err = runCorrect(*scenario, nodes, body, *workers, *stock, *users, *repeat, true, false)
	case "lock-correct":
		res, err = runLockCorrect(nodes, body, *workers, *stock, *users, *repeat)
	case "lua-tps":
		res, err = runTPS("lua-tps", "成功路径：SISMEMBER + 判断库存 + DECR + SADD，一次 EVALSHA", nodes, body["stock.lua"], *workers, *duration, *warmup, *stock, false)
	case "lua-soldout-tps":
		res, err = runSoldOutTPS(nodes, body["soldout.lua"], *workers, *duration, *warmup)
	case "lock-tps":
		res, err = runLockTPS(nodes, body, *workers, *duration, *warmup, *stock)
	case "local-tps":
		res, err = runLocalTPS(*workers, *duration, *warmup)
	case "mem-tps":
		res, err = runMemTPS(*workers, *duration, *warmup, *stock)
	case "local-shed":
		res, err = runShed(nodes, body["stock.lua"], *workers, *attempts, *stock)
	case "shard-one-soldout":
		res, err = runShardSoldOut("shard-one-soldout", "两个售罄 key 在同一个 Redis 进程。单线程，分 key 不会把 QPS 翻倍。", nodes[:1], body["soldout.lua"], *workers, *duration, *warmup)
	case "shard-two-soldout":
		if len(nodes) < 2 {
			fatal(fmt.Errorf("shard-two-soldout needs two addrs"))
		}
		res, err = runShardSoldOut("shard-two-soldout", "两个售罄 key 分到两个 Redis 进程，内存合计仍是 64MB。", nodes[:2], body["soldout.lua"], *workers, *duration, *warmup)
	case "ratelimit":
		res, err = runRate(nodes, body, *workers, *attempts, *limit)
	case "http-local":
		res, err = runHTTP(false, "", *workers, *duration, *warmup)
	case "http-redis":
		res, err = runHTTP(true, body["soldout.lua"], *workers, *duration, *warmup)
	default:
		fatal(fmt.Errorf("unknown scenario %s", *scenario))
	}
	if err != nil {
		fatal(err)
	}
	enc, _ := json.MarshalIndent(res, "", "  ")
	fmt.Println(string(enc))
	if *out != "" {
		if err := os.WriteFile(*out, append(enc, '\n'), 0o644); err != nil {
			fatal(err)
		}
	}
	if !res.Pass {
		os.Exit(2)
	}
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "error: %v\n", err)
	os.Exit(1)
}

type node struct {
	addr  string
	conns []*Conn
	sha   string
	pid   int
}

func prepare(addrs []string, script string, workers int) ([]*node, error) {
	nodes := make([]*node, 0, len(addrs))
	for _, addr := range addrs {
		n := &node{addr: addr}
		c0, err := dial(addr)
		if err != nil {
			return nil, err
		}
		if _, err := c0.cmd("FLUSHDB"); err != nil {
			return nil, err
		}
		if _, err := c0.cmd("SCRIPT", "FLUSH"); err != nil {
			return nil, err
		}
		sha, err := c0.scriptLoad(script)
		if err != nil {
			return nil, err
		}
		info, err := c0.info("server")
		if err != nil {
			return nil, err
		}
		pid, _ := strconv.Atoi(infoValue(info, "process_id"))
		n.sha = sha
		n.pid = pid
		n.conns = []*Conn{c0}
		for i := 1; i < workers; i++ {
			c, err := dial(addr)
			if err != nil {
				return nil, err
			}
			n.conns = append(n.conns, c)
		}
		nodes = append(nodes, n)
	}
	return nodes, nil
}

func closeNodes(nodes []*node) {
	for _, n := range nodes {
		for _, c := range n.conns {
			c.Close()
		}
	}
}

func commandsProcessed(nodes []*node) int64 {
	var total int64
	for _, n := range nodes {
		info, err := n.conns[0].info("stats")
		if err != nil {
			continue
		}
		v, _ := strconv.ParseInt(infoValue(info, "total_commands_processed"), 10, 64)
		total += v
	}
	return total
}

func usedMemory(nodes []*node) int64 {
	var total int64
	for _, n := range nodes {
		info, err := n.conns[0].info("memory")
		if err != nil {
			continue
		}
		v, _ := strconv.ParseInt(infoValue(info, "used_memory"), 10, 64)
		total += v
	}
	return total
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
	// fields[0] 是 state（原第 3 列），utime 是原第 14 列，下标 11。
	ut, _ := strconv.ParseFloat(fields[11], 64)
	st, _ := strconv.ParseFloat(fields[12], 64)
	return ut + st
}

func applyCounters(res *result, code int, err error) {
	if err != nil {
		res.Errors++
		return
	}
	switch code {
	case codeOK:
		res.Success++
	case codeSoldOut:
		res.SoldOut++
	case codeDup:
		res.Duplicate++
	case codeBusy:
		res.Busy++
	case codeLimited:
		res.RateLimited++
	default:
		res.Errors++
	}
}

func runCorrect(name string, addrs []string, body map[string]string, workers int, stock int64, users, repeat int, lua bool, wantOversell bool) (result, error) {
	nodes, err := prepare(addrs[:1], body["stock.lua"], workers)
	if err != nil {
		return result{}, err
	}
	defer closeNodes(nodes)
	n := nodes[0]
	if _, err := n.conns[0].cmd("SET", "sec:stock:1", strconv.FormatInt(stock, 10)); err != nil {
		return result{}, err
	}
	before := commandsProcessed(nodes)
	cpu0 := cpuTicks(n.pid)
	start := time.Now()

	total := users * repeat
	var next int64
	var wg sync.WaitGroup
	var mu sync.Mutex
	acc := result{Scenario: name, Workers: workers, InitialStock: stock, Addrs: n.addr}
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			defer wg.Done()
			c := n.conns[w]
			local := result{}
			for {
				i := atomic.AddInt64(&next, 1) - 1
				if i >= int64(total) {
					break
				}
				user := strconv.Itoa(int(i) % users)
				var code int64
				var opErr error
				if lua {
					code, opErr = c.evalSHA(n.sha, 2, "sec:stock:1", "sec:win:1", user)
				} else {
					code, opErr = unsafeDecr(c, "sec:stock:1", "sec:win:1", user)
				}
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
				}
			}
			mu.Lock()
			acc.Ops += local.Ops
			acc.Success += local.Success
			acc.SoldOut += local.SoldOut
			acc.Duplicate += local.Duplicate
			acc.Errors += local.Errors
			mu.Unlock()
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)
	finalStock, err := n.conns[0].getInt("sec:stock:1")
	if err != nil {
		return result{}, err
	}
	winners, err := n.conns[0].scard("sec:win:1")
	if err != nil {
		return result{}, err
	}
	acc.ElapsedSec = elapsed.Seconds()
	acc.QPS = float64(acc.Ops) / elapsed.Seconds()
	acc.FinalStock = finalStock
	acc.Winners = winners
	acc.RedisCommands = commandsProcessed(nodes) - before
	acc.RedisCPUSec = (cpuTicks(n.pid) - cpu0) / 100
	acc.RedisRSSKB = rssKB(n.pid)
	acc.UsedMemory = usedMemory(nodes)
	wantDup := stock * int64(repeat-1)
	wantSold := int64(users)*int64(repeat) - stock - wantDup
	invariant := winners == stock && finalStock == 0 && acc.Success == stock && acc.Duplicate == wantDup && acc.SoldOut == wantSold && acc.Errors == 0
	if wantOversell {
		acc.Pass = !invariant && acc.Errors == 0 && (winners > stock || acc.Success > winners || finalStock < 0 || acc.Success != winners || finalStock+winners != stock)
		acc.Note = "非原子 GET/SET。本场景通过条件是确实打出超卖或库存对不上。"
		acc.Detail = fmt.Sprintf("invariantHeld=%v winners=%d success=%d soldOut=%d dup=%d finalStock=%d", invariant, winners, acc.Success, acc.SoldOut, acc.Duplicate, finalStock)
	} else {
		acc.Pass = invariant
		acc.Note = "Lua 原子扣减。库存归零，成功人数等于库存，赢的人的重复请求记为重复，没赢的记为售罄。"
		acc.Detail = fmt.Sprintf("wantSuccess=%d wantDup=%d wantSoldOut=%d", stock, wantDup, wantSold)
	}
	return acc, nil
}

func unsafeDecr(c *Conn, stockKey, winKey, user string) (int64, error) {
	// 先看是不是已经买到，这样售罄之后重复请求仍然返回「重复」而不是「售罄」。
	// 判断和写入分成多次往返，没有锁的时候会超卖。
	member, err := c.cmd("SISMEMBER", winKey, user)
	if err != nil {
		return 0, err
	}
	if member.Int == 1 {
		return codeDup, nil
	}
	rep, err := c.cmd("GET", stockKey)
	if err != nil {
		return 0, err
	}
	if rep.Nil {
		return codeSoldOut, nil
	}
	cur, err := strconv.ParseInt(rep.Str, 10, 64)
	if err != nil {
		return 0, err
	}
	if cur <= 0 {
		return codeSoldOut, nil
	}
	if _, err := c.cmd("SET", stockKey, strconv.FormatInt(cur-1, 10)); err != nil {
		return 0, err
	}
	if _, err := c.cmd("SADD", winKey, user); err != nil {
		return 0, err
	}
	return codeOK, nil
}

func runLockCorrect(addrs []string, body map[string]string, workers int, stock int64, users, repeat int) (result, error) {
	nodes, err := prepare(addrs[:1], body["unlock.lua"], workers)
	if err != nil {
		return result{}, err
	}
	defer closeNodes(nodes)
	n := nodes[0]
	if _, err := n.conns[0].cmd("SET", "sec:stock:1", strconv.FormatInt(stock, 10)); err != nil {
		return result{}, err
	}
	before := commandsProcessed(nodes)
	start := time.Now()
	total := users * repeat
	var next int64
	var wg sync.WaitGroup
	var mu sync.Mutex
	acc := result{Scenario: "lock-correct", Workers: workers, InitialStock: stock, Addrs: n.addr}
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			defer wg.Done()
			c := n.conns[w]
			local := result{}
			for {
				i := atomic.AddInt64(&next, 1) - 1
				if i >= int64(total) {
					break
				}
				user := strconv.Itoa(int(i) % users)
				code, opErr := lockedDecr(c, n.sha, user, fmt.Sprintf("%d-%d", w, i))
				local.Ops++
				if opErr != nil {
					local.Errors++
					continue
				}
				spins := 0
				for code == codeBusy && spins < 2000 {
					spins++
					code, opErr = lockedDecr(c, n.sha, user, fmt.Sprintf("%d-%d-%d", w, i, spins))
					local.Busy++
					local.Ops++
					if opErr != nil {
						local.Errors++
						break
					}
				}
				switch code {
				case codeOK:
					local.Success++
				case codeSoldOut:
					local.SoldOut++
				case codeDup:
					local.Duplicate++
				case codeBusy:
					local.Busy++
				}
			}
			mu.Lock()
			acc.Ops += local.Ops
			acc.Success += local.Success
			acc.SoldOut += local.SoldOut
			acc.Duplicate += local.Duplicate
			acc.Busy += local.Busy
			acc.Errors += local.Errors
			mu.Unlock()
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)
	finalStock, err := n.conns[0].getInt("sec:stock:1")
	if err != nil {
		return result{}, err
	}
	winners, err := n.conns[0].scard("sec:win:1")
	if err != nil {
		return result{}, err
	}
	acc.ElapsedSec = elapsed.Seconds()
	acc.QPS = float64(total) / elapsed.Seconds()
	acc.FinalStock = finalStock
	acc.Winners = winners
	acc.RedisCommands = commandsProcessed(nodes) - before
	acc.RedisRSSKB = rssKB(n.pid)
	acc.UsedMemory = usedMemory(nodes)
	wantDup := stock * int64(repeat-1)
	wantSold := int64(users)*int64(repeat) - stock - wantDup
	acc.Pass = winners == stock && finalStock == 0 && acc.Success == stock && acc.Duplicate == wantDup && acc.SoldOut == wantSold && acc.Errors == 0
	acc.Note = "同一商品一把锁，临界区里做读改写。结果正确，但请求在锁上排队。"
	acc.Detail = fmt.Sprintf("wantSuccess=%d wantDup=%d wantSoldOut=%d businessQPS=%.0f", stock, wantDup, wantSold, acc.QPS)
	return acc, nil
}

func lockedDecr(c *Conn, unlockSHA, user, token string) (int64, error) {
	rep, err := c.cmd("SET", "sec:lock:1", token, "NX", "PX", "5000")
	if err != nil {
		return 0, err
	}
	if rep.Nil {
		return codeBusy, nil
	}
	defer func() {
		_, _ = c.evalSHA(unlockSHA, 1, "sec:lock:1", token)
	}()
	return unsafeDecr(c, "sec:stock:1", "sec:win:1", user)
}

type tpsAcc struct {
	ops     int64
	success int64
	soldOut int64
	dup     int64
	busy    int64
	errs    int64
	h       *hist
}

func runTPS(name, note string, addrs []string, script string, workers int, duration, warmup time.Duration, stock int64, soldOutScript bool) (result, error) {
	nodes, err := prepare(addrs[:1], script, workers)
	if err != nil {
		return result{}, err
	}
	defer closeNodes(nodes)
	n := nodes[0]
	if _, err := n.conns[0].cmd("SET", "sec:stock:1", strconv.FormatInt(stock, 10)); err != nil {
		return result{}, err
	}
	return driveTPS(name, note, nodes, workers, duration, warmup, stock, func(c *Conn, _ int, id int64) (int64, error) {
		user := strconv.FormatInt(id, 10)
		if soldOutScript {
			return c.evalSHA(n.sha, 1, "sec:stock:1")
		}
		return c.evalSHA(n.sha, 2, "sec:stock:1", "sec:win:1", user)
	})
}

func runShardSoldOut(name, note string, addrs []string, script string, workers int, duration, warmup time.Duration) (result, error) {
	nodes, err := prepare(addrs, script, workers)
	if err != nil {
		return result{}, err
	}
	defer closeNodes(nodes)
	for i, n := range nodes {
		if _, err := n.conns[0].cmd("SET", fmt.Sprintf("sec:stock:1:%d", i), "0"); err != nil {
			return result{}, err
		}
	}
	return driveTPS(name, note, nodes, workers, duration, warmup, 0, func(c *Conn, worker int, _ int64) (int64, error) {
		bucket := worker % len(nodes)
		return c.evalSHA(nodes[bucket].sha, 1, fmt.Sprintf("sec:stock:1:%d", bucket))
	})
}

func runSoldOutTPS(addrs []string, script string, workers int, duration, warmup time.Duration) (result, error) {
	return runTPS("lua-soldout-tps", "库存已是 0，脚本只做一次 GET。这是售罄后、本地标记生效前打到 Redis 的路径。", addrs, script, workers, duration, warmup, 0, true)
}

func driveTPS(name, note string, nodes []*node, workers int, duration, warmup time.Duration, initial int64, op func(*Conn, int, int64) (int64, error)) (result, error) {
	type binding struct {
		c   *Conn
		sha string
	}
	binds := make([]binding, workers)
	for i := 0; i < workers; i++ {
		n := nodes[i%len(nodes)]
		binds[i] = binding{c: n.conns[i/len(nodes)%len(n.conns)], sha: n.sha}
		_ = binds[i].sha
	}
	// 上面的取连接方式在 workers > len(conns) 时会撞车。prepare 给每个节点建了 workers 条连接，
	// 单节点用 conns[i]，多节点每个节点也有 workers 条，取 conns[i] 即可，节点按 i%len 选择。
	for i := 0; i < workers; i++ {
		n := nodes[i%len(nodes)]
		binds[i] = binding{c: n.conns[i]}
	}

	var seq int64
	warmEnd := time.Now().Add(warmup)
	for time.Now().Before(warmEnd) {
		var wg sync.WaitGroup
		wg.Add(workers)
		for i := 0; i < workers; i++ {
			go func(i int) {
				defer wg.Done()
				deadline := time.Now().Add(50 * time.Millisecond)
				for time.Now().Before(deadline) {
					id := atomic.AddInt64(&seq, 1)
					_, _ = op(binds[i].c, i, id)
				}
			}(i)
		}
		wg.Wait()
	}

	hists := make([]*hist, workers)
	locals := make([]tpsAcc, workers)
	before := commandsProcessed(nodes)
	cpu0 := make([]float64, len(nodes))
	for i, n := range nodes {
		cpu0[i] = cpuTicks(n.pid)
	}
	start := time.Now()
	end := start.Add(duration)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		hists[i] = newHist()
		go func(i int) {
			defer wg.Done()
			c := binds[i].c
			h := hists[i]
			for time.Now().Before(end) {
				id := atomic.AddInt64(&seq, 1)
				t0 := time.Now()
				code, err := op(c, i, id)
				h.add(time.Since(t0).Nanoseconds())
				locals[i].ops++
				if err != nil {
					locals[i].errs++
					continue
				}
				switch code {
				case codeOK:
					locals[i].success++
				case codeSoldOut:
					locals[i].soldOut++
				case codeDup:
					locals[i].dup++
				case codeBusy:
					locals[i].busy++
				}
			}
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)
	merged := newHist()
	res := result{Scenario: name, Note: note, Workers: workers, InitialStock: initial}
	var addr []string
	for _, n := range nodes {
		addr = append(addr, n.addr)
	}
	res.Addrs = strings.Join(addr, ",")
	for i := 0; i < workers; i++ {
		res.Ops += locals[i].ops
		res.Success += locals[i].success
		res.SoldOut += locals[i].soldOut
		res.Duplicate += locals[i].dup
		res.Busy += locals[i].busy
		res.Errors += locals[i].errs
		merged.merge(hists[i])
	}
	res.ElapsedSec = elapsed.Seconds()
	if elapsed > 0 {
		res.QPS = float64(res.Ops) / elapsed.Seconds()
	}
	res.RedisCommands = commandsProcessed(nodes) - before
	var cpu1 float64
	var rss int64
	for i, n := range nodes {
		cpu1 += cpuTicks(n.pid) - cpu0[i]
		rss += rssKB(n.pid)
	}
	res.RedisCPUSec = cpu1 / 100
	if elapsed > 0 {
		res.RedisCPUpct = res.RedisCPUSec / elapsed.Seconds() * 100
	}
	res.RedisRSSKB = rss
	res.UsedMemory = usedMemory(nodes)
	res.P50us = merged.le(0.50) / 1000
	res.P99us = merged.le(0.99) / 1000
	res.P999us = merged.le(0.999) / 1000
	res.Maxus = merged.max / 1000
	res.FinalStock = sumStock(nodes)
	res.Winners = sumWinners(nodes)
	res.Pass = res.Errors == 0 && res.Ops > 0 && res.QPS > 0
	if name == "lua-tps" || name == "lock-tps" {
		res.Pass = res.Pass && res.Success > 0 && res.FinalStock >= 0 && res.FinalStock+res.Winners == initial
	}
	if strings.Contains(name, "soldout") {
		res.Pass = res.Pass && res.SoldOut == res.Ops && res.Success == 0
	}
	return res, nil
}

func sumStock(nodes []*node) int64 {
	var total int64
	if len(nodes) == 1 {
		v, err := nodes[0].conns[0].getInt("sec:stock:1")
		if err != nil {
			return -1
		}
		// 分片但只有一个进程时 key 带桶号，两个 key 都读。
		v0, err0 := nodes[0].conns[0].getInt("sec:stock:1:0")
		v1, err1 := nodes[0].conns[0].getInt("sec:stock:1:1")
		if err0 == nil && err1 == nil && (v0 != 0 || v1 != 0) {
			return v0 + v1
		}
		return v
	}
	for i, n := range nodes {
		v, err := n.conns[0].getInt(fmt.Sprintf("sec:stock:1:%d", i))
		if err != nil {
			return -1
		}
		total += v
	}
	return total
}

func sumWinners(nodes []*node) int64 {
	var total int64
	if len(nodes) == 1 {
		v, _ := nodes[0].conns[0].scard("sec:win:1")
		v0, _ := nodes[0].conns[0].scard("sec:win:1:0")
		v1, _ := nodes[0].conns[0].scard("sec:win:1:1")
		if v0 != 0 || v1 != 0 {
			return v0 + v1
		}
		return v
	}
	for i, n := range nodes {
		v, _ := n.conns[0].scard(fmt.Sprintf("sec:win:1:%d", i))
		total += v
	}
	return total
}

func runLockTPS(addrs []string, body map[string]string, workers int, duration, warmup time.Duration, stock int64) (result, error) {
	nodes, err := prepare(addrs[:1], body["unlock.lua"], workers)
	if err != nil {
		return result{}, err
	}
	defer closeNodes(nodes)
	n := nodes[0]
	if _, err := n.conns[0].cmd("SET", "sec:stock:1", strconv.FormatInt(stock, 10)); err != nil {
		return result{}, err
	}
	var spin int64
	return driveTPS("lock-tps", "每个请求都先 SET NX 抢同一把锁，抢到后才读改写。热 key 上这是反模式。", nodes, workers, duration, warmup, stock, func(c *Conn, _ int, id int64) (int64, error) {
		token := strconv.FormatInt(id, 10)
		code, err := lockedDecr(c, n.sha, token, token)
		if err != nil {
			return 0, err
		}
		if code == codeBusy {
			atomic.AddInt64(&spin, 1)
		}
		return code, nil
	})
}

func runLocalTPS(workers int, duration, warmup time.Duration) (result, error) {
	var sold atomic.Bool
	sold.Store(true)
	var seq int64
	warmEnd := time.Now().Add(warmup)
	for time.Now().Before(warmEnd) {
		var wg sync.WaitGroup
		wg.Add(workers)
		for i := 0; i < workers; i++ {
			go func() {
				defer wg.Done()
				deadline := time.Now().Add(50 * time.Millisecond)
				for time.Now().Before(deadline) {
					if sold.Load() {
						atomic.AddInt64(&seq, 1)
					}
				}
			}()
		}
		wg.Wait()
	}
	hists := make([]*hist, workers)
	var ops int64
	start := time.Now()
	end := start.Add(duration)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		hists[i] = newHist()
		go func(i int) {
			defer wg.Done()
			h := hists[i]
			var local int64
			for time.Now().Before(end) {
				t0 := time.Now()
				_ = sold.Load()
				h.add(time.Since(t0).Nanoseconds())
				local++
			}
			atomic.AddInt64(&ops, local)
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)
	merged := newHist()
	for _, h := range hists {
		merged.merge(h)
	}
	res := result{
		Scenario:   "local-tps",
		Note:       "售罄标记命中后的进程内拒绝，不再访问 Redis。",
		Workers:    workers,
		ElapsedSec: elapsed.Seconds(),
		Ops:        ops,
		SoldOut:    ops,
		P50us:      merged.le(0.50) / 1000,
		P99us:      merged.le(0.99) / 1000,
		P999us:     merged.le(0.999) / 1000,
		Maxus:      merged.max / 1000,
		Pass:       ops > 0,
	}
	if elapsed > 0 {
		res.QPS = float64(ops) / elapsed.Seconds()
	}
	return res, nil
}

func runMemTPS(workers int, duration, warmup time.Duration, stock int64) (result, error) {
	var mu sync.Mutex
	left := stock
	var winners int64
	op := func() int {
		mu.Lock()
		defer mu.Unlock()
		if left <= 0 {
			return codeSoldOut
		}
		left--
		winners++
		return codeOK
	}
	warmEnd := time.Now().Add(warmup)
	for time.Now().Before(warmEnd) {
		var wg sync.WaitGroup
		wg.Add(workers)
		for i := 0; i < workers; i++ {
			go func() {
				defer wg.Done()
				deadline := time.Now().Add(20 * time.Millisecond)
				for time.Now().Before(deadline) {
					op()
				}
			}()
		}
		wg.Wait()
	}
	left = stock
	winners = 0
	hists := make([]*hist, workers)
	locals := make([]tpsAcc, workers)
	start := time.Now()
	end := start.Add(duration)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		hists[i] = newHist()
		go func(i int) {
			defer wg.Done()
			for time.Now().Before(end) {
				t0 := time.Now()
				code := op()
				hists[i].add(time.Since(t0).Nanoseconds())
				locals[i].ops++
				if code == codeOK {
					locals[i].success++
				} else {
					locals[i].soldOut++
				}
			}
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)
	merged := newHist()
	res := result{Scenario: "mem-tps", Note: "单进程互斥锁扣减。只在这一台机器里正确，进程退出库存就没了，多开几台会超卖。", Workers: workers, InitialStock: stock}
	for i := 0; i < workers; i++ {
		res.Ops += locals[i].ops
		res.Success += locals[i].success
		res.SoldOut += locals[i].soldOut
		merged.merge(hists[i])
	}
	res.ElapsedSec = elapsed.Seconds()
	res.QPS = float64(res.Ops) / elapsed.Seconds()
	res.FinalStock = left
	res.Winners = winners
	res.P50us = merged.le(0.50) / 1000
	res.P99us = merged.le(0.99) / 1000
	res.P999us = merged.le(0.999) / 1000
	res.Maxus = merged.max / 1000
	res.Pass = left == 0 && winners == stock && res.Success == stock && res.Errors == 0
	res.Detail = "库存远小于请求量，所以绝大多数时间花在锁里读到 0。"
	return res, nil
}

func runShed(addrs []string, script string, workers int, attempts, stock int64) (result, error) {
	without, err := shedOnce(addrs, script, workers, attempts, stock, false)
	if err != nil {
		return result{}, err
	}
	with, err := shedOnce(addrs, script, workers, attempts, stock, true)
	if err != nil {
		return result{}, err
	}
	with.Scenario = "local-shed"
	with.Note = "同一批请求，先不带本地售罄标记，再带上。Redis 命令数应当掉一个数量级以上。"
	with.Detail = fmt.Sprintf("commandsWithout=%d commandsWith=%d qpsWithout=%.0f qpsWith=%.0f successWithout=%d successWith=%d",
		without.RedisCommands, with.RedisCommands, without.QPS, with.QPS, without.Success, with.Success)
	ratioOK := with.RedisCommands*10 < without.RedisCommands
	with.Pass = without.Pass && with.Success == stock && without.Success == stock && with.Errors == 0 && without.Errors == 0 && ratioOK
	return with, nil
}

func shedOnce(addrs []string, script string, workers int, attempts, stock int64, local bool) (result, error) {
	nodes, err := prepare(addrs[:1], script, workers)
	if err != nil {
		return result{}, err
	}
	defer closeNodes(nodes)
	n := nodes[0]
	if _, err := n.conns[0].cmd("SET", "sec:stock:1", strconv.FormatInt(stock, 10)); err != nil {
		return result{}, err
	}
	var sold atomic.Bool
	var next int64
	var wg sync.WaitGroup
	var mu sync.Mutex
	acc := result{Workers: workers, InitialStock: stock, Addrs: n.addr}
	before := commandsProcessed(nodes)
	start := time.Now()
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			defer wg.Done()
			c := n.conns[w]
			localRes := result{}
			for {
				if local && sold.Load() {
					i := atomic.AddInt64(&next, 1) - 1
					if i >= attempts {
						break
					}
					localRes.Ops++
					localRes.SoldOut++
					continue
				}
				i := atomic.AddInt64(&next, 1) - 1
				if i >= attempts {
					break
				}
				user := strconv.FormatInt(i, 10)
				code, opErr := c.evalSHA(n.sha, 2, "sec:stock:1", "sec:win:1", user)
				localRes.Ops++
				if opErr != nil {
					localRes.Errors++
					continue
				}
				switch code {
				case codeOK:
					localRes.Success++
				case codeSoldOut:
					localRes.SoldOut++
					if local {
						sold.Store(true)
					}
				case codeDup:
					localRes.Duplicate++
				}
			}
			mu.Lock()
			acc.Ops += localRes.Ops
			acc.Success += localRes.Success
			acc.SoldOut += localRes.SoldOut
			acc.Duplicate += localRes.Duplicate
			acc.Errors += localRes.Errors
			mu.Unlock()
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)
	finalStock, err := n.conns[0].getInt("sec:stock:1")
	if err != nil {
		return result{}, err
	}
	winners, err := n.conns[0].scard("sec:win:1")
	if err != nil {
		return result{}, err
	}
	acc.ElapsedSec = elapsed.Seconds()
	acc.QPS = float64(acc.Ops) / elapsed.Seconds()
	acc.FinalStock = finalStock
	acc.Winners = winners
	acc.RedisCommands = commandsProcessed(nodes) - before
	acc.Pass = winners == stock && finalStock == 0 && acc.Success == stock
	return acc, nil
}

func runRate(addrs []string, body map[string]string, workers int, attempts int64, limit int) (result, error) {
	nodes, err := prepare(addrs[:1], body["ratelimit.lua"], workers)
	if err != nil {
		return result{}, err
	}
	defer closeNodes(nodes)
	n := nodes[0]
	stockSHA, err := n.conns[0].scriptLoad(body["stock.lua"])
	if err != nil {
		return result{}, err
	}
	const stock int64 = 100000
	if _, err := n.conns[0].cmd("SET", "sec:stock:1", strconv.FormatInt(stock, 10)); err != nil {
		return result{}, err
	}
	var next int64
	var wg sync.WaitGroup
	var mu sync.Mutex
	acc := result{Scenario: "ratelimit", Workers: workers, InitialStock: stock, Addrs: n.addr, Note: "固定窗口。测试把窗口 id 钉死，避免跨秒导致断言飘。"}
	before := commandsProcessed(nodes)
	start := time.Now()
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			defer wg.Done()
			c := n.conns[w]
			local := result{}
			for {
				i := atomic.AddInt64(&next, 1) - 1
				if i >= attempts {
					break
				}
				user := strconv.Itoa(int(i % 20))
				pass, opErr := c.evalSHA(n.sha, 1, "sec:rl:"+user+":frozen", strconv.Itoa(limit), "2000")
				local.Ops++
				if opErr != nil {
					local.Errors++
					continue
				}
				if pass == 0 {
					local.RateLimited++
					continue
				}
				code, opErr := c.evalSHA(stockSHA, 2, "sec:stock:1", "sec:win:1", user)
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
				}
			}
			mu.Lock()
			acc.Ops += local.Ops
			acc.Success += local.Success
			acc.SoldOut += local.SoldOut
			acc.Duplicate += local.Duplicate
			acc.RateLimited += local.RateLimited
			acc.Errors += local.Errors
			mu.Unlock()
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)
	finalStock, err := n.conns[0].getInt("sec:stock:1")
	if err != nil {
		return result{}, err
	}
	winners, err := n.conns[0].scard("sec:win:1")
	if err != nil {
		return result{}, err
	}
	acc.ElapsedSec = elapsed.Seconds()
	acc.QPS = float64(acc.Ops) / elapsed.Seconds()
	acc.FinalStock = finalStock
	acc.Winners = winners
	acc.RedisCommands = commandsProcessed(nodes) - before
	// 放行之后仍走一人一单。20 个用户最多成功 20 次，其余放行记为重复，超过窗口的记为限流。
	acc.Pass = acc.Errors == 0 && winners == 20 && acc.Success == 20 && acc.Duplicate == int64(20*(limit-1)) && acc.RateLimited == attempts-int64(20*limit) && finalStock == stock-20
	acc.Detail = fmt.Sprintf("users=20 limit=%d wantSuccess=20 wantDup=%d wantLimited=%d", limit, 20*(limit-1), attempts-int64(20*limit))
	return acc, nil
}
