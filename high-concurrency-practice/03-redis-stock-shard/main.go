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
	codeSoldOut = 0
	codeOK      = 1
	codeDup     = 2
)

type nodeStat struct {
	Addr       string  `json:"addr"`
	Pid        int     `json:"pid"`
	CPUs       string  `json:"cpus"`
	CPUPercent float64 `json:"cpuPercent"`
	RSSKB      int64   `json:"rssKB"`
	UsedMemory int64   `json:"usedMemory"`
	FinalStock int64   `json:"finalStock"`
	Winners    int64   `json:"winners"`
}

type result struct {
	Scenario      string     `json:"scenario"`
	Note          string     `json:"note"`
	Addrs         string     `json:"addrs"`
	Workers       int        `json:"workers"`
	ElapsedSec    float64    `json:"elapsedSec"`
	Ops           int64      `json:"ops"`
	QPS           float64    `json:"qps"`
	Success       int64      `json:"success"`
	SoldOut       int64      `json:"soldOut"`
	Duplicate     int64      `json:"duplicate"`
	Errors        int64      `json:"errors"`
	InitialStock  int64      `json:"initialStock"`
	FinalStock    int64      `json:"finalStock"`
	Winners       int64      `json:"winners"`
	RedisCommands int64      `json:"redisCommands"`
	RedisCPUSec   float64    `json:"redisCpuSec"`
	RedisCPUpct   float64    `json:"redisCpuPercent"`
	ClientCPUSec  float64    `json:"clientCpuSec"`
	ClientCPUpct  float64    `json:"clientCpuPercent"`
	RedisRSSKB    int64      `json:"redisRssKB"`
	UsedMemory    int64      `json:"usedMemory"`
	P50us         int64      `json:"p50us"`
	P99us         int64      `json:"p99us"`
	P999us        int64      `json:"p999us"`
	Maxus         int64      `json:"maxus"`
	Pass          bool       `json:"pass"`
	Detail        string     `json:"detail"`
	Nodes         []nodeStat `json:"nodes,omitempty"`
}

func main() {
	scenario := flag.String("scenario", "", "see run.sh")
	addrs := flag.String("addrs", "127.0.0.1:6379", "comma separated redis addrs")
	workers := flag.Int("workers", 32, "goroutines, one connection each")
	duration := flag.Duration("duration", 4*time.Second, "throughput window after warmup")
	warmup := flag.Duration("warmup", time.Second, "throughput warmup")
	stock := flag.Int64("stock", 200000, "total stock; two-node scenarios split it in half")
	users := flag.Int("users", 400, "distinct users in correctness runs")
	repeat := flag.Int("repeat", 10, "requests per user in correctness runs")
	luaDir := flag.String("lua", "lua", "directory of lua scripts")
	out := flag.String("out", "", "write this result json here")
	flag.Parse()

	stockLua, err := os.ReadFile(*luaDir + "/stock.lua")
	if err != nil {
		fatal(err)
	}
	takeLua, err := os.ReadFile(*luaDir + "/take.lua")
	if err != nil {
		fatal(err)
	}
	nodes := strings.Split(*addrs, ",")
	var res result
	switch *scenario {
	case "hash-correct":
		res, err = runHashed("hash-correct", "用户按 id 取模进两个进程，库存对半。一人一单只在自己的桶里成立。", nodes, string(stockLua), *workers, *users, *repeat, nil, "even")
	case "hash-skew":
		res, err = runHashed("hash-skew", "流量全部打到 0 号桶。1 号桶的库存会剩下来，不能当成还能卖。", nodes, string(stockLua), *workers, *users, *repeat, nil, "skew")
	case "hash-unfair":
		res, err = runHashed("hash-unfair", "流量对半，库存 10 和 90。薄桶先售罄时，厚桶里还有库存，取模用户进不去。", nodes, string(stockLua), *workers, *users, *repeat, []int64{10, 90}, "unfair")
	case "fallback-dup":
		res, err = runFallbackDup(nodes, string(stockLua), *users)
	case "rebalance-gap":
		res, err = runRebalanceGap(nodes, *stock)
	case "rebalance-unsafe":
		res, err = runRebalanceUnsafe(nodes, string(stockLua), *workers, *stock)
	case "rebalance-lua":
		res, err = runRebalanceLua(nodes, string(stockLua), string(takeLua), *workers, *stock)
	case "one-tps":
		res, err = runOneTPS(nodes, string(stockLua), *workers, *duration, *warmup, *stock)
	case "two-keys-tps":
		res, err = runTwoKeysTPS(nodes, string(stockLua), *workers, *duration, *warmup, *stock)
	case "two-tps":
		res, err = runTwoTPS(nodes, string(stockLua), *workers, *duration, *warmup, *stock)
	case "drain-one":
		res, err = runDrain("drain-one", "一个进程把库存卖完的墙钟时间。", nodes[:1], string(stockLua), *workers, *stock, false)
	case "drain-two":
		res, err = runDrain("drain-two", "同样的库存对半放到两个进程，卖完的墙钟时间。", nodes, string(stockLua), *workers, *stock, true)
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
	if workers < 1 {
		return nil, fmt.Errorf("workers must be >= 1")
	}
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

func mustTwo(addrs []string) error {
	if len(addrs) < 2 {
		return fmt.Errorf("this scenario needs two redis addrs")
	}
	return nil
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

func memoryOf(n *node) int64 {
	info, err := n.conns[0].info("memory")
	if err != nil {
		return 0
	}
	v, _ := strconv.ParseInt(infoValue(info, "used_memory"), 10, 64)
	return v
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

func cpusAllowed(pid int) string {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "Cpus_allowed_list:") {
			return strings.TrimSpace(strings.TrimPrefix(line, "Cpus_allowed_list:"))
		}
	}
	return ""
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

func setStock(n *node, key string, stock int64) error {
	_, err := n.conns[0].cmd("SET", key, strconv.FormatInt(stock, 10))
	return err
}

func runHashed(name, note string, addrs []string, script string, workers, users, repeat int, stocks []int64, mode string) (result, error) {
	if err := mustTwo(addrs); err != nil {
		return result{}, err
	}
	if users < 2 || repeat < 1 || users%2 != 0 {
		return result{}, fmt.Errorf("users must be even and at least 2, repeat >= 1")
	}
	nodes, err := prepare(addrs[:2], script, workers)
	if err != nil {
		return result{}, err
	}
	defer closeNodes(nodes)
	if stocks == nil {
		stocks = []int64{50, 50}
	}
	if len(stocks) != 2 {
		return result{}, fmt.Errorf("stocks must have 2 values")
	}
	for i, n := range nodes {
		if err := setStock(n, "sec:stock:1", stocks[i]); err != nil {
			return result{}, err
		}
	}
	shardUsers := users / 2
	perShard := int64(shardUsers * repeat)
	if mode == "skew" {
		shardUsers = users
		perShard = int64(users * repeat)
	}
	var next [2]int64
	var noted atomic.Bool
	var fatLeft atomic.Int64
	var fatConn *Conn
	if mode == "unfair" {
		fatConn, err = dial(nodes[1].addr)
		if err != nil {
			return result{}, err
		}
		defer fatConn.Close()
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	acc := result{
		Scenario:     name,
		Note:         note,
		Workers:      workers,
		InitialStock: stocks[0] + stocks[1],
		Addrs:        nodes[0].addr + "," + nodes[1].addr,
	}
	before := commandsProcessed(nodes)
	start := time.Now()
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			defer wg.Done()
			shard := w % 2
			if mode == "skew" {
				shard = 0
			}
			c := nodes[shard].conns[w]
			local := result{}
			for {
				i := atomic.AddInt64(&next[shard], 1) - 1
				if i >= perShard {
					break
				}
				var user int
				if mode == "skew" {
					user = int(i % int64(users))
				} else {
					user = int(i%int64(users/2))*2 + shard
				}
				code, opErr := c.evalSHA(nodes[shard].sha, 2, "sec:stock:1", "sec:win:1", strconv.Itoa(user))
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
					if mode == "unfair" && shard == 0 && noted.CompareAndSwap(false, true) {
						left, readErr := fatConn.getInt("sec:stock:1")
						if readErr != nil {
							local.Errors++
							break
						}
						fatLeft.Store(left)
					}
				case codeDup:
					local.Duplicate++
				default:
					local.Errors++
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
	acc.ElapsedSec = time.Since(start).Seconds()
	if acc.ElapsedSec > 0 {
		acc.QPS = float64(acc.Ops) / acc.ElapsedSec
	}
	acc.RedisCommands = commandsProcessed(nodes) - before
	fillNodes(&acc, nodes, keySingle)
	overlap, err := overlapWinners(nodes)
	if err != nil {
		return result{}, err
	}
	s0, w0 := stocks[0], acc.Nodes[0].Winners
	s1, w1 := stocks[1], acc.Nodes[1].Winners
	acc.Detail = fmt.Sprintf("stock0=%d win0=%d final0=%d stock1=%d win1=%d final1=%d overlap=%d fatLeft=%d",
		s0, w0, acc.Nodes[0].FinalStock, s1, w1, acc.Nodes[1].FinalStock, overlap, fatLeft.Load())
	switch mode {
	case "even":
		ok0 := shardExact(w0, acc.Nodes[0].FinalStock, s0, users/2, repeat)
		ok1 := shardExact(w1, acc.Nodes[1].FinalStock, s1, users/2, repeat)
		dup, sold := expectSplit(s0, users/2, repeat, s1, users/2, repeat)
		acc.Pass = acc.Errors == 0 && overlap == 0 && ok0 && ok1 && acc.Success == s0+s1 && acc.Duplicate == dup && acc.SoldOut == sold
	case "skew":
		ok0 := shardExact(w0, acc.Nodes[0].FinalStock, s0, users, repeat)
		dup, sold := expectSplit(s0, users, repeat, 0, 0, 0)
		acc.Pass = acc.Errors == 0 && overlap == 0 && ok0 && w1 == 0 && acc.Nodes[1].FinalStock == s1 && s1 > 0 && acc.Success == s0 && acc.Duplicate == dup && acc.SoldOut == sold
	case "unfair":
		ok0 := shardExact(w0, acc.Nodes[0].FinalStock, s0, users/2, repeat)
		ok1 := shardExact(w1, acc.Nodes[1].FinalStock, s1, users/2, repeat)
		dup, sold := expectSplit(s0, users/2, repeat, s1, users/2, repeat)
		acc.Pass = acc.Errors == 0 && overlap == 0 && ok0 && ok1 && acc.Success == s0+s1 && acc.Duplicate == dup && acc.SoldOut == sold && fatLeft.Load() > 0
	default:
		return result{}, fmt.Errorf("unknown hash mode %s", mode)
	}
	return acc, nil
}

func expectSplit(stockA int64, usersA, repeatA int, stockB int64, usersB, repeatB int) (int64, int64) {
	dupA, soldA := expectTraffic(stockA, usersA, repeatA)
	dupB, soldB := expectTraffic(stockB, usersB, repeatB)
	return dupA + dupB, soldA + soldB
}

func expectTraffic(stock int64, users, repeat int) (int64, int64) {
	if users == 0 || repeat == 0 || stock == 0 {
		return 0, 0
	}
	dup := stock * int64(repeat-1)
	sold := int64(users*repeat) - stock - dup
	return dup, sold
}

func shardExact(winners, final, stock int64, users, repeat int) bool {
	if int64(users) < stock {
		return false
	}
	wantDup := stock * int64(repeat-1)
	wantSold := int64(users*repeat) - stock - wantDup
	return winners == stock && final == 0 && wantDup >= 0 && wantSold >= 0
}

func overlapWinners(nodes []*node) (int, error) {
	a, err := nodes[0].conns[0].stringArray("SMEMBERS", "sec:win:1")
	if err != nil {
		return 0, err
	}
	b, err := nodes[1].conns[0].stringArray("SMEMBERS", "sec:win:1")
	if err != nil {
		return 0, err
	}
	set := make(map[string]struct{}, len(a))
	for _, u := range a {
		set[u] = struct{}{}
	}
	n := 0
	for _, u := range b {
		if _, ok := set[u]; ok {
			n++
		}
	}
	return n, nil
}

func runFallbackDup(addrs []string, script string, users int) (result, error) {
	if err := mustTwo(addrs); err != nil {
		return result{}, err
	}
	if users < 2 || users%2 != 0 {
		return result{}, fmt.Errorf("users must be even")
	}
	nodes, err := prepare(addrs[:2], script, 1)
	if err != nil {
		return result{}, err
	}
	defer closeNodes(nodes)
	half := int64(users / 2)
	for _, n := range nodes {
		if err := setStock(n, "sec:stock:1", half); err != nil {
			return result{}, err
		}
	}
	acc := result{
		Scenario:     "fallback-dup",
		Note:         "同一个用户同时打两个桶，桶之间没有共享的已购集合。库存能卖完，但一个人可以买到两件。",
		Workers:      2,
		InitialStock: half * 2,
		Addrs:        nodes[0].addr + "," + nodes[1].addr,
	}
	before := commandsProcessed(nodes)
	start := time.Now()
	wins := [2]map[int]struct{}{{}, {}}
	for u := 0; u < users; u++ {
		var wg sync.WaitGroup
		codes := [2]int64{}
		errs := [2]error{}
		wg.Add(2)
		for s := 0; s < 2; s++ {
			go func(s int) {
				defer wg.Done()
				codes[s], errs[s] = nodes[s].conns[0].evalSHA(nodes[s].sha, 2, "sec:stock:1", "sec:win:1", strconv.Itoa(u))
			}(s)
		}
		wg.Wait()
		for s := 0; s < 2; s++ {
			acc.Ops++
			if errs[s] != nil {
				acc.Errors++
				continue
			}
			switch codes[s] {
			case codeOK:
				acc.Success++
				wins[s][u] = struct{}{}
			case codeSoldOut:
				acc.SoldOut++
			case codeDup:
				acc.Duplicate++
			default:
				acc.Errors++
			}
		}
	}
	acc.ElapsedSec = time.Since(start).Seconds()
	if acc.ElapsedSec > 0 {
		acc.QPS = float64(acc.Ops) / acc.ElapsedSec
	}
	acc.RedisCommands = commandsProcessed(nodes) - before
	fillNodes(&acc, nodes, keySingle)
	overlap := 0
	for u := range wins[0] {
		if _, ok := wins[1][u]; ok {
			overlap++
		}
	}
	distinct := len(wins[0])
	for u := range wins[1] {
		if _, ok := wins[0][u]; !ok {
			distinct++
		}
	}
	acc.Detail = fmt.Sprintf("overlap=%d distinct=%d half=%d", overlap, distinct, half)
	acc.Pass = acc.Errors == 0 && overlap == int(half) && distinct == int(half) && acc.Success == half*2 &&
		acc.SoldOut == half*2 && acc.FinalStock == 0 && acc.Winners == half*2 && acc.Duplicate == 0
	return acc, nil
}

func runRebalanceGap(addrs []string, stock int64) (result, error) {
	if err := mustTwo(addrs); err != nil {
		return result{}, err
	}
	if stock <= 0 {
		return result{}, fmt.Errorf("stock must be positive")
	}
	nodes, err := prepare(addrs[:2], "return 1", 1)
	if err != nil {
		return result{}, err
	}
	defer closeNodes(nodes)
	if err := setStock(nodes[0], "sec:stock:1", 0); err != nil {
		return result{}, err
	}
	if err := setStock(nodes[1], "sec:stock:1", stock); err != nil {
		return result{}, err
	}
	v, err := nodes[1].conns[0].getInt("sec:stock:1")
	if err != nil {
		return result{}, err
	}
	if _, err := nodes[1].conns[0].cmd("SET", "sec:stock:1", "0"); err != nil {
		return result{}, err
	}
	acc := result{
		Scenario:     "rebalance-gap",
		Note:         "从 1 号桶把库存读走并清零，进程在写入 0 号桶之前停掉。这批库存从两个进程里都消失。",
		Workers:      1,
		InitialStock: stock,
		Addrs:        nodes[0].addr + "," + nodes[1].addr,
	}
	fillNodes(&acc, nodes, keySingle)
	acc.Pass = v == stock && acc.FinalStock == 0 && acc.Winners == 0 && acc.Errors == 0
	acc.Detail = fmt.Sprintf("read=%d final=%d lost=%d", v, acc.FinalStock, v-acc.FinalStock)
	acc.Ops = 2
	return acc, nil
}

func runRebalanceUnsafe(addrs []string, script string, workers int, stock int64) (result, error) {
	if err := mustTwo(addrs); err != nil {
		return result{}, err
	}
	nodes, err := prepare(addrs[:2], script, workers)
	if err != nil {
		return result{}, err
	}
	defer closeNodes(nodes)
	if err := setStock(nodes[0], "sec:stock:1", 0); err != nil {
		return result{}, err
	}
	if err := setStock(nodes[1], "sec:stock:1", stock); err != nil {
		return result{}, err
	}
	var stop atomic.Bool
	var seq atomic.Int64
	var errs atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			defer wg.Done()
			c := nodes[1].conns[w]
			for !stop.Load() {
				id := seq.Add(1)
				_, opErr := c.evalSHA(nodes[1].sha, 2, "sec:stock:1", "sec:win:1", strconv.FormatInt(id, 10))
				if opErr != nil {
					errs.Add(1)
				}
			}
		}(w)
	}
	time.Sleep(20 * time.Millisecond)
	admin, err := dial(nodes[1].addr)
	if err != nil {
		stop.Store(true)
		wg.Wait()
		return result{}, err
	}
	defer admin.Close()
	read, err := admin.getInt("sec:stock:1")
	if err != nil {
		stop.Store(true)
		wg.Wait()
		return result{}, err
	}
	time.Sleep(30 * time.Millisecond)
	if _, err := admin.cmd("SET", "sec:stock:1", "0"); err != nil {
		stop.Store(true)
		wg.Wait()
		return result{}, err
	}
	if _, err := nodes[0].conns[0].cmd("INCRBY", "sec:stock:1", strconv.FormatInt(read, 10)); err != nil {
		stop.Store(true)
		wg.Wait()
		return result{}, err
	}
	time.Sleep(10 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
	acc := result{
		Scenario:     "rebalance-unsafe",
		Note:         "搬库存用 GET 再 SET。中间还有人在买，按读到的旧值加到另一个进程，账上的库存会变多。",
		Workers:      workers,
		InitialStock: stock,
		Addrs:        nodes[0].addr + "," + nodes[1].addr,
		ElapsedSec:   time.Since(start).Seconds(),
		Errors:       errs.Load(),
	}
	fillNodes(&acc, nodes, keySingle)
	accounted := acc.Winners + acc.FinalStock
	acc.Detail = fmt.Sprintf("read=%d accounted=%d phantom=%d", read, accounted, accounted-stock)
	acc.Pass = acc.Errors == 0 && read > 0 && accounted > stock && acc.Nodes[1].FinalStock == 0
	return acc, nil
}

func runRebalanceLua(addrs []string, stockLua, takeLua string, workers int, stock int64) (result, error) {
	if err := mustTwo(addrs); err != nil {
		return result{}, err
	}
	nodes, err := prepare(addrs[:2], stockLua, workers)
	if err != nil {
		return result{}, err
	}
	defer closeNodes(nodes)
	admin, err := dial(nodes[1].addr)
	if err != nil {
		return result{}, err
	}
	defer admin.Close()
	takeSHA, err := admin.scriptLoad(takeLua)
	if err != nil {
		return result{}, err
	}
	if err := setStock(nodes[0], "sec:stock:1", 0); err != nil {
		return result{}, err
	}
	if err := setStock(nodes[1], "sec:stock:1", stock); err != nil {
		return result{}, err
	}
	var stop atomic.Bool
	var seq atomic.Int64
	var errs atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			defer wg.Done()
			c := nodes[1].conns[w]
			for !stop.Load() {
				id := seq.Add(1)
				_, opErr := c.evalSHA(nodes[1].sha, 2, "sec:stock:1", "sec:win:1", strconv.FormatInt(id, 10))
				if opErr != nil {
					errs.Add(1)
				}
			}
		}(w)
	}
	time.Sleep(30 * time.Millisecond)
	taken, err := admin.evalSHA(takeSHA, 1, "sec:stock:1")
	if err != nil {
		stop.Store(true)
		wg.Wait()
		return result{}, err
	}
	if _, err := nodes[0].conns[0].cmd("INCRBY", "sec:stock:1", strconv.FormatInt(taken, 10)); err != nil {
		stop.Store(true)
		wg.Wait()
		return result{}, err
	}
	time.Sleep(10 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
	acc := result{
		Scenario:     "rebalance-lua",
		Note:         "源桶用 Lua 一次取走剩余库存，再 INCRBY 到目标桶。两边的写入都做完时，卖出加剩余等于最初库存。",
		Workers:      workers,
		InitialStock: stock,
		Addrs:        nodes[0].addr + "," + nodes[1].addr,
		ElapsedSec:   time.Since(start).Seconds(),
		Errors:       errs.Load(),
	}
	fillNodes(&acc, nodes, keySingle)
	accounted := acc.Winners + acc.FinalStock
	acc.Detail = fmt.Sprintf("taken=%d accounted=%d movedTo0=%d", taken, accounted, acc.Nodes[0].FinalStock)
	acc.Pass = acc.Errors == 0 && taken > 0 && accounted == stock && acc.Nodes[0].FinalStock == taken && acc.Nodes[1].FinalStock == 0
	return acc, nil
}

type keyMode int

const (
	keySingle keyMode = iota
	keyTwoOnFirst
)

func fillNodes(res *result, nodes []*node, mode keyMode) {
	res.Nodes = res.Nodes[:0]
	var stock, winners, rss, mem int64
	for _, n := range nodes {
		st, win := readNode(n, mode)
		ns := nodeStat{
			Addr:       n.addr,
			Pid:        n.pid,
			CPUs:       cpusAllowed(n.pid),
			RSSKB:      rssKB(n.pid),
			UsedMemory: memoryOf(n),
			FinalStock: st,
			Winners:    win,
		}
		res.Nodes = append(res.Nodes, ns)
		stock += st
		winners += win
		rss += ns.RSSKB
		mem += ns.UsedMemory
		if mode == keyTwoOnFirst {
			break
		}
	}
	res.FinalStock = stock
	res.Winners = winners
	res.RedisRSSKB = rss
	res.UsedMemory = mem
}

func readNode(n *node, mode keyMode) (int64, int64) {
	if mode == keyTwoOnFirst {
		s0, _ := n.conns[0].getInt("sec:stock:1:0")
		s1, _ := n.conns[0].getInt("sec:stock:1:1")
		w0, _ := n.conns[0].scard("sec:win:1:0")
		w1, _ := n.conns[0].scard("sec:win:1:1")
		return s0 + s1, w0 + w1
	}
	s, _ := n.conns[0].getInt("sec:stock:1")
	w, _ := n.conns[0].scard("sec:win:1")
	return s, w
}

type tpsAcc struct {
	ops     int64
	success int64
	soldOut int64
	dup     int64
	errs    int64
	h       *hist
}

func runOneTPS(addrs []string, script string, workers int, duration, warmup time.Duration, stock int64) (result, error) {
	nodes, err := prepare(addrs[:1], script, workers)
	if err != nil {
		return result{}, err
	}
	defer closeNodes(nodes)
	if err := setStock(nodes[0], "sec:stock:1", stock); err != nil {
		return result{}, err
	}
	return drive("one-tps", "一个进程、一个库存 key 的成功扣减。后面的双进程拿这组当对照。", nodes, workers, duration, warmup, stock, keySingle, func(c *Conn, n *node, id int64) (int64, error) {
		return c.evalSHA(n.sha, 2, "sec:stock:1", "sec:win:1", strconv.FormatInt(id, 10))
	})
}

func runTwoKeysTPS(addrs []string, script string, workers int, duration, warmup time.Duration, stock int64) (result, error) {
	if stock%2 != 0 {
		return result{}, fmt.Errorf("stock must be even")
	}
	nodes, err := prepare(addrs[:1], script, workers)
	if err != nil {
		return result{}, err
	}
	defer closeNodes(nodes)
	half := stock / 2
	if err := setStock(nodes[0], "sec:stock:1:0", half); err != nil {
		return result{}, err
	}
	if err := setStock(nodes[0], "sec:stock:1:1", half); err != nil {
		return result{}, err
	}
	res, err := drive("two-keys-tps", "两个库存 key 放在同一个进程。Redis 还是一个线程，成功 QPS 不该翻倍。", nodes, workers, duration, warmup, stock, keyTwoOnFirst, func(c *Conn, n *node, id int64) (int64, error) {
		shard := id % 2
		return c.evalSHA(n.sha, 2, fmt.Sprintf("sec:stock:1:%d", shard), fmt.Sprintf("sec:win:1:%d", shard), strconv.FormatInt(id, 10))
	})
	if err != nil {
		return result{}, err
	}
	s0, _ := nodes[0].conns[0].getInt("sec:stock:1:0")
	s1, _ := nodes[0].conns[0].getInt("sec:stock:1:1")
	w0, _ := nodes[0].conns[0].scard("sec:win:1:0")
	w1, _ := nodes[0].conns[0].scard("sec:win:1:1")
	extra := fmt.Sprintf("stock0=%d stock1=%d win0=%d win1=%d", s0, s1, w0, w1)
	if res.Detail == "" {
		res.Detail = extra
	} else {
		res.Detail = res.Detail + " " + extra
	}
	res.Pass = res.Pass && w0 > 0 && w1 > 0 && s0 > 0 && s1 > 0
	return res, nil
}

func runTwoTPS(addrs []string, script string, workers int, duration, warmup time.Duration, stock int64) (result, error) {
	if err := mustTwo(addrs); err != nil {
		return result{}, err
	}
	if stock%2 != 0 {
		return result{}, fmt.Errorf("stock must be even")
	}
	if workers < 2 {
		return result{}, fmt.Errorf("two-tps needs at least 2 workers")
	}
	nodes, err := prepare(addrs[:2], script, workers)
	if err != nil {
		return result{}, err
	}
	defer closeNodes(nodes)
	half := stock / 2
	for _, n := range nodes {
		if err := setStock(n, "sec:stock:1", half); err != nil {
			return result{}, err
		}
	}
	return drive("two-tps", "成功扣减按 worker 固定分到两个进程。每个进程一份库存、一份已购集合。", nodes, workers, duration, warmup, stock, keySingle, func(c *Conn, n *node, id int64) (int64, error) {
		return c.evalSHA(n.sha, 2, "sec:stock:1", "sec:win:1", strconv.FormatInt(id, 10))
	})
}

func drive(name, note string, nodes []*node, workers int, duration, warmup time.Duration, initial int64, mode keyMode, op func(*Conn, *node, int64) (int64, error)) (result, error) {
	type binding struct {
		c *Conn
		n *node
	}
	binds := make([]binding, workers)
	for i := 0; i < workers; i++ {
		n := nodes[i%len(nodes)]
		binds[i] = binding{c: n.conns[i], n: n}
	}
	var seq atomic.Int64
	warmEnd := time.Now().Add(warmup)
	for time.Now().Before(warmEnd) {
		var wg sync.WaitGroup
		wg.Add(workers)
		for i := 0; i < workers; i++ {
			go func(i int) {
				defer wg.Done()
				deadline := time.Now().Add(50 * time.Millisecond)
				for time.Now().Before(deadline) && time.Now().Before(warmEnd) {
					id := seq.Add(1)
					_, _ = op(binds[i].c, binds[i].n, id)
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
	client0 := cpuTicks(os.Getpid())
	start := time.Now()
	end := start.Add(duration)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		hists[i] = newHist()
		go func(i int) {
			defer wg.Done()
			c := binds[i].c
			n := binds[i].n
			h := hists[i]
			for time.Now().Before(end) {
				id := seq.Add(1)
				t0 := time.Now()
				code, err := op(c, n, id)
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
				default:
					locals[i].errs++
				}
			}
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)
	client1 := cpuTicks(os.Getpid())
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
		res.Errors += locals[i].errs
		merged.merge(hists[i])
	}
	res.ElapsedSec = elapsed.Seconds()
	if elapsed > 0 {
		res.QPS = float64(res.Ops) / elapsed.Seconds()
	}
	res.RedisCommands = commandsProcessed(nodes) - before
	var cpu1 float64
	for i, n := range nodes {
		cpu1 += cpuTicks(n.pid) - cpu0[i]
	}
	res.RedisCPUSec = cpu1 / 100
	res.ClientCPUSec = (client1 - client0) / 100
	if elapsed > 0 {
		res.RedisCPUpct = res.RedisCPUSec / elapsed.Seconds() * 100
		res.ClientCPUpct = res.ClientCPUSec / elapsed.Seconds() * 100
	}
	fillNodes(&res, nodes, mode)
	for i := range res.Nodes {
		ticks := cpuTicks(nodes[i].pid) - cpu0[i]
		if elapsed > 0 {
			res.Nodes[i].CPUPercent = (ticks / 100) / elapsed.Seconds() * 100
		}
	}
	res.P50us = merged.le(0.50) / 1000
	res.P99us = merged.le(0.99) / 1000
	res.P999us = merged.le(0.999) / 1000
	res.Maxus = merged.max / 1000
	res.Pass = res.Errors == 0 && res.Ops > 0 && res.Success == res.Ops && res.SoldOut == 0 && res.Duplicate == 0 &&
		res.FinalStock > 0 && res.Winners+res.FinalStock == initial
	return res, nil
}

func runDrain(name, note string, addrs []string, script string, workers int, stock int64, split bool) (result, error) {
	if split {
		if err := mustTwo(addrs); err != nil {
			return result{}, err
		}
		if stock%2 != 0 {
			return result{}, fmt.Errorf("stock must be even")
		}
		addrs = addrs[:2]
	} else {
		addrs = addrs[:1]
	}
	if workers < len(addrs) {
		return result{}, fmt.Errorf("need at least one worker per redis")
	}
	nodes, err := prepare(addrs, script, workers)
	if err != nil {
		return result{}, err
	}
	defer closeNodes(nodes)
	each := stock
	if split {
		each = stock / 2
	}
	for _, n := range nodes {
		if err := setStock(n, "sec:stock:1", each); err != nil {
			return result{}, err
		}
	}
	total := each * int64(len(nodes))
	type binding struct {
		c *Conn
		n *node
	}
	binds := make([]binding, workers)
	for i := 0; i < workers; i++ {
		n := nodes[i%len(nodes)]
		binds[i] = binding{c: n.conns[i], n: n}
	}
	var seq atomic.Int64
	var success atomic.Int64
	var hit atomic.Int64
	var stop atomic.Bool
	var errs atomic.Int64
	var ops atomic.Int64
	var sold atomic.Int64
	hists := make([]*hist, workers)
	cpu0 := make([]float64, len(nodes))
	for i, n := range nodes {
		cpu0[i] = cpuTicks(n.pid)
	}
	client0 := cpuTicks(os.Getpid())
	before := commandsProcessed(nodes)
	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		hists[i] = newHist()
		go func(i int) {
			defer wg.Done()
			for !stop.Load() {
				id := seq.Add(1)
				t0 := time.Now()
				code, opErr := binds[i].c.evalSHA(binds[i].n.sha, 2, "sec:stock:1", "sec:win:1", strconv.FormatInt(id, 10))
				hists[i].add(time.Since(t0).Nanoseconds())
				ops.Add(1)
				if opErr != nil {
					errs.Add(1)
					continue
				}
				if code == codeOK {
					n := success.Add(1)
					if n == total {
						hit.Store(time.Now().UnixNano())
						stop.Store(true)
					}
				} else if code == codeSoldOut {
					sold.Add(1)
				}
			}
		}(i)
	}
	deadline := time.Now().Add(30 * time.Second)
	for hit.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	stop.Store(true)
	wg.Wait()
	joined := time.Since(start)
	client1 := cpuTicks(os.Getpid())
	res := result{
		Scenario:     name,
		Note:         note,
		Workers:      workers,
		InitialStock: total,
		Success:      success.Load(),
		SoldOut:      sold.Load(),
		Errors:       errs.Load(),
		Ops:          ops.Load(),
	}
	var addr []string
	for _, n := range nodes {
		addr = append(addr, n.addr)
	}
	res.Addrs = strings.Join(addr, ",")
	if ts := hit.Load(); ts > 0 {
		res.ElapsedSec = time.Unix(0, ts).Sub(start).Seconds()
		if res.ElapsedSec > 0 {
			res.QPS = float64(total) / res.ElapsedSec
		}
	}
	res.RedisCommands = commandsProcessed(nodes) - before
	var cpu1 float64
	for i, n := range nodes {
		cpu1 += cpuTicks(n.pid) - cpu0[i]
	}
	res.RedisCPUSec = cpu1 / 100
	res.ClientCPUSec = (client1 - client0) / 100
	if joined > 0 {
		res.RedisCPUpct = res.RedisCPUSec / joined.Seconds() * 100
		res.ClientCPUpct = res.ClientCPUSec / joined.Seconds() * 100
	}
	fillNodes(&res, nodes, keySingle)
	for i := range res.Nodes {
		ticks := cpuTicks(nodes[i].pid) - cpu0[i]
		if joined > 0 {
			res.Nodes[i].CPUPercent = (ticks / 100) / joined.Seconds() * 100
		}
	}
	merged := newHist()
	for i := 0; i < workers; i++ {
		merged.merge(hists[i])
	}
	res.P50us = merged.le(0.50) / 1000
	res.P99us = merged.le(0.99) / 1000
	res.P999us = merged.le(0.999) / 1000
	res.Maxus = merged.max / 1000
	res.Detail = fmt.Sprintf("joinSec=%.6f each=%d", joined.Seconds(), each)
	balanced := true
	for _, ns := range res.Nodes {
		if ns.Winners != each || ns.FinalStock != 0 {
			balanced = false
		}
	}
	res.Pass = res.Errors == 0 && hit.Load() > 0 && res.Success == total && res.Winners == total && res.FinalStock == 0 && balanced
	return res, nil
}
