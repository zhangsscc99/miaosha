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
	"syscall"
	"time"
)

const (
	codeSoldOut = 0
	codeOK      = 1
	codeDup     = 2
)

type result struct {
	Scenario        string  `json:"scenario"`
	Note            string  `json:"note"`
	Addr            string  `json:"addr"`
	Cgroup          string  `json:"cgroup,omitempty"`
	Parent          string  `json:"parentCgroup,omitempty"`
	Workers         int     `json:"workers"`
	ElapsedSec      float64 `json:"elapsedSec"`
	Ops             int64   `json:"ops"`
	QPS             float64 `json:"qps"`
	Success         int64   `json:"success"`
	SoldOut         int64   `json:"soldOut"`
	Duplicate       int64   `json:"duplicate"`
	Errors          int64   `json:"errors"`
	InitialStock    int64   `json:"initialStock"`
	FinalStock      int64   `json:"finalStock"`
	Winners         int64   `json:"winners"`
	RedisCPUSec     float64 `json:"redisCpuSec"`
	RedisCPUpct     float64 `json:"redisCpuPercent"`
	SelfCPUSec      float64 `json:"selfCpuSec"`
	SelfCPUpct      float64 `json:"selfCpuPercent"`
	RedisRSSKB      int64   `json:"redisRssKB"`
	UsedMemory      int64   `json:"usedMemory"`
	P50us           int64   `json:"p50us"`
	P99us           int64   `json:"p99us"`
	P999us          int64   `json:"p999us"`
	Maxus           int64   `json:"maxus"`
	Over1ms         int64   `json:"over1ms"`
	Over10ms        int64   `json:"over10ms"`
	Over50ms        int64   `json:"over50ms"`
	CPUMax          string  `json:"cpuMax"`
	CPUWeight       string  `json:"cpuWeight"`
	CgUsageUsec     int64   `json:"cgroupUsageUsec"`
	NrPeriods       int64   `json:"nrPeriods"`
	NrThrottled     int64   `json:"nrThrottled"`
	ThrottledUsec   int64   `json:"throttledUsec"`
	ThrottlePct     float64 `json:"throttlePctOfWall"`
	ParentUsageUsec int64   `json:"parentUsageUsec"`
	ParentThrottled int64   `json:"parentThrottledUsec"`
	MemCurrent      int64   `json:"memCurrent"`
	MemPeak         int64   `json:"memPeak"`
	MemMax          string  `json:"memMax"`
	OOM             int64   `json:"oom"`
	OOMKill         int64   `json:"oomKill"`
	Pid             int     `json:"pid"`
	PidAlive        bool    `json:"pidAlive"`
	KeysWritten     int64   `json:"keysWritten"`
	LastErr         string  `json:"lastErr,omitempty"`
	Pass            bool    `json:"pass"`
	Detail          string  `json:"detail"`
}

func main() {
	scenario := flag.String("scenario", "soldout", "soldout, success, mem-reject, mem-kill")
	addr := flag.String("addr", "127.0.0.1:6379", "redis address")
	workers := flag.Int("workers", 16, "goroutines, one connection each")
	duration := flag.Duration("duration", 4*time.Second, "measure window")
	warmup := flag.Duration("warmup", time.Second, "warmup before the window")
	stock := flag.Int64("stock", 5_000_000, "initial stock for the success path")
	luaDir := flag.String("lua", "lua", "lua directory")
	cgroup := flag.String("cgroup", "", "cgroup directory to read cpu.stat and memory.events")
	parent := flag.String("parent-cgroup", "", "optional parent cgroup, read during the same window")
	note := flag.String("note", "", "overrides the default note")
	out := flag.String("out", "", "write json here")
	startMS := flag.Int64("start-unix-ms", 0, "sleep until this unix milli before warmup")
	valueBytes := flag.Int("value-bytes", 256*1024, "mem-fill value size")
	keys := flag.Int("keys", 200, "mem-fill key count cap")
	maxMemory := flag.Int64("maxmemory", 0, "CONFIG SET maxmemory bytes before mem-fill; 0 leaves the config")
	flag.Parse()

	runtime.GOMAXPROCS(4)
	if *startMS > 0 {
		if d := time.Until(time.UnixMilli(*startMS)); d > 0 {
			time.Sleep(d)
		}
	}

	var res result
	var err error
	switch *scenario {
	case "soldout":
		body := mustRead(*luaDir + "/soldout.lua")
		res, err = runLoop("soldout", defaultNote(*note, "库存已经是 0。脚本只做一次 GET。测的是 cgroup 限额下的售罄读。"), *addr, body, true, *workers, *duration, *warmup, 0, *cgroup, *parent)
	case "success":
		body := mustRead(*luaDir + "/stock.lua")
		res, err = runLoop("success", defaultNote(*note, "成功路径：SISMEMBER、判断库存、DECR、SADD，一次 EVALSHA。库存足够大，窗口内不会卖完。"), *addr, body, false, *workers, *duration, *warmup, *stock, *cgroup, *parent)
	case "mem-reject":
		res, err = runMem("mem-reject", defaultNote(*note, "cgroup 内存限额留得比 maxmemory 宽。写到 maxmemory 之后 Redis 返回 OOM，进程还在。"), *addr, *cgroup, *valueBytes, *keys, *maxMemory, false)
	case "mem-kill":
		res, err = runMem("mem-kill", defaultNote(*note, "cgroup memory.max 比 Redis maxmemory 紧。内核先杀进程，客户端看不到 Redis 的 OOM 文本。"), *addr, *cgroup, *valueBytes, *keys, *maxMemory, true)
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

func defaultNote(flagNote, fallback string) string {
	if flagNote != "" {
		return flagNote
	}
	return fallback
}

func mustRead(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		fatal(err)
	}
	return string(b)
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "error: %v\n", err)
	os.Exit(1)
}

func runLoop(name, note, addr, script string, soldOut bool, workers int, duration, warmup time.Duration, stock int64, cgroup, parent string) (result, error) {
	conns := make([]*Conn, workers)
	for i := 0; i < workers; i++ {
		c, err := dial(addr)
		if err != nil {
			return result{}, err
		}
		conns[i] = c
		defer c.Close()
	}
	if _, err := conns[0].cmd("FLUSHDB"); err != nil {
		return result{}, err
	}
	if _, err := conns[0].cmd("SCRIPT", "FLUSH"); err != nil {
		return result{}, err
	}
	sha, err := conns[0].scriptLoad(script)
	if err != nil {
		return result{}, err
	}
	info, err := conns[0].info("server")
	if err != nil {
		return result{}, err
	}
	pid, _ := strconv.Atoi(infoValue(info, "process_id"))
	if soldOut {
		if _, err := conns[0].cmd("SET", "sec:stock:1", "0"); err != nil {
			return result{}, err
		}
	} else {
		if _, err := conns[0].cmd("SET", "sec:stock:1", strconv.FormatInt(stock, 10)); err != nil {
			return result{}, err
		}
	}

	var seq int64
	warmEnd := time.Now().Add(warmup)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(i int) {
			defer wg.Done()
			for time.Now().Before(warmEnd) {
				id := atomic.AddInt64(&seq, 1)
				_, _ = call(conns[i], sha, soldOut, id)
			}
		}(i)
	}
	wg.Wait()

	cpu0 := cpuTicks(pid)
	self0 := cpuTicks(os.Getpid())
	cg0 := readCgroup(cgroup)
	par0 := readCgroup(parent)
	hists := make([]*hist, workers)
	type local struct {
		ops, ok, sold, dup, errs int64
	}
	locals := make([]local, workers)
	start := time.Now()
	end := start.Add(duration)
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		hists[i] = newHist()
		go func(i int) {
			defer wg.Done()
			h := hists[i]
			for time.Now().Before(end) {
				id := atomic.AddInt64(&seq, 1)
				t0 := time.Now()
				code, opErr := call(conns[i], sha, soldOut, id)
				h.add(time.Since(t0).Nanoseconds())
				locals[i].ops++
				if opErr != nil {
					locals[i].errs++
					continue
				}
				switch code {
				case codeOK:
					locals[i].ok++
				case codeSoldOut:
					locals[i].sold++
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
	cg1 := readCgroup(cgroup)
	par1 := readCgroup(parent)
	merged := newHist()
	res := result{
		Scenario:     name,
		Note:         note,
		Addr:         addr,
		Cgroup:       cgroup,
		Parent:       parent,
		Workers:      workers,
		InitialStock: stock,
		Pid:          pid,
		PidAlive:     pidAlive(pid),
	}
	for i := 0; i < workers; i++ {
		res.Ops += locals[i].ops
		res.Success += locals[i].ok
		res.SoldOut += locals[i].sold
		res.Duplicate += locals[i].dup
		res.Errors += locals[i].errs
		merged.merge(hists[i])
	}
	res.ElapsedSec = elapsed.Seconds()
	if elapsed > 0 {
		res.QPS = float64(res.Ops) / elapsed.Seconds()
	}
	fillCPU(&res, pid, cpu0, self0, elapsed)
	fillCgroup(&res, cg0, cg1, par0, par1, elapsed)
	res.P50us = merged.le(0.50) / 1000
	res.P99us = merged.le(0.99) / 1000
	res.P999us = merged.le(0.999) / 1000
	res.Maxus = merged.max / 1000
	res.Over1ms = merged.over1ms
	res.Over10ms = merged.over10
	res.Over50ms = merged.over50
	if res.PidAlive {
		res.FinalStock, _ = conns[0].getInt("sec:stock:1")
		res.Winners, _ = conns[0].scard("sec:win:1")
		if mem, err := conns[0].info("memory"); err == nil {
			res.UsedMemory, _ = strconv.ParseInt(infoValue(mem, "used_memory"), 10, 64)
		}
		res.RedisRSSKB = rssKB(pid)
	} else {
		res.FinalStock = -1
	}
	if soldOut {
		res.Pass = res.Errors == 0 && res.Ops > 1000 && res.SoldOut == res.Ops && res.PidAlive && res.OOMKill == 0
		res.Detail = fmt.Sprintf("soldOut=%d ops=%d", res.SoldOut, res.Ops)
	} else {
		// 预热的那一秒也在扣库存，所以窗口内的 success 小于集合里的人数。
		// 不变量是集合人数加剩余库存等于初始库存。
		res.Pass = res.Errors == 0 && res.Success > 0 && res.Duplicate == 0 && res.SoldOut == 0 &&
			res.PidAlive && res.OOMKill == 0 && res.Winners >= res.Success &&
			res.FinalStock+res.Winners == stock && res.FinalStock >= 0
		res.Detail = fmt.Sprintf("success=%d winners=%d warmupSuccess=%d finalStock=%d", res.Success, res.Winners, res.Winners-res.Success, res.FinalStock)
	}
	if res.NrPeriods > 0 {
		res.Detail += fmt.Sprintf(" throttledPeriods=%d/%d", res.NrThrottled, res.NrPeriods)
	}
	return res, nil
}

func call(c *Conn, sha string, soldOut bool, id int64) (int64, error) {
	if soldOut {
		return c.evalSHA(sha, 1, "sec:stock:1")
	}
	return c.evalSHA(sha, 2, "sec:stock:1", "sec:win:1", strconv.FormatInt(id, 10))
}

func runMem(name, note, addr, cgroup string, valueBytes, keys int, maxMemory int64, wantKill bool) (result, error) {
	c, err := dial(addr)
	if err != nil {
		return result{}, err
	}
	defer c.Close()
	if _, err := c.cmd("FLUSHDB"); err != nil {
		return result{}, err
	}
	if maxMemory > 0 {
		if _, err := c.cmd("CONFIG", "SET", "maxmemory", strconv.FormatInt(maxMemory, 10)); err != nil {
			return result{}, err
		}
	}
	info, err := c.info("server")
	if err != nil {
		return result{}, err
	}
	pid, _ := strconv.Atoi(infoValue(info, "process_id"))
	cg0 := readCgroup(cgroup)
	payload := strings.Repeat("a", valueBytes)
	res := result{
		Scenario: name,
		Note:     note,
		Addr:     addr,
		Cgroup:   cgroup,
		Workers:  1,
		Pid:      pid,
	}
	start := time.Now()
	var lastErr string
	for i := 0; i < keys; i++ {
		_, err := c.cmd("SET", "mem:"+strconv.Itoa(i), payload)
		if err != nil {
			lastErr = err.Error()
			break
		}
		res.KeysWritten++
	}
	res.ElapsedSec = time.Since(start).Seconds()
	res.LastErr = lastErr
	res.Ops = res.KeysWritten
	cg1 := readCgroup(cgroup)
	fillCgroup(&res, cg0, cg1, cgroupSnap{}, cgroupSnap{}, time.Since(start))
	res.PidAlive = pidAlive(pid)
	if res.PidAlive {
		if mem, err := c.info("memory"); err == nil {
			res.UsedMemory, _ = strconv.ParseInt(infoValue(mem, "used_memory"), 10, 64)
		}
		res.RedisRSSKB = rssKB(pid)
	}
	if wantKill {
		res.Pass = res.KeysWritten > 0 && (!res.PidAlive || res.OOMKill > 0)
		res.Detail = fmt.Sprintf("keys=%d pidAlive=%v oomKill=%d lastErr=%s", res.KeysWritten, res.PidAlive, res.OOMKill, lastErr)
	} else {
		res.Pass = res.PidAlive && res.OOMKill == 0 && res.KeysWritten > 0 && strings.Contains(lastErr, "OOM")
		res.Detail = fmt.Sprintf("keys=%d usedMemory=%d lastErr=%s", res.KeysWritten, res.UsedMemory, lastErr)
	}
	return res, nil
}

func fillCPU(res *result, pid int, cpu0, self0 float64, elapsed time.Duration) {
	res.RedisCPUSec = (cpuTicks(pid) - cpu0) / 100
	res.SelfCPUSec = (cpuTicks(os.Getpid()) - self0) / 100
	if elapsed > 0 {
		res.RedisCPUpct = res.RedisCPUSec / elapsed.Seconds() * 100
		res.SelfCPUpct = res.SelfCPUSec / elapsed.Seconds() * 100
	}
	res.RedisRSSKB = rssKB(pid)
}

type cgroupSnap struct {
	usage, user, system, periods, throttled, throttledUsec int64
	oom, oomKill, memCurrent, memPeak                      int64
	cpuMax, cpuWeight, memMax                              string
	ok                                                     bool
}

func readCgroup(dir string) cgroupSnap {
	if dir == "" {
		return cgroupSnap{}
	}
	s := cgroupSnap{ok: true, cpuMax: strings.TrimSpace(readText(dir + "/cpu.max")), cpuWeight: strings.TrimSpace(readText(dir + "/cpu.weight")), memMax: strings.TrimSpace(readText(dir + "/memory.max"))}
	stat := readKV(dir + "/cpu.stat")
	s.usage = stat["usage_usec"]
	s.user = stat["user_usec"]
	s.system = stat["system_usec"]
	s.periods = stat["nr_periods"]
	s.throttled = stat["nr_throttled"]
	s.throttledUsec = stat["throttled_usec"]
	ev := readKV(dir + "/memory.events")
	s.oom = ev["oom"]
	s.oomKill = ev["oom_kill"]
	s.memCurrent = readIntFile(dir + "/memory.current")
	s.memPeak = readIntFile(dir + "/memory.peak")
	return s
}

func fillCgroup(res *result, before, after, parentBefore, parentAfter cgroupSnap, elapsed time.Duration) {
	if after.ok {
		res.CPUMax = after.cpuMax
		res.CPUWeight = after.cpuWeight
		res.MemMax = after.memMax
		res.CgUsageUsec = after.usage - before.usage
		res.NrPeriods = after.periods - before.periods
		res.NrThrottled = after.throttled - before.throttled
		res.ThrottledUsec = after.throttledUsec - before.throttledUsec
		res.OOM = after.oom - before.oom
		res.OOMKill = after.oomKill - before.oomKill
		res.MemCurrent = after.memCurrent
		res.MemPeak = after.memPeak
		if elapsed > 0 {
			res.ThrottlePct = float64(res.ThrottledUsec) / float64(elapsed.Microseconds()) * 100
		}
	}
	if parentAfter.ok {
		res.ParentUsageUsec = parentAfter.usage - parentBefore.usage
		res.ParentThrottled = parentAfter.throttledUsec - parentBefore.throttledUsec
	}
}

func readText(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return string(b)
}

func readIntFile(path string) int64 {
	s := strings.TrimSpace(readText(path))
	if s == "" || s == "max" {
		return 0
	}
	v, _ := strconv.ParseInt(s, 10, 64)
	return v
}

func readKV(path string) map[string]int64 {
	out := map[string]int64{}
	for _, line := range strings.Split(readText(path), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		v, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			continue
		}
		out[fields[0]] = v
	}
	return out
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

func pidAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil
}
