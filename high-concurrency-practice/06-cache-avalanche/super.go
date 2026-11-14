package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"
)

type result struct {
	Scenario       string        `json:"scenario"`
	Note           string        `json:"note"`
	Mode           string        `json:"mode"`
	Keys           int           `json:"keys"`
	Repeat         int           `json:"repeat"`
	Instances      int           `json:"instances"`
	Workers        int           `json:"workersPerInstance"`
	Pool           int           `json:"originPool"`
	ServiceMs      int           `json:"serviceMs"`
	Hz             int           `json:"hz,omitempty"`
	PayloadBytes   int           `json:"payloadBytes"`
	ProbeSQLNs     int64         `json:"probeSqlNs,omitempty"`
	ProbeSlowNs    int64         `json:"probeSlowNs,omitempty"`
	BurnLoops      int           `json:"burnLoops,omitempty"`
	Ops            int64         `json:"ops"`
	QPS            float64       `json:"qps"`
	Hits           int64         `json:"hits"`
	Misses         int64         `json:"misses"`
	Errors         int64         `json:"errors"`
	Polls          int64         `json:"polls"`
	OriginLoads    int64         `json:"originLoads"`
	ChildLoads     int64         `json:"childLoads"`
	SlotAvgNs      int64         `json:"originSlotAvgNs,omitempty"`
	SlotMaxNs      int64         `json:"originSlotMaxNs,omitempty"`
	RedisCmds      int64         `json:"redisCommands"`
	ChildRedisCmds int64         `json:"childRedisCmds"`
	DBSize         int64         `json:"dbSize"`
	UsedMemory     int64         `json:"usedMemory"`
	FinalMemory    int64         `json:"finalMemory,omitempty"`
	BaseMemory     int64         `json:"baseMemory,omitempty"`
	Evicted        int64         `json:"evictedKeys"`
	RedisRSSKB     int64         `json:"redisRssKB"`
	RedisCPUSec    float64       `json:"redisCpuSec"`
	RedisCPUpct    float64       `json:"redisCpuPercent"`
	OriginCPUSec   float64       `json:"originCpuSec"`
	OriginCPUpct   float64       `json:"originCpuPercent"`
	ClientCPUSec   float64       `json:"clientCpuSec"`
	ClientCPUpct   float64       `json:"clientCpuPercent"`
	WallSec        float64       `json:"wallSec"`
	P50us          int64         `json:"p50us"`
	P99us          int64         `json:"p99us"`
	Maxus          int64         `json:"maxus"`
	MissP50us      int64         `json:"missP50us"`
	MissP99us      int64         `json:"missP99us"`
	MissMaxus      int64         `json:"missMaxus"`
	HitP99us       int64         `json:"hitP99us"`
	PeakOrigin     int64         `json:"peakOrigin"`
	PeakOrigin2    int64         `json:"peakOrigin2"`
	BucketMs       int           `json:"bucketMs,omitempty"`
	OriginBuckets  []int64       `json:"originBuckets,omitempty"`
	MissBuckets    []int64       `json:"missBuckets,omitempty"`
	Slide100ms     int64         `json:"slide100ms"`
	MaxDrop        int64         `json:"maxDrop,omitempty"`
	MaxPingUs      int64         `json:"maxPingUs,omitempty"`
	FinalKeys      int64         `json:"finalKeys,omitempty"`
	FinalAvgTTL    int64         `json:"finalAvgTtlMs,omitempty"`
	Samples        []sample      `json:"samples,omitempty"`
	Pass           bool          `json:"pass"`
	Detail         string        `json:"detail"`
	Children       []childResult `json:"children,omitempty"`
}

type sample struct {
	Tms          int64 `json:"tMs"`
	Alive        int64 `json:"alive"`
	ExpiredKeys  int64 `json:"expiredKeys"`
	KeyspaceKeys int64 `json:"keyspaceKeys"`
	AvgTTLMs     int64 `json:"avgTtlMs"`
	PingUs       int64 `json:"pingUs"`
	SampleUs     int64 `json:"sampleUs"`
}

func runSuper(cfg config) (result, error) {
	if strings.HasPrefix(cfg.Mode, "expire-") {
		return runExpire(cfg)
	}
	return runLoad(cfg)
}

func runLoad(cfg config) (result, error) {
	payload := goodsPayload()
	var org *origin
	if needsOrigin(cfg.Mode) {
		var err error
		org, err = startOrigin("/tmp/miaosha-avalanche-goods.db", payload, cfg.Pool, time.Duration(cfg.ServiceMs)*time.Millisecond)
		if err != nil {
			return result{}, err
		}
		cfg.Origin = org.addr
	}
	rc, err := dial(cfg.Addr)
	if err != nil {
		return result{}, err
	}
	defer rc.Close()
	base, err := prepareLoad(rc, cfg, payload)
	if err != nil {
		return result{}, err
	}
	pid, err := redisPID(rc)
	if err != nil {
		return result{}, err
	}
	b, err := newBarrier(cfg.Instances)
	if err != nil {
		return result{}, err
	}
	cfg.Barrier = b.addr
	cmds, readers, err := startChildren(cfg)
	defer cleanup(cmds)
	if err != nil {
		return result{}, err
	}
	clk := loadClk()
	var before int64
	var redis0, origin0, client0 float64
	var wallStart time.Time
	var sampleErr error
	if err := b.release(func() {
		if strings.HasPrefix(cfg.Mode, "warm-") {
			sampleErr = pexpireAll(rc, cfg.Mode, cfg.Keys)
			if sampleErr != nil {
				return
			}
		}
		before, sampleErr = commandCount(rc)
		if org != nil {
			org.markStart()
		}
		redis0 = cpuTicks(pid)
		origin0 = cpuTicks(os.Getpid())
		client0 = sumCPU(cmds)
		wallStart = time.Now()
	}); err != nil {
		return result{}, err
	}
	if sampleErr != nil {
		return result{}, sampleErr
	}
	children, err := readChildren(readers)
	if err != nil {
		return result{}, err
	}
	wall := time.Since(wallStart)
	redis1 := cpuTicks(pid)
	origin1 := cpuTicks(os.Getpid())
	client1 := sumCPU(cmds)
	if err := waitAll(cmds); err != nil {
		return result{}, err
	}
	after, err := commandCount(rc)
	if err != nil {
		return result{}, err
	}
	if strings.HasPrefix(cfg.Mode, "gate-") {
		if _, err := rc.cmd("DEL", "avalanche:gate"); err != nil {
			return result{}, err
		}
	}
	dbSize, err := dbSize(rc)
	if err != nil {
		return result{}, err
	}
	evicted, err := evictedKeys(rc)
	if err != nil {
		return result{}, err
	}
	res := assemble(cfg, payload, children, org, after-before-1, wall, dbSize, evicted, base)
	res.RedisCPUSec = (redis1 - redis0) / clk
	res.OriginCPUSec = (origin1 - origin0) / clk
	res.ClientCPUSec = (client1 - client0) / clk
	if wall > 0 {
		res.RedisCPUpct = res.RedisCPUSec / wall.Seconds() * 100
		res.OriginCPUpct = res.OriginCPUSec / wall.Seconds() * 100
		res.ClientCPUpct = res.ClientCPUSec / wall.Seconds() * 100
		res.QPS = float64(res.Ops) / wall.Seconds()
	}
	res.RedisRSSKB = rssKB(pid)
	res.UsedMemory = usedMemory(rc)
	res.Pass, res.Detail = checkLoad(cfg, &res)
	for i := range res.Children {
		res.Children[i].HistBins = nil
		res.Children[i].MissBins = nil
		res.Children[i].HitBins = nil
		res.Children[i].MissBuckets = nil
	}
	return res, nil
}

func prepareLoad(c *Conn, cfg config, payload string) (int64, error) {
	if _, err := c.cmd("FLUSHDB"); err != nil {
		return 0, err
	}
	if _, err := c.cmd("CONFIG", "SET", "hz", "10"); err != nil {
		return 0, err
	}
	base := usedMemory(c)
	switch {
	case strings.HasPrefix(cfg.Mode, "warm-"):
		if err := fillKeys(c, cfg.Keys, payload, 0); err != nil {
			return 0, err
		}
	case strings.HasPrefix(cfg.Mode, "local-"):
		if err := fillKeys(c, cfg.Keys, payload, 120); err != nil {
			return 0, err
		}
	}
	return base, nil
}

func fillKeys(c *Conn, n int, payload string, exSec int) error {
	return pipeline(c, n, func(i int) []string {
		if exSec > 0 {
			return []string{"SET", keyName(i), payload, "EX", strconv.Itoa(exSec)}
		}
		return []string{"SET", keyName(i), payload}
	})
}

func pexpireAll(c *Conn, mode string, n int) error {
	return pipeline(c, n, func(i int) []string {
		return []string{"PEXPIRE", keyName(i), strconv.Itoa(ttlFor(mode, i))}
	})
}

func assemble(cfg config, payload string, children []childResult, org *origin, redisCmds int64, wall time.Duration, dbSize, evicted, base int64) result {
	res := result{
		Scenario: cfg.Scenario, Note: cfg.Note, Mode: cfg.Mode,
		Keys: cfg.Keys, Repeat: cfg.Repeat, Instances: cfg.Instances, Workers: cfg.Workers,
		Pool: cfg.Pool, ServiceMs: cfg.ServiceMs, PayloadBytes: len(payload),
		RedisCmds: redisCmds, DBSize: dbSize, Evicted: evicted, BaseMemory: base,
		WallSec: wall.Seconds(), Children: children,
	}
	if org != nil {
		res.ProbeSQLNs = org.ProbeSQLNs
		res.ProbeSlowNs = org.ProbeSlowNs
		res.BurnLoops = org.BurnLoops
		res.OriginLoads = org.loads.Load()
		if res.OriginLoads > 0 {
			res.SlotAvgNs = org.serviceSum.Load() / res.OriginLoads
		}
		res.SlotMaxNs = org.serviceMax.Load()
		res.OriginBuckets = org.bucketSnap()
		res.BucketMs = 100
		res.PeakOrigin = slideMax(res.OriginBuckets, 1)
		res.PeakOrigin2 = slideMax(res.OriginBuckets, 2)
	}
	sort.Slice(children, func(i, j int) bool { return children[i].Instance < children[j].Instance })
	all := newHist()
	miss := newHist()
	hit := newHist()
	buckets := make([]int64, localBucketsN)
	for _, ch := range children {
		res.Ops += ch.Ops
		res.Hits += ch.Hits
		res.Misses += ch.Misses
		res.Errors += ch.Errors
		res.Polls += ch.Polls
		res.ChildLoads += ch.Loads
		res.ChildRedisCmds += ch.RedisCmds
		all.addSnap(ch.HistBins, ch.HistN, ch.HistSum, ch.HistMax)
		miss.addSnap(ch.MissBins, ch.MissN, ch.MissSum, ch.MissMax)
		hit.addSnap(ch.HitBins, ch.HitN, ch.HitSum, ch.HitMax)
		for i := range buckets {
			if i < len(ch.MissBuckets) {
				buckets[i] += ch.MissBuckets[i]
			}
		}
	}
	res.P50us = all.le(0.50) / 1000
	res.P99us = all.le(0.99) / 1000
	res.Maxus = all.max / 1000
	res.MissP50us = miss.le(0.50) / 1000
	res.MissP99us = miss.le(0.99) / 1000
	res.MissMaxus = miss.max / 1000
	res.HitP99us = hit.le(0.99) / 1000
	res.MissBuckets = trimTail(buckets)
	res.Slide100ms = slideMax(buckets, 2)
	return res
}

// splitCold 把前 400 毫秒当成冷启动，后面按 200 毫秒窗口取峰值。
func splitCold(buckets []int64) (head, later int64) {
	for i := 0; i < 4 && i < len(buckets); i++ {
		head += buckets[i]
	}
	if len(buckets) > 4 {
		later = slideMax(buckets[4:], 2)
	}
	return head, later
}

func trimTail(b []int64) []int64 {
	i := len(b)
	for i > 0 && b[i-1] == 0 {
		i--
	}
	return append([]int64(nil), b[:i]...)
}

func checkLoad(cfg config, res *result) (bool, string) {
	if res.Errors != 0 {
		return false, fmt.Sprintf("errors %d", res.Errors)
	}
	if res.Evicted != 0 {
		return false, fmt.Sprintf("evicted %d", res.Evicted)
	}
	if needsOrigin(cfg.Mode) && res.ChildLoads != res.OriginLoads {
		return false, fmt.Sprintf("child loads %d != origin %d", res.ChildLoads, res.OriginLoads)
	}
	switch cfg.Mode {
	case "burst-plain", "burst-plain-p4":
		// 有多少请求赶在 SET 之前冲进来，取决于调度。峰值由 4 个核卡住，大约每 100 毫秒 190 次。
		if res.OriginLoads < int64(cfg.Keys) || res.OriginLoads > int64(cfg.Keys*cfg.Repeat*cfg.Instances) {
			return false, fmt.Sprintf("plain origin %d", res.OriginLoads)
		}
		if res.PeakOrigin < 140 || res.PeakOrigin > 260 {
			return false, fmt.Sprintf("plain peak %d", res.PeakOrigin)
		}
		if res.DBSize != int64(cfg.Keys) {
			return false, fmt.Sprintf("dbsize %d", res.DBSize)
		}
	case "burst-flight":
		// 进程内合并后再 GET 一次，别的进程可能已经写好。预跑大约 650 到 700。
		if res.OriginLoads < 500 || res.OriginLoads > 900 {
			return false, fmt.Sprintf("flight origin %d not in [500,900]", res.OriginLoads)
		}
		if res.DBSize != int64(cfg.Keys) {
			return false, fmt.Sprintf("dbsize %d", res.DBSize)
		}
	case "burst-onekey-plain":
		lo := int64(cfg.Workers * cfg.Instances / 2)
		hi := int64(cfg.Workers*cfg.Instances + 8)
		if res.OriginLoads < lo || res.OriginLoads > hi {
			return false, fmt.Sprintf("onekey plain origin %d not in [%d,%d]", res.OriginLoads, lo, hi)
		}
		if res.DBSize != 1 {
			return false, fmt.Sprintf("dbsize %d", res.DBSize)
		}
	case "burst-onekey-flight":
		if res.OriginLoads < 1 || res.OriginLoads > int64(cfg.Instances+2) {
			return false, fmt.Sprintf("onekey flight origin %d", res.OriginLoads)
		}
		if res.DBSize != 1 {
			return false, fmt.Sprintf("dbsize %d", res.DBSize)
		}
	case "burst-rebuild":
		if res.OriginLoads != int64(cfg.Keys) {
			return false, fmt.Sprintf("rebuild origin %d want %d", res.OriginLoads, cfg.Keys)
		}
		if res.DBSize != int64(cfg.Keys) {
			return false, fmt.Sprintf("dbsize %d", res.DBSize)
		}
	case "gate-plain":
		want := int64(cfg.Keys * cfg.Repeat * cfg.Instances)
		if res.OriginLoads != want || res.DBSize != int64(cfg.Keys) {
			return false, fmt.Sprintf("gate plain origin %d db %d want %d", res.OriginLoads, res.DBSize, want)
		}
		if res.PeakOrigin < 140 || res.PeakOrigin > 260 {
			return false, fmt.Sprintf("gate plain peak %d", res.PeakOrigin)
		}
	case "gate-onekey-plain":
		want := int64(cfg.Keys * cfg.Repeat * cfg.Instances)
		if res.OriginLoads != want || res.DBSize != 1 {
			return false, fmt.Sprintf("gate onekey plain origin %d db %d want %d", res.OriginLoads, res.DBSize, want)
		}
		if res.PeakOrigin < 140 || res.PeakOrigin > 260 {
			return false, fmt.Sprintf("gate onekey peak %d", res.PeakOrigin)
		}
	case "gate-flight":
		want := int64(cfg.Keys * cfg.Instances)
		if res.OriginLoads != want || res.DBSize != int64(cfg.Keys) {
			return false, fmt.Sprintf("gate flight origin %d db %d want %d", res.OriginLoads, res.DBSize, want)
		}
	case "gate-onekey-flight":
		if res.OriginLoads != int64(cfg.Instances) || res.DBSize != 1 {
			return false, fmt.Sprintf("gate onekey flight origin %d db %d", res.OriginLoads, res.DBSize)
		}
	case "gate-rebuild":
		if res.OriginLoads != int64(cfg.Keys) || res.DBSize != int64(cfg.Keys) {
			return false, fmt.Sprintf("gate rebuild origin %d db %d", res.OriginLoads, res.DBSize)
		}
	case "warm-fixed":
		if res.OriginLoads < int64(cfg.Keys) {
			return false, fmt.Sprintf("fixed origin %d", res.OriginLoads)
		}
		if res.PeakOrigin2 < int64(cfg.Keys)*3/4 {
			return false, fmt.Sprintf("fixed peak2 %d < 3/4 of %d", res.PeakOrigin2, cfg.Keys)
		}
	case "warm-jitter":
		if res.OriginLoads < int64(cfg.Keys) {
			return false, fmt.Sprintf("jitter origin %d", res.OriginLoads)
		}
		if res.PeakOrigin2 > int64(cfg.Keys)/2 {
			return false, fmt.Sprintf("jitter peak2 %d > half of %d", res.PeakOrigin2, cfg.Keys)
		}
	case "cold-fixed":
		head, later := splitCold(res.OriginBuckets)
		if head < int64(cfg.Keys)*3/4 || later < int64(cfg.Keys)*3/4 {
			return false, fmt.Sprintf("cold fixed head %d later %d keys %d", head, later, cfg.Keys)
		}
	case "cold-jitter":
		// 第一波是冷启动，抖动救不了。后面的波才应该散开。
		head, later := splitCold(res.OriginBuckets)
		if head < int64(cfg.Keys)*3/4 {
			return false, fmt.Sprintf("cold jitter head %d keys %d", head, cfg.Keys)
		}
		if later > int64(cfg.Keys)/2 {
			return false, fmt.Sprintf("cold jitter later %d > half of %d", later, cfg.Keys)
		}
	case "local-fixed":
		want := int64(cfg.Keys * cfg.Instances)
		if res.Misses != want {
			return false, fmt.Sprintf("fixed misses %d want %d", res.Misses, want)
		}
		if res.Slide100ms < want*4/5 {
			return false, fmt.Sprintf("fixed slide %d < 4/5 of %d", res.Slide100ms, want)
		}
	case "local-proc":
		want := int64(cfg.Keys * cfg.Instances)
		if res.Misses != want {
			return false, fmt.Sprintf("proc misses %d want %d", res.Misses, want)
		}
		if res.Slide100ms > int64(cfg.Keys)*14/10 {
			return false, fmt.Sprintf("proc slide %d > 1.4 keys", res.Slide100ms)
		}
	case "local-key":
		want := int64(cfg.Keys * cfg.Instances)
		if res.Misses != want {
			return false, fmt.Sprintf("key misses %d want %d", res.Misses, want)
		}
		if res.Slide100ms > want*45/100 {
			return false, fmt.Sprintf("key slide %d > 0.45 of %d", res.Slide100ms, want)
		}
	default:
		return false, "unknown mode"
	}
	return true, "ok"
}

func runExpire(cfg config) (result, error) {
	rc, err := dial(cfg.Addr)
	if err != nil {
		return result{}, err
	}
	defer rc.Close()
	if _, err := rc.cmd("FLUSHDB"); err != nil {
		return result{}, err
	}
	if strings.HasPrefix(cfg.Mode, "expire-active") {
		rep, err := rc.cmd("CONFIG", "SET", "hz", strconv.Itoa(cfg.Hz))
		if err != nil {
			return result{}, err
		}
		if rep.Str != "OK" {
			return result{}, fmt.Errorf("hz %q", rep.Str)
		}
	}
	base := usedMemory(rc)
	payload := "x"
	if cfg.Mode == "expire-same" || cfg.Mode == "expire-jitter" {
		payload = goodsPayload()
	}
	if err := fillKeys(rc, cfg.Keys, payload, 0); err != nil {
		return result{}, err
	}
	pid, err := redisPID(rc)
	if err != nil {
		return result{}, err
	}
	filled := usedMemory(rc)
	// 相对过期，避免和本机时钟对不齐。同一批命令里发完。
	now := time.Now()
	if err := pipeline(rc, cfg.Keys, func(i int) []string {
		ms := cfg.WindowMs
		if cfg.Mode == "expire-jitter" {
			// 采样窗口是 3 秒时，把过期铺在 500ms 到 2500ms。
			ms = 500 + (i*97)%2001
		} else if strings.HasPrefix(cfg.Mode, "expire-active") || cfg.Mode == "expire-same" {
			ms = 500
		}
		return []string{"PEXPIRE", keyName(i), strconv.Itoa(ms)}
	}); err != nil {
		return result{}, err
	}
	_ = now
	res := result{
		Scenario: cfg.Scenario, Note: cfg.Note, Mode: cfg.Mode,
		Keys: cfg.Keys, Hz: cfg.Hz, PayloadBytes: len(payload),
		BaseMemory: base, UsedMemory: filled, BucketMs: cfg.SampleMs,
	}
	if cfg.Mode == "expire-same" || cfg.Mode == "expire-jitter" {
		res.Samples, res.MaxDrop, err = sampleAlive(rc, cfg)
	} else {
		res.Samples, res.MaxPingUs, err = sampleActive(rc, cfg)
	}
	if err != nil {
		return result{}, err
	}
	evicted, err := evictedKeys(rc)
	if err != nil {
		return result{}, err
	}
	res.Evicted = evicted
	res.RedisRSSKB = rssKB(pid)
	res.FinalMemory = usedMemory(rc)
	res.DBSize, _ = dbSize(rc)
	res.Pass, res.Detail = checkExpire(cfg, &res)
	return res, nil
}

func sampleAlive(c *Conn, cfg config) ([]sample, int64, error) {
	start := time.Now()
	deadline := start.Add(time.Duration(cfg.WindowMs) * time.Millisecond)
	next := start
	var out []sample
	var prev int64 = -1
	var maxDrop int64
	for time.Now().Before(deadline) {
		if lag := time.Until(next); lag > 0 {
			time.Sleep(lag)
		}
		next = next.Add(time.Duration(cfg.SampleMs) * time.Millisecond)
		t0 := time.Now()
		alive, err := existsAlive(c, cfg.Keys)
		if err != nil {
			return nil, 0, err
		}
		s := sample{Tms: time.Since(start).Milliseconds(), Alive: alive, SampleUs: time.Since(t0).Microseconds()}
		out = append(out, s)
		if prev >= 0 {
			drop := prev - alive
			if drop > maxDrop {
				maxDrop = drop
			}
		}
		prev = alive
	}
	return out, maxDrop, nil
}

func sampleActive(c *Conn, cfg config) ([]sample, int64, error) {
	start := time.Now()
	deadline := start.Add(time.Duration(cfg.WindowMs) * time.Millisecond)
	next := start
	var out []sample
	var maxPing int64
	for time.Now().Before(deadline) {
		if lag := time.Until(next); lag > 0 {
			time.Sleep(lag)
		}
		next = next.Add(time.Duration(cfg.SampleMs) * time.Millisecond)
		t0 := time.Now()
		if _, err := c.cmd("PING"); err != nil {
			return nil, 0, err
		}
		ping := time.Since(t0).Microseconds()
		if ping > maxPing {
			maxPing = ping
		}
		st, err := c.info("stats")
		if err != nil {
			return nil, 0, err
		}
		ks, err := c.info("keyspace")
		if err != nil {
			return nil, 0, err
		}
		expired, _ := strconv.ParseInt(infoValue(st, "expired_keys"), 10, 64)
		keys, avg, _ := parseKeyspace(ks)
		out = append(out, sample{
			Tms: time.Since(start).Milliseconds(), ExpiredKeys: expired,
			KeyspaceKeys: keys, AvgTTLMs: avg, PingUs: ping,
		})
	}
	return out, maxPing, nil
}

func maxAliveDrop(samples []sample, steps int) int64 {
	var best int64
	if steps < 1 {
		steps = 1
	}
	for i := steps; i < len(samples); i++ {
		d := samples[i-steps].Alive - samples[i].Alive
		if d > best {
			best = d
		}
	}
	return best
}

func parseKeyspace(info string) (int64, int64, bool) {
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimRight(line, "\r")
		if !strings.HasPrefix(line, "db0:") {
			continue
		}
		var keys, avg int64
		for _, part := range strings.Split(strings.TrimPrefix(line, "db0:"), ",") {
			kv := strings.SplitN(part, "=", 2)
			if len(kv) != 2 {
				continue
			}
			n, _ := strconv.ParseInt(kv[1], 10, 64)
			switch kv[0] {
			case "keys":
				keys = n
			case "avg_ttl":
				avg = n
			}
		}
		return keys, avg, true
	}
	return 0, 0, false
}

func checkExpire(cfg config, res *result) (bool, string) {
	if res.Evicted != 0 {
		return false, fmt.Sprintf("evicted %d", res.Evicted)
	}
	if len(res.Samples) < 5 {
		return false, "too few samples"
	}
	switch cfg.Mode {
	case "expire-same":
		drop := maxAliveDrop(res.Samples, 2)
		if drop < int64(cfg.Keys)*4/5 {
			return false, fmt.Sprintf("same 2-sample drop %d (1-sample %d) < 4/5 of %d", drop, res.MaxDrop, cfg.Keys)
		}
		last := res.Samples[len(res.Samples)-1].Alive
		if last != 0 {
			return false, fmt.Sprintf("same still alive %d", last)
		}
	case "expire-jitter":
		if res.MaxDrop > int64(cfg.Keys)/5 {
			return false, fmt.Sprintf("jitter drop %d > 1/5 of %d", res.MaxDrop, cfg.Keys)
		}
		last := res.Samples[len(res.Samples)-1].Alive
		if last != 0 {
			return false, fmt.Sprintf("jitter still alive %d", last)
		}
		if res.Samples[0].Alive < int64(cfg.Keys)*9/10 {
			return false, fmt.Sprintf("jitter started at %d", res.Samples[0].Alive)
		}
	default:
		if res.Samples[0].KeyspaceKeys < int64(cfg.Keys)*9/10 {
			return false, fmt.Sprintf("active start keys %d", res.Samples[0].KeyspaceKeys)
		}
	}
	return true, "ok"
}

func commandCount(c *Conn) (int64, error) {
	info, err := c.info("stats")
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(infoValue(info, "total_commands_processed"), 10, 64)
}

func evictedKeys(c *Conn) (int64, error) {
	info, err := c.info("stats")
	if err != nil {
		return 0, err
	}
	v := infoValue(info, "evicted_keys")
	if v == "" {
		return 0, nil
	}
	return strconv.ParseInt(v, 10, 64)
}

func dbSize(c *Conn) (int64, error) {
	rep, err := c.cmd("DBSIZE")
	if err != nil {
		return 0, err
	}
	return rep.Int, nil
}

func usedMemory(c *Conn) int64 {
	info, err := c.info("memory")
	if err != nil {
		return 0
	}
	v, _ := strconv.ParseInt(infoValue(info, "used_memory"), 10, 64)
	return v
}

func redisPID(c *Conn) (int, error) {
	info, err := c.info("server")
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(strings.TrimSpace(infoValue(info, "process_id")))
}

type barrier struct {
	ln    net.Listener
	addr  string
	n     int
	conns chan net.Conn
}

func newBarrier(n int) (*barrier, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	b := &barrier{ln: ln, addr: ln.Addr().String(), n: n, conns: make(chan net.Conn, n)}
	go func() {
		for i := 0; i < n; i++ {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			b.conns <- c
		}
	}()
	return b, nil
}

func (b *barrier) release(sample func()) error {
	got := make([]net.Conn, 0, b.n)
	deadline := time.After(60 * time.Second)
	for len(got) < b.n {
		select {
		case c := <-b.conns:
			got = append(got, c)
		case <-deadline:
			return fmt.Errorf("barrier got %d/%d", len(got), b.n)
		}
	}
	if sample != nil {
		sample()
	}
	for _, c := range got {
		if _, err := c.Write([]byte{'G'}); err != nil {
			c.Close()
			return err
		}
		c.Close()
	}
	b.ln.Close()
	return nil
}

func startChildren(cfg config) ([]*exec.Cmd, []*bufio.Reader, error) {
	cmds := make([]*exec.Cmd, 0, cfg.Instances)
	readers := make([]*bufio.Reader, 0, cfg.Instances)
	for i := 0; i < cfg.Instances; i++ {
		args := []string{
			"-role", "child",
			"-scenario", cfg.Scenario,
			"-mode", cfg.Mode,
			"-addr", cfg.Addr,
			"-origin", cfg.Origin,
			"-instance", strconv.Itoa(i),
			"-instances", strconv.Itoa(cfg.Instances),
			"-workers", strconv.Itoa(cfg.Workers),
			"-keys", strconv.Itoa(cfg.Keys),
			"-repeat", strconv.Itoa(cfg.Repeat),
			"-pool", strconv.Itoa(cfg.Pool),
			"-service-ms", strconv.Itoa(cfg.ServiceMs),
			"-duration", cfg.Duration.String(),
			"-barrier", cfg.Barrier,
		}
		cmd := exec.Command(os.Args[0], args...)
		out, err := cmd.StdoutPipe()
		if err != nil {
			return cmds, readers, err
		}
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			return cmds, readers, err
		}
		cmds = append(cmds, cmd)
		readers = append(readers, bufio.NewReaderSize(out, 1<<20))
	}
	return cmds, readers, nil
}

func readChildren(readers []*bufio.Reader) ([]childResult, error) {
	out := make([]childResult, 0, len(readers))
	errc := make(chan error, len(readers))
	got := make(chan childResult, len(readers))
	for _, r := range readers {
		r := r
		go func() {
			var ch childResult
			dec := json.NewDecoder(r)
			if err := dec.Decode(&ch); err != nil {
				errc <- err
				return
			}
			io.Copy(io.Discard, r)
			got <- ch
		}()
	}
	timeout := time.After(90 * time.Second)
	for len(out) < len(readers) {
		select {
		case err := <-errc:
			return nil, err
		case ch := <-got:
			out = append(out, ch)
		case <-timeout:
			return nil, fmt.Errorf("timed out waiting for children, got %d/%d", len(out), len(readers))
		}
	}
	return out, nil
}

func cleanup(cmds []*exec.Cmd) {
	for _, cmd := range cmds {
		if cmd == nil || cmd.Process == nil || cmd.ProcessState != nil {
			continue
		}
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}
}

func waitAll(cmds []*exec.Cmd) error {
	var msgs []string
	for i, cmd := range cmds {
		if err := cmd.Wait(); err != nil {
			msgs = append(msgs, fmt.Sprintf("child %d: %v", i, err))
		}
	}
	if len(msgs) > 0 {
		return fmt.Errorf("%s", strings.Join(msgs, "; "))
	}
	return nil
}

func sumCPU(cmds []*exec.Cmd) float64 {
	var sum float64
	for _, cmd := range cmds {
		if cmd.Process == nil {
			continue
		}
		sum += cpuTicks(cmd.Process.Pid)
	}
	return sum
}

func loadClk() float64 {
	out, err := exec.Command("getconf", "CLK_TCK").Output()
	if err != nil {
		return 100
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	if err != nil || v <= 0 {
		return 100
	}
	return v
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
