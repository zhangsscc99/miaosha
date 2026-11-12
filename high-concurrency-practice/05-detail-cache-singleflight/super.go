package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

type result struct {
	Scenario       string        `json:"scenario"`
	Note           string        `json:"note"`
	Mode           string        `json:"mode"`
	Addrs          string        `json:"addrs"`
	OriginAddr     string        `json:"originAddr"`
	Instances      int           `json:"instances"`
	Workers        int           `json:"workersPerInstance"`
	Attempts       int64         `json:"attemptsPerInstance"`
	DurationSec    float64       `json:"durationSec"`
	Follow         int           `json:"followPerWorker"`
	Pool           int           `json:"originPool"`
	LockMs         int           `json:"lockMs"`
	PayloadBytes   int           `json:"payloadBytes"`
	ProbeSQLNs     int64         `json:"probeSqlNs"`
	ProbeSlowNs    int64         `json:"probeSlowNs"`
	BurnLoops      int           `json:"burnLoops"`
	Ops            int64         `json:"ops"`
	QPS            float64       `json:"qps"`
	Hits           int64         `json:"hits"`
	Misses         int64         `json:"misses"`
	Errors         int64         `json:"errors"`
	Polls          int64         `json:"polls"`
	Stale          int64         `json:"stale"`
	Fresh          int64         `json:"fresh"`
	NullHits       int64         `json:"nullHits"`
	Refreshed      int64         `json:"refreshed"`
	ChildLoads     int64         `json:"childLoads"`
	FollowLoads    int64         `json:"followLoads"`
	FollowOps      int64         `json:"followOps"`
	OriginLoads    int64         `json:"originLoads"`
	OriginFast     int64         `json:"originFast"`
	OriginSlow     int64         `json:"originSlow"`
	OriginMiss     int64         `json:"originMiss"`
	SlowAvgNs      int64         `json:"slowAvgNs"`
	SlowMaxNs      int64         `json:"slowMaxNs"`
	RedisCmds      int64         `json:"redisCommands"`
	ChildRedisCmds int64         `json:"childRedisCmds"`
	RedisCPUSec    float64       `json:"redisCpuSec"`
	RedisCPUpct    float64       `json:"redisCpuPercent"`
	OriginCPUSec   float64       `json:"originCpuSec"`
	OriginCPUpct   float64       `json:"originCpuPercent"`
	ClientCPUSec   float64       `json:"clientCpuSec"`
	ClientCPUpct   float64       `json:"clientCpuPercent"`
	RedisRSSKB     int64         `json:"redisRssKB"`
	UsedMemory     int64         `json:"usedMemory"`
	WallSec        float64       `json:"wallSec"`
	FinalMatch     bool          `json:"finalMatch"`
	FinalFresh     bool          `json:"finalFresh"`
	FinalNil       bool          `json:"finalNil"`
	LockHeld       bool          `json:"lockHeld"`
	NullCached     bool          `json:"nullCached"`
	P50ns          int64         `json:"p50ns"`
	P99ns          int64         `json:"p99ns"`
	P999ns         int64         `json:"p999ns"`
	Maxns          int64         `json:"maxns"`
	P50us          int64         `json:"p50us"`
	P99us          int64         `json:"p99us"`
	P999us         int64         `json:"p999us"`
	Maxus          int64         `json:"maxus"`
	PerInstance    string        `json:"perInstanceLoads"`
	Pass           bool          `json:"pass"`
	Detail         string        `json:"detail"`
	Children       []childResult `json:"children,omitempty"`
}

func runSuper(cfg config) (result, error) {
	fix, err := os.ReadFile(cfg.Fixture)
	if err != nil {
		return result{}, err
	}
	fixture := string(fix)
	service := time.Duration(cfg.ServiceMs) * time.Millisecond
	org, err := startOrigin("/tmp/miaosha-detail-goods.db", fix, cfg.Pool, service)
	if err != nil {
		return result{}, err
	}
	cfg.OriginAddr = org.addr

	rc, err := dial(cfg.Addr)
	if err != nil {
		return result{}, err
	}
	defer rc.Close()
	if err := prepareRedis(rc, cfg.Mode, fixture); err != nil {
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
		before, sampleErr = commandCount(rc)
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
	snap := org.snap()
	final, finalNil, fresh, match, err := readFinal(rc, cfg.Mode, fixture)
	if err != nil {
		return result{}, err
	}
	lockRep, err := rc.get(lockKey)
	if err != nil {
		return result{}, err
	}
	missRep, err := rc.get(missKey)
	if err != nil {
		return result{}, err
	}
	_ = final
	used := usedMemory(rc)
	res := assemble(cfg, fixture, children, snap, org, after-before-1, wall, match, fresh, finalNil, !lockRep.Nil, missRep.Str == nullMark)
	res.RedisCPUSec = (redis1 - redis0) / clk
	res.OriginCPUSec = (origin1 - origin0) / clk
	res.ClientCPUSec = (client1 - client0) / clk
	if wall > 0 {
		res.RedisCPUpct = res.RedisCPUSec / wall.Seconds() * 100
		res.OriginCPUpct = res.OriginCPUSec / wall.Seconds() * 100
		res.ClientCPUpct = res.ClientCPUSec / wall.Seconds() * 100
	}
	res.RedisRSSKB = rssKB(pid)
	res.UsedMemory = used
	res.Pass, res.Detail = check(cfg, &res)
	for i := range res.Children {
		res.Children[i].HistBins = nil
		res.Children[i].HistN = 0
		res.Children[i].HistSum = 0
		res.Children[i].HistMax = 0
	}
	return res, nil
}

func prepareRedis(c *Conn, mode, fixture string) error {
	if _, err := c.cmd("FLUSHDB"); err != nil {
		return err
	}
	switch mode {
	case "redis-get", "redis-get-touch":
		return c.setEX(detailKey, fixture, 120)
	case "logical":
		return c.setPlain(detailKey, "0\n"+fixture)
	default:
		return nil
	}
}

func readFinal(c *Conn, mode, fixture string) (string, bool, bool, bool, error) {
	rep, err := c.get(detailKey)
	if err != nil {
		return "", false, false, false, err
	}
	if mode == "logical" {
		if rep.Nil {
			return "", true, false, false, nil
		}
		exp, body, ok := splitMeta(rep.Str)
		fresh := ok && exp > time.Now().UnixMilli()
		return rep.Str, false, fresh, ok && body == fixture && fresh, nil
	}
	if rep.Nil {
		return "", true, false, false, nil
	}
	return rep.Str, false, false, rep.Str == fixture, nil
}

func assemble(cfg config, fixture string, children []childResult, snap originSnap, org *origin, redisCmds int64, wall time.Duration, match, fresh, finalNil, lockHeld, nullCached bool) result {
	res := result{
		Scenario:     cfg.Scenario,
		Note:         cfg.Note,
		Mode:         cfg.Mode,
		Addrs:        cfg.Addr,
		OriginAddr:   cfg.OriginAddr,
		Instances:    cfg.Instances,
		Workers:      cfg.Workers,
		Attempts:     cfg.Attempts,
		DurationSec:  cfg.Duration.Seconds(),
		Follow:       cfg.Follow,
		Pool:         cfg.Pool,
		LockMs:       cfg.LockMs,
		PayloadBytes: len(fixture),
		ProbeSQLNs:   org.ProbeSQLNs,
		ProbeSlowNs:  org.ProbeSlowNs,
		BurnLoops:    org.BurnLoops,
		RedisCmds:    redisCmds,
		OriginLoads:  snap.Loads,
		OriginFast:   snap.Fast,
		OriginSlow:   snap.Slow,
		OriginMiss:   snap.Miss,
		SlowMaxNs:    snap.SlowMax,
		WallSec:      wall.Seconds(),
		FinalMatch:   match,
		FinalFresh:   fresh,
		FinalNil:     finalNil,
		LockHeld:     lockHeld,
		NullCached:   nullCached,
		Children:     children,
	}
	if snap.Slow > 0 {
		res.SlowAvgNs = snap.SlowSum / snap.Slow
	}
	h := newHist()
	parts := make([]string, 0, len(children))
	for _, ch := range children {
		res.Ops += ch.Ops
		res.Hits += ch.Hits
		res.Misses += ch.Misses
		res.Errors += ch.Errors
		res.Polls += ch.Polls
		res.Stale += ch.Stale
		res.Fresh += ch.Fresh
		res.NullHits += ch.NullHits
		res.ChildLoads += ch.Loads
		res.FollowLoads += ch.FollowLoads
		res.FollowOps += ch.FollowOps
		res.Refreshed += ch.Refreshed
		res.ChildRedisCmds += ch.RedisCmds
		h.addSnap(ch.HistBins, ch.HistN, ch.HistSum, ch.HistMax)
		parts = append(parts, fmt.Sprintf("%d:%d", ch.Instance, ch.Loads))
	}
	res.PerInstance = strings.Join(parts, ",")
	res.P50ns = h.le(0.50)
	res.P99ns = h.le(0.99)
	res.P999ns = h.le(0.999)
	res.Maxns = h.max
	res.P50us = res.P50ns / 1000
	res.P99us = res.P99ns / 1000
	res.P999us = res.P999ns / 1000
	res.Maxus = res.Maxns / 1000
	if wall > 0 {
		res.QPS = float64(res.Ops) / wall.Seconds()
	}
	return res
}

func check(cfg config, res *result) (bool, string) {
	fail := func(msg string) (bool, string) {
		return false, msg
	}
	if res.ChildLoads != res.OriginLoads {
		return fail(fmt.Sprintf("load accounting child=%d origin=%d", res.ChildLoads, res.OriginLoads))
	}
	if cfg.Duration == 0 {
		want := int64(cfg.Instances) * cfg.Attempts
		if res.Ops != want {
			return fail(fmt.Sprintf("ops=%d want=%d", res.Ops, want))
		}
	}
	switch cfg.Mode {
	case "redis-get", "redis-get-touch":
		if res.Errors != 0 || res.OriginLoads != 0 || !res.FinalMatch || res.Ops < 1000 {
			return fail(fmt.Sprintf("errors=%d origin=%d match=%v ops=%d", res.Errors, res.OriginLoads, res.FinalMatch, res.Ops))
		}
	case "local-hit":
		if res.Errors != 0 || res.OriginLoads != 0 || res.Ops < 1000 {
			return fail(fmt.Sprintf("errors=%d origin=%d ops=%d", res.Errors, res.OriginLoads, res.Ops))
		}
	case "sql-fast":
		if res.Errors != 0 || res.OriginLoads != res.Ops || res.OriginFast != res.Ops {
			return fail(fmt.Sprintf("errors=%d origin=%d fast=%d ops=%d", res.Errors, res.OriginLoads, res.OriginFast, res.Ops))
		}
	case "stampede-fast":
		if res.Errors != 0 || !res.FinalMatch || res.OriginLoads < 1 || res.OriginFast != res.OriginLoads {
			return fail(fmt.Sprintf("errors=%d origin=%d fast=%d match=%v", res.Errors, res.OriginLoads, res.OriginFast, res.FinalMatch))
		}
	case "stampede-slow":
		if res.Errors != 0 || !res.FinalMatch || res.OriginLoads != res.Ops || res.OriginSlow != res.Ops {
			return fail(fmt.Sprintf("errors=%d origin=%d slow=%d ops=%d match=%v", res.Errors, res.OriginLoads, res.OriginSlow, res.Ops, res.FinalMatch))
		}
	case "singleflight":
		if res.Errors != 0 || !res.FinalMatch || res.OriginLoads < 1 || res.OriginLoads > int64(cfg.Instances) || res.OriginSlow != res.OriginLoads {
			return fail(fmt.Sprintf("errors=%d origin=%d instances=%d match=%v", res.Errors, res.OriginLoads, cfg.Instances, res.FinalMatch))
		}
	case "distlock":
		if res.Errors != 0 || !res.FinalMatch || res.OriginLoads != 1 || res.LockHeld || res.Refreshed != 1 {
			return fail(fmt.Sprintf("errors=%d origin=%d refreshed=%d lock=%v match=%v", res.Errors, res.OriginLoads, res.Refreshed, res.LockHeld, res.FinalMatch))
		}
	case "logical":
		if res.Errors != 0 || !res.FinalFresh || res.OriginLoads != 1 || res.Refreshed != 1 || res.LockHeld {
			return fail(fmt.Sprintf("errors=%d origin=%d refreshed=%d fresh=%d stale=%d lock=%v finalFresh=%v", res.Errors, res.OriginLoads, res.Refreshed, res.Fresh, res.Stale, res.LockHeld, res.FinalFresh))
		}
		if res.Fresh+res.Stale != res.Ops {
			return fail(fmt.Sprintf("fresh+stale=%d ops=%d", res.Fresh+res.Stale, res.Ops))
		}
	case "local-logical":
		if res.Errors != 0 || res.OriginLoads != int64(cfg.Instances) || res.Refreshed != int64(cfg.Instances) {
			return fail(fmt.Sprintf("errors=%d origin=%d refreshed=%d instances=%d", res.Errors, res.OriginLoads, res.Refreshed, cfg.Instances))
		}
		if res.Fresh+res.Stale != res.Ops {
			return fail(fmt.Sprintf("fresh+stale=%d ops=%d", res.Fresh+res.Stale, res.Ops))
		}
		if res.Ops >= 50 && res.ProbeSlowNs > 0 && res.P99ns > res.ProbeSlowNs/2 {
			return fail(fmt.Sprintf("p99ns=%d waited, probeSlow=%d", res.P99ns, res.ProbeSlowNs))
		}
	case "lock-short":
		if res.Errors != 0 || !res.FinalMatch || res.OriginLoads < 2 || res.OriginSlow != res.OriginLoads {
			return fail(fmt.Sprintf("errors=%d origin=%d match=%v", res.Errors, res.OriginLoads, res.FinalMatch))
		}
	case "lock-stuck":
		if res.OriginLoads != 0 || res.Errors != res.Ops || res.FinalMatch || !res.LockHeld || !res.FinalNil {
			return fail(fmt.Sprintf("errors=%d ops=%d origin=%d lock=%v nil=%v match=%v", res.Errors, res.Ops, res.OriginLoads, res.LockHeld, res.FinalNil, res.FinalMatch))
		}
	case "penetrate":
		if res.Errors != 0 || res.OriginLoads != res.Ops || res.NullCached || !res.FinalNil {
			return fail(fmt.Sprintf("errors=%d origin=%d ops=%d null=%v", res.Errors, res.OriginLoads, res.Ops, res.NullCached))
		}
	case "negative":
		if res.Errors != 0 || res.OriginLoads != 1 || res.FollowLoads != 0 || !res.NullCached || res.FollowOps != int64(cfg.Workers*cfg.Follow) {
			return fail(fmt.Sprintf("errors=%d origin=%d followLoads=%d followOps=%d null=%v", res.Errors, res.OriginLoads, res.FollowLoads, res.FollowOps, res.NullCached))
		}
	case "negative-race":
		if res.Errors != 0 || res.OriginLoads < 2 || res.FollowLoads != 0 || !res.NullCached {
			return fail(fmt.Sprintf("errors=%d origin=%d followLoads=%d null=%v", res.Errors, res.OriginLoads, res.FollowLoads, res.NullCached))
		}
	default:
		return fail("unknown mode " + cfg.Mode)
	}
	return true, fmt.Sprintf("ops=%d qps=%.0f origin=%d slow=%d fast=%d miss=%d errors=%d p99ns=%d maxns=%d redisCmds=%d childRedis=%d",
		res.Ops, res.QPS, res.OriginLoads, res.OriginSlow, res.OriginFast, res.OriginMiss, res.Errors, res.P99ns, res.Maxns, res.RedisCmds, res.ChildRedisCmds)
}

func redisPID(c *Conn) (int, error) {
	info, err := c.info("server")
	if err != nil {
		return 0, err
	}
	pid, err := strconv.Atoi(infoValue(info, "process_id"))
	if err != nil || pid <= 0 {
		return 0, fmt.Errorf("redis pid: %q", infoValue(info, "process_id"))
	}
	return pid, nil
}

func commandCount(c *Conn) (int64, error) {
	info, err := c.info("stats")
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(infoValue(info, "total_commands_processed"), 10, 64)
}

func usedMemory(c *Conn) int64 {
	info, err := c.info("memory")
	if err != nil {
		return 0
	}
	v, _ := strconv.ParseInt(infoValue(info, "used_memory"), 10, 64)
	return v
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
	deadline := time.After(30 * time.Second)
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
			"-origin", cfg.OriginAddr,
			"-fixture", cfg.Fixture,
			"-lua", cfg.LuaDir,
			"-instance", strconv.Itoa(i),
			"-instances", strconv.Itoa(cfg.Instances),
			"-workers", strconv.Itoa(cfg.Workers),
			"-attempts", strconv.FormatInt(cfg.Attempts, 10),
			"-duration", cfg.Duration.String(),
			"-follow", strconv.Itoa(cfg.Follow),
			"-pool", strconv.Itoa(cfg.Pool),
			"-service-ms", strconv.Itoa(cfg.ServiceMs),
			"-lock-ms", strconv.Itoa(cfg.LockMs),
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
		readers = append(readers, bufio.NewReader(out))
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
	for len(out) < len(readers) {
		select {
		case err := <-errc:
			return nil, err
		case ch := <-got:
			out = append(out, ch)
		case <-time.After(60 * time.Second):
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
