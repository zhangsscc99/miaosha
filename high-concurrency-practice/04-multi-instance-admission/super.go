package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

const (
	codeSoldOut = 0
	codeOK      = 1
	codeDup     = 2
)

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
	deadline := time.After(20 * time.Second)
	for len(got) < b.n {
		select {
		case c := <-b.conns:
			got = append(got, c)
		case <-deadline:
			return fmt.Errorf("barrier %s got %d/%d children", b.addr, len(got), b.n)
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

func runSuper(cfg config) (result, error) {
	if err := validate(cfg); err != nil {
		return result{}, err
	}
	clk := loadClk()
	c, err := dial(cfg.Addr)
	if err != nil {
		return result{}, err
	}
	defer c.Close()
	stockSHA, admitSHA, pid, err := setupRedis(c, cfg)
	if err != nil {
		return result{}, err
	}
	cfg.StockSHA = stockSHA
	cfg.AdmitSHA = admitSHA

	b1, err := newBarrier(cfg.Instances)
	if err != nil {
		return result{}, err
	}
	var b2 *barrier
	barrier2 := ""
	if cfg.Phases >= 2 {
		b2, err = newBarrier(cfg.Instances)
		if err != nil {
			return result{}, err
		}
		barrier2 = b2.addr
	}
	cmds, readers, err := startChildren(cfg, b1.addr, barrier2)
	defer cleanup(cmds)
	if err != nil {
		return result{}, err
	}

	before, err := commandCount(c)
	if err != nil {
		return result{}, err
	}
	var redis0, client0 float64
	var wallStart time.Time
	if err := b1.release(func() {
		redis0 = cpuTicks(pid)
		client0 = sumCPU(cmds)
		wallStart = time.Now()
	}); err != nil {
		return result{}, err
	}
	phase1, err := readAll(readers)
	if err != nil {
		return result{}, err
	}
	var phase2 []childResult
	extraCmds := int64(1) // the INFO that reads the counter afterwards
	if cfg.Phases >= 2 {
		if _, err := c.cmd("INCRBY", "sec:stock:1", strconv.FormatInt(cfg.Restock, 10)); err != nil {
			return result{}, err
		}
		extraCmds++
		if err := b2.release(nil); err != nil {
			return result{}, err
		}
		phase2, err = readAll(readers)
		if err != nil {
			return result{}, err
		}
	}
	redis1 := cpuTicks(pid)
	client1 := sumCPU(cmds)
	wall := time.Since(wallStart)
	if err := waitAll(cmds); err != nil {
		return result{}, err
	}
	after, err := commandCount(c)
	if err != nil {
		return result{}, err
	}
	finalStock, err := c.getInt("sec:stock:1")
	if err != nil {
		return result{}, err
	}
	winners, err := c.scard("sec:win:1")
	if err != nil {
		return result{}, err
	}
	used := usedMemory(c)
	res := assemble(cfg, phase1, phase2, after-before-extraCmds, finalStock, winners, used, rssKB(pid))
	res.RedisCPUSec = (redis1 - redis0) / clk
	res.ClientCPUSec = (client1 - client0) / clk
	res.WallSec = wall.Seconds()
	if wall > 0 {
		res.RedisCPUpct = res.RedisCPUSec / wall.Seconds() * 100
		res.ClientCPUpct = res.ClientCPUSec / wall.Seconds() * 100
	}
	res.Pass, res.Detail = check(cfg, &res)
	for i := range res.Children {
		res.Children[i].Hist = nil
	}
	return res, nil
}

func validate(cfg config) error {
	if cfg.Scenario == "" {
		return fmt.Errorf("missing -scenario")
	}
	if cfg.Instances < 1 || cfg.Workers < 1 {
		return fmt.Errorf("instances and workers must be >= 1")
	}
	if cfg.Phases < 1 || cfg.Phases > 2 {
		return fmt.Errorf("phases must be 1 or 2")
	}
	if cfg.Phases == 1 && cfg.Attempts <= 0 && cfg.Duration <= 0 {
		return fmt.Errorf("need -attempts or -duration")
	}
	if cfg.Phases == 2 && (cfg.Attempts <= 0 || cfg.Phase2 <= 0) {
		return fmt.Errorf("two phases need -attempts and -phase2")
	}
	if cfg.Bad && cfg.Instances < 2 {
		return fmt.Errorf("bad instance needs at least 2 instances")
	}
	switch cfg.Gate {
	case "none", "local", "redis":
	default:
		return fmt.Errorf("gate must be none, local, or redis")
	}
	if cfg.Gate == "redis" && cfg.Limit <= 0 {
		return fmt.Errorf("redis gate needs -limit")
	}
	if cfg.Gate == "local" && cfg.Quota > 0 && cfg.Burst < 1 {
		return fmt.Errorf("local quota needs -burst >= 1")
	}
	return nil
}

func setupRedis(c *Conn, cfg config) (string, string, int, error) {
	stockBody, err := os.ReadFile(cfg.LuaDir + "/stock.lua")
	if err != nil {
		return "", "", 0, err
	}
	admitBody, err := os.ReadFile(cfg.LuaDir + "/admit.lua")
	if err != nil {
		return "", "", 0, err
	}
	if _, err := c.cmd("FLUSHDB"); err != nil {
		return "", "", 0, err
	}
	if _, err := c.cmd("SCRIPT", "FLUSH"); err != nil {
		return "", "", 0, err
	}
	stockSHA, err := c.scriptLoad(string(stockBody))
	if err != nil {
		return "", "", 0, err
	}
	admitSHA, err := c.scriptLoad(string(admitBody))
	if err != nil {
		return "", "", 0, err
	}
	if _, err := c.cmd("SET", "sec:stock:1", strconv.FormatInt(cfg.Stock, 10)); err != nil {
		return "", "", 0, err
	}
	info, err := c.info("server")
	if err != nil {
		return "", "", 0, err
	}
	pid, _ := strconv.Atoi(infoValue(info, "process_id"))
	if pid <= 0 {
		return "", "", 0, fmt.Errorf("redis pid missing")
	}
	return stockSHA, admitSHA, pid, nil
}

func startChildren(cfg config, barrier1, barrier2 string) ([]*exec.Cmd, []*bufio.Reader, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, nil, err
	}
	var cmds []*exec.Cmd
	var readers []*bufio.Reader
	fail := func(err error) ([]*exec.Cmd, []*bufio.Reader, error) {
		cleanup(cmds)
		return nil, nil, err
	}
	for i := 0; i < cfg.Instances; i++ {
		gate := cfg.Gate
		quota := cfg.Quota
		burst := cfg.Burst
		if cfg.Bad && i == 0 {
			gate = "none"
			quota = 0
			burst = 0
		}
		args := []string{
			"-role", "child",
			"-instance", strconv.Itoa(i),
			"-workers", strconv.Itoa(cfg.Workers),
			"-attempts", strconv.FormatInt(cfg.Attempts, 10),
			"-duration", cfg.Duration.String(),
			"-stock-sha", cfg.StockSHA,
			"-admit-sha", cfg.AdmitSHA,
			"-addr", cfg.Addr,
			"-soldout-flag=" + strconv.FormatBool(cfg.UseFlag),
			"-quota", strconv.FormatFloat(quota, 'f', -1, 64),
			"-burst", strconv.FormatFloat(burst, 'f', -1, 64),
			"-probe", cfg.Probe.String(),
			"-gate", gate,
			"-limit", strconv.Itoa(cfg.Limit),
			"-window-ms", strconv.Itoa(cfg.WindowMs),
			"-phases", strconv.Itoa(cfg.Phases),
			"-phase2", cfg.Phase2.String(),
			"-barrier1", barrier1,
			"-barrier2", barrier2,
		}
		cmd := exec.Command(exe, args...)
		cmd.Stderr = os.Stderr
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			return fail(err)
		}
		if err := cmd.Start(); err != nil {
			return fail(err)
		}
		cmds = append(cmds, cmd)
		readers = append(readers, bufio.NewReaderSize(stdout, 64*1024))
	}
	return cmds, readers, nil
}

func readAll(readers []*bufio.Reader) ([]childResult, error) {
	out := make([]childResult, len(readers))
	errCh := make(chan error, len(readers))
	done := make(chan struct{})
	for i, r := range readers {
		go func(i int, r *bufio.Reader) {
			line, err := readLine(r, 60*time.Second)
			if err != nil {
				errCh <- fmt.Errorf("child %d: %w", i, err)
				return
			}
			var cr childResult
			if err := json.Unmarshal(line, &cr); err != nil {
				errCh <- fmt.Errorf("child %d json: %w", i, err)
				return
			}
			out[i] = cr
			done <- struct{}{}
		}(i, r)
	}
	for n := 0; n < len(readers); n++ {
		select {
		case err := <-errCh:
			return nil, err
		case <-done:
		}
	}
	return out, nil
}

func readLine(r *bufio.Reader, timeout time.Duration) ([]byte, error) {
	type got struct {
		b   []byte
		err error
	}
	ch := make(chan got, 1)
	go func() {
		b, err := r.ReadBytes('\n')
		ch <- got{b, err}
	}()
	select {
	case g := <-ch:
		if g.err != nil {
			return nil, g.err
		}
		return g.b, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("timeout waiting for result")
	}
}

func assemble(cfg config, phase1, phase2 []childResult, commands, finalStock, winners, used, rss int64) result {
	initial := cfg.Stock
	if cfg.Phases >= 2 {
		initial += cfg.Restock
	}
	res := result{
		Scenario:            cfg.Scenario,
		Note:                cfg.Note,
		Addrs:               cfg.Addr,
		Instances:           cfg.Instances,
		WorkersPerInstance:  cfg.Workers,
		AttemptsPerInstance: cfg.Attempts,
		DurationSec:         cfg.Duration.Seconds(),
		UseFlag:             cfg.UseFlag,
		QuotaPerSec:         cfg.Quota,
		Burst:               cfg.Burst,
		ProbeMs:             cfg.Probe.Milliseconds(),
		Gate:                cfg.Gate,
		Limit:               cfg.Limit,
		WindowMs:            cfg.WindowMs,
		BadInstance:         cfg.Bad,
		InitialStock:        initial,
		FinalStock:          finalStock,
		Winners:             winners,
		RedisCommands:       commands,
		RedisRSSKB:          rss,
		UsedMemory:          used,
		FlagAtMsMin:         -1,
		FlagAtMsMax:         -1,
	}
	children := append([]childResult{}, phase1...)
	children = append(children, phase2...)
	merged := newHist()
	var maxEl float64
	var evals []string
	for _, c := range phase1 {
		addChild(&res, c)
		if c.Hist != nil {
			merged.addSnap(*c.Hist)
		}
		if c.ElapsedSec > maxEl {
			maxEl = c.ElapsedSec
		}
		if c.FlagAtMs >= 0 && (res.FlagAtMsMin < 0 || c.FlagAtMs < res.FlagAtMsMin) {
			res.FlagAtMsMin = c.FlagAtMs
		}
		if c.FlagAtMs > res.FlagAtMsMax {
			res.FlagAtMsMax = c.FlagAtMs
		}
		evals = append(evals, fmt.Sprintf("%d:%d", c.Instance, c.Evals))
	}
	if len(phase2) > 0 {
		p2 := &phaseSum{ElapsedSec: 0}
		h2 := newHist()
		for _, c := range phase2 {
			addChild(&res, c)
			p2.Evals += c.Evals
			p2.Success += c.Success
			p2.LocalReject += c.LocalReject
			p2.Probes += c.Probes
			p2.Errors += c.Errors
			if c.ElapsedSec > p2.ElapsedSec {
				p2.ElapsedSec = c.ElapsedSec
			}
			if c.Hist != nil {
				h2.addSnap(*c.Hist)
			}
		}
		p2.P99us = h2.le(0.99) / 1000
		res.Phase2 = p2
	}
	res.Children = children
	res.ElapsedSec = maxEl
	res.PerInstanceEvals = strings.Join(evals, ",")
	res.P50us = merged.le(0.50) / 1000
	res.P99us = merged.le(0.99) / 1000
	res.P999us = merged.le(0.999) / 1000
	res.Maxus = merged.max / 1000
	if maxEl > 0 {
		var p1ops, p1evals int64
		for _, c := range phase1 {
			p1ops += c.Ops
			p1evals += c.Evals
		}
		// 多进程同时跑。分母用最长的那个进程，得到的是这一窗口里打到同一个 Redis 的 QPS。
		res.QPS = float64(p1ops) / maxEl
		res.RedisEvalQPS = float64(p1evals) / maxEl
	}
	return res
}

func addChild(res *result, c childResult) {
	res.Ops += c.Ops
	res.RedisEvals += c.Evals
	res.StockEvals += c.StockEvals
	res.LocalReject += c.LocalReject
	res.QuotaReject += c.QuotaReject
	res.Probes += c.Probes
	res.Success += c.Success
	res.SoldOut += c.SoldOut
	res.Duplicate += c.Duplicate
	res.Errors += c.Errors
}

func check(cfg config, res *result) (bool, string) {
	var reasons []string
	fail := func(format string, args ...any) {
		reasons = append(reasons, fmt.Sprintf(format, args...))
	}
	if res.Errors != 0 {
		fail("errors=%d", res.Errors)
	}
	if res.Duplicate != 0 {
		fail("duplicate=%d", res.Duplicate)
	}
	if res.Winners != res.Success {
		fail("winners=%d success=%d", res.Winners, res.Success)
	}
	if res.Success+res.FinalStock != res.InitialStock {
		fail("success+final=%d initial=%d", res.Success+res.FinalStock, res.InitialStock)
	}
	accounted := res.LocalReject + res.QuotaReject + res.StockEvals + res.Errors
	if res.Ops != accounted {
		fail("ops=%d accounted=%d", res.Ops, accounted)
	}
	switch cfg.Scenario {
	case "one-ok":
		if res.Success != 1 || res.RedisEvals != 1 || res.FinalStock != 0 {
			fail("one-ok success=%d evals=%d final=%d", res.Success, res.RedisEvals, res.FinalStock)
		}
		// EVALSHA + SISMEMBER + GET + DECR + SADD。7.0.15 会把脚本里的 redis.call 算进命令数。
		if res.RedisCommands != 5 {
			fail("commands=%d want 5", res.RedisCommands)
		}
	case "one-sold":
		if res.RedisEvals != 1 || res.Success != 0 || res.SoldOut != 1 {
			fail("one-sold evals=%d success=%d soldOut=%d", res.RedisEvals, res.Success, res.SoldOut)
		}
		// EVALSHA + SISMEMBER + GET。
		if res.RedisCommands != 3 {
			fail("commands=%d want 3", res.RedisCommands)
		}
	case "sellout":
		requireFlagSell(cfg, res, fail)
	case "sellout-noflag":
		if cfg.UseFlag {
			fail("flag should be off")
		}
		want := cfg.Attempts * int64(cfg.Instances)
		if res.RedisEvals != want || res.Ops != want {
			fail("evals=%d ops=%d want=%d", res.RedisEvals, res.Ops, want)
		}
		if res.Success != cfg.Stock {
			fail("success=%d stock=%d", res.Success, cfg.Stock)
		}
	case "soldout-hold":
		if !cfg.UseFlag {
			fail("flag should be on")
		}
		if res.Success != 0 || res.FinalStock != 0 {
			fail("success=%d final=%d", res.Success, res.FinalStock)
		}
		bound := int64(cfg.Instances * cfg.Workers * 4)
		if res.RedisEvals > bound || res.RedisEvals < int64(cfg.Instances) {
			fail("evals=%d bound=%d", res.RedisEvals, bound)
		}
		if res.LocalReject < res.RedisEvals*10 {
			fail("localReject=%d evals=%d", res.LocalReject, res.RedisEvals)
		}
		if !allFlagged(res, 1) {
			fail("an instance never observed sold-out")
		}
	case "soldout-open":
		requireOpenRedis(cfg, res, fail)
		if res.Success != 0 {
			fail("success=%d", res.Success)
		}
	case "flood-open":
		requireOpenRedis(cfg, res, fail)
		if res.FinalStock <= 0 {
			fail("stock exhausted")
		}
	case "flood-quota":
		requireLocalQuota(cfg, res, fail)
		if res.FinalStock <= 0 {
			fail("stock exhausted")
		}
		if res.QuotaReject <= res.RedisEvals {
			fail("quotaReject=%d evals=%d", res.QuotaReject, res.RedisEvals)
		}
	case "undersell":
		requireLocalQuota(cfg, res, fail)
		if res.Success < 100 || res.Success > res.InitialStock/5 {
			fail("success=%d initial=%d", res.Success, res.InitialStock)
		}
		if res.FinalStock <= res.InitialStock/2 {
			fail("final=%d initial=%d", res.FinalStock, res.InitialStock)
		}
	case "drain":
		if !cfg.UseFlag || cfg.Gate != "local" || cfg.Quota <= 0 {
			fail("drain needs flag and local quota")
		}
		if res.Success != cfg.Stock || res.FinalStock != 0 {
			fail("success=%d final=%d stock=%d", res.Success, res.FinalStock, cfg.Stock)
		}
		bound := cfg.Stock + int64(cfg.Instances*cfg.Workers*6)
		if res.RedisEvals > bound || res.RedisEvals < cfg.Stock {
			fail("evals=%d bound=%d", res.RedisEvals, bound)
		}
		if !allFlagged(res, 1) {
			fail("an instance never observed sold-out")
		}
	case "restock-stale", "restock-probe":
		checkRestock(cfg, res, fail)
	case "bad-instance":
		checkBad(cfg, res, fail)
	case "redis-gate":
		if cfg.Gate != "redis" || cfg.UseFlag {
			fail("redis-gate setup")
		}
		if res.StockEvals != int64(cfg.Limit) {
			fail("stockEvals=%d limit=%d", res.StockEvals, cfg.Limit)
		}
		if res.RedisEvals < res.StockEvals*5 {
			fail("evals=%d stockEvals=%d", res.RedisEvals, res.StockEvals)
		}
		if res.RedisCPUpct < 20 {
			fail("redis cpu %.1f", res.RedisCPUpct)
		}
	case "local-gate":
		requireLocalQuota(cfg, res, fail)
		if res.QuotaReject <= res.RedisEvals {
			fail("quotaReject=%d evals=%d", res.QuotaReject, res.RedisEvals)
		}
	default:
		fail("unknown scenario %s", cfg.Scenario)
	}
	summary := fmt.Sprintf("evals=%d stockEvals=%d commands=%d evalQps=%.0f cpu=%.1f success=%d final=%d localReject=%d quotaReject=%d probes=%d p99us=%d",
		res.RedisEvals, res.StockEvals, res.RedisCommands, res.RedisEvalQPS, res.RedisCPUpct,
		res.Success, res.FinalStock, res.LocalReject, res.QuotaReject, res.Probes, res.P99us)
	if len(reasons) == 0 {
		return true, summary
	}
	return false, strings.Join(reasons, "; ") + " | " + summary
}

func requireFlagSell(cfg config, res *result, fail func(string, ...any)) {
	if !cfg.UseFlag {
		fail("flag should be on")
	}
	if res.Success != cfg.Stock {
		fail("success=%d stock=%d", res.Success, cfg.Stock)
	}
	bound := cfg.Stock*2 + int64(cfg.Instances*cfg.Workers*6)
	if res.RedisEvals > bound || res.RedisEvals < cfg.Stock {
		fail("evals=%d bound=%d", res.RedisEvals, bound)
	}
	if !allFlagged(res, 1) {
		fail("an instance never observed sold-out")
	}
}

func requireOpenRedis(cfg config, res *result, fail func(string, ...any)) {
	if cfg.UseFlag || cfg.Quota > 0 || cfg.Gate != "none" {
		fail("open redis setup")
	}
	if res.RedisEvals != res.Ops {
		fail("evals=%d ops=%d", res.RedisEvals, res.Ops)
	}
	if res.RedisEvalQPS < 30000 {
		fail("evalQps=%.0f", res.RedisEvalQPS)
	}
	if res.RedisCPUpct < 20 {
		fail("redis cpu %.1f", res.RedisCPUpct)
	}
}

func requireLocalQuota(cfg config, res *result, fail func(string, ...any)) {
	if cfg.Gate != "local" || cfg.Quota <= 0 {
		fail("local quota setup")
	}
	for _, c := range res.Children {
		if c.Phase != 1 || c.Gate != "local" {
			continue
		}
		if !inBand(c, cfg.Burst) {
			fail("instance %d evals=%d elapsed=%.3f quota=%.0f burst=%.0f", c.Instance, c.Evals, c.ElapsedSec, c.QuotaPerSec, cfg.Burst)
		}
	}
}

func checkRestock(cfg config, res *result, fail func(string, ...any)) {
	if !cfg.UseFlag || res.Phase2 == nil {
		fail("restock setup")
	}
	p1success := res.Success - res.Phase2.Success
	p1evals := res.RedisEvals - res.Phase2.Evals
	if p1success != cfg.Stock {
		fail("phase1 success=%d stock=%d", p1success, cfg.Stock)
	}
	bound := cfg.Stock*2 + int64(cfg.Instances*cfg.Workers*6)
	if p1evals > bound {
		fail("phase1 evals=%d bound=%d", p1evals, bound)
	}
	if !allFlagged(res, 1) {
		fail("an instance never observed sold-out in phase 1")
	}
	if cfg.Scenario == "restock-stale" {
		if cfg.Probe != 0 {
			fail("stale probe should be 0")
		}
		if res.Phase2.Success != 0 || res.Phase2.Evals != 0 {
			fail("phase2 success=%d evals=%d", res.Phase2.Success, res.Phase2.Evals)
		}
		if res.Phase2.LocalReject < 100 {
			fail("phase2 localReject=%d", res.Phase2.LocalReject)
		}
		if res.FinalStock != cfg.Restock {
			fail("final=%d restock=%d", res.FinalStock, cfg.Restock)
		}
		return
	}
	if cfg.Probe <= 0 {
		fail("probe required")
	}
	if res.Phase2.Success != cfg.Restock || res.FinalStock != 0 {
		fail("phase2 success=%d final=%d restock=%d", res.Phase2.Success, res.FinalStock, cfg.Restock)
	}
	if res.Phase2.Evals < cfg.Restock {
		fail("phase2 evals=%d", res.Phase2.Evals)
	}
	p2bound := cfg.Restock + int64(cfg.Instances*cfg.Workers*8)
	if res.Phase2.Evals > p2bound {
		fail("phase2 evals=%d bound=%d", res.Phase2.Evals, p2bound)
	}
}

func checkBad(cfg config, res *result, fail func(string, ...any)) {
	if !cfg.Bad || cfg.Gate != "local" {
		fail("bad-instance setup")
	}
	if res.FinalStock <= 0 {
		fail("stock exhausted")
	}
	var bad, goodMax int64
	goodN := 0
	for _, c := range res.Children {
		if c.Phase != 1 {
			continue
		}
		if c.Gate == "none" {
			bad += c.Evals
			continue
		}
		goodN++
		if c.Evals > goodMax {
			goodMax = c.Evals
		}
		if !inBand(c, cfg.Burst) {
			fail("good instance %d evals=%d elapsed=%.3f", c.Instance, c.Evals, c.ElapsedSec)
		}
		if c.QuotaReject <= c.Evals {
			fail("good instance %d quotaReject=%d evals=%d", c.Instance, c.QuotaReject, c.Evals)
		}
	}
	if goodN < 1 || bad < 10000 || bad < goodMax*3 {
		fail("bad=%d goodMax=%d", bad, goodMax)
	}
}

func inBand(c childResult, burst float64) bool {
	if c.QuotaPerSec <= 0 || c.ElapsedSec <= 0 {
		return false
	}
	got := float64(c.Evals)
	exp := c.QuotaPerSec*c.ElapsedSec + burst
	if got > exp*1.25+50 {
		return false
	}
	if got < c.QuotaPerSec*c.ElapsedSec*0.70 {
		return false
	}
	return true
}

func allFlagged(res *result, phase int) bool {
	n := 0
	for _, c := range res.Children {
		if c.Phase != phase {
			continue
		}
		n++
		if c.FlagAtMs < 0 {
			return false
		}
	}
	return n > 0
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
