package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var algos = []string{"fixed", "fixedhot", "slide", "log", "bucket"}

type scripts struct {
	sha map[string]string
}

type runner struct {
	c      *Conn
	s      scripts
	addr   string
	out    string
	only   string
	pid    int
	clk    float64
	failed []string
}

func loadScripts(c *Conn, dir string) (scripts, error) {
	s := scripts{sha: map[string]string{}}
	for _, name := range []string{"fixed", "fixedhot", "slide", "log", "bucket", "slotof", "slotscan"} {
		b, err := os.ReadFile(filepath.Join(dir, name+".lua"))
		if err != nil {
			return s, err
		}
		sha, err := c.scriptLoad(string(b))
		if err != nil {
			return s, fmt.Errorf("%s: %w", name, err)
		}
		s.sha[name] = sha
	}
	return s, nil
}

func (r *runner) want(name string) bool {
	return r.only == "" || strings.Contains(name, r.only)
}

func (r *runner) save(name string, res result) {
	if !r.want(name) {
		return
	}
	if err := writeResult(r.out, name, res); err != nil {
		r.failed = append(r.failed, name+": "+err.Error())
		fmt.Fprintf(os.Stderr, "%s write error: %v\n", name, err)
		return
	}
	fmt.Fprintf(os.Stderr, "%s pass=%v %s\n", name, res.Pass, res.Detail)
	if !res.Pass {
		r.failed = append(r.failed, name)
	}
}

func (r *runner) reconnect() error {
	if r.c != nil {
		r.c.Close()
	}
	c, err := dial(r.addr)
	if err != nil {
		return err
	}
	r.c = c
	return nil
}

func (r *runner) flush() error {
	if err := r.c.flushDB(); err != nil {
		if e := r.reconnect(); e != nil {
			return err
		}
		return r.c.flushDB()
	}
	return nil
}

func (r *runner) serial(algo, key string, limit, window, now, extra, n int64) (int64, error) {
	sha := r.s.sha[algo]
	var admits int64
	for i := int64(0); i < n; i++ {
		v, err := r.c.evalInt(sha, key, limit, window, now, extra)
		if err != nil {
			_ = r.reconnect()
			return admits, err
		}
		admits += v
	}
	return admits, nil
}

func (r *runner) pipelineN(algo, key string, limit, window, now, extra, n int64) (int64, time.Duration, error) {
	sha := r.s.sha[algo]
	t0 := time.Now()
	for i := int64(0); i < n; i++ {
		if err := r.c.writeEval(sha, key, limit, window, now, extra); err != nil {
			_ = r.reconnect()
			return 0, 0, err
		}
	}
	if err := r.c.flush(); err != nil {
		_ = r.reconnect()
		return 0, 0, err
	}
	var admits int64
	for i := int64(0); i < n; i++ {
		rep, err := readReply(r.c.r)
		if err != nil {
			_ = r.reconnect()
			return admits, time.Since(t0), err
		}
		admits += rep.Int
	}
	return admits, time.Since(t0), nil
}

func (r *runner) pipelinePair(algo, key string, limit, window, now1, now2, extra, n1, n2 int64) (int64, int64, time.Duration, error) {
	sha := r.s.sha[algo]
	t0 := time.Now()
	for i := int64(0); i < n1; i++ {
		if err := r.c.writeEval(sha, key, limit, window, now1, extra); err != nil {
			_ = r.reconnect()
			return 0, 0, 0, err
		}
	}
	for i := int64(0); i < n2; i++ {
		if err := r.c.writeEval(sha, key, limit, window, now2, extra); err != nil {
			_ = r.reconnect()
			return 0, 0, 0, err
		}
	}
	if err := r.c.flush(); err != nil {
		_ = r.reconnect()
		return 0, 0, 0, err
	}
	var a1, a2 int64
	for i := int64(0); i < n1; i++ {
		rep, err := readReply(r.c.r)
		if err != nil {
			_ = r.reconnect()
			return a1, a2, time.Since(t0), err
		}
		a1 += rep.Int
	}
	for i := int64(0); i < n2; i++ {
		rep, err := readReply(r.c.r)
		if err != nil {
			_ = r.reconnect()
			return a1, a2, time.Since(t0), err
		}
		a2 += rep.Int
	}
	return a1, a2, time.Since(t0), nil
}

func (r *runner) delta(fn func() error) (int64, error) {
	before, err := r.c.commands()
	if err != nil {
		return 0, err
	}
	if err := fn(); err != nil {
		return 0, err
	}
	after, err := r.c.commands()
	if err != nil {
		return 0, err
	}
	// after 那次 INFO 算进了 after。
	return after - before - 1, nil
}

func ms(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func stamps(now, n int64) []int64 {
	ts := make([]int64, n)
	for i := range ts {
		ts[i] = now
	}
	return ts
}

func (r *runner) runAll() error {
	if r.want("commands") {
		r.save("commands", r.commandProbe())
	}
	if r.want("slot-scan") {
		r.save("slot-scan", r.slotScan())
	}
	r.shapes()
	r.pipes()
	if r.want("bucket-spread") {
		r.save("bucket-spread", r.replayBucket("bucket-spread", 100, 100))
	}
	if r.want("bucket-cap1") {
		r.save("bucket-cap1", r.replayBucket("bucket-cap1", 100, 1))
	}
	r.races()
	if r.want("ttl-short") {
		r.save("ttl-short", r.ttlShort())
	}
	if r.want("interceptor-serial") {
		r.save("interceptor-serial", r.interceptorSerial())
	}
	if r.want("interceptor-forced") {
		r.save("interceptor-forced", r.interceptorForced())
	}
	if r.want("interceptor-free") {
		r.save("interceptor-free", r.interceptorFree())
	}
	if r.want("interceptor-ttl") {
		r.save("interceptor-ttl", r.interceptorTTL())
	}
	if r.want("memory") {
		r.save("memory", r.memory())
	}
	for _, algo := range algos {
		name := "realclock-" + algo
		if r.want(name) {
			r.save(name, r.realclock(algo))
		}
	}
	r.benches()
	if r.want("local-spike") {
		r.save("local-spike", localSpike())
	}
	if r.want("local-multi") {
		r.save("local-multi", localMulti())
	}
	if r.want("local-bench") {
		r.save("local-bench", localBench())
	}
	if len(r.failed) > 0 {
		return fmt.Errorf("failed: %s", strings.Join(r.failed, ", "))
	}
	return nil
}

func (r *runner) commandProbe() result {
	res := result{
		Scenario: "commands",
		Note:     "total_commands_processed 含 EVALSHA 和脚本里的 redis.call。差值已减去收尾那次 INFO。固定窗口拒绝不再 INCR；fixedhot 拒绝也 INCR。",
	}
	if err := r.flush(); err != nil {
		return done(res, false, err.Error())
	}
	type spec struct {
		name string
		algo string
		now  int64
		lim  int64
		nth  int
		want int64
	}
	// nth 是这一下是第几次调用（从 1 开始），用来区分第一次放行和随后的拒绝。
	specs := []spec{
		{"fixed-admit", "fixed", 100, 2, 1, 4},
		{"fixed-admit-2", "fixed", 100, 2, 2, 4},
		{"fixed-reject", "fixed", 100, 2, 3, 2},
		{"fixedhot-admit", "fixedhot", 100, 2, 1, 3},
		{"fixedhot-admit-2", "fixedhot", 100, 2, 2, 2},
		{"fixedhot-reject", "fixedhot", 100, 2, 3, 2},
		{"slide-admit", "slide", 100, 1, 1, 5},
		{"slide-reject", "slide", 100, 1, 2, 3},
		{"log-admit", "log", 100, 1, 1, 7},
		{"log-reject", "log", 100, 1, 2, 4},
		{"bucket-admit", "bucket", 100, 1, 1, 4},
		{"bucket-reject", "bucket", 100, 1, 2, 4},
	}
	// 每个算法单独的 key，调用序自己数。
	seen := map[string]int{}
	pass := true
	var bad []string
	for _, sp := range specs {
		seen[sp.algo]++
		if seen[sp.algo] != sp.nth {
			return done(res, false, "probe order "+sp.name)
		}
		key := "cmd:" + sp.algo
		d, err := r.delta(func() error {
			_, err := r.c.evalInt(r.s.sha[sp.algo], key, sp.lim, 1000, sp.now, 0)
			return err
		})
		ph := phase{Name: sp.name, Commands: d, Want: sp.want, Admits: 0}
		if err != nil {
			pass = false
			ph.Note = err.Error()
			bad = append(bad, sp.name+" "+err.Error())
		} else if d != sp.want {
			pass = false
			bad = append(bad, fmt.Sprintf("%s got %d want %d", sp.name, d, sp.want))
			ph.Note = "mismatch"
		}
		res.Phases = append(res.Phases, ph)
	}
	detail := "all matched"
	if !pass {
		detail = strings.Join(bad, "; ")
	}
	return done(res, pass, detail)
}

func (r *runner) slotScan() result {
	res := result{
		Scenario: "slot-scan",
		WindowMs: 1000,
		Note:     "修正是用窗口号乘回去核对。bad 是未修正的 math.floor 和修正结果不一致的毫秒数。slotof 必须和 Go 的整数除法一致。",
	}
	type band struct {
		name string
		from int64
		to   int64
	}
	now := time.Now().UnixMilli()
	bands := []band{
		{"from-zero", 0, 200000},
		{"around-now", now, now + 200000},
		{"around-2^52", int64(1) << 52, (int64(1) << 52) + 50000},
	}
	pass := true
	var bits []string
	for _, b := range bands {
		rep, err := r.c.evalShaRaw(0, r.s.sha["slotscan"], "1000", strconv.FormatInt(b.from, 10), strconv.FormatInt(b.to, 10))
		if err != nil {
			return done(res, false, b.name+": "+err.Error())
		}
		parts := strings.Split(rep.Str, ",")
		if len(parts) != 3 {
			return done(res, false, "scan reply "+rep.Str)
		}
		badN, _ := strconv.ParseInt(parts[0], 10, 64)
		first, _ := strconv.ParseInt(parts[1], 10, 64)
		checked, _ := strconv.ParseInt(parts[2], 10, 64)
		res.Phases = append(res.Phases, phase{
			Name: b.name, Admits: badN, Attempts: checked, Final: first,
			Note: "admits 字段这里是不一致的次数，final 是第一次不一致的 now，没有则是 -1",
		})
		bits = append(bits, fmt.Sprintf("%s bad=%d checked=%d", b.name, badN, checked))
	}
	samples := []int64{0, 1, 999, 1000, 1001, 1998, 1999, 2000, now, int64(1) << 52, (int64(1) << 52) + 999, (int64(1) << 52) + 1000}
	for _, sample := range samples {
		rep, err := r.c.evalShaRaw(0, r.s.sha["slotof"], strconv.FormatInt(sample, 10), "1000")
		if err != nil {
			return done(res, false, err.Error())
		}
		if rep.Int != slotOf(sample, 1000) {
			pass = false
			bits = append(bits, fmt.Sprintf("slot %d lua %d go %d", sample, rep.Int, slotOf(sample, 1000)))
		}
	}
	if pass {
		bits = append(bits, "slotof matched")
	}
	return done(res, pass, strings.Join(bits, "; "))
}

func (r *runner) shapes() {
	for _, algo := range algos {
		name := "same-" + algo
		if r.want(name) {
			r.save(name, r.same(algo))
		}
		name = "gap1-" + algo
		if r.want(name) {
			r.save(name, r.gap(algo, 999, 1000))
		}
		name = "gap999-" + algo
		if r.want(name) {
			r.save(name, r.gap(algo, 999, 1998))
		}
	}
}

func (r *runner) same(algo string) result {
	const limit int64 = 100
	const window int64 = 1000
	const now int64 = 500
	res := result{
		Scenario: "same-" + algo, Algo: algo, Limit: limit, WindowMs: window,
		Note: "同一个时间戳上连续打两倍次数。正确的放行是上限本身。fixed 的计数停在上限，fixedhot 把拒绝也计进去。",
	}
	if err := r.flush(); err != nil {
		return done(res, false, err.Error())
	}
	key := "same:" + algo
	attempts := limit * 2
	admits, err := r.serial(algo, key, limit, window, now, 0, attempts)
	if err != nil {
		return done(res, false, err.Error())
	}
	res.Attempts = attempts
	res.Admits = admits
	res.Rejects = attempts - admits
	pass := admits == limit
	switch algo {
	case "fixed", "slide":
		n, ok, err := r.c.getInt(fmt.Sprintf("%s:%d", key, slotOf(now, window)))
		if err != nil || !ok || n != limit {
			pass = false
			res.FinalCount = n
			return done(res, false, fmt.Sprintf("admits %d final %d ok %v err %v", admits, n, ok, err))
		}
		res.FinalCount = n
	case "fixedhot":
		n, ok, err := r.c.getInt(fmt.Sprintf("%s:%d", key, slotOf(now, window)))
		if err != nil || !ok || n != attempts {
			pass = false
			res.FinalCount = n
			return done(res, false, fmt.Sprintf("admits %d final %d want counter %d", admits, n, attempts))
		}
		res.FinalCount = n
	case "log":
		rep, err := r.c.cmd("ZCARD", key)
		if err != nil || rep.Int != limit {
			return done(res, false, fmt.Sprintf("zcard %d err %v", rep.Int, err))
		}
		res.Members = rep.Int
	}
	return done(res, pass, fmt.Sprintf("admits %d rejects %d final %d", admits, attempts-admits, res.FinalCount))
}

func wantGap(algo string, before, after, limit, window int64) (int64, int64) {
	if algo == "fixed" || algo == "fixedhot" {
		return limit, limit
	}
	if after-before == 1 {
		return limit, 0
	}
	// 999 -> 1998，窗口 1000。滑动计数和令牌桶都会再放进 99 个。
	if algo == "log" {
		return limit, 0
	}
	return limit, 99
}

func (r *runner) gap(algo string, before, after int64) result {
	const limit int64 = 100
	const window int64 = 1000
	secondN := limit * 2
	name := fmt.Sprintf("gap%d-%s", after-before, algo)
	res := result{
		Scenario: name, Algo: algo, Limit: limit, WindowMs: window, LogicalGapMs: after - before,
		Note: "调用方传入两个时间戳。第一段打满上限，第二段打两倍次数。窗口内最多含差值小于窗口的放行。",
	}
	if err := r.flush(); err != nil {
		return done(res, false, err.Error())
	}
	key := name + ":" + algo
	t0 := time.Now()
	a1, err := r.serial(algo, key, limit, window, before, 0, limit)
	if err != nil {
		return done(res, false, err.Error())
	}
	a2, err := r.serial(algo, key, limit, window, after, 0, secondN)
	if err != nil {
		return done(res, false, err.Error())
	}
	res.WallMs = ms(time.Since(t0))
	res.AdmitsBefore = a1
	res.AdmitsAfter = a2
	res.Admits = a1 + a2
	ts := append(stamps(before, a1), stamps(after, a2)...)
	res.MaxInWindow = maxInWindow(ts, window)
	wb, wa := wantGap(algo, before, after, limit, window)
	pass := a1 == wb && a2 == wa
	if algo == "bucket" {
		ob, oa := oracleBucket(before, after, limit, limit, limit, secondN)
		if a1 != ob || a2 != oa {
			pass = false
		}
		if ob != wb || oa != wa {
			pass = false
			return done(res, false, fmt.Sprintf("oracle %d+%d drifted from %d+%d", ob, oa, wb, wa))
		}
	}
	wantMax := maxInWindow(append(stamps(before, wb), stamps(after, wa)...), window)
	if res.MaxInWindow != wantMax {
		pass = false
	}
	return done(res, pass, fmt.Sprintf("前 %d 后 %d，窗口内最多 %d，逻辑间隔 %dms，串行墙钟 %.3fms",
		a1, a2, res.MaxInWindow, after-before, res.WallMs))
}

func oracleBucket(before, after, rate, cap, n1, n2 int64) (int64, int64) {
	b := memBucket{rate: rate, window: 1000, capN: cap}
	a1, _ := countAllows(before, int(n1), b.allow)
	a2, _ := countAllows(after, int(n2), b.allow)
	return a1, a2
}

func (r *runner) pipes() {
	for _, n := range []int64{100, 1000, 5000} {
		for _, algo := range algos {
			name := fmt.Sprintf("pipe-%d-%s", n, algo)
			if r.want(name) {
				r.save(name, r.pipe(algo, n))
			}
		}
	}
}

func (r *runner) pipe(algo string, limit int64) result {
	const window int64 = 1000
	name := fmt.Sprintf("pipe-%d-%s", limit, algo)
	res := result{
		Scenario: name, Algo: algo, Limit: limit, WindowMs: window, LogicalGapMs: 1,
		Note: "两段请求写进同一次管线，服务端背靠背执行。墙钟含一次往返。这是尖峰的宽度，不是 4 秒吞吐。",
	}
	if err := r.flush(); err != nil {
		return done(res, false, err.Error())
	}
	tPing := time.Now()
	if _, err := r.c.cmd("PING"); err != nil {
		return done(res, false, err.Error())
	}
	res.PingUs = time.Since(tPing).Microseconds()
	key := name
	a1, a2, wall, err := r.pipelinePair(algo, key, limit, window, 999, 1000, 0, limit, limit)
	if err != nil {
		return done(res, false, err.Error())
	}
	res.AdmitsBefore = a1
	res.AdmitsAfter = a2
	res.Admits = a1 + a2
	res.WallMs = ms(wall)
	ts := append(stamps(999, a1), stamps(1000, a2)...)
	res.MaxInWindow = maxInWindow(ts, window)
	wb, wa := wantGap(algo, 999, 1000, limit, window)
	if algo == "bucket" {
		// 1ms 会补进 rate/1000 个令牌。100/s 是 0，1000/s 是 1，5000/s 是 5。
		wb, wa = oracleBucket(999, 1000, limit, limit, limit, limit)
		if wa > limit/10 {
			return done(res, false, fmt.Sprintf("oracle second %d too high", wa))
		}
	}
	pass := a1 == wb && a2 == wa
	return done(res, pass, fmt.Sprintf("前 %d 后 %d，窗口内最多 %d，管线墙钟 %.3fms，ping %dus",
		a1, a2, res.MaxInWindow, res.WallMs, res.PingUs))
}

func (r *runner) replayBucket(name string, rate, cap int64) result {
	res := result{
		Scenario: name, Algo: "bucket", Limit: rate, WindowMs: 1000,
		Note: "逐步对照进程里的毫令牌规则。cap 等于速率时，铺满一个窗口的放行数会接近两倍。cap=1 时任意一段里不超过速率。",
	}
	if err := r.flush(); err != nil {
		return done(res, false, err.Error())
	}
	b := memBucket{rate: rate, window: 1000, capN: cap}
	key := name
	var mismatch int64
	var redisTs []int64
	var redisAdmits int64
	step := func(now, n int64) error {
		for i := int64(0); i < n; i++ {
			want := b.allow(now)
			got, err := r.c.evalInt(r.s.sha["bucket"], key, rate, 1000, now, cap)
			if err != nil {
				return err
			}
			if (got == 1) != want {
				mismatch++
			}
			if got == 1 {
				redisAdmits++
				redisTs = append(redisTs, now)
			}
		}
		return nil
	}
	if err := step(0, 150); err != nil {
		return done(res, false, err.Error())
	}
	if err := step(1, 100); err != nil {
		return done(res, false, err.Error())
	}
	for now := int64(10); now <= 1000; now += 10 {
		if err := step(now, 1); err != nil {
			return done(res, false, err.Error())
		}
	}
	res.Admits = redisAdmits
	res.MaxInWindow = maxInWindow(redisTs, 1000)
	res.Errors = mismatch
	pass := mismatch == 0
	if cap == rate && (res.MaxInWindow != 199 || redisAdmits != 200) {
		pass = false
	}
	if cap == 1 && (res.MaxInWindow != 100 || redisAdmits != 101) {
		pass = false
	}
	return done(res, pass, fmt.Sprintf("admits %d maxInWindow %d mismatch %d", redisAdmits, res.MaxInWindow, mismatch))
}

func (r *runner) races() {
	for _, algo := range algos {
		name := "race-" + algo
		if r.want(name) {
			r.save(name, r.race(algo))
		}
	}
}

func (r *runner) race(algo string) result {
	const (
		workers = 64
		each    = 20
		limit   = 100
		window  = 1000
		now     = int64(1_000_000)
	)
	attempts := int64(workers * each)
	res := result{
		Scenario: "race-" + algo, Algo: algo, Limit: limit, WindowMs: window,
		Workers: workers, Attempts: attempts,
		Note: "64 个连接同时打同一个窗口。Lua 串行执行，放行正好是上限。fixedhot 的计数等于尝试次数。",
	}
	if err := r.flush(); err != nil {
		return done(res, false, err.Error())
	}
	key := "race:" + algo
	var admits, errors atomic.Int64
	var wg sync.WaitGroup
	ready := sync.WaitGroup{}
	start := make(chan struct{})
	sha := r.s.sha[algo]
	for i := 0; i < workers; i++ {
		wg.Add(1)
		ready.Add(1)
		go func() {
			defer wg.Done()
			c, err := dial(r.addr)
			ready.Done()
			if err != nil {
				errors.Add(1)
				return
			}
			defer c.Close()
			<-start
			for k := 0; k < each; k++ {
				v, err := c.evalInt(sha, key, limit, window, now, 0)
				if err != nil {
					errors.Add(1)
					return
				}
				admits.Add(v)
			}
		}()
	}
	ready.Wait()
	close(start)
	wg.Wait()
	res.Admits = admits.Load()
	res.Errors = errors.Load()
	res.Rejects = attempts - res.Admits
	pass := res.Errors == 0 && res.Admits == limit
	slotKey := fmt.Sprintf("%s:%d", key, slotOf(now, window))
	switch algo {
	case "fixed", "slide":
		n, ok, err := r.c.getInt(slotKey)
		res.FinalCount = n
		if err != nil || !ok || n != limit {
			pass = false
		}
	case "fixedhot":
		n, ok, err := r.c.getInt(slotKey)
		res.FinalCount = n
		if err != nil || !ok || n != attempts {
			pass = false
		}
	case "log":
		rep, err := r.c.cmd("ZCARD", key)
		if err != nil || rep.Int != limit {
			pass = false
		}
		res.Members = rep.Int
	}
	return done(res, pass, fmt.Sprintf("admits %d rejects %d errors %d final %d", res.Admits, res.Rejects, res.Errors, res.FinalCount))
}

func (r *runner) ttlShort() result {
	res := result{
		Scenario: "ttl-short", Algo: "fixed", Limit: 30, WindowMs: 10000,
		Note: "窗口 id 还在同一个 10 秒槽里。TTL 写成 2 秒时，真实睡过 2.3 秒后计数没了，又能放行一倍。TTL 写成两个窗口的那份还在。仓库里的脚本把 expire 写成 2，key 本身带了秒，所以 1 秒窗口盖得住；窗口比 2 秒长时盖不住。",
	}
	if err := r.flush(); err != nil {
		return done(res, false, err.Error())
	}
	shortAdmits, err := r.serial("fixed", "ttl:short", 30, 10000, 0, 2000, 30)
	if err != nil {
		return done(res, false, err.Error())
	}
	longAdmits, err := r.serial("fixed", "ttl:long", 30, 10000, 0, 20000, 30)
	if err != nil {
		return done(res, false, err.Error())
	}
	extraS, err := r.serial("fixed", "ttl:short", 30, 10000, 0, 2000, 1)
	if err != nil {
		return done(res, false, err.Error())
	}
	extraL, err := r.serial("fixed", "ttl:long", 30, 10000, 0, 20000, 1)
	if err != nil {
		return done(res, false, err.Error())
	}
	time.Sleep(2300 * time.Millisecond)
	_, shortOK, err := r.c.getInt("ttl:short:0")
	if err != nil {
		return done(res, false, err.Error())
	}
	longN, longOK, err := r.c.getInt("ttl:long:0")
	if err != nil {
		return done(res, false, err.Error())
	}
	// 逻辑时间 100 仍落在 0 号槽。
	againS, err := r.serial("fixed", "ttl:short", 30, 10000, 100, 2000, 30)
	if err != nil {
		return done(res, false, err.Error())
	}
	againL, err := r.serial("fixed", "ttl:long", 30, 10000, 100, 20000, 1)
	if err != nil {
		return done(res, false, err.Error())
	}
	res.AdmitsBefore = shortAdmits
	res.AdmitsAfter = againS
	res.FinalCount = longN
	pass := shortAdmits == 30 && longAdmits == 30 && extraS == 0 && extraL == 0 &&
		!shortOK && longOK && longN == 30 && againS == 30 && againL == 0
	return done(res, pass, fmt.Sprintf("short %d then extra %d after sleep %d present=%v; long %d extra %d after %d final %d",
		shortAdmits, extraS, againS, shortOK, longAdmits, extraL, againL, longN))
}

func intercept(c *Conn, key string, max, ttlSec int64) (bool, error) {
	rep, err := c.cmd("GET", key)
	if err != nil {
		return false, err
	}
	if rep.Nil {
		if _, err := c.cmd("SETEX", key, strconv.FormatInt(ttlSec, 10), "1"); err != nil {
			return false, err
		}
		return true, nil
	}
	n, err := strconv.ParseInt(rep.Str, 10, 64)
	if err != nil {
		return false, err
	}
	if n < max {
		if _, err := c.cmd("INCR", key); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func (r *runner) interceptorSerial() result {
	res := result{
		Scenario: "interceptor-serial", Limit: 5, WindowMs: 5000,
		Note: "AccessInterceptor 的顺序：GET 为空就 SETEX 1，小于上限就 INCR，否则拒绝。单连接时正好 5 次。注解上的 maxCount=5、seconds=5。",
	}
	if err := r.flush(); err != nil {
		return done(res, false, err.Error())
	}
	var admits int64
	for i := 0; i < 20; i++ {
		ok, err := intercept(r.c, "ia:serial", 5, 5)
		if err != nil {
			return done(res, false, err.Error())
		}
		if ok {
			admits++
		}
	}
	final, ok, err := r.c.getInt("ia:serial")
	if err != nil {
		return done(res, false, err.Error())
	}
	res.Attempts = 20
	res.Admits = admits
	res.Rejects = 20 - admits
	res.FinalCount = final
	pass := admits == 5 && ok && final == 5
	return done(res, pass, fmt.Sprintf("admits %d final %d", admits, final))
}

func (r *runner) interceptorForced() result {
	const workers = 128
	res := result{
		Scenario: "interceptor-forced", Limit: 5, Workers: workers, WindowMs: 5000,
		Note: "128 个调用都先 GET 到空，再各自 SETEX 1。代码在两次调用之间没有原子性，这个交错是允许的。",
	}
	if err := r.flush(); err != nil {
		return done(res, false, err.Error())
	}
	type obs struct {
		isNil bool
		err   error
	}
	seen := make([]obs, workers)
	var phase1 sync.WaitGroup
	phase1.Add(workers)
	phase2 := make(chan struct{})
	var admits, errors atomic.Int64
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(i int) {
			defer wg.Done()
			c, err := dial(r.addr)
			if err != nil {
				seen[i].err = err
				errors.Add(1)
				phase1.Done()
				return
			}
			defer c.Close()
			rep, err := c.cmd("GET", "ia:forced")
			if err != nil {
				seen[i].err = err
				errors.Add(1)
				phase1.Done()
				return
			}
			seen[i].isNil = rep.Nil
			phase1.Done()
			<-phase2
			if !seen[i].isNil {
				return
			}
			if _, err := c.cmd("SETEX", "ia:forced", "5", "1"); err != nil {
				errors.Add(1)
				return
			}
			admits.Add(1)
		}(i)
	}
	phase1.Wait()
	close(phase2)
	wg.Wait()
	final, ok, err := r.c.getInt("ia:forced")
	if err != nil {
		return done(res, false, err.Error())
	}
	nils := 0
	for _, s := range seen {
		if s.isNil {
			nils++
		}
	}
	res.Admits = admits.Load()
	res.Errors = errors.Load()
	res.Attempts = workers
	res.FinalCount = final
	res.Members = int64(nils)
	pass := res.Errors == 0 && nils == workers && res.Admits == workers && ok && final == 1
	return done(res, pass, fmt.Sprintf("nil %d admits %d final %d errors %d", nils, res.Admits, final, res.Errors))
}

func (r *runner) interceptorFree() result {
	const workers = 128
	res := result{
		Scenario: "interceptor-free", Limit: 5, Workers: workers, WindowMs: 5000,
		Note: "不插入屏障，128 个连接一起起跑，各做一次。跑 3 轮。每一轮放行都应该大于 5。具体次数随调度变，三轮都写下来。",
	}
	if err := r.flush(); err != nil {
		return done(res, false, err.Error())
	}
	pass := true
	for round := 0; round < 3; round++ {
		key := fmt.Sprintf("ia:free:%d", round)
		var admits, errors atomic.Int64
		var wg sync.WaitGroup
		var ready sync.WaitGroup
		start := make(chan struct{})
		wg.Add(workers)
		ready.Add(workers)
		for i := 0; i < workers; i++ {
			go func() {
				defer wg.Done()
				c, err := dial(r.addr)
				ready.Done()
				if err != nil {
					errors.Add(1)
					return
				}
				defer c.Close()
				<-start
				ok, err := intercept(c, key, 5, 5)
				if err != nil {
					errors.Add(1)
					return
				}
				if ok {
					admits.Add(1)
				}
			}()
		}
		ready.Wait()
		close(start)
		wg.Wait()
		final, _, _ := r.c.getInt(key)
		ph := phase{Name: fmt.Sprintf("round-%d", round+1), Admits: admits.Load(), Final: final, Attempts: workers}
		if errors.Load() != 0 {
			pass = false
			ph.Note = fmt.Sprintf("errors %d", errors.Load())
		}
		if admits.Load() <= 5 {
			pass = false
		}
		res.Rounds = append(res.Rounds, ph)
		res.Admits += admits.Load()
		res.Errors += errors.Load()
	}
	return done(res, pass, fmt.Sprintf("rounds %d/%d %d/%d %d/%d",
		res.Rounds[0].Admits, res.Rounds[0].Final,
		res.Rounds[1].Admits, res.Rounds[1].Final,
		res.Rounds[2].Admits, res.Rounds[2].Final))
}

func (r *runner) interceptorTTL() result {
	res := result{
		Scenario: "interceptor-ttl", Limit: 5, WindowMs: 5000,
		Note: "注解是 5 秒 5 次。INCR 不刷新 TTL。额度留到 TTL 末尾再用，过期后立刻又有 5 次。窗口内最多看的是这 5000ms。",
	}
	if err := r.flush(); err != nil {
		return done(res, false, err.Error())
	}
	if _, err := r.c.cmd("SETEX", "ia:pttl", "5", "1"); err != nil {
		return done(res, false, err.Error())
	}
	p1rep, err := r.c.cmd("PTTL", "ia:pttl")
	if err != nil {
		return done(res, false, err.Error())
	}
	time.Sleep(200 * time.Millisecond)
	p2rep, err := r.c.cmd("PTTL", "ia:pttl")
	if err != nil {
		return done(res, false, err.Error())
	}
	if _, err := r.c.cmd("INCR", "ia:pttl"); err != nil {
		return done(res, false, err.Error())
	}
	p3rep, err := r.c.cmd("PTTL", "ia:pttl")
	if err != nil {
		return done(res, false, err.Error())
	}
	p1, p2, p3 := p1rep.Int, p2rep.Int, p3rep.Int
	res.Phases = append(res.Phases, phase{Name: "pttl-before-sleep", Final: p1}, phase{Name: "pttl-after-200ms", Final: p2}, phase{Name: "pttl-after-incr", Final: p3})
	pttlOK := p2 < p1 && p2 < 4900 && p3 <= p2 && p3 < 4900

	t0 := time.Now()
	if _, err := r.c.cmd("SETEX", "ia:edge", "5", "1"); err != nil {
		return done(res, false, err.Error())
	}
	ts := []int64{t0.UnixMilli()}
	deadline := time.Now().Add(6 * time.Second)
	var pttl int64
	ready := false
	for time.Now().Before(deadline) {
		rep, err := r.c.cmd("PTTL", "ia:edge")
		if err != nil {
			return done(res, false, err.Error())
		}
		pttl = rep.Int
		if pttl < 0 {
			break
		}
		if pttl <= 900 && pttl >= 300 {
			ready = true
			break
		}
		time.Sleep(40 * time.Millisecond)
	}
	if !ready {
		return done(res, false, fmt.Sprintf("pttl proof %v %d %d %d; edge pttl %d", pttlOK, p1, p2, p3, pttl))
	}
	var late int64
	for i := 0; i < 10; i++ {
		ok, err := intercept(r.c, "ia:edge", 5, 5)
		if err != nil {
			return done(res, false, err.Error())
		}
		if !ok {
			break
		}
		late++
		ts = append(ts, time.Now().UnixMilli())
	}
	gone := false
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		rep, err := r.c.cmd("GET", "ia:edge")
		if err != nil {
			return done(res, false, err.Error())
		}
		if rep.Nil {
			gone = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	var post int64
	for i := 0; i < 8; i++ {
		ok, err := intercept(r.c, "ia:edge", 5, 5)
		if err != nil {
			return done(res, false, err.Error())
		}
		if !ok {
			continue
		}
		post++
		ts = append(ts, time.Now().UnixMilli())
	}
	res.Timestamps = ts
	res.AdmitsBefore = late
	res.AdmitsAfter = post
	res.Admits = 1 + late + post
	res.MaxInWindow = maxInWindow(ts, 5000)
	if len(ts) >= 2 {
		res.WallMs = float64(ts[len(ts)-1] - ts[1])
	}
	pass := pttlOK && ready && gone && late == 4 && post == 5 && res.MaxInWindow >= 9
	return done(res, pass, fmt.Sprintf("pttl %d -> %d -> %d; late %d post %d max %d span %dms",
		p1, p2, p3, late, post, res.MaxInWindow, int64(res.WallMs)))
}

func (r *runner) memory() result {
	res := result{
		Scenario: "memory", Algo: "log",
		Note: "滑动日志每个放行一个有序集合成员。used_memory 的差包含序号键和分配器。MEMORY USAGE 是这个键的估计。固定窗口对照是同一个整数键上 INCR 10 万次。规划用较大的那个每成员字节。",
	}
	pass := true
	var notes []string
	for _, n := range []int64{10000, 100000} {
		if err := r.flush(); err != nil {
			return done(res, false, err.Error())
		}
		base, err := r.c.usedMemory()
		if err != nil {
			return done(res, false, err.Error())
		}
		key := fmt.Sprintf("mem:%d", n)
		// 窗口足够大，这一批不会被裁掉。
		var admits int64
		const chunk = 500
		for off := int64(0); off < n; off += chunk {
			m := int64(chunk)
			if off+m > n {
				m = n - off
			}
			a, _, err := r.pipelineN("log", key, n, 10_000_000, 1_000_000, 0, m)
			if err != nil {
				return done(res, false, err.Error())
			}
			admits += a
		}
		after, err := r.c.usedMemory()
		if err != nil {
			return done(res, false, err.Error())
		}
		rep, err := r.c.cmd("ZCARD", key)
		if err != nil {
			return done(res, false, err.Error())
		}
		keyBytes, ok, err := r.c.memoryUsage(key)
		if err != nil || !ok {
			return done(res, false, fmt.Sprintf("usage %v", err))
		}
		seqBytes, _, _ := r.c.memoryUsage(key + ":seq")
		delta := after - base
		ph := phase{
			Name: fmt.Sprintf("log-%d", n), Attempts: n, Admits: admits, Final: rep.Int,
			Commands: delta, Want: keyBytes,
			Note: fmt.Sprintf("commands 字段是 used_memory 差，want 是 zset 的 MEMORY USAGE，序号键 %d 字节", seqBytes),
		}
		res.Phases = append(res.Phases, ph)
		if admits != n || rep.Int != n {
			pass = false
		}
		notes = append(notes, fmt.Sprintf("n=%d delta=%d usage=%d perDelta=%.2f perUsage=%.2f",
			n, delta, keyBytes, float64(delta)/float64(n), float64(keyBytes)/float64(n)))
		if n == 100000 {
			res.Members = n
			res.MemoryDelta = delta
			res.KeyBytes = keyBytes
			res.BytesPerMember = float64(delta) / float64(n)
			res.UsedMemory = after
			res.Admits = admits
		}
	}
	if err := r.flush(); err != nil {
		return done(res, false, err.Error())
	}
	base, err := r.c.usedMemory()
	if err != nil {
		return done(res, false, err.Error())
	}
	admits, err := r.serial("fixed", "mem:fixed", 100000, 1000, 500, 0, 100000)
	if err != nil {
		return done(res, false, err.Error())
	}
	after, err := r.c.usedMemory()
	if err != nil {
		return done(res, false, err.Error())
	}
	fixedBytes, _, _ := r.c.memoryUsage("mem:fixed:0")
	res.Phases = append(res.Phases, phase{
		Name: "fixed-100000", Admits: admits, Commands: after - base, Want: fixedBytes,
		Note: "commands 是 used_memory 差，want 是这个字符串键的 MEMORY USAGE",
	})
	notes = append(notes, fmt.Sprintf("fixedDelta=%d fixedUsage=%d", after-base, fixedBytes))
	return done(res, pass, strings.Join(notes, "; "))
}

func (r *runner) realclock(algo string) result {
	const limit int64 = 100
	res := result{
		Scenario: "realclock-" + algo, Algo: algo, Limit: limit, WindowMs: 1000,
		Note: "now=0，窗口用 Redis TIME。先用同样长度的管线量墙钟，再提前这个时间加 20ms 站到秒边界前。第二段要在边界后 50ms 内开始，自身墙钟不超过 80ms，否则换一次。固定窗口两侧都要满额。滑动日志第二段是 0。滑动计数和令牌桶第二段要小于上限的五分之一。",
	}
	if err := r.flush(); err != nil {
		return done(res, false, err.Error())
	}
	_, warm, err := r.pipelineN(algo, "warm:"+algo, limit, 1000, -1, 0, limit)
	if err != nil {
		return done(res, false, "warm: "+err.Error())
	}
	margin := warm + 20*time.Millisecond
	if margin > 200*time.Millisecond {
		return done(res, false, fmt.Sprintf("warmup %.1fms too slow", ms(warm)))
	}
	var won *clockAttempt
	for i := 0; i < 12; i++ {
		at, err := r.oneClock(algo, limit, margin)
		if err != nil {
			at.Why = err.Error()
		}
		res.ClockAttempts = append(res.ClockAttempts, at)
		if at.OK {
			copyAt := at
			won = &copyAt
			break
		}
	}
	if won == nil {
		why := ""
		if n := len(res.ClockAttempts); n > 0 {
			why = res.ClockAttempts[n-1].Why
		}
		return done(res, false, fmt.Sprintf("warmup %.1fms; no tight attempt: %s", ms(warm), why))
	}
	res.AdmitsBefore = won.AdmitsBefore
	res.AdmitsAfter = won.AdmitsAfter
	res.WallMs = won.WallBeforeMs + won.WallAfterMs
	res.LogicalGapMs = int64(won.GapMs)
	return done(res, true, fmt.Sprintf("warmup %.1fms; 前 %d 后 %d，第一段墙钟 %.2fms，间隔 %.0fms，第二段墙钟 %.2fms，尝试 %d 次",
		ms(warm), won.AdmitsBefore, won.AdmitsAfter, won.WallBeforeMs, won.GapMs, won.WallAfterMs, len(res.ClockAttempts)))
}

func (r *runner) oneClock(algo string, limit int64, margin time.Duration) (clockAttempt, error) {
	var at clockAttempt
	if err := r.flush(); err != nil {
		return at, err
	}
	now, err := r.c.timeMS()
	if err != nil {
		return at, err
	}
	marginMs := margin.Milliseconds()
	if marginMs < 1 {
		marginMs = 1
	}
	next := (now/1000 + 1) * 1000
	if now > next-marginMs {
		next = (now/1000 + 2) * 1000
	}
	target := next - marginMs
	for {
		now, err = r.c.timeMS()
		if err != nil {
			return at, err
		}
		if now >= target {
			break
		}
		if d := target - now; d > 8 {
			time.Sleep(time.Duration(d-3) * time.Millisecond)
		}
	}
	at.RedisMsStart = now
	key := "clock:" + algo
	t0 := time.Now()
	a1, _, err := r.pipelineN(algo, key, limit, 1000, -1, 0, limit)
	if err != nil {
		return at, err
	}
	at.WallBeforeMs = ms(time.Since(t0))
	mid, err := r.c.timeMS()
	if err != nil {
		return at, err
	}
	at.RedisMsMid = mid
	at.AdmitsBefore = a1
	wantSec := next/1000 - 1
	if mid/1000 != wantSec {
		at.Why = fmt.Sprintf("first burst ended in second %d want %d", mid/1000, wantSec)
		return at, nil
	}
	if a1 != limit {
		at.Why = fmt.Sprintf("first admits %d", a1)
		return at, nil
	}
	spinStart := time.Now()
	var boundary int64
	for {
		boundary, err = r.c.timeMS()
		if err != nil {
			return at, err
		}
		if boundary >= next {
			break
		}
		if time.Since(spinStart) > 500*time.Millisecond {
			at.Why = "boundary not reached"
			return at, nil
		}
	}
	at.GapMs = float64(boundary - mid)
	t1 := time.Now()
	a2, _, err := r.pipelineN(algo, key, limit, 1000, -1, 0, limit)
	if err != nil {
		return at, err
	}
	at.WallAfterMs = ms(time.Since(t1))
	end, err := r.c.timeMS()
	if err != nil {
		return at, err
	}
	at.RedisMsEnd = end
	at.AdmitsAfter = a2
	if end/1000 != next/1000 {
		at.Why = fmt.Sprintf("second burst ended in second %d want %d", end/1000, next/1000)
		return at, nil
	}
	if at.GapMs > 50 || at.WallAfterMs > 80 {
		at.Why = fmt.Sprintf("not tight gap %.0f wall2 %.1f", at.GapMs, at.WallAfterMs)
		return at, nil
	}
	if algo == "fixed" || algo == "fixedhot" {
		if a2 != limit {
			at.Why = fmt.Sprintf("second admits %d want %d", a2, limit)
			return at, nil
		}
	} else if algo == "log" {
		if a2 != 0 {
			at.Why = fmt.Sprintf("log second admits %d", a2)
			return at, nil
		}
	} else if a2 >= limit/5 {
		at.Why = fmt.Sprintf("second admits %d not below %d", a2, limit/5)
		return at, nil
	}
	at.OK = true
	return at, nil
}

func (r *runner) benches() {
	admit := []string{"fixed", "fixedhot", "slide", "log", "bucket"}
	for _, algo := range admit {
		for _, w := range []int{16, 32} {
			name := fmt.Sprintf("bench-admit-%s-w%d", algo, w)
			if !r.want(name) {
				continue
			}
			r.save(name, r.bench(benchSpec{
				name: name, algo: algo, workers: w,
				limit: 100_000_000, window: 1000, extra: 0, pin: -1,
				warmup: time.Second, dur: 4 * time.Second,
			}))
		}
	}
	for _, algo := range admit {
		name := fmt.Sprintf("bench-reject-%s-w16", algo)
		if !r.want(name) {
			continue
		}
		// 拒绝路径不刷新 TTL 的三种脚本，默认 2 秒会在测量窗里过期，然后重新放行。
		// 这里把 TTL 拉到 30 秒。bucket 的第 4 个参数是容量，不能拿来当 TTL。
		extra := int64(0)
		if algo != "bucket" && algo != "log" {
			extra = 30_000
		}
		r.save(name, r.bench(benchSpec{
			name: name, algo: algo, workers: 16,
			limit: 100, window: 1000, extra: extra, pin: 50_000, reject: true,
			warmup: time.Second, dur: 4 * time.Second,
		}))
	}
}

func localSpike() result {
	res := result{
		Scenario: "local-spike",
		Note:     "进程内决策，时间戳是传入的，不读时钟。墙钟是把两段额度放出去要多久。不是接口 QPS，也不引用 CPU。",
	}
	pass := true
	for _, n := range []int64{100, 100_000, 1_000_000} {
		f := memFixed{limit: n, window: 1000}
		t0 := time.Now()
		var a1, a2 int64
		for i := int64(0); i < n; i++ {
			if f.allow(999) {
				a1++
			}
		}
		for i := int64(0); i < n; i++ {
			if f.allow(1000) {
				a2++
			}
		}
		wall := time.Since(t0)
		if a1 != n || a2 != n || f.allow(1000) {
			pass = false
		}
		res.Phases = append(res.Phases, phase{
			Name: fmt.Sprintf("fixed-%d", n), Admits: a1 + a2, Attempts: n * 2, WallMs: ms(wall),
		})
	}
	for _, n := range []int64{100_000} {
		lg := memLog{limit: n, window: 1000}
		t0 := time.Now()
		var a1, a2 int64
		for i := int64(0); i < n; i++ {
			if lg.allow(999) {
				a1++
			}
		}
		for i := int64(0); i < n; i++ {
			if lg.allow(1000) {
				a2++
			}
		}
		if a1 != n || a2 != 0 {
			pass = false
		}
		res.Phases = append(res.Phases, phase{Name: "log-100000", Admits: a1 + a2, WallMs: ms(time.Since(t0))})
		bk := memBucket{rate: n, window: 1000, capN: n}
		t0 = time.Now()
		a1, a2 = 0, 0
		for i := int64(0); i < n; i++ {
			if bk.allow(999) {
				a1++
			}
		}
		for i := int64(0); i < n; i++ {
			if bk.allow(1000) {
				a2++
			}
		}
		// 10 万/秒的桶，空掉之后 1ms 补进 100 个令牌，不是再放一整窗。
		if a1 != n || a2 != n/1000 {
			pass = false
		}
		res.Phases = append(res.Phases, phase{
			Name: "bucket-100000", Admits: a1 + a2, Attempts: a2, WallMs: ms(time.Since(t0)),
			Note: "attempts 字段这里是第二段放行",
		})
	}
	return done(res, pass, fmt.Sprintf("fixed-100 wall %.3fms; fixed-1e6 wall %.3fms",
		res.Phases[0].WallMs, res.Phases[2].WallMs))
}

func localMulti() result {
	res := result{
		Scenario: "local-multi", Limit: 100, Workers: 4, WindowMs: 1000,
		Note: "四个独立的固定窗口，各自在 999 和 1000 放行 100。加起来 800。共用一个对象是 200。本地计数器不会替别的进程守额度。",
	}
	var total int64
	for i := 0; i < 4; i++ {
		f := memFixed{limit: 100, window: 1000}
		var n int64
		for k := 0; k < 200; k++ {
			if f.allow(999) {
				n++
			}
		}
		for k := 0; k < 200; k++ {
			if f.allow(1000) {
				n++
			}
		}
		total += n
		res.Rounds = append(res.Rounds, phase{Name: fmt.Sprintf("local-%d", i), Admits: n})
		if n != 200 {
			return done(res, false, fmt.Sprintf("instance %d admits %d", i, n))
		}
	}
	shared := memFixed{limit: 100, window: 1000}
	var one int64
	for k := 0; k < 200; k++ {
		if shared.allow(999) {
			one++
		}
	}
	for k := 0; k < 200; k++ {
		if shared.allow(1000) {
			one++
		}
	}
	res.Admits = total
	res.FinalCount = one
	pass := total == 800 && one == 200
	return done(res, pass, fmt.Sprintf("4 个对象 %d，共用 %d", total, one))
}

func localBench() result {
	res := result{
		Scenario: "local-bench",
		Note:     "单协程空转 1 秒，每次读一次时间再做一次内存判断。这是决策本身的速度，不是 HTTP，也不是 Redis。比较的是同一段循环里几种规则差多少。",
	}
	type job struct {
		name  string
		allow func(int64) bool
		setup func()
	}
	// 窗口用 1 天，1 秒循环不会跨槽。上限要留出 limit*window 的 int64 空间。
	fixed := memFixed{limit: 1 << 62, window: 86_400_000}
	slide := memSlide{limit: 1_000_000_000, window: 86_400_000}
	log := memLog{limit: 1_000_000_000, window: 1000}
	bucket := memBucket{rate: 1_000_000_000, window: 86_400_000, capN: 1_000_000_000}
	reject := memFixed{limit: 1, window: 1 << 62}
	reject.allow(time.Now().UnixMilli())
	jobs := []job{
		{"fixed-admit", fixed.allow, nil},
		{"fixed-reject", reject.allow, nil},
		{"slide-admit", slide.allow, nil},
		{"log-admit", log.allow, nil},
		{"bucket-admit", bucket.allow, nil},
	}
	pass := true
	for _, j := range jobs {
		end := time.Now().Add(time.Second)
		var n, admits int64
		t0 := time.Now()
		c0 := cpuTicks(os.Getpid())
		for time.Now().Before(end) {
			now := time.Now().UnixMilli()
			if j.allow(now) {
				admits++
			}
			n++
		}
		wall := time.Since(t0)
		cpu := cpuTicks(os.Getpid()) - c0
		ph := phase{Name: j.name, Attempts: n, Admits: admits, WallMs: ms(wall)}
		if wall > 0 {
			ph.Note = fmt.Sprintf("qps %.0f clientCpu %.1f%%", float64(n)/wall.Seconds(), cpu/100/wall.Seconds()*100)
		}
		if n < 1000 {
			pass = false
		}
		if j.name == "fixed-reject" && admits != 0 {
			pass = false
		}
		if j.name != "fixed-reject" && admits != n {
			pass = false
		}
		res.Phases = append(res.Phases, ph)
	}
	return done(res, pass, "1s loops finished")
}
