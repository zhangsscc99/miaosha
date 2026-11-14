package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

func withBox(fn func(*box) (map[string]any, error)) (map[string]any, error) {
	b, err := openBox()
	if err != nil {
		return nil, err
	}
	defer b.close()
	res, err := fn(b)
	if rerr := b.restore(); rerr != nil {
		if err != nil {
			return nil, err
		}
		if res == nil {
			return nil, rerr
		}
		res["restoreError"] = rerr.Error()
		res["pass"] = false
		res["detail"] = fmt.Sprint(res["detail"]) + " restore: " + rerr.Error()
	}
	return res, err
}

func writeKeys(c *Conn, prefix string, n int, val string) error {
	for i := 0; i < n; i++ {
		if _, err := c.cmd("SET", fmt.Sprintf("%s%06d", prefix, i), val); err != nil {
			return err
		}
	}
	return nil
}

func getStr(c *Conn, key string) (string, bool, error) {
	rep, err := c.cmd("GET", key)
	if err != nil {
		return "", false, err
	}
	if rep.Nil {
		return "", false, nil
	}
	return rep.Str, true, nil
}

func getInt(c *Conn, key string) (int64, error) {
	s, ok, err := getStr(c, key)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, fmt.Errorf("missing %s", key)
	}
	return strconv.ParseInt(s, 10, 64)
}

func tryIncr(c *Conn, key string, n int) (ok, rejected int64, other error) {
	for i := 0; i < n; i++ {
		_, err := c.cmd("INCR", key)
		if err == nil {
			ok++
			continue
		}
		if isCode(err, "NOREPLICAS") {
			rejected++
			continue
		}
		return ok, rejected, err
	}
	return ok, rejected, nil
}

func scenarioReady() (map[string]any, error) {
	return withBox(func(b *box) (map[string]any, error) {
		want := 1
		if b.r2 != nil {
			want = 2
		}
		if err := waitSlaves(b.m, want, 15*time.Second); err != nil {
			return nil, err
		}
		if err := waitCaughtUp(b.m, b.r, 15*time.Second); err != nil {
			return nil, err
		}
		srv, err := b.m.info("server")
		if err != nil {
			return nil, err
		}
		mem, err := b.m.info("memory")
		if err != nil {
			return nil, err
		}
		rep, err := b.m.info("replication")
		if err != nil {
			return nil, err
		}
		diskless, err := configGet(b.m, "repl-diskless-sync")
		if err != nil {
			return nil, err
		}
		delay, err := configGet(b.m, "repl-diskless-sync-delay")
		if err != nil {
			return nil, err
		}
		backlog, err := configGet(b.m, "repl-backlog-size")
		if err != nil {
			return nil, err
		}
		maxmem, err := configGet(b.m, "maxmemory")
		if err != nil {
			return nil, err
		}
		ro, err := configGet(b.r, "replica-read-only")
		if err != nil {
			return nil, err
		}
		stale, err := configGet(b.r, "replica-serve-stale-data")
		if err != nil {
			return nil, err
		}
		mpid, _ := pidOf(b.m)
		rpid, _ := pidOf(b.r)
		r2pid := 0
		if b.r2 != nil {
			r2pid, _ = pidOf(b.r2)
		}
		f := map[string]any{
			"scenario":             "ready",
			"note":                 "主从进程起来之后的配置。从库 INFO 的 role 仍是 slave。",
			"redisVersion":         infoValue(srv, "redis_version"),
			"redisMode":            infoValue(srv, "redis_mode"),
			"os":                   infoValue(srv, "os"),
			"gcc":                  infoValue(srv, "gcc_version"),
			"allocator":            infoValue(mem, "mem_allocator"),
			"role":                 infoValue(rep, "role"),
			"connectedSlaves":      infoInt(rep, "connected_slaves"),
			"disklessSync":         diskless,
			"disklessSyncDelaySec": delay,
			"backlog":              backlog,
			"maxmemory":            maxmem,
			"replicaReadOnly":      ro,
			"serveStaleData":       stale,
			"masterUsedMemory":     infoInt(mem, "used_memory"),
			"masterRssKB":          rssKB(mpid),
			"replicaRssKB":         rssKB(rpid),
			"replica2RssKB":        rssKB(r2pid),
			"masterPid":            mpid,
			"replicaPid":           rpid,
			"replica2Pid":          r2pid,
		}
		rmem, err := b.r.info("memory")
		if err != nil {
			return nil, err
		}
		f["replicaUsedMemory"] = infoInt(rmem, "used_memory")
		pass := infoValue(srv, "redis_version") == "7.0.15" && infoInt(rep, "connected_slaves") == int64(want) && ro == "yes"
		return finish(f, pass, fmt.Sprintf("slaves=%d diskless=%s delay=%s backlog=%s ro=%s", want, diskless, delay, backlog, ro)), nil
	})
}

func scenarioCopy() (map[string]any, error) {
	return withBox(func(b *box) (map[string]any, error) {
		if err := b.setReplicaCount(1); err != nil {
			return nil, err
		}
		if err := b.flush(); err != nil {
			return nil, err
		}
		const n = 1000
		if err := writeKeys(b.m, "c", n, "v"); err != nil {
			return nil, err
		}
		w, err := b.waitReplicas(1, 2000)
		if err != nil {
			return nil, err
		}
		match, miss, bad := 0, 0, 0
		for i := 0; i < n; i++ {
			s, ok, err := getStr(b.r, fmt.Sprintf("c%06d", i))
			if err != nil {
				return nil, err
			}
			if !ok {
				miss++
			} else if s == "v" {
				match++
			} else {
				bad++
			}
		}
		ms, err := dbsize(b.m)
		if err != nil {
			return nil, err
		}
		rs, err := dbsize(b.r)
		if err != nil {
			return nil, err
		}
		f := map[string]any{
			"scenario": "copy", "note": "主库写 1000 个键，WAIT 1 之后逐个读从库。",
			"keys": n, "waitReplicas": w, "match": match, "miss": miss, "mismatch": bad,
			"masterKeys": ms, "replicaKeys": rs,
		}
		pass := w == 1 && match == n && miss == 0 && bad == 0 && ms == n && rs == n
		return finish(f, pass, fmt.Sprintf("wait=%d match=%d miss=%d bad=%d db=%d/%d", w, match, miss, bad, ms, rs)), nil
	})
}

func scenarioReadonly() (map[string]any, error) {
	return withBox(func(b *box) (map[string]any, error) {
		if err := b.setReplicaCount(1); err != nil {
			return nil, err
		}
		_, rerr := b.r.cmd("SET", "ro", "1")
		if _, err := b.m.cmd("SET", "ro", "from-master"); err != nil {
			return nil, err
		}
		w, err := b.waitReplicas(1, 2000)
		if err != nil {
			return nil, err
		}
		got, ok, err := getStr(b.r, "ro")
		if err != nil {
			return nil, err
		}
		f := map[string]any{
			"scenario":     "readonly",
			"note":         "从库默认 replica-read-only。直接 SET 会被拒绝，复制进来的 SET 可以读到。",
			"replicaError": errString(rerr), "waitReplicas": w, "replicaValue": got, "replicaHit": ok,
		}
		pass := isCode(rerr, "READONLY") && w == 1 && ok && got == "from-master"
		return finish(f, pass, fmt.Sprintf("err=%v wait=%d got=%s", rerr, w, got)), nil
	})
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func scenarioWaitOK() (map[string]any, error) {
	return withBox(func(b *box) (map[string]any, error) {
		if err := b.setReplicaCount(1); err != nil {
			return nil, err
		}
		const n = 20
		got1 := 0
		for i := 0; i < n; i++ {
			val := strconv.Itoa(i)
			if _, err := b.m.cmd("SET", "w", val); err != nil {
				return nil, err
			}
			rep, err := b.m.cmd("WAIT", "1", "1000")
			if err != nil {
				return nil, err
			}
			if rep.Int == 1 {
				got1++
			}
			s, ok, err := getStr(b.r, "w")
			if err != nil {
				return nil, err
			}
			if !ok || s != val {
				return finish(map[string]any{"scenario": "wait-ok", "i": i, "got": s}, false, "replica missed a waited write"), nil
			}
		}
		f := map[string]any{"scenario": "wait-ok", "note": "从库在线时，同一条连接 SET 之后 WAIT 1 1000。", "n": n, "wait1": got1}
		return finish(f, got1 == n, fmt.Sprintf("wait1=%d/%d", got1, n)), nil
	})
}

func scenarioWaitDown() (map[string]any, error) {
	return withBox(func(b *box) (map[string]any, error) {
		if err := b.setReplicaCount(1); err != nil {
			return nil, err
		}
		if _, err := b.m.cmd("SET", "a", "1"); err != nil {
			return nil, err
		}
		w1, err := b.waitReplicas(1, 2000)
		if err != nil {
			return nil, err
		}
		if err := replicaof(b.r, "127.0.0.1", deadPort); err != nil {
			return nil, err
		}
		if err := waitSlaves(b.m, 0, 5*time.Second); err != nil {
			return nil, err
		}
		old, ok, err := getStr(b.r, "a")
		if err != nil {
			return nil, err
		}
		if _, err := b.m.cmd("SET", "b", "1"); err != nil {
			return nil, err
		}
		rep, err := b.m.cmd("WAIT", "1", "300")
		if err != nil {
			return nil, err
		}
		_, bOnReplica, err := getStr(b.r, "b")
		if err != nil {
			return nil, err
		}
		_, bOnMaster, err := getStr(b.m, "b")
		if err != nil {
			return nil, err
		}
		if err := b.attach(b.r); err != nil {
			return nil, err
		}
		if err := waitCaughtUp(b.m, b.r, 10*time.Second); err != nil {
			return nil, err
		}
		after, okAfter, err := getStr(b.r, "b")
		if err != nil {
			return nil, err
		}
		f := map[string]any{
			"scenario":   "wait-down",
			"note":       "从库断开后 WAIT 返回 0，但主库上的 SET 已经生效，不会回滚。从库在默认配置下仍能读到断开前的旧键。",
			"waitOnline": w1, "waitDown": rep.Int,
			"oldKeyOnReplica": ok && old == "1", "newKeyOnReplicaBefore": bOnReplica, "newKeyOnMaster": bOnMaster,
			"newKeyOnReplicaAfter": okAfter && after == "1",
		}
		pass := w1 == 1 && rep.Int == 0 && ok && old == "1" && !bOnReplica && bOnMaster && okAfter && after == "1"
		return finish(f, pass, fmt.Sprintf("wait %d then %d old=%s newBefore=%v after=%s", w1, rep.Int, old, bOnReplica, after)), nil
	})
}

func scenarioStaleData() (map[string]any, error) {
	return withBox(func(b *box) (map[string]any, error) {
		if err := b.setReplicaCount(1); err != nil {
			return nil, err
		}
		if _, err := b.m.cmd("SET", "s", "old"); err != nil {
			return nil, err
		}
		if _, err := b.waitReplicas(1, 2000); err != nil {
			return nil, err
		}
		if err := replicaof(b.r, "127.0.0.1", deadPort); err != nil {
			return nil, err
		}
		if err := waitSlaves(b.m, 0, 5*time.Second); err != nil {
			return nil, err
		}
		s, ok, err := getStr(b.r, "s")
		if err != nil {
			return nil, err
		}
		served := ok && s == "old" && err == nil
		if err := configSet(b.r, "replica-serve-stale-data", "no"); err != nil {
			return nil, err
		}
		_, gerr := b.r.cmd("GET", "s")
		if err := configSet(b.r, "replica-serve-stale-data", "yes"); err != nil {
			return nil, err
		}
		s2, ok2, err := getStr(b.r, "s")
		if err != nil {
			return nil, err
		}
		f := map[string]any{
			"scenario":           "stale-data",
			"note":               "链路断开时，默认仍返回旧值。replica-serve-stale-data no 之后 GET 被拒绝。这不是 NO ONE，从库角色还在。",
			"servedWhileDefault": served, "refusedError": errString(gerr), "servedAfterYes": ok2 && s2 == "old",
		}
		pass := served && isCode(gerr, "MASTERDOWN") && ok2 && s2 == "old"
		return finish(f, pass, fmt.Sprintf("default=%v refuse=%v after=%s", served, gerr, s2)), nil
	})
}

func scenarioStale() (map[string]any, error) {
	return withBox(func(b *box) (map[string]any, error) {
		if err := b.flush(); err != nil {
			return nil, err
		}
		if err := b.setReplicaCount(1); err != nil {
			return nil, err
		}
		n := C.keys
		if n < 1 {
			n = 1000
		}
		var match, miss, bad, errors int64
		err := withDelay(b, C.delay, func() error {
			for i := 0; i < n; i++ {
				key := fmt.Sprintf("s%06d", i)
				if _, err := b.m.cmd("SET", key, "v"); err != nil {
					return err
				}
				s, ok, err := getStr(b.r, key)
				if err != nil {
					errors++
					continue
				}
				if !ok {
					miss++
				} else if s == "v" {
					match++
				} else {
					bad++
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		f := map[string]any{
			"scenario": "stale",
			"note":     "主库 SET 返回之后立刻读从库。delay 大于 0 时，复制流量经过本机代理，每方向 sleep 一次，不是第二台机器。",
			"keys":     n, "match": match, "miss": miss, "mismatch": bad, "errors": errors,
			"delayMs": C.delay.Milliseconds(),
		}
		pass := errors == 0 && bad == 0 && match+miss == int64(n)
		return finish(f, pass, fmt.Sprintf("delayMs=%d match=%d miss=%d bad=%d err=%d", C.delay.Milliseconds(), match, miss, bad, errors)), nil
	})
}

func withDelay(b *box, d time.Duration, fn func() error) error {
	if d <= 0 {
		return fn()
	}
	p, err := startDelayProxy("127.0.0.1:16379", C.master, d)
	if err != nil {
		return err
	}
	defer p.Close()
	if err := b.setReplicaCount(1); err != nil {
		return err
	}
	if err := replicaof(b.r, "127.0.0.1", "16379"); err != nil {
		return err
	}
	if err := waitCaughtUp(b.m, b.r, 15*time.Second); err != nil {
		return err
	}
	return fn()
}

func scenarioMin(need int) (map[string]any, error) {
	return withBox(func(b *box) (map[string]any, error) {
		if err := b.setReplicaCount(need); err != nil {
			return nil, err
		}
		if err := configSet(b.m, "min-replicas-max-lag", "10"); err != nil {
			return nil, err
		}
		if err := configSet(b.m, "min-replicas-to-write", "1"); err != nil {
			return nil, err
		}
		if _, err := b.m.cmd("SET", "min", "0"); err != nil {
			return nil, err
		}
		okA, rejA, err := tryIncr(b.m, "min", 20)
		if err != nil {
			return nil, err
		}
		if err := replicaof(b.r, "127.0.0.1", deadPort); err != nil {
			return nil, err
		}
		left := need - 1
		if err := waitSlaves(b.m, left, 5*time.Second); err != nil {
			return nil, err
		}
		okB, rejB, err := tryIncr(b.m, "min", 20)
		if err != nil {
			return nil, err
		}
		var okC, rejC int64
		if need == 2 {
			if err := replicaof(b.r2, "127.0.0.1", deadPort); err != nil {
				return nil, err
			}
			if err := waitSlaves(b.m, 0, 5*time.Second); err != nil {
				return nil, err
			}
			okC, rejC, err = tryIncr(b.m, "min", 20)
			if err != nil {
				return nil, err
			}
		}
		val, err := getInt(b.m, "min")
		if err != nil {
			return nil, err
		}
		f := map[string]any{
			"scenario":  fmt.Sprintf("min-%d", need),
			"note":      "min-replicas-to-write 1 且 max-lag 10。断开的是复制连接，不是 kill。写被拒绝时键不会增加。max-lag 设成 0 会把这项检查关掉，这里没有设 0。",
			"okWhileUp": okA, "rejectedWhileUp": rejA,
			"okAfterFirstDrop": okB, "rejectedAfterFirstDrop": rejB,
			"okAfterSecondDrop": okC, "rejectedAfterSecondDrop": rejC,
			"finalValue": val,
		}
		var pass bool
		if need == 1 {
			pass = okA == 20 && rejA == 0 && okB == 0 && rejB == 20 && val == 20
		} else {
			pass = okA == 20 && okB == 20 && rejB == 0 && okC == 0 && rejC == 20 && val == 40
		}
		return finish(f, pass, fmt.Sprintf("A %d/%d B %d/%d C %d/%d val=%d", okA, rejA, okB, rejB, okC, rejC, val)), nil
	})
}

func scenarioLag() (map[string]any, error) {
	return withBox(func(b *box) (map[string]any, error) {
		if err := b.setReplicaCount(1); err != nil {
			return nil, err
		}
		if err := configSet(b.m, "min-replicas-max-lag", "1"); err != nil {
			return nil, err
		}
		if err := configSet(b.m, "min-replicas-to-write", "1"); err != nil {
			return nil, err
		}
		if _, err := b.m.cmd("SET", "lag", "0"); err != nil {
			return nil, err
		}
		done := make(chan error, 1)
		go func() {
			c, err := dial(C.replica)
			if err != nil {
				done <- err
				return
			}
			defer c.Close()
			_, err = c.cmd("DEBUG", "SLEEP", "6")
			done <- err
		}()
		time.Sleep(200 * time.Millisecond)
		select {
		case err := <-done:
			if err != nil {
				return nil, fmt.Errorf("debug sleep: %w", err)
			}
		default:
		}
		t0 := time.Now()
		var applied int64
		var elapsedMs int64
		var lagAt, goodAt int64
		saw := false
		for time.Since(t0) < 7*time.Second {
			select {
			case err := <-done:
				if err != nil {
					return nil, fmt.Errorf("debug sleep: %w", err)
				}
			default:
			}
			_, err := b.m.cmd("INCR", "lag")
			if err == nil {
				applied++
				continue
			}
			if isCode(err, "NOREPLICAS") {
				saw = true
				elapsedMs = time.Since(t0).Milliseconds()
				info, ierr := b.m.info("replication")
				if ierr == nil {
					lagAt, _ = slaveLag(info)
					goodAt = infoInt(info, "min_slaves_good_slaves")
				}
				break
			}
			return nil, err
		}
		before, err := getInt(b.m, "lag")
		if err != nil {
			return nil, err
		}
		_, secondErr := b.m.cmd("INCR", "lag")
		after, err := getInt(b.m, "lag")
		if err != nil {
			return nil, err
		}
		secondRejected := isCode(secondErr, "NOREPLICAS") && after == before
		var debugErr error
		select {
		case debugErr = <-done:
		case <-time.After(20 * time.Second):
			return nil, fmt.Errorf("debug sleep did not return")
		}
		recovered := false
		var recoverMs int64
		t1 := time.Now()
		for time.Since(t1) < 5*time.Second {
			_, err := b.m.cmd("INCR", "lag")
			if err == nil {
				recovered = true
				recoverMs = time.Since(t1).Milliseconds()
				break
			}
			if !isCode(err, "NOREPLICAS") {
				return nil, err
			}
			time.Sleep(50 * time.Millisecond)
		}
		f := map[string]any{
			"scenario":            "lag",
			"note":                "从库 DEBUG SLEEP 6 秒，主线程不回 ack。min-replicas-max-lag 是 1，good 计数由每秒一次的 replicationCron 更新。拒绝发生之前已经成功的 INCR 留在主库。",
			"appliedBeforeReject": applied, "elapsedToRejectMs": elapsedMs,
			"lagAtReject": lagAt, "goodSlavesAtReject": goodAt,
			"secondRejected": secondRejected, "valueUnchanged": after == before,
			"valueAtReject": before, "recovered": recovered, "recoverMs": recoverMs,
			"debugError": errString(debugErr),
		}
		pass := saw && secondRejected && recovered && debugErr == nil && elapsedMs >= 1000
		return finish(f, pass, fmt.Sprintf("applied=%d rejectMs=%d lag=%d good=%d recoverMs=%d debug=%v", applied, elapsedMs, lagAt, goodAt, recoverMs, debugErr)), nil
	})
}

func scenarioPartial() (map[string]any, error) {
	return withBox(func(b *box) (map[string]any, error) {
		if err := b.setReplicaCount(1); err != nil {
			return nil, err
		}
		if err := configSet(b.m, "repl-backlog-size", "1mb"); err != nil {
			return nil, err
		}
		if err := b.flush(); err != nil {
			return nil, err
		}
		if err := writeKeys(b.m, "p", 30, "v"); err != nil {
			return nil, err
		}
		if _, err := b.waitReplicas(1, 2000); err != nil {
			return nil, err
		}
		before, err := readSync(b.m)
		if err != nil {
			return nil, err
		}
		if err := replicaof(b.r, "127.0.0.1", deadPort); err != nil {
			return nil, err
		}
		if err := waitSlaves(b.m, 0, 5*time.Second); err != nil {
			return nil, err
		}
		if err := writeKeys(b.m, "q", 15, "v"); err != nil {
			return nil, err
		}
		if err := b.attach(b.r); err != nil {
			return nil, err
		}
		if err := waitCaughtUp(b.m, b.r, 10*time.Second); err != nil {
			return nil, err
		}
		after, err := readSync(b.m)
		if err != nil {
			return nil, err
		}
		s, ok, err := getStr(b.r, "q000014")
		if err != nil {
			return nil, err
		}
		rs, err := dbsize(b.r)
		if err != nil {
			return nil, err
		}
		fullDelta := after.full - before.full
		okDelta := after.partialOK - before.partialOK
		errDelta := after.partialErr - before.partialErr
		f := map[string]any{
			"scenario":  "partial",
			"note":      "从库改指一个没人听的端口，缓存的复制 ID 还在。主库只多写 15 个小键，积压是 1MB。再指回主库。",
			"fullDelta": fullDelta, "partialOkDelta": okDelta, "partialErrDelta": errDelta,
			"replicaKeys": rs, "lateKey": ok && s == "v",
		}
		pass := okDelta >= 1 && fullDelta == 0 && ok && s == "v" && rs == 45
		return finish(f, pass, fmt.Sprintf("fullDelta=%d partialOk=%d partialErr=%d keys=%d", fullDelta, okDelta, errDelta, rs)), nil
	})
}

func scenarioFull() (map[string]any, error) {
	return withBox(func(b *box) (map[string]any, error) {
		if err := configSet(b.m, "repl-backlog-size", "16384"); err != nil {
			return nil, err
		}
		got, err := configGet(b.m, "repl-backlog-size")
		if err != nil {
			return nil, err
		}
		if err := b.setReplicaCount(1); err != nil {
			return nil, err
		}
		if err := b.flush(); err != nil {
			return nil, err
		}
		if err := writeKeys(b.m, "f", 10, "v"); err != nil {
			return nil, err
		}
		if _, err := b.waitReplicas(1, 2000); err != nil {
			return nil, err
		}
		before, err := readSync(b.m)
		if err != nil {
			return nil, err
		}
		if err := replicaof(b.r, "127.0.0.1", deadPort); err != nil {
			return nil, err
		}
		if err := waitSlaves(b.m, 0, 5*time.Second); err != nil {
			return nil, err
		}
		payload := strings.Repeat("x", 200)
		const extra = 800
		if err := writeKeys(b.m, "g", extra, payload); err != nil {
			return nil, err
		}
		info, err := b.m.info("replication")
		if err != nil {
			return nil, err
		}
		hist := infoInt(info, "repl_backlog_histlen")
		if err := b.attach(b.r); err != nil {
			return nil, err
		}
		t0 := time.Now()
		if err := waitCaughtUp(b.m, b.r, 15*time.Second); err != nil {
			return nil, err
		}
		wallMs := time.Since(t0).Milliseconds()
		time.Sleep(50 * time.Millisecond)
		after, err := readSync(b.m)
		if err != nil {
			return nil, err
		}
		s, ok, err := getStr(b.r, "g000799")
		if err != nil {
			return nil, err
		}
		rs, err := dbsize(b.r)
		if err != nil {
			return nil, err
		}
		fullDelta := after.full - before.full
		okDelta := after.partialOK - before.partialOK
		f := map[string]any{
			"scenario":      "full",
			"note":          "积压收成 16384 字节后断开，再写 800 个 200 字节的值。部分同步放不下这段，重连走全量。墙钟含 replicationCron，不拿它当传输速度。",
			"backlogConfig": got, "backlogHistlen": hist,
			"fullDelta": fullDelta, "partialOkDelta": okDelta,
			"forkUsec": after.forkUsec, "forksDelta": after.forks - before.forks, "cowBytes": after.cow,
			"wallMs": wallMs, "replicaKeys": rs, "lateKey": ok && len(s) == 200,
		}
		pass := fullDelta >= 1 && okDelta == 0 && ok && len(s) == 200 && rs == 10+extra
		return finish(f, pass, fmt.Sprintf("backlog=%s hist=%d full=%d partial=%d fork=%d wallMs=%d keys=%d", got, hist, fullDelta, okDelta, after.forkUsec, wallMs, rs)), nil
	})
}

func scenarioPromote() (map[string]any, error) {
	return withBox(func(b *box) (map[string]any, error) {
		if err := b.setReplicaCount(1); err != nil {
			return nil, err
		}
		if err := b.flush(); err != nil {
			return nil, err
		}
		if _, err := b.m.cmd("SET", "k", "from-master"); err != nil {
			return nil, err
		}
		if _, err := b.waitReplicas(1, 2000); err != nil {
			return nil, err
		}
		before, err := readSync(b.m)
		if err != nil {
			return nil, err
		}
		if err := promote(b.r); err != nil {
			return nil, err
		}
		roleR, err := roleOf(b.r)
		if err != nil {
			return nil, err
		}
		if _, err := b.m.cmd("SET", "k", "on-master"); err != nil {
			return nil, err
		}
		if _, err := b.r.cmd("SET", "k", "on-replica"); err != nil {
			return nil, err
		}
		mv, _, err := getStr(b.m, "k")
		if err != nil {
			return nil, err
		}
		rv, _, err := getStr(b.r, "k")
		if err != nil {
			return nil, err
		}
		if err := b.attach(b.r); err != nil {
			return nil, err
		}
		if err := waitCaughtUp(b.m, b.r, 15*time.Second); err != nil {
			return nil, err
		}
		after, err := readSync(b.m)
		if err != nil {
			return nil, err
		}
		rv2, _, err := getStr(b.r, "k")
		if err != nil {
			return nil, err
		}
		role2, err := roleOf(b.r)
		if err != nil {
			return nil, err
		}
		f := map[string]any{
			"scenario":         "promote",
			"note":             "REPLICAOF NO ONE 之后两边都能写同一个键。再挂回原来的主库时，从库自己收下的那次写入不会合并。没有 Sentinel，没有隔离写入。",
			"roleAfterPromote": roleR, "masterValue": mv, "replicaValue": rv,
			"roleAfterRejoin": role2, "replicaValueAfterRejoin": rv2,
			"fullDelta": after.full - before.full, "partialOkDelta": after.partialOK - before.partialOK,
		}
		pass := roleR == "master" && mv == "on-master" && rv == "on-replica" && rv2 == "on-master" && role2 == "slave"
		return finish(f, pass, fmt.Sprintf("split %s/%s rejoin %s fullDelta=%d", mv, rv, rv2, after.full-before.full)), nil
	})
}

func scenarioGap() (map[string]any, error) {
	return withBox(func(b *box) (map[string]any, error) {
		if err := b.flush(); err != nil {
			return nil, err
		}
		if err := b.setReplicaCount(1); err != nil {
			return nil, err
		}
		if _, err := b.m.cmd("SET", "c", "0"); err != nil {
			return nil, err
		}
		if _, err := b.waitReplicas(1, 2000); err != nil {
			return nil, err
		}
		var maxGap, minGap int64
		var sum int64
		var samples, positive, negative, readErr int64
		var gaps []int64
		var writes int64
		err := withDelay(b, C.delay, func() error {
			wc, err := dial(C.master)
			if err != nil {
				return err
			}
			defer wc.Close()
			mc, err := dial(C.master)
			if err != nil {
				return err
			}
			defer mc.Close()
			rc, err := dial(C.replica)
			if err != nil {
				return err
			}
			defer rc.Close()
			stop := time.Now().Add(time.Second)
			done := make(chan struct{})
			go func() {
				defer close(done)
				for time.Now().Before(stop) {
					if _, err := wc.cmd("INCR", "c"); err != nil {
						return
					}
					writes++
				}
			}()
			for time.Now().Before(stop) {
				time.Sleep(5 * time.Millisecond)
				mv, err1 := getInt(mc, "c")
				rv, err2 := getInt(rc, "c")
				if err1 != nil || err2 != nil {
					readErr++
					continue
				}
				gap := mv - rv
				gaps = append(gaps, gap)
				sum += gap
				samples++
				if samples == 1 || gap > maxGap {
					maxGap = gap
				}
				if samples == 1 || gap < minGap {
					minGap = gap
				}
				if gap > 0 {
					positive++
				} else if gap < 0 {
					negative++
				}
			}
			<-done
			return nil
		})
		if err != nil {
			return nil, err
		}
		sort.Slice(gaps, func(i, j int) bool { return gaps[i] < gaps[j] })
		p50 := int64(0)
		if len(gaps) > 0 {
			p50 = gaps[len(gaps)/2]
		}
		mean := 0.0
		if samples > 0 {
			mean = float64(sum) / float64(samples)
		}
		f := map[string]any{
			"scenario": "gap",
			"note":     "一个连接 INCR 一秒。每 5 毫秒先读主库再读从库。差值含这两次读取本身的间隔。delay 是代理 sleep，不是第二台机器。",
			"delayMs":  C.delay.Milliseconds(), "writes": writes, "samples": samples,
			"maxGap": maxGap, "minGap": minGap, "p50Gap": p50, "meanGap": mean,
			"positive": positive, "negative": negative, "readErrors": readErr,
		}
		pass := samples > 100 && readErr == 0
		return finish(f, pass, fmt.Sprintf("delayMs=%d writes=%d samples=%d max=%d p50=%d mean=%.1f neg=%d", C.delay.Milliseconds(), writes, samples, maxGap, p50, mean, negative)), nil
	})
}

func scenarioDelay5() (map[string]any, error) {
	return withBox(func(b *box) (map[string]any, error) {
		if err := b.setReplicaCount(1); err != nil {
			return nil, err
		}
		if err := b.flush(); err != nil {
			return nil, err
		}
		if err := writeKeys(b.m, "d", 10, "v"); err != nil {
			return nil, err
		}
		if _, err := b.waitReplicas(1, 2000); err != nil {
			return nil, err
		}
		if err := configSet(b.m, "repl-diskless-sync", "yes"); err != nil {
			return nil, err
		}
		if err := configSet(b.m, "repl-diskless-sync-delay", "5"); err != nil {
			return nil, err
		}
		wall, before, after, err := forceFull(b, 20*time.Second)
		if err != nil {
			return nil, err
		}
		rs, err := dbsize(b.r)
		if err != nil {
			return nil, err
		}
		f := map[string]any{
			"scenario": "delay5",
			"note":     "磁盘复制的等待只在 repl-diskless-sync 打开时生效。默认是 5 秒，这次配置文件写成 0，这一组临时改成 5。墙钟含 replicationCron。数据只有 10 个键。",
			"wallMs":   wall.Milliseconds(), "fullDelta": after.full - before.full,
			"partialOkDelta": after.partialOK - before.partialOK, "replicaKeys": rs,
		}
		pass := after.full-before.full >= 1 && rs == 10
		return finish(f, pass, fmt.Sprintf("wallMs=%d fullDelta=%d keys=%d", wall.Milliseconds(), after.full-before.full, rs)), nil
	})
}

func forceFull(b *box, timeout time.Duration) (time.Duration, syncCnt, syncCnt, error) {
	before, err := readSync(b.m)
	if err != nil {
		return 0, syncCnt{}, syncCnt{}, err
	}
	t0 := time.Now()
	if err := promote(b.r); err != nil {
		return 0, syncCnt{}, syncCnt{}, err
	}
	if err := b.attach(b.r); err != nil {
		return 0, syncCnt{}, syncCnt{}, err
	}
	if err := waitCaughtUp(b.m, b.r, timeout); err != nil {
		return 0, syncCnt{}, syncCnt{}, err
	}
	wall := time.Since(t0)
	time.Sleep(50 * time.Millisecond)
	after, err := readSync(b.m)
	return wall, before, after, err
}
