package main

import (
	"fmt"
	"strings"
	"time"
)

func scenarioMemory() (map[string]any, error) {
	return withBox(func(b *box) (map[string]any, error) {
		if err := b.setReplicaCount(1); err != nil {
			return nil, err
		}
		if err := b.flush(); err != nil {
			return nil, err
		}
		if err := waitCaughtUp(b.m, b.r, 10*time.Second); err != nil {
			return nil, err
		}
		m0, err := b.m.info("memory")
		if err != nil {
			return nil, err
		}
		r0, err := b.r.info("memory")
		if err != nil {
			return nil, err
		}
		const n = 30000
		payload := strings.Repeat("x", 200)
		t0 := time.Now()
		if err := writeKeys(b.m, "m", n, payload); err != nil {
			return nil, err
		}
		loadSec := time.Since(t0).Seconds()
		if err := waitCaughtUp(b.m, b.r, 30*time.Second); err != nil {
			return nil, err
		}
		m1, err := b.m.info("memory")
		if err != nil {
			return nil, err
		}
		r1, err := b.r.info("memory")
		if err != nil {
			return nil, err
		}
		usage, err := b.m.cmd("MEMORY", "USAGE", "m000000")
		if err != nil {
			return nil, err
		}
		ms, err := dbsize(b.m)
		if err != nil {
			return nil, err
		}
		rs, err := dbsize(b.r)
		if err != nil {
			return nil, err
		}
		s, ok, err := getStr(b.r, "m029999")
		if err != nil {
			return nil, err
		}
		mpid, _ := pidOf(b.m)
		rpid, _ := pidOf(b.r)
		masterDelta := infoInt(m1, "used_memory") - infoInt(m0, "used_memory")
		replicaDelta := infoInt(r1, "used_memory") - infoInt(r0, "used_memory")
		f := map[string]any{
			"scenario": "memory",
			"note":     "30000 个键，值 200 字节。从库在线，写的过程中增量复制。字节是 used_memory 的差，含这段时间积压的变化。",
			"keys":     n, "valueBytes": 200, "loadSec": loadSec,
			"masterKeys": ms, "replicaKeys": rs,
			"masterUsedBefore": infoInt(m0, "used_memory"), "masterUsedAfter": infoInt(m1, "used_memory"),
			"replicaUsedBefore": infoInt(r0, "used_memory"), "replicaUsedAfter": infoInt(r1, "used_memory"),
			"masterDelta": masterDelta, "replicaDelta": replicaDelta,
			"masterBytesPerKey":   float64(masterDelta) / float64(n),
			"replicaBytesPerKey":  float64(replicaDelta) / float64(n),
			"memoryUsage":         usage.Int,
			"masterBacklogBefore": infoInt(m0, "mem_replication_backlog"),
			"masterBacklogAfter":  infoInt(m1, "mem_replication_backlog"),
			"replicaBacklogAfter": infoInt(r1, "mem_replication_backlog"),
			"masterRssKB":         rssKB(mpid), "replicaRssKB": rssKB(rpid),
			"lateKey": ok && len(s) == 200,
		}
		pass := ms == n && rs == n && ok && len(s) == 200 && usage.Int > 200
		return finish(f, pass, fmt.Sprintf("keys=%d/%d masterDelta=%d replicaDelta=%d usage=%d loadSec=%.3f", ms, rs, masterDelta, replicaDelta, usage.Int, loadSec)), nil
	})
}

func scenarioSync() (map[string]any, error) {
	return withBox(func(b *box) (map[string]any, error) {
		disk := C.from == "disk"
		mode := "yes"
		if disk {
			mode = "no"
		}
		if err := configSet(b.m, "repl-diskless-sync", mode); err != nil {
			return nil, err
		}
		if err := configSet(b.m, "repl-diskless-sync-delay", "0"); err != nil {
			return nil, err
		}
		if err := b.setReplicaCount(1); err != nil {
			return nil, err
		}
		ms, err := dbsize(b.m)
		if err != nil {
			return nil, err
		}
		mem, err := b.m.info("memory")
		if err != nil {
			return nil, err
		}
		samples := make([]map[string]any, 0, 3)
		ok := true
		for i := 0; i < 3; i++ {
			wall, before, after, err := forceFull(b, 30*time.Second)
			if err != nil {
				return nil, err
			}
			fullDelta := after.full - before.full
			if fullDelta < 1 {
				ok = false
			}
			samples = append(samples, map[string]any{
				"wallMs": wall.Milliseconds(), "fullDelta": fullDelta,
				"partialOkDelta": after.partialOK - before.partialOK,
				"forksDelta":     after.forks - before.forks,
				"forkUsec":       after.forkUsec, "cowBytes": after.cow,
			})
		}
		rs, err := dbsize(b.r)
		if err != nil {
			return nil, err
		}
		f := map[string]any{
			"scenario": "sync-" + C.from,
			"note":     "对已经在从库里的数据集连续做 3 次全量同步。REPLICAOF NO ONE 再挂回去。墙钟含每秒一次的 replicationCron，fork 用 latest_fork_usec。磁盘复制是 repl-diskless-sync no。",
			"diskless": mode, "masterKeys": ms, "replicaKeys": rs,
			"masterUsedMemory": infoInt(mem, "used_memory"),
			"samples":          samples,
		}
		pass := ok && ms > 0 && rs == ms
		return finish(f, pass, fmt.Sprintf("mode=%s keys=%d samples=%d", mode, ms, len(samples))), nil
	})
}

func scenarioBuffer() (map[string]any, error) {
	return withBox(func(b *box) (map[string]any, error) {
		if err := b.setReplicaCount(1); err != nil {
			return nil, err
		}
		if err := configSet(b.m, "maxmemory", "8mb"); err != nil {
			return nil, err
		}
		if err := b.flush(); err != nil {
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
			_, err = c.cmd("DEBUG", "SLEEP", "5")
			done <- err
		}()
		time.Sleep(200 * time.Millisecond)
		payload := strings.Repeat("x", 200)
		success := 0
		oom := 0
		var usedAt, backAt int64
		t0 := time.Now()
		sleptThrough := true
		debugDone := false
		var debugErr error
		for i := 0; i < 100000; i++ {
			select {
			case debugErr = <-done:
				debugDone = true
				sleptThrough = false
				if debugErr != nil {
					return nil, fmt.Errorf("debug sleep: %w", debugErr)
				}
			default:
			}
			_, err := b.m.cmd("SET", fmt.Sprintf("z%06d", i), payload)
			if err == nil {
				success++
				continue
			}
			if isCode(err, "OOM") {
				oom++
				mem, ierr := b.m.info("memory")
				if ierr == nil {
					usedAt = infoInt(mem, "used_memory")
					backAt = infoInt(mem, "mem_replication_backlog")
				}
				break
			}
			return nil, err
		}
		firstMs := time.Since(t0).Milliseconds()
		extraOOM, extraOK := 0, 0
		for i := 0; i < 20; i++ {
			_, err := b.m.cmd("SET", fmt.Sprintf("y%06d", i), "v")
			if err == nil {
				extraOK++
				continue
			}
			if isCode(err, "OOM") {
				extraOOM++
				continue
			}
			return nil, err
		}
		if err := configSet(b.m, "maxmemory", "64mb"); err != nil {
			return nil, err
		}
		if !debugDone {
			select {
			case debugErr = <-done:
			case <-time.After(20 * time.Second):
				return nil, fmt.Errorf("debug sleep did not return")
			}
		}
		if err := waitCaughtUp(b.m, b.r, 20*time.Second); err != nil {
			return nil, err
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
			"scenario": "buffer",
			"note":     "这一组把 maxmemory 临时改成 8mb，从库 DEBUG SLEEP。用来看复制缓冲和数据集一起把主库写满。测完改回 64mb。不是 64mb 配置下的容量。",
			"success":  success, "oom": oom, "extraOOM": extraOOM, "extraOK": extraOK,
			"usedMemoryAtOOM": usedAt, "backlogAtOOM": backAt,
			"firstOomMs": firstMs, "sleptThrough": sleptThrough,
			"masterKeys": ms, "replicaKeys": rs, "debugError": errString(debugErr),
		}
		pass := oom >= 1 && extraOOM == 20 && extraOK == 0 && debugErr == nil && ms == int64(success) && rs == ms
		return finish(f, pass, fmt.Sprintf("success=%d oom=%d extraOOM=%d used=%d master=%d replica=%d slept=%v", success, oom, extraOOM, usedAt, ms, rs, sleptThrough)), nil
	})
}

func scenarioLoss() (map[string]any, error) {
	b, err := openBox()
	if err != nil {
		return nil, err
	}
	defer b.close()
	if err := b.setReplicaCount(1); err != nil {
		return nil, err
	}
	if err := b.flush(); err != nil {
		return nil, err
	}
	const base, extra = 100, 200
	if err := writeKeys(b.m, "b", base, "1"); err != nil {
		return nil, err
	}
	w, err := b.waitReplicas(1, 2000)
	if err != nil {
		return nil, err
	}
	if err := replicaof(b.r, "127.0.0.1", deadPort); err != nil {
		return nil, err
	}
	if err := waitSlaves(b.m, 0, 5*time.Second); err != nil {
		return nil, err
	}
	if err := writeKeys(b.m, "e", extra, "1"); err != nil {
		return nil, err
	}
	ms, err := dbsize(b.m)
	if err != nil {
		return nil, err
	}
	pid, err := pidOf(b.m)
	if err != nil {
		return nil, err
	}
	if err := killRedis(pid); err != nil {
		return nil, err
	}
	rs, err := dbsize(b.r)
	if err != nil {
		return nil, err
	}
	_, baseOK, err := getStr(b.r, "b000000")
	if err != nil {
		return nil, err
	}
	_, extraOK, err := getStr(b.r, "e000000")
	if err != nil {
		return nil, err
	}
	f := map[string]any{
		"scenario": "loss",
		"note":     "100 个键已经 WAIT 到从库，然后断开，再写 200 个，kill -9 主库。主库没有 AOF。从库还指着那个没人听的端口。",
		"base":     base, "extra": extra, "waitReplicas": w,
		"masterKeysBeforeKill": ms, "replicaKeys": rs,
		"replicaHasBase": baseOK, "replicaHasExtra": extraOK, "masterPid": pid,
	}
	pass := w == 1 && ms == base+extra && rs == base && baseOK && !extraOK
	return finish(f, pass, fmt.Sprintf("wait=%d master=%d replica=%d extra=%v", w, ms, rs, extraOK)), nil
}

func scenarioLossAfter() (map[string]any, error) {
	b, err := openBox()
	if err != nil {
		return nil, err
	}
	defer b.close()
	ms, err := dbsize(b.m)
	if err != nil {
		return nil, err
	}
	rs, err := dbsize(b.r)
	if err != nil {
		return nil, err
	}
	_, baseR, err := getStr(b.r, "b000000")
	if err != nil {
		return nil, err
	}
	_, extraR, err := getStr(b.r, "e000000")
	if err != nil {
		return nil, err
	}
	_, baseM, err := getStr(b.m, "b000000")
	if err != nil {
		return nil, err
	}
	_, extraM, err := getStr(b.m, "e000000")
	if err != nil {
		return nil, err
	}
	f := map[string]any{
		"scenario":   "loss-after",
		"note":       "主库用空目录重新拉起，没有 AOF。从库进程没重启，仍指着断开的地址，所以不会被空主库盖掉。",
		"masterKeys": ms, "replicaKeys": rs,
		"replicaHasBase": baseR, "replicaHasExtra": extraR,
		"masterHasBase": baseM, "masterHasExtra": extraM,
	}
	pass := ms == 0 && rs == 100 && baseR && !extraR && !baseM && !extraM
	return finish(f, pass, fmt.Sprintf("master=%d replica=%d", ms, rs)), nil
}

func scenarioAOFCut() (map[string]any, error) {
	b, err := openBox()
	if err != nil {
		return nil, err
	}
	defer b.close()
	ps, err := b.m.info("persistence")
	if err != nil {
		return nil, err
	}
	if infoInt(ps, "aof_enabled") != 1 {
		return nil, fmt.Errorf("aof is not enabled")
	}
	if err := configSet(b.m, "appendfsync", C.fsync); err != nil {
		return nil, err
	}
	got, err := configGet(b.m, "appendfsync")
	if err != nil {
		return nil, err
	}
	if err := b.setReplicaCount(1); err != nil {
		return nil, err
	}
	if err := b.flush(); err != nil {
		return nil, err
	}
	emptyBytes, err := dirBytes(C.datadir)
	if err != nil {
		return nil, err
	}
	const base, extra = 100, 200
	t0 := time.Now()
	if err := writeKeys(b.m, "b", base, "1"); err != nil {
		return nil, err
	}
	w, err := b.waitReplicas(1, 5000)
	if err != nil {
		return nil, err
	}
	baseSec := time.Since(t0).Seconds()
	if err := replicaof(b.r, "127.0.0.1", deadPort); err != nil {
		return nil, err
	}
	if err := waitSlaves(b.m, 0, 5*time.Second); err != nil {
		return nil, err
	}
	t1 := time.Now()
	if err := writeKeys(b.m, "e", extra, "1"); err != nil {
		return nil, err
	}
	extraSec := time.Since(t1).Seconds()
	ms, err := dbsize(b.m)
	if err != nil {
		return nil, err
	}
	fullBytes, err := dirBytes(C.datadir)
	if err != nil {
		return nil, err
	}
	ps, err = b.m.info("persistence")
	if err != nil {
		return nil, err
	}
	pid, err := pidOf(b.m)
	if err != nil {
		return nil, err
	}
	if err := killRedis(pid); err != nil {
		return nil, err
	}
	rs, err := dbsize(b.r)
	if err != nil {
		return nil, err
	}
	_, baseOK, err := getStr(b.r, "b000000")
	if err != nil {
		return nil, err
	}
	_, extraOK, err := getStr(b.r, "e000000")
	if err != nil {
		return nil, err
	}
	f := map[string]any{
		"scenario": "aof-cut-" + C.fsync,
		"note":     "AOF 开着。100 个键复制到从库后断开，再写 200 个，立刻 kill -9。这是杀进程，不是断电。目录字节含 Redis 7 的 AOF 清单和可能的 RDB 前导。",
		"fsync":    got, "waitReplicas": w, "base": base, "extra": extra,
		"baseSec": baseSec, "extraSec": extraSec,
		"masterKeysBeforeKill": ms, "replicaKeys": rs,
		"replicaHasBase": baseOK, "replicaHasExtra": extraOK,
		"dirBytesBeforeWrites": emptyBytes, "dirBytesAfterWrites": fullBytes,
		"aofCurrentSize": infoInt(ps, "aof_current_size"),
	}
	pass := got == C.fsync && w == 1 && ms == base+extra && rs == base && baseOK && !extraOK
	return finish(f, pass, fmt.Sprintf("fsync=%s wait=%d master=%d replica=%d dir=%d extraSec=%.3f", got, w, ms, rs, fullBytes, extraSec)), nil
}

func scenarioAOFAfter() (map[string]any, error) {
	b, err := openBox()
	if err != nil {
		return nil, err
	}
	defer b.close()
	if _, err := b.m.cmd("PING"); err != nil {
		return nil, err
	}
	ms, err := dbsize(b.m)
	if err != nil {
		return nil, err
	}
	rs, err := dbsize(b.r)
	if err != nil {
		return nil, err
	}
	_, baseR, err := getStr(b.r, "b000000")
	if err != nil {
		return nil, err
	}
	_, extraR, err := getStr(b.r, "e000000")
	if err != nil {
		return nil, err
	}
	_, baseM, err := getStr(b.m, "b000000")
	if err != nil {
		return nil, err
	}
	_, extraM, err := getStr(b.m, "e000000")
	if err != nil {
		return nil, err
	}
	ps, err := b.m.info("persistence")
	if err != nil {
		return nil, err
	}
	nbytes, err := dirBytes(C.datadir)
	if err != nil {
		return nil, err
	}
	f := map[string]any{
		"scenario":   "aof-after-" + C.fsync,
		"note":       "同一个数据目录把主库拉起来。从库仍指着断开的地址，避免空数据或重放把它盖掉。主库键数是这次 kill -9 之后 AOF 还在不在。fsync 是这一组写入时用的策略，不是重启后的配置。",
		"fsync":      C.fsync,
		"masterKeys": ms, "replicaKeys": rs,
		"masterHasBase": baseM, "masterHasExtra": extraM,
		"replicaHasBase": baseR, "replicaHasExtra": extraR,
		"aofEnabled": infoInt(ps, "aof_enabled"),
		"dirBytes":   nbytes, "aofCurrentSize": infoInt(ps, "aof_current_size"),
	}
	pass := infoInt(ps, "aof_enabled") == 1 && rs == 100 && baseR && !extraR
	return finish(f, pass, fmt.Sprintf("master=%d base=%v extra=%v replica=%d", ms, baseM, extraM, rs)), nil
}
