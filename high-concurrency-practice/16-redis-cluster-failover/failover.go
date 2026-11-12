package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

func debugSleep(addr string, sec int) <-chan error {
	ch := make(chan error, 1)
	go func() {
		c, err := dial(addr, 2*time.Second)
		if err != nil {
			ch <- err
			return
		}
		defer c.Close()
		c.SetDeadline(time.Now().Add(time.Duration(sec+20) * time.Second))
		v, err := c.call("DEBUG", "SLEEP", strconv.Itoa(sec))
		if err != nil {
			ch <- err
			return
		}
		if v.Err != "" {
			ch <- fmt.Errorf("%s", v.Err)
			return
		}
		ch <- nil
	}()
	return ch
}

func incrN(addr, key string, n int) (int64, error) {
	c, err := dial(addr, 2*time.Second)
	if err != nil {
		return 0, err
	}
	defer c.Close()
	var last int64
	for i := 0; i < n; i++ {
		v, err := c.call("INCR", key)
		if err != nil {
			return last, err
		}
		if v.Err != "" {
			return last, fmt.Errorf("%s", v.Err)
		}
		last = v.Int
	}
	return last, nil
}

type raceStat struct {
	OK     int64
	ErrN   int64
	Sample string
	MaxNs  int64
	Slow50 int64
}

func raceCmd(addr, key, cmd string, n int, stop <-chan struct{}, alive time.Duration) raceStat {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var st raceStat
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			c, err := dial(addr, 2*time.Second)
			if err != nil {
				mu.Lock()
				st.ErrN++
				if st.Sample == "" {
					st.Sample = err.Error()
				}
				mu.Unlock()
				return
			}
			defer c.Close()
			c.SetDeadline(time.Now().Add(alive))
			for {
				select {
				case <-stop:
					return
				default:
				}
				t0 := time.Now()
				v, err := c.call(cmd, key)
				dt := time.Since(t0).Nanoseconds()
				mu.Lock()
				if dt > st.MaxNs {
					st.MaxNs = dt
				}
				if dt > 50_000_000 {
					st.Slow50++
				}
				if err != nil || v.Err != "" {
					st.ErrN++
					if st.Sample == "" {
						if err != nil {
							st.Sample = err.Error()
						} else {
							st.Sample = v.Err
						}
					}
					mu.Unlock()
					return
				}
				st.OK++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return st
}

func waitRole(addr, want string, timeout time.Duration) (int64, string) {
	c, err := dial(addr, time.Second)
	if err != nil {
		return -1, err.Error()
	}
	defer c.Close()
	t0 := time.Now()
	last := ""
	for time.Since(t0) < timeout {
		c.SetDeadline(time.Now().Add(400 * time.Millisecond))
		v, err := c.call("ROLE")
		if err != nil {
			last = err.Error()
			c.Close()
			c, err = dial(addr, time.Second)
			if err != nil {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			continue
		}
		last = roleName(v)
		if last == want {
			return time.Since(t0).Milliseconds(), last
		}
		time.Sleep(5 * time.Millisecond)
	}
	return -1, last
}

func probeSets(addr, key, val string, dur time.Duration) map[string]any {
	t0 := time.Now()
	var okN, errN int
	firstOK := int64(-1)
	sample := ""
	c, err := dial(addr, time.Second)
	if err != nil {
		return map[string]any{"ok": 0, "err": 1, "firstOKMs": -1, "sample": err.Error()}
	}
	defer c.Close()
	for time.Since(t0) < dur {
		c.SetDeadline(time.Now().Add(300 * time.Millisecond))
		v, err := c.call("SET", key, val)
		if err != nil {
			errN++
			if sample == "" {
				sample = err.Error()
			}
			c.Close()
			c, err = dial(addr, time.Second)
			if err != nil {
				if sample == "" {
					sample = err.Error()
				}
				time.Sleep(20 * time.Millisecond)
				continue
			}
			continue
		}
		if v.Err != "" || v.Str != "OK" {
			errN++
			if sample == "" {
				sample = replyText(v)
			}
			continue
		}
		okN++
		if firstOK < 0 {
			firstOK = time.Since(t0).Milliseconds()
		}
	}
	return map[string]any{"ok": okN, "err": errN, "firstOKMs": firstOK, "sample": sample, "ms": time.Since(t0).Milliseconds()}
}

func (rn *runner) oneSleep(sec int) (map[string]any, error) {
	w := rn.w
	if err := w.refresh(); err != nil {
		return nil, err
	}
	p, err := w.onePair(fmt.Sprintf("stuck%d", sec))
	if err != nil {
		return nil, err
	}
	other := w.otherMaster(p.master)
	skey, _, err := w.keyFor(fmt.Sprintf("stuckLive%d", sec), other)
	if err != nil {
		return nil, err
	}
	val := fmt.Sprintf("asleep-%d", sec)
	if _, err := p.master.do("SET", p.key, val); err != nil {
		return nil, err
	}
	if _, err := other.do("SET", skey, "up"); err != nil {
		return nil, err
	}
	if err := w.waitCaughtUp(p.master, 3*time.Second); err != nil {
		return nil, err
	}
	logFrom := w.logLen(p.replica.Port)
	t0 := time.Now()
	ch := debugSleep(p.master.Addr, sec)
	promoteMs := int64(-1)
	downMs := int64(-1)
	oldSlaveMs := int64(-1)
	both := false
	sleepMs := int64(-1)
	var sleepErr error
	replicaOK, liveOK, liveDown := 0, 0, 0
	deadline := time.Now().Add(time.Duration(sec+8) * time.Second)
	for time.Now().Before(deadline) {
		el := time.Since(t0).Milliseconds()
		select {
		case err := <-ch:
			if sleepMs < 0 {
				sleepMs = el
				sleepErr = err
			}
		default:
		}
		rv, rerr := dialCall(p.replica.Addr, "ROLE")
		if rerr == nil && roleName(rv) == "master" && promoteMs < 0 {
			promoteMs = el
		}
		if promoteMs < 0 {
			if got := readKey(p.replica.Addr, p.key); got == val {
				replicaOK++
			}
		}
		lv, lerr := dialCall(other.Addr, "GET", skey)
		if lerr == nil && lv.Str == "up" {
			liveOK++
		} else if lerr == nil && strings.HasPrefix(lv.Err, "CLUSTERDOWN") {
			liveDown++
			if downMs < 0 {
				downMs = el
			}
		}
		if sleepMs >= 0 {
			oldRole, _ := dialRole(p.master.Addr)
			if oldRole == "master" && promoteMs >= 0 {
				both = true
			}
			if oldRole == "slave" && oldSlaveMs < 0 {
				oldSlaveMs = el
			}
		}
		if sleepMs >= 0 && promoteMs >= 0 && oldSlaveMs >= 0 {
			break
		}
		if sleepMs >= 0 && promoteMs < 0 && time.Since(t0) > time.Duration(sec)*time.Second+6*time.Second {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if sleepMs < 0 {
		return nil, fmt.Errorf("DEBUG SLEEP %d did not return", sec)
	}
	row := map[string]any{
		"seconds": sec, "master": p.master.Addr, "replica": p.replica.Addr, "other": other.Addr,
		"promoteMs": promoteMs, "downMs": downMs, "oldSlaveMs": oldSlaveMs, "bothMaster": both,
		"replicaGetOK": replicaOK, "liveGetOK": liveOK, "liveDown": liveDown,
		"sleepMs": sleepMs, "sleepErr": errString(sleepErr),
	}
	return rn.finishSleep(p, val, logFrom, row)
}

func (rn *runner) finishSleep(p pair, val string, logFrom int64, row map[string]any) (map[string]any, error) {
	w := rn.w
	promoteMs, _ := row["promoteMs"].(int64)
	oldMs := int64(-1)
	oldRole := ""
	if promoteMs >= 0 {
		oldMs, oldRole = waitRole(p.master.Addr, "slave", 8*time.Second)
		if oldRole == "" {
			oldRole, _ = dialRole(p.master.Addr)
		}
	} else {
		oldRole, _ = dialRole(p.master.Addr)
	}
	row["oldRoleMs"] = oldMs
	row["oldRole"] = oldRole
	row["valueOnReplica"] = readKey(p.replica.Addr, p.key)
	row["valueOnOld"] = readKey(p.master.Addr, p.key)
	row["want"] = val
	row["logs"] = w.logNew(p.replica.Port, logFrom, failNeedles)
	if err := w.waitHealthy(15 * time.Second); err != nil {
		row["healthy"] = err.Error()
	} else {
		row["healthy"] = ""
		row["valueOnReplica"] = readKey(p.replica.Addr, p.key)
		row["valueOnOld"] = readKey(p.master.Addr, p.key)
	}
	return row, nil
}

func (rn *runner) scenarioSleepStuck() (result, error) {
	res := result{Scenario: "sleep-stuck", Note: "主库主线程 DEBUG SLEEP。node-timeout 是 2000。3 秒做三次，8 秒做一次。同机 nanosleep，不是网络分区。旁边有副本。"}
	var shorts []map[string]any
	for i := 0; i < 3; i++ {
		row, err := rn.oneSleep(3)
		if err != nil {
			res.put("shorts", shorts)
			res.Detail = err.Error()
			return res, nil
		}
		shorts = append(shorts, row)
		if row["healthy"] != "" || row["valueOnReplica"] != "asleep-3" {
			res.put("shorts", shorts)
			res.Detail = fmt.Sprintf("sleep3 trial %d value=%v healthy=%v promoteMs=%v", i, row["valueOnReplica"], row["healthy"], row["promoteMs"])
			return res, nil
		}
	}
	long, err := rn.oneSleep(8)
	if err != nil {
		res.put("shorts", shorts)
		res.Detail = err.Error()
		return res, nil
	}
	res.put("shorts", shorts)
	res.put("long", long)
	promoted := 0
	var ms []string
	for _, row := range shorts {
		if row["promoteMs"].(int64) >= 0 {
			promoted++
		}
		ms = append(ms, fmt.Sprint(row["promoteMs"]))
	}
	res.Detail = fmt.Sprintf("sleep3 promoteMs=%s; sleep8 promoteMs=%v downMs=%v oldSlaveMs=%v", strings.Join(ms, ","), long["promoteMs"], long["downMs"], long["oldSlaveMs"])
	res.Pass = long["sleepErr"] == "" && long["healthy"] == "" && long["valueOnReplica"] == "asleep-8" && long["oldRole"] == "slave" && long["promoteMs"].(int64) >= 0
	res.put("shortPromoted", promoted)
	return res, nil
}

func readKey(addr, key string) string {
	role, err := dialRole(addr)
	if err != nil {
		return err.Error()
	}
	if role == "slave" {
		c, err := dial(addr, time.Second)
		if err != nil {
			return err.Error()
		}
		defer c.Close()
		c.SetDeadline(time.Now().Add(2 * time.Second))
		if _, err := c.call("READONLY"); err != nil {
			return err.Error()
		}
		v, err := c.call("GET", key)
		if err != nil {
			return err.Error()
		}
		return replyText(v)
	}
	v, err := dialCall(addr, "GET", key)
	if err != nil {
		return err.Error()
	}
	return replyText(v)
}

func readOwner(addr, key string) (string, string) {
	v, err := dialCall(addr, "GET", key)
	if err != nil {
		return err.Error(), addr
	}
	if kind, _, dest, ok := parseRedirect(v.Err); ok && kind == "MOVED" {
		v2, err2 := dialCall(dest, "GET", key)
		if err2 != nil {
			return err2.Error(), dest
		}
		return replyText(v2), dest
	}
	return replyText(v), addr
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func (rn *runner) coordinated(name, note, mode string, trials int) (result, error) {
	w := rn.w
	res := result{Scenario: name, Note: note}
	var rows []map[string]any
	for i := 0; i < trials; i++ {
		p, err := w.onePair(fmt.Sprintf("%s%d", name, i))
		if err != nil {
			res.Detail = err.Error()
			return res, nil
		}
		if _, err := p.master.do("SET", p.key, "0"); err != nil {
			res.Detail = err.Error()
			return res, nil
		}
		if err := w.waitCaughtUp(p.master, 3*time.Second); err != nil {
			res.Detail = err.Error()
			return res, nil
		}
		logM := w.logLen(p.master.Port)
		logR := w.logLen(p.replica.Port)
		epochM, epochR := p.master.Epoch, p.replica.Epoch
		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		var writes, reads raceStat
		go func() {
			defer wg.Done()
			writes = raceCmd(p.master.Addr, p.key, "INCR", 8, stop, 20*time.Second)
		}()
		go func() {
			defer wg.Done()
			reads = raceCmd(p.master.Addr, p.key, "GET", 4, stop, 20*time.Second)
		}()
		time.Sleep(80 * time.Millisecond)
		args := []string{"CLUSTER", "FAILOVER"}
		if mode != "" {
			args = append(args, mode)
		}
		tCmd := time.Now()
		v, err := dialCall(p.replica.Addr, args...)
		cmdMs := time.Since(tCmd).Milliseconds()
		cmdErr := ""
		if err != nil {
			cmdErr = err.Error()
		} else if v.Err != "" {
			cmdErr = v.Err
		}
		roleWait := 3 * time.Second
		if mode == "FORCE" {
			roleWait = 500 * time.Millisecond
		}
		roleMs, roleGot := waitRole(p.replica.Addr, "master", roleWait)
		oldMs := int64(-1)
		oldRole := ""
		if roleGot == "master" {
			oldMs, oldRole = waitRole(p.master.Addr, "slave", 3*time.Second)
		} else {
			oldRole, _ = dialRole(p.master.Addr)
		}
		close(stop)
		wg.Wait()
		final, owner := readOwner(p.replica.Addr, p.key)
		finalN, finalErr := strconv.ParseInt(final, 10, 64)
		row := map[string]any{
			"trial": i, "master": p.master.Addr, "replica": p.replica.Addr,
			"cmdMs": cmdMs, "cmdReply": replyText(v), "cmdErr": cmdErr,
			"replicaRoleMs": roleMs, "replicaRole": roleGot,
			"oldRoleMs": oldMs, "oldRole": oldRole,
			"incrOK": writes.OK, "incrErr": writes.ErrN, "incrSample": writes.Sample,
			"incrMaxMs": writes.MaxNs / 1e6, "incrSlow50": writes.Slow50,
			"getOK": reads.OK, "getErr": reads.ErrN, "getSample": reads.Sample,
			"getMaxMs": reads.MaxNs / 1e6,
			"final":    final, "finalOwner": owner, "finalMinusIncrOK": finalN - writes.OK,
			"stuck":             roleGot == "master" && oldRole == "slave",
			"epochMasterBefore": epochM, "epochReplicaBefore": epochR,
			"logs": append(w.logNew(p.replica.Port, logR, failNeedles), w.logNew(p.master.Port, logM, failNeedles)...),
		}
		if err := w.refresh(); err == nil {
			row["epochReplicaAfter"] = p.replica.Epoch
			row["epochMasterAfter"] = p.master.Epoch
			row["replicaFlags"] = p.replica.Flags
			row["masterFlags"] = p.master.Flags
		}
		rows = append(rows, row)
		if err := w.waitHealthy(15 * time.Second); err != nil {
			res.put("trials", rows)
			res.Detail = fmt.Sprintf("trial %d healthy: %v", i, err)
			return res, nil
		}
		stuck, _ := row["stuck"].(bool)
		if finalErr != nil || (mode == "" && (!stuck || finalN != writes.OK)) {
			res.put("trials", rows)
			res.Detail = fmt.Sprintf("trial %d roleMs=%d stuck=%v final=%s incr=%d", i, roleMs, stuck, final, writes.OK)
			return res, nil
		}
	}
	res.put("trials", rows)
	res.Pass = len(rows) == trials
	if len(rows) > 0 {
		res.Detail = fmt.Sprintf("trials=%d lastFinal=%v lastIncrOK=%v", len(rows), rows[len(rows)-1]["final"], rows[len(rows)-1]["incrOK"])
	}
	return res, nil
}

var failNeedles = []string{
	"Manual failover", "Forced failover", "Taking over", "Failover election",
	"configEpoch", "Cluster state", "Start of election", "as failing", "Reconfiguring",
}

func (rn *runner) scenarioManual() (result, error) {
	return rn.coordinated("manual", "CLUSTER FAILOVER，主库还活着。命令马上返回，切换在后面发生。写会被 pause，读不会。三次都记下来，不平均。", "", 3)
}

func (rn *runner) scenarioForce() (result, error) {
	return rn.coordinated("force", "CLUSTER FAILOVER FORCE。不等主库暂停客户端，直接选举。主库进程还在。三次都记下来，不平均。", "FORCE", 3)
}

func (rn *runner) scenarioTakeoverAlive() (result, error) {
	w := rn.w
	p, err := w.onePair("take")
	if err != nil {
		return result{}, err
	}
	if _, err := p.master.do("SET", p.key, "0"); err != nil {
		return result{}, err
	}
	if err := w.waitCaughtUp(p.master, 3*time.Second); err != nil {
		return result{}, err
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	var writes raceStat
	go func() {
		defer wg.Done()
		writes = raceCmd(p.master.Addr, p.key, "INCR", 8, stop, 15*time.Second)
	}()
	time.Sleep(50 * time.Millisecond)
	type sample struct {
		ms       int64
		old, neu string
	}
	var mu sync.Mutex
	var samples []sample
	stopPoll := make(chan struct{})
	go func() {
		ca, err1 := dial(p.master.Addr, time.Second)
		cb, err2 := dial(p.replica.Addr, time.Second)
		if err1 != nil || err2 != nil {
			return
		}
		defer ca.Close()
		defer cb.Close()
		t0 := time.Now()
		for {
			select {
			case <-stopPoll:
				return
			default:
			}
			ca.SetDeadline(time.Now().Add(300 * time.Millisecond))
			cb.SetDeadline(time.Now().Add(300 * time.Millisecond))
			ra, _ := ca.call("ROLE")
			rb, _ := cb.call("ROLE")
			mu.Lock()
			if len(samples) < 5000 {
				samples = append(samples, sample{time.Since(t0).Milliseconds(), roleName(ra), roleName(rb)})
			}
			mu.Unlock()
			time.Sleep(2 * time.Millisecond)
		}
	}()
	time.Sleep(30 * time.Millisecond)
	logR := w.logLen(p.replica.Port)
	tCmd := time.Now()
	v, err := dialCall(p.replica.Addr, "CLUSTER", "FAILOVER", "TAKEOVER")
	cmdMs := time.Since(tCmd).Milliseconds()
	oldMs, oldRole := waitRole(p.master.Addr, "slave", 8*time.Second)
	close(stopPoll)
	close(stop)
	wg.Wait()
	fin, _ := dialCall(p.replica.Addr, "GET", p.key)
	final := replyText(fin)
	finalN, finalErr := strconv.ParseInt(final, 10, 64)
	bothFirst, bothLast := int64(-1), int64(-1)
	for _, s := range samples {
		if s.old == "master" && s.neu == "master" {
			if bothFirst < 0 {
				bothFirst = s.ms
			}
			bothLast = s.ms
		}
	}
	res := result{Scenario: "takeover-alive", Note: "主库还活着时 CLUSTER FAILOVER TAKEOVER。不选举。两边可能同时是 master，后写进旧主库的递增会在它降成副本时丢掉。"}
	res.put("master", p.master.Addr)
	res.put("replica", p.replica.Addr)
	res.put("cmdMs", cmdMs)
	res.put("cmdReply", replyText(v))
	res.put("cmdErr", errString(err))
	res.put("oldRoleMs", oldMs)
	res.put("oldRole", oldRole)
	res.put("bothMasterFirstMs", bothFirst)
	res.put("bothMasterLastMs", bothLast)
	res.put("samples", len(samples))
	res.put("incrOK", writes.OK)
	res.put("incrErr", writes.ErrN)
	res.put("incrSample", writes.Sample)
	res.put("incrMaxMs", writes.MaxNs/1e6)
	res.put("final", final)
	res.put("finalMinusIncrOK", finalN-writes.OK)
	res.put("logs", w.logNew(p.replica.Port, logR, failNeedles))
	if err := w.waitHealthy(15 * time.Second); err != nil {
		res.Detail = err.Error()
		return res, nil
	}
	res.Pass = oldRole == "slave" && finalErr == nil && replyText(v) == "OK"
	res.Detail = fmt.Sprintf("both=%d..%d incrOK=%d final=%s oldRoleMs=%d", bothFirst, bothLast, writes.OK, final, oldMs)
	return res, nil
}

func (rn *runner) scenarioKillAuto() (result, error) {
	return rn.killN("kill-auto", "kill -9 主库，cluster-require-full-coverage 仍是 yes。副本接槽之前，活着的槽也会 CLUSTERDOWN。已经复制的值应还在。三次不平均。重启是空数据，不是断电。", true, 3)
}

func (rn *runner) scenarioKillCoverage() (result, error) {
	w := rn.w
	if err := w.configSetLive("cluster-require-full-coverage", "no"); err != nil {
		return result{}, err
	}
	res, err := rn.killN("kill-coverage-no", "同一台机器上把 cluster-require-full-coverage 设成 no 再 kill -9 一个主库。看活着的槽还答不答。测完改回 yes。", false, 1)
	_ = w.configSetLive("cluster-require-full-coverage", "yes")
	if err != nil {
		return res, err
	}
	if herr := w.waitHealthy(20 * time.Second); herr != nil {
		res.Pass = false
		res.Detail = res.Detail + " restore: " + herr.Error()
	}
	return res, nil
}

func (rn *runner) killN(name, note string, wantDown bool, trials int) (result, error) {
	w := rn.w
	res := result{Scenario: name, Note: note}
	var rows []map[string]any
	for i := 0; i < trials; i++ {
		if err := w.refresh(); err != nil {
			res.Detail = err.Error()
			return res, nil
		}
		p, err := w.onePair(fmt.Sprintf("k%d%d", i, time.Now().UnixNano()%1000))
		if err != nil {
			res.Detail = err.Error()
			return res, nil
		}
		other := w.otherMaster(p.master)
		if other == nil {
			res.Detail = "no other master"
			return res, nil
		}
		skey, _, err := w.keyFor(fmt.Sprintf("live%d", i), other)
		if err != nil {
			res.Detail = err.Error()
			return res, nil
		}
		if _, err := p.master.do("SET", p.key, "100"); err != nil {
			res.Detail = err.Error()
			return res, nil
		}
		if _, err := other.do("SET", skey, "up"); err != nil {
			res.Detail = err.Error()
			return res, nil
		}
		if err := w.waitCaughtUp(p.master, 3*time.Second); err != nil {
			res.Detail = err.Error()
			return res, nil
		}
		sc, err := dial(other.Addr, time.Second)
		if err != nil {
			res.Detail = err.Error()
			return res, nil
		}
		rc, err := dial(p.replica.Addr, time.Second)
		if err != nil {
			sc.Close()
			res.Detail = err.Error()
			return res, nil
		}
		logO := w.logLen(other.Port)
		logR := w.logLen(p.replica.Port)
		if err := w.kill(p.master); err != nil {
			sc.Close()
			rc.Close()
			res.Detail = err.Error()
			return res, nil
		}
		t0 := time.Now()
		firstDown, lastDown, recovered := int64(-1), int64(-1), int64(-1)
		roleMs, movedMs, stateFail, stateOK := int64(-1), int64(-1), int64(-1), int64(-1)
		var downN, okN int64
		deadReply := ""
		deadAt := int64(-1)
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			el := time.Since(t0).Milliseconds()
			sc.SetDeadline(time.Now().Add(400 * time.Millisecond))
			gv, gerr := sc.call("GET", skey)
			if gerr != nil {
				sc.Close()
				sc, _ = dial(other.Addr, time.Second)
			} else if strings.HasPrefix(gv.Err, "CLUSTERDOWN") {
				if firstDown < 0 {
					firstDown = el
				}
				lastDown = el
				downN++
			} else if gv.Str == "up" {
				okN++
				if firstDown >= 0 && recovered < 0 {
					recovered = el
				}
			}
			rc.SetDeadline(time.Now().Add(400 * time.Millisecond))
			rv, rerr := rc.call("ROLE")
			if rerr != nil {
				rc.Close()
				rc, _ = dial(p.replica.Addr, time.Second)
			} else if roleName(rv) == "master" && roleMs < 0 {
				roleMs = el
			}
			if sc != nil {
				dv, derr := sc.call("GET", p.key)
				if derr == nil {
					txt := replyText(dv)
					if !strings.HasPrefix(txt, "CLUSTERDOWN") && deadAt < 0 {
						deadAt = el
						deadReply = txt
					}
					if strings.Contains(txt, p.replica.Addr) && movedMs < 0 {
						movedMs = el
					}
				}
			}
			ci, cerr := w.clusterInfo(other)
			if cerr == nil {
				if ci["cluster_state"] == "fail" && stateFail < 0 {
					stateFail = el
				}
				if ci["cluster_state"] == "ok" && stateFail >= 0 && stateOK < 0 {
					stateOK = el
				}
			}
			if roleMs >= 0 && (recovered >= 0 || !wantDown) && time.Since(t0) > 500*time.Millisecond && (deadAt >= 0 || time.Since(t0) > 15*time.Second) {
				if !wantDown || recovered >= 0 {
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
		}
		stock := ""
		for try := 0; try < 100; try++ {
			stockV, serr := dialCall(p.replica.Addr, "GET", p.key)
			if serr == nil {
				stock = replyText(stockV)
			} else {
				stock = serr.Error()
			}
			if stock == "100" {
				break
			}
			time.Sleep(30 * time.Millisecond)
		}
		sc.Close()
		rc.Close()
		_, _ = dialCall(p.replica.Addr, "SET", p.key, "kept")
		restartAt := time.Now()
		rerr := w.restart(p.master)
		pingMs := time.Since(restartAt).Milliseconds()
		probe := probeSets(p.master.Addr, p.key, "ghost", 1500*time.Millisecond)
		slaveMs, slaveRole := waitRole(p.master.Addr, "slave", 15*time.Second)
		herr := w.waitHealthy(20 * time.Second)
		_ = w.refresh()
		owner := ""
		final := ""
		if w.owner[p.slot] != nil {
			owner = w.owner[p.slot].Addr
			fv, _ := dialCall(owner, "GET", p.key)
			final = replyText(fv)
		}
		row := map[string]any{
			"trial": i, "killed": p.master.Addr, "replica": p.replica.Addr, "survivor": other.Addr,
			"firstDownMs": firstDown, "lastDownMs": lastDown, "recoveredMs": recovered,
			"downPolls": downN, "okPolls": okN,
			"roleMs": roleMs, "movedMs": movedMs, "deadReplyMs": deadAt, "deadReply": deadReply,
			"stateFailMs": stateFail, "stateOkMs": stateOK,
			"stock":      stock,
			"restartErr": errString(rerr), "pingMs": pingMs, "probe": probe,
			"oldRoleMs": slaveMs, "oldRole": slaveRole,
			"ownerAfter": owner, "final": final,
			"healthy": errString(herr),
			"logs":    append(w.logNew(other.Port, logO, failNeedles), w.logNew(p.replica.Port, logR, failNeedles)...),
		}
		rows = append(rows, row)
		good := roleMs >= 0 && stock == "100" && herr == nil
		if wantDown {
			good = good && firstDown >= 0 && recovered >= firstDown
		} else {
			good = good && downN == 0 && okN > 0
		}
		if !good {
			res.put("trials", rows)
			res.Detail = fmt.Sprintf("trial %d roleMs=%d stock=%s down=%d recovered=%d okPolls=%d healthy=%v", i, roleMs, stock, firstDown, recovered, okN, herr)
			return res, nil
		}
	}
	res.put("trials", rows)
	res.Pass = len(rows) == trials
	res.Detail = fmt.Sprintf("trials=%d", len(rows))
	return res, nil
}

func killReplicaClients(m *node) (int64, string) {
	v, err := m.call("CLIENT", "KILL", "TYPE", "replica")
	if err != nil {
		return 0, err.Error()
	}
	if v.Err != "" {
		return 0, v.Err
	}
	return v.Int, ""
}

func (rn *runner) behindOnce(tag string, dropLink bool) (map[string]any, error) {
	w := rn.w
	p, err := w.onePair(tag)
	if err != nil {
		return nil, err
	}
	if _, err := p.master.do("SET", p.key, "0"); err != nil {
		return nil, err
	}
	if err := w.waitCaughtUp(p.master, 3*time.Second); err != nil {
		return nil, err
	}
	ch := debugSleep(p.replica.Addr, 6)
	time.Sleep(300 * time.Millisecond)
	killedClients := int64(-1)
	killClientErr := ""
	if dropLink {
		killedClients, killClientErr = killReplicaClients(p.master)
	}
	acked, err := incrN(p.master.Addr, p.key, 2000)
	if err != nil {
		return nil, err
	}
	got, _ := dialCall(p.master.Addr, "GET", p.key)
	if err := w.kill(p.master); err != nil {
		return nil, err
	}
	t0 := time.Now()
	roleMs, roleGot := waitRole(p.replica.Addr, "master", 20*time.Second)
	fin, _ := dialCall(p.replica.Addr, "GET", p.key)
	final := replyText(fin)
	finalN, _ := strconv.ParseInt(final, 10, 64)
	sleepErr := <-ch
	rerr := w.restart(p.master)
	herr := w.waitHealthy(20 * time.Second)
	row := map[string]any{
		"tag": tag, "dropLink": dropLink, "killed": p.master.Addr, "replica": p.replica.Addr,
		"acked": acked, "masterGet": replyText(got),
		"killedClients": killedClients, "killClientErr": killClientErr,
		"roleMs": roleMs, "role": roleGot,
		"final": final, "lost": acked - finalN,
		"sinceKillMs": time.Since(t0).Milliseconds(),
		"sleepErr":    errString(sleepErr),
		"restartErr":  errString(rerr), "healthy": errString(herr),
	}
	if roleMs < 0 || herr != nil {
		return row, fmt.Errorf("%s roleMs=%d final=%s healthy=%v", tag, roleMs, final, herr)
	}
	return row, nil
}

func (rn *runner) scenarioKillBehind() (result, error) {
	res := result{Scenario: "kill-behind", Note: "副本主线程睡 6 秒时，主库 INCR 2000 次再 kill -9。先不拆复制连接，再在睡着之后 CLIENT KILL TYPE replica。同机 nanosleep，不是断网。"}
	socket, err := rn.behindOnce("socket", false)
	res.put("socket", socket)
	if err != nil {
		res.Detail = err.Error()
		return res, nil
	}
	var rows []map[string]any
	for i := 0; i < 3; i++ {
		row, err := rn.behindOnce(fmt.Sprintf("drop%d", i), true)
		rows = append(rows, row)
		res.put("dropped", rows)
		if err != nil {
			res.Detail = err.Error()
			return res, nil
		}
		lost, _ := row["lost"].(int64)
		if lost <= 0 {
			res.Detail = fmt.Sprintf("drop%d lost=%d final=%v", i, lost, row["final"])
			return res, nil
		}
	}
	res.Pass = true
	res.Detail = fmt.Sprintf("socketLost=%v dropLost=%v,%v,%v", socket["lost"], rows[0]["lost"], rows[1]["lost"], rows[2]["lost"])
	return res, nil
}

func (rn *runner) scenarioNoFailover() (result, error) {
	w := rn.w
	p, err := w.onePair("nofo")
	if err != nil {
		return result{}, err
	}
	if err := w.configSet(p.replica, "cluster-replica-no-failover", "yes"); err != nil {
		return result{}, err
	}
	cfg, _ := p.replica.call("CONFIG", "GET", "cluster-replica-no-failover")
	if _, err := p.master.do("SET", p.key, "100"); err != nil {
		return result{}, err
	}
	if err := w.waitCaughtUp(p.master, 3*time.Second); err != nil {
		return result{}, err
	}
	other := w.otherMaster(p.master)
	skey, _, err := w.keyFor("nofoLive", other)
	if err != nil {
		return result{}, err
	}
	if _, err := other.do("SET", skey, "up"); err != nil {
		return result{}, err
	}
	if err := w.kill(p.master); err != nil {
		return result{}, err
	}
	t0 := time.Now()
	promoted := false
	var lastRole, lastState string
	stateFail := int64(-1)
	for time.Since(t0) < 10*time.Second {
		role, _ := dialRole(p.replica.Addr)
		lastRole = role
		if role == "master" {
			promoted = true
			break
		}
		ci, cerr := w.clusterInfo(other)
		if cerr == nil {
			lastState = ci["cluster_state"]
			if lastState == "fail" && stateFail < 0 {
				stateFail = time.Since(t0).Milliseconds()
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	waited := time.Since(t0).Milliseconds()
	own, _ := dialCall(other.Addr, "GET", skey)
	dead, _ := dialCall(other.Addr, "GET", p.key)
	if err := w.configSet(other, "cluster-allow-reads-when-down", "yes"); err != nil {
		return result{}, err
	}
	own2, _ := dialCall(other.Addr, "GET", skey)
	set2, _ := dialCall(other.Addr, "SET", skey, "no")
	dead2, _ := dialCall(other.Addr, "GET", p.key)
	_ = w.configSet(other, "cluster-allow-reads-when-down", "no")
	rerr := w.restart(p.master)
	time.Sleep(500 * time.Millisecond)
	herr := w.waitHealthy(20 * time.Second)
	_ = w.refresh()
	masterNow := ""
	replicaNow := ""
	if p.master != nil {
		mv, _ := dialCall(p.master.Addr, "GET", p.key)
		masterNow = replyText(mv)
	}
	rv, _ := dialCall(p.replica.Addr, "GET", p.key)
	replicaNow = replyText(rv)
	_ = w.configSet(p.replica, "cluster-replica-no-failover", "no")
	res := result{Scenario: "no-failover", Note: "副本上 cluster-replica-no-failover yes。kill -9 主库后等 10 秒。然后在还活着的主库上打开 cluster-allow-reads-when-down。主库没有 AOF，重启是空进程。"}
	res.put("config", replyList(cfg))
	res.put("killed", p.master.Addr)
	res.put("replica", p.replica.Addr)
	res.put("survivor", other.Addr)
	res.put("waitedMs", waited)
	res.put("promoted", promoted)
	res.put("lastRole", lastRole)
	res.put("stateFailMs", stateFail)
	res.put("stateAtEnd", lastState)
	res.put("getOwn", replyText(own))
	res.put("getDead", replyText(dead))
	res.put("getOwnWhenDown", replyText(own2))
	res.put("setOwnWhenDown", replyText(set2))
	res.put("getDeadWhenDown", replyText(dead2))
	res.put("restartErr", errString(rerr))
	res.put("healthy", errString(herr))
	res.put("valueOnRestarted", masterNow)
	res.put("valueOnReplica", replicaNow)
	res.Detail = fmt.Sprintf("promoted=%v stateFail=%d own=%s ownRead=%s set=%s masterVal=%s replicaVal=%s", promoted, stateFail, replyText(own), replyText(own2), replyText(set2), masterNow, replicaNow)
	res.Pass = !promoted && herr == nil && strings.Contains(replyList(cfg), "yes")
	return res, nil
}

func (rn *runner) scenarioRank() (result, error) {
	w := rn.w
	p, err := w.onePair("rank")
	if err != nil {
		return result{}, err
	}
	extra, err := w.addNode(7207)
	if err != nil {
		return result{}, err
	}
	defer func() {
		_, _ = extra.call("SHUTDOWN", "NOSAVE")
		extra.close()
		w.forget(extra.ID)
		w.drop(7207)
	}()
	if _, err := p.master.do("CLUSTER", "MEET", "127.0.0.1", "7207"); err != nil {
		return result{}, err
	}
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if err := w.refresh(); err == nil && extra.ID != "" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if extra.ID == "" {
		return result{Scenario: "rank", Pass: false, Detail: "7207 has no id"}, nil
	}
	if _, err := extra.do("CLUSTER", "REPLICATE", p.master.ID); err != nil {
		return result{}, err
	}
	if err := w.waitCaughtUp(p.master, 8*time.Second); err != nil {
		return result{}, err
	}
	if _, err := p.master.do("SET", p.key, "0"); err != nil {
		return result{}, err
	}
	if err := w.waitCaughtUp(p.master, 5*time.Second); err != nil {
		return result{}, err
	}
	logA := w.logLen(p.replica.Port)
	logB := w.logLen(extra.Port)
	ch := debugSleep(extra.Addr, 12)
	time.Sleep(300 * time.Millisecond)
	acked, err := incrN(p.master.Addr, p.key, 2000)
	if err != nil {
		return result{}, err
	}
	info, _ := p.master.call("INFO", "replication")
	if err := w.kill(p.master); err != nil {
		return result{}, err
	}
	t0 := time.Now()
	var winner string
	var winMs int64 = -1
	deadline = time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		ra, _ := dialRole(p.replica.Addr)
		rb, _ := dialRole(extra.Addr)
		if ra == "master" {
			winner = p.replica.Addr
			winMs = time.Since(t0).Milliseconds()
			break
		}
		if rb == "master" {
			winner = extra.Addr
			winMs = time.Since(t0).Milliseconds()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	fin := ""
	if winner != "" {
		fv, _ := dialCall(winner, "GET", p.key)
		fin = replyText(fv)
	}
	sleepErr := <-ch
	afterRole, _ := dialRole(extra.Addr)
	rerr := w.restart(p.master)
	herr := w.waitHealthy(20 * time.Second)
	res := result{Scenario: "rank", Note: "同一个主库两个副本。7207 的主线程睡 12 秒，期间主库 INCR 2000 次，然后 kill -9。醒着的那个副本应该留下这 2000。"}
	res.put("master", p.master.Addr)
	res.put("awake", p.replica.Addr)
	res.put("asleep", extra.Addr)
	res.put("acked", acked)
	res.put("info", info.Str)
	res.put("winner", winner)
	res.put("winMs", winMs)
	res.put("final", fin)
	res.put("asleepRoleAfterWake", afterRole)
	res.put("sleepErr", errString(sleepErr))
	res.put("restartErr", errString(rerr))
	res.put("healthy", errString(herr))
	res.put("awakeLogs", w.logNew(p.replica.Port, logA, failNeedles))
	res.put("asleepLogs", w.logNew(extra.Port, logB, failNeedles))
	finalN, _ := strconv.ParseInt(fin, 10, 64)
	res.Pass = winner == p.replica.Addr && finalN == acked && afterRole != "master" && herr == nil
	res.Detail = fmt.Sprintf("winner=%s final=%s acked=%d asleepAfter=%s", winner, fin, acked, afterRole)
	return res, nil
}

func (rn *runner) scenarioMajority() (result, error) {
	w := rn.w
	if err := w.refresh(); err != nil {
		return result{}, err
	}
	ms := w.masters()
	if len(ms) != 3 {
		return result{Scenario: "majority", Pass: false, Detail: fmt.Sprintf("masters=%d", len(ms))}, nil
	}
	survivor := ms[0]
	v1, v2 := ms[1], ms[2]
	rs1 := w.replicasOf(v1.ID)
	rs2 := w.replicasOf(v2.ID)
	if len(rs1) == 0 || len(rs2) == 0 {
		return result{Scenario: "majority", Pass: false, Detail: "missing replica"}, nil
	}
	r1, r2 := rs1[0], rs2[0]
	key, slot, err := w.keyFor("maj", v1)
	if err != nil {
		return result{}, err
	}
	key2, _, err := w.keyFor("maj2", v2)
	if err != nil {
		return result{}, err
	}
	skey, _, err := w.keyFor("majs", survivor)
	if err != nil {
		return result{}, err
	}
	if _, err := v1.do("SET", key, "before"); err != nil {
		return result{}, err
	}
	if _, err := v2.do("SET", key2, "slot2"); err != nil {
		return result{}, err
	}
	if _, err := survivor.do("SET", skey, "s"); err != nil {
		return result{}, err
	}
	if err := w.waitCaughtUp(v1, 3*time.Second); err != nil {
		return result{}, err
	}
	if err := w.kill(v1); err != nil {
		return result{}, err
	}
	if err := w.kill(v2); err != nil {
		return result{}, err
	}
	t0 := time.Now()
	auto := false
	var last1, last2, state string
	var slotsFail, slotsPfail string
	for time.Since(t0) < 10*time.Second {
		last1, _ = dialRole(r1.Addr)
		last2, _ = dialRole(r2.Addr)
		if last1 == "master" || last2 == "master" {
			auto = true
			break
		}
		ci, cerr := w.clusterInfo(survivor)
		if cerr == nil {
			state = ci["cluster_state"]
			slotsFail = ci["cluster_slots_fail"]
			slotsPfail = ci["cluster_slots_pfail"]
		}
		time.Sleep(50 * time.Millisecond)
	}
	waited := time.Since(t0).Milliseconds()
	tCmd := time.Now()
	tv, terr := dialCall(r1.Addr, "CLUSTER", "FAILOVER", "TAKEOVER")
	cmdMs := time.Since(tCmd).Milliseconds()
	roleMs, roleGot := waitRole(r1.Addr, "master", 3*time.Second)
	setv, _ := dialCall(r1.Addr, "SET", key, "takeover")
	covErr := w.configSet(r1, "cluster-require-full-coverage", "no")
	set2, _ := dialCall(r1.Addr, "SET", key, "takeover")
	ask, _ := dialCall(survivor.Addr, "GET", key)
	ask2, _ := dialCall(survivor.Addr, "GET", key2)
	ci, _ := w.clusterInfo(survivor)
	rerr1 := w.restart(v1)
	probe := probeSets(v1.Addr, key, "ghost", 1500*time.Millisecond)
	rerr2 := w.restart(v2)
	_ = w.configSetLive("cluster-require-full-coverage", "yes")
	herr := w.waitHealthy(25 * time.Second)
	_ = w.refresh()
	owner := ""
	final := ""
	if slot >= 0 && slot < len(w.owner) && w.owner[slot] != nil {
		owner = w.owner[slot].Addr
		fv, _ := dialCall(owner, "GET", key)
		final = replyText(fv)
	}
	res := result{Scenario: "majority", Note: "杀掉 3 个主库里的 2 个。等 10 秒。然后 CLUSTER FAILOVER TAKEOVER。集群仍是 fail 时，关掉 full coverage 也不接受写入。没有第二台机器，没有网络分区。"}
	res.put("survivor", survivor.Addr)
	res.put("killed", []string{v1.Addr, v2.Addr})
	res.put("replicas", []string{r1.Addr, r2.Addr})
	res.put("waitedMs", waited)
	res.put("autoPromoted", auto)
	res.put("role1", last1)
	res.put("role2", last2)
	res.put("stateDuring", state)
	res.put("slotsFailDuring", slotsFail)
	res.put("slotsPfailDuring", slotsPfail)
	res.put("cmdMs", cmdMs)
	res.put("cmdReply", replyText(tv))
	res.put("cmdErr", errString(terr))
	res.put("roleMs", roleMs)
	res.put("roleAfter", roleGot)
	res.put("setWhileDown", replyText(setv))
	res.put("coverageErr", errString(covErr))
	res.put("setAfterCoverageNo", replyText(set2))
	res.put("askFromSurvivor", replyText(ask))
	res.put("askOtherDeadSlot", replyText(ask2))
	if ci != nil {
		res.put("stateAfterTakeover", ci["cluster_state"])
		res.put("slotsFailAfter", ci["cluster_slots_fail"])
		res.put("knownAfter", ci["cluster_known_nodes"])
	}
	res.put("restart1", errString(rerr1))
	res.put("restart2", errString(rerr2))
	res.put("probe", probe)
	res.put("healthy", errString(herr))
	res.put("ownerAfter", owner)
	res.put("final", final)
	probeOK := -1
	if n, ok := probe["ok"].(int); ok {
		probeOK = n
	}
	res.Detail = fmt.Sprintf("auto=%v role=%s setDown=%s setAfter=%s final=%s owner=%s pfail=%s fail=%s probeOK=%d", auto, roleGot, replyText(setv), replyText(set2), final, owner, slotsPfail, slotsFail, probeOK)
	res.Pass = !auto && roleGot == "master" && strings.HasPrefix(replyText(setv), "CLUSTERDOWN") && strings.HasPrefix(replyText(set2), "CLUSTERDOWN") && herr == nil && owner == r1.Addr && final == "before" && probeOK == 0
	return res, nil
}
