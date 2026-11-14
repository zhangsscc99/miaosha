package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func logf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
}

func writeResult(name string, res map[string]any) error {
	if _, ok := res["pass"]; !ok {
		return fmt.Errorf("%s missing pass", name)
	}
	if err := os.MkdirAll("results", 0o755); err != nil {
		return err
	}
	res["scenario"] = name
	b, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	path := filepath.Join("results", name+".json")
	if err := os.WriteFile(path, b, 0o644); err != nil {
		return err
	}
	logf("wrote %s pass=%v", path, res["pass"])
	return nil
}

func finish(name string, res map[string]any) error {
	if err := writeResult(name, res); err != nil {
		return err
	}
	pass, _ := res["pass"].(bool)
	if !pass {
		return fmt.Errorf("%s pass is false\n%s", name, tailLogs())
	}
	return nil
}

type ev struct {
	ch      string
	payload string
	at      time.Time
}

type sub struct {
	c      *Conn
	events chan ev
}

func subscribe(port int, channels ...string) (*sub, error) {
	c, err := dial(addr(port), time.Second)
	if err != nil {
		return nil, err
	}
	parts := make([]string, 0, len(channels)+1)
	parts = append(parts, "SUBSCRIBE")
	parts = append(parts, channels...)
	if err := c.write(parts...); err != nil {
		c.Close()
		return nil, err
	}
	s := &sub{c: c, events: make(chan ev, 128)}
	go func() {
		defer close(s.events)
		for {
			v, err := readVal(c.r)
			if err != nil {
				return
			}
			if v.Kind != '*' || len(v.Elems) < 3 || v.Elems[0].Str != "message" {
				continue
			}
			select {
			case s.events <- ev{ch: v.Elems[1].Str, payload: v.Elems[2].Str, at: time.Now()}:
			default:
			}
		}
	}()
	return s, nil
}

func (s *sub) Close() {
	if s != nil && s.c != nil {
		s.c.Close()
		s.c = nil
	}
}

var failoverChannels = []string{
	"+sdown", "+odown", "+try-failover", "+elected-leader", "+selected-slave",
	"+switch-master", "+failover-end", "+promoted-slave",
	"-failover-abort-no-good-slave", "-failover-abort-not-elected",
	"+failover-end-for-timeout",
}

type watchOpt struct {
	sentinels []int
	replicas  []int
	oldPort   int
	origin    time.Time
	timeout   time.Duration
	marks     []time.Duration
	sub       *sub
	onPromote func()
}

func stickyMasterPort(s *sticky) (int, error) {
	v, err := s.cmd("SENTINEL", "get-master-addr-by-name", "miaosha")
	if err != nil {
		return 0, err
	}
	if v.Kind != '*' || len(v.Elems) != 2 {
		return 0, fmt.Errorf("bad addr")
	}
	return strconv.Atoi(v.Elems[1].Str)
}

func stickyRole(s *sticky) (string, int, string, error) {
	v, err := s.cmd("ROLE")
	if err != nil {
		return "", 0, "", err
	}
	if v.Kind != '*' || len(v.Elems) < 1 {
		return "", 0, "", fmt.Errorf("bad role")
	}
	role := v.Elems[0].Str
	var mp int
	var state string
	if role == "slave" && len(v.Elems) >= 4 {
		mp = valInt(v.Elems[2])
		state = v.Elems[3].Str
	}
	return role, mp, state, nil
}

func awaitPromotion(opt watchOpt) map[string]any {
	sent := map[int]*sticky{}
	for _, p := range opt.sentinels {
		sent[p] = &sticky{addr: addr(p)}
	}
	rep := map[int]*sticky{}
	for _, p := range opt.replicas {
		rep[p] = &sticky{addr: addr(p)}
	}
	defer func() {
		for _, s := range sent {
			s.Close()
		}
		for _, s := range rep {
			s.Close()
		}
	}()

	addrAt := map[string]int64{}
	var firstAddrUs int64
	var firstRoleUs int64
	var pingUs int64
	newPort := 0
	var marks []map[string]any
	markI := 0
	deadline := opt.origin.Add(opt.timeout)
	for time.Now().Before(deadline) {
		elapsed := time.Since(opt.origin)
		if markI < len(opt.marks) && elapsed >= opt.marks[markI] {
			snap := map[string]any{"us": elapsed.Microseconds(), "addrs": map[string]int{}}
			addrs := snap["addrs"].(map[string]int)
			for _, p := range opt.sentinels {
				if mp, err := stickyMasterPort(sent[p]); err == nil {
					addrs[strconv.Itoa(p)] = mp
				}
			}
			roles := map[string]string{}
			for _, p := range opt.replicas {
				if role, _, _, err := stickyRole(rep[p]); err == nil {
					roles[strconv.Itoa(p)] = role
				}
			}
			snap["roles"] = roles
			marks = append(marks, snap)
			markI++
		}
		for _, p := range opt.sentinels {
			mp, err := stickyMasterPort(sent[p])
			key := strconv.Itoa(p)
			if err != nil || mp == opt.oldPort || addrAt[key] != 0 {
				continue
			}
			addrAt[key] = elapsed.Microseconds()
			if firstAddrUs == 0 {
				firstAddrUs = addrAt[key]
				newPort = mp
			}
		}
		for _, p := range opt.replicas {
			role, _, _, err := stickyRole(rep[p])
			if err != nil || role != "master" || firstRoleUs != 0 {
				continue
			}
			firstRoleUs = elapsed.Microseconds()
			newPort = p
			if opt.onPromote != nil {
				opt.onPromote()
			}
		}
		if newPort != 0 && firstRoleUs > 0 && pingUs == 0 {
			if st := rep[newPort]; st != nil {
				if _, err := st.cmd("PING"); err == nil {
					pingUs = time.Since(opt.origin).Microseconds()
				}
			}
		}
		if pingUs > 0 && len(addrAt) == len(opt.sentinels) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	endAddrs := map[string]int{}
	for _, p := range opt.sentinels {
		if mp, err := stickyMasterPort(sent[p]); err == nil {
			endAddrs[strconv.Itoa(p)] = mp
		}
	}
	endRoles := map[string]string{}
	for _, p := range opt.replicas {
		if role, mp, state, err := stickyRole(rep[p]); err == nil {
			endRoles[strconv.Itoa(p)] = role
			if role == "slave" {
				endRoles[strconv.Itoa(p)+"-master"] = strconv.Itoa(mp)
				endRoles[strconv.Itoa(p)+"-state"] = state
			}
		}
	}
	if opt.sub != nil {
		time.Sleep(200 * time.Millisecond)
		opt.sub.Close()
	}
	firstEvent := map[string]int64{}
	var events []map[string]any
	if opt.sub != nil {
		for e := range opt.sub.events {
			us := e.at.Sub(opt.origin).Microseconds()
			if _, ok := firstEvent[e.ch]; !ok && us >= 0 {
				firstEvent[e.ch] = us
			}
			if len(events) < 60 {
				events = append(events, map[string]any{"ch": e.ch, "us": us, "payload": e.payload})
			}
		}
	}
	agreed := 0
	for _, mp := range endAddrs {
		if newPort != 0 && mp == newPort {
			agreed++
		}
	}
	return map[string]any{
		"newPort":      newPort,
		"firstAddrUs":  firstAddrUs,
		"firstRoleUs":  firstRoleUs,
		"pingUs":       pingUs,
		"addrUs":       addrAt,
		"endAddrs":     endAddrs,
		"endRoles":     endRoles,
		"marks":        marks,
		"firstEventUs": firstEvent,
		"events":       events,
		"agreed":       agreed,
		"promoted":     firstRoleUs > 0 && newPort != 0 && newPort != opt.oldPort,
	}
}

func killAndWatch(sentinels, replicas []int, oldPort int, timeout time.Duration, marks []time.Duration) (map[string]any, error) {
	sub, err := subscribe(sentinels[0], failoverChannels...)
	if err != nil {
		return nil, err
	}
	time.Sleep(100 * time.Millisecond)
	origin := time.Now()
	if err := killPort(oldPort); err != nil {
		sub.Close()
		return nil, err
	}
	killDoneUs := time.Since(origin).Microseconds()
	res := awaitPromotion(watchOpt{
		sentinels: sentinels,
		replicas:  replicas,
		oldPort:   oldPort,
		origin:    origin,
		timeout:   timeout,
		marks:     marks,
		sub:       sub,
	})
	res["killDoneUs"] = killDoneUs
	res["subscribed"] = sentinels[0]
	return res, nil
}

func killPort(port int) error {
	pid := pidOf(port)
	if pid == 0 {
		return fmt.Errorf("no pid for %d", port)
	}
	return killIfRedis(pid)
}

func oneReplica(upstream string) clusterOpts {
	if upstream == "" {
		upstream = addr(portM)
	}
	return clusterOpts{
		replicas:        simpleReplicas(1, upstream),
		quorum:          2,
		downAfter:       500,
		failoverTimeout: 20000,
		sentinels:       3,
	}
}

func procSnap(port int) map[string]any {
	pid := pidOf(port)
	mem, _ := infoSection(port, "memory")
	return map[string]any{
		"port":       port,
		"pid":        pid,
		"rssKB":      rssKB(pid),
		"usedMemory": infoInt(mem, "used_memory"),
	}
}

func scenarioManual() error {
	logf("== manual")
	stopAll()
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	if err := writeDataConf(dataOpts{port: portM, priority: 100}); err != nil {
		return err
	}
	if err := launch(portM, false); err != nil {
		return err
	}
	if err := writeDataConf(dataOpts{port: portR1, replicaOf: addr(portM), priority: 100, announce: true}); err != nil {
		return err
	}
	if err := launch(portR1, false); err != nil {
		return err
	}
	if err := waitReplicaOf(portR1, portM, 8*time.Second); err != nil {
		return err
	}
	var samples []map[string]any
	pass := true
	for i := 0; i < 5; i++ {
		if i > 0 {
			c, err := dial(addr(portR1), time.Second)
			if err != nil {
				return err
			}
			_, err = c.cmd("REPLICAOF", "127.0.0.1", strconv.Itoa(portM))
			c.Close()
			if err != nil {
				return err
			}
			if err := waitReplicaOf(portR1, portM, 8*time.Second); err != nil {
				return err
			}
		}
		c, err := dial(addr(portR1), time.Second)
		if err != nil {
			return err
		}
		t0 := time.Now()
		_, err = c.cmd("REPLICAOF", "NO", "ONE")
		cmdUs := time.Since(t0).Microseconds()
		c.Close()
		if err != nil {
			return err
		}
		roleUs := cmdUs
		role, _, _, rerr := roleNow(portR1)
		if rerr != nil || role != "master" {
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				role, _, _, rerr = roleNow(portR1)
				roleUs = time.Since(t0).Microseconds()
				if rerr == nil && role == "master" {
					break
				}
				time.Sleep(time.Millisecond)
			}
		}
		ok := rerr == nil && role == "master"
		if !ok {
			pass = false
		}
		samples = append(samples, map[string]any{
			"i": i, "commandUs": cmdUs, "roleUs": roleUs, "role": role, "ok": ok,
		})
	}
	return finish("manual", map[string]any{
		"pass": pass, "samples": samples, "sentinels": 0,
		"note": "REPLICAOF NO ONE on the same host, no Sentinel",
	})
}

func scenarioSteady() error {
	logf("== steady")
	if err := startCluster(oneReplica("")); err != nil {
		return err
	}
	debug, err := sentinelDebug(portS1)
	if err != nil {
		return err
	}
	ck, ckErr := ckquorum(portS1)
	ckText := ck
	if ckErr != nil {
		ckText = ckErr.Error()
	}
	procs := []map[string]any{
		procSnap(portM), procSnap(portR1), procSnap(portS1), procSnap(portS2), procSnap(portS3),
	}
	readyPass := ckErr == nil && debug["INFO-PERIOD"] == "200" && debug["PING-PERIOD"] == "1000"
	if err := finish("ready", map[string]any{
		"pass": readyPass, "ckquorum": ckText, "debug": debug, "procs": procs,
		"infoPeriodMs": debug["INFO-PERIOD"], "pingPeriodMs": debug["PING-PERIOD"],
		"publishPeriodMs": debug["PUBLISH-PERIOD"],
	}); err != nil {
		return err
	}

	before, err := infoSection(portM, "stats")
	if err != nil {
		return err
	}
	cmd0 := infoInt(before, "total_commands_processed")
	pids := []int{pidOf(portM), pidOf(portS1), pidOf(portS2), pidOf(portS3)}
	ticks := make([]float64, len(pids))
	for i, pid := range pids {
		ticks[i] = cpuTicks(pid)
	}
	t0 := time.Now()
	time.Sleep(4 * time.Second)
	elapsed := time.Since(t0).Seconds()
	cpus := make([]float64, len(pids))
	for i, pid := range pids {
		cpus[i] = round1(cpuPercent(ticks[i], cpuTicks(pid), elapsed))
	}
	after, err := infoSection(portM, "stats")
	if err != nil {
		return err
	}
	cmd1 := infoInt(after, "total_commands_processed")
	if err := finish("idle", map[string]any{
		"pass": true, "seconds": round1(elapsed),
		"masterCommands": cmd1 - cmd0,
		"cpu": map[string]any{
			"master": cpus[0], "s1": cpus[1], "s2": cpus[2], "s3": cpus[3],
		},
		"pids": pids,
	}); err != nil {
		return err
	}

	if err := setKey(portM, "benchkey", "x"); err != nil {
		return err
	}
	sentinelRuns := []map[string]any{}
	for _, n := range []int{1, 16, 64} {
		row, err := benchOne(pidOf(portS1), addr(portS1), n, func(c *Conn) error {
			v, err := c.cmd("SENTINEL", "get-master-addr-by-name", "miaosha")
			if err != nil {
				return err
			}
			if v.Kind != '*' || len(v.Elems) != 2 {
				return fmt.Errorf("bad addr")
			}
			return nil
		})
		if err != nil {
			return err
		}
		row["op"] = "get-master-addr-by-name"
		row["target"] = portS1
		masterCPU := row["extraCpu"]
		_ = masterCPU
		sentinelRuns = append(sentinelRuns, row)
	}
	getRow, err := benchOne(pidOf(portM), addr(portM), 16, func(c *Conn) error {
		_, err := c.cmd("GET", "benchkey")
		return err
	})
	if err != nil {
		return err
	}
	getRow["op"] = "GET"
	getRow["target"] = portM
	pass := true
	for _, row := range sentinelRuns {
		if row["errors"].(int64) != 0 {
			pass = false
		}
	}
	if getRow["errors"].(int64) != 0 {
		pass = false
	}
	if err := finish("bench", map[string]any{
		"pass": pass, "sentinel": sentinelRuns, "get": getRow,
		"note": "4s window after 1s warmup, same host, no pipeline",
	}); err != nil {
		return err
	}

	c, err := dial(addr(portS1), time.Second)
	if err != nil {
		return err
	}
	_, setErr := c.cmd("SENTINEL", "SET", "miaosha", "client-reconfig-script", "/tmp/miaosha-evil.sh")
	c.Close()
	msg := ""
	if setErr != nil {
		msg = setErr.Error()
	}
	m, mErr := sentinelMaster(portS1)
	denied := strings.Contains(msg, "denied")
	return finish("deny-script", map[string]any{
		"pass":  denied && mErr == nil && m["client-reconfig-script"] == "",
		"error": msg, "clientReconfigScript": m["client-reconfig-script"],
	})
}

func sentinelDebug(port int) (map[string]string, error) {
	c, err := dial(addr(port), time.Second)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	v, err := c.cmd("SENTINEL", "DEBUG")
	if err != nil {
		return nil, err
	}
	return pairs(v)
}

func benchOne(pid int, address string, workers int, fn func(*Conn) error) (map[string]any, error) {
	type box struct{ c *Conn }
	boxes := make([]*box, workers)
	for i := 0; i < workers; i++ {
		c, err := dial(address, 2*time.Second)
		if err != nil {
			return nil, err
		}
		boxes[i] = &box{c: c}
	}
	defer func() {
		for _, b := range boxes {
			if b.c != nil {
				b.c.Close()
			}
		}
	}()
	var wg sync.WaitGroup
	warmEnd := time.Now().Add(time.Second)
	for _, b := range boxes {
		wg.Add(1)
		go func(b *box) {
			defer wg.Done()
			for time.Now().Before(warmEnd) {
				if b.c == nil {
					c, err := dial(address, time.Second)
					if err != nil {
						time.Sleep(time.Millisecond)
						continue
					}
					b.c = c
				}
				if err := fn(b.c); err != nil {
					b.c.Close()
					b.c = nil
				}
			}
		}(b)
	}
	wg.Wait()
	hists := make([]*hist, workers)
	errN := make([]int64, workers)
	end := time.Now().Add(4 * time.Second)
	cpu0 := cpuTicks(pid)
	self0 := cpuTicks(os.Getpid())
	t0 := time.Now()
	for i, b := range boxes {
		hists[i] = newHist()
		wg.Add(1)
		go func(i int, b *box) {
			defer wg.Done()
			for time.Now().Before(end) {
				if b.c == nil {
					c, err := dial(address, time.Second)
					if err != nil {
						errN[i]++
						time.Sleep(time.Millisecond)
						continue
					}
					b.c = c
				}
				start := time.Now()
				err := fn(b.c)
				if err != nil {
					errN[i]++
					b.c.Close()
					b.c = nil
					continue
				}
				hists[i].add(time.Since(start).Nanoseconds())
			}
		}(i, b)
	}
	wg.Wait()
	elapsed := time.Since(t0).Seconds()
	total := newHist()
	var errors int64
	for i := range hists {
		total.merge(hists[i])
		errors += errN[i]
	}
	qps := 0.0
	if elapsed > 0 {
		qps = float64(total.n) / elapsed
	}
	cpu := round1(cpuPercent(cpu0, cpuTicks(pid), elapsed))
	self := round1(cpuPercent(self0, cpuTicks(os.Getpid()), elapsed))
	return map[string]any{
		"workers": workers, "ops": total.n, "errors": errors,
		"seconds": round1(elapsed), "qps": qps,
		"p50us": total.le(0.50) / 1000, "p99us": total.le(0.99) / 1000,
		"maxus": total.max / 1000, "cpu": cpu, "clientCpu": self,
		"saturated": cpu >= 90,
	}, nil
}

func scenarioFailover500() error {
	logf("== failover-500")
	var samples []map[string]any
	pass := true
	for i := 0; i < 3; i++ {
		logf("failover-500 sample %d", i)
		if err := startCluster(oneReplica("")); err != nil {
			return err
		}
		if err := setKey(portM, "stock", "100"); err != nil {
			return err
		}
		if err := waitKey(portR1, "stock", "100", 3*time.Second); err != nil {
			return err
		}
		res, err := killAndWatch([]int{portS1, portS2, portS3}, []int{portR1}, portM, 15*time.Second, nil)
		if err != nil {
			return err
		}
		got, ok, gerr := getKey(portR1, "stock")
		res["i"] = i
		res["stockAfter"] = got
		res["stockOk"] = gerr == nil && ok && got == "100"
		line, _ := monitorLine(portS1)
		res["monitorLine"] = line
		res["replicaofLines"] = confLines(portR1, "replicaof")
		samplePass := res["promoted"] == true && res["newPort"] == portR1 && res["stockOk"] == true && res["agreed"] == 3
		res["samplePass"] = samplePass
		if !samplePass {
			pass = false
		}
		samples = append(samples, res)
	}
	return finish("failover-500", map[string]any{
		"pass": pass, "downAfterMs": 500, "samples": samples,
		"poll": "5ms",
	})
}

func scenarioFailover30000() error {
	logf("== failover-30000")
	opt := oneReplica("")
	opt.downAfter = 30000
	opt.failoverTimeout = 60000
	if err := startCluster(opt); err != nil {
		return err
	}
	if err := setKey(portM, "stock", "100"); err != nil {
		return err
	}
	if err := waitKey(portR1, "stock", "100", 3*time.Second); err != nil {
		return err
	}
	res, err := killAndWatch([]int{portS1, portS2, portS3}, []int{portR1}, portM, 45*time.Second, []time.Duration{20 * time.Second})
	if err != nil {
		return err
	}
	got, ok, gerr := getKey(portR1, "stock")
	res["stockAfter"] = got
	res["stockOk"] = gerr == nil && ok && got == "100"
	stillOld := false
	if marks, ok := res["marks"].([]map[string]any); ok && len(marks) > 0 {
		addrs, _ := marks[0]["addrs"].(map[string]int)
		roles, _ := marks[0]["roles"].(map[string]string)
		stillOld = len(addrs) == 3
		for _, p := range addrs {
			if p != portM {
				stillOld = false
			}
		}
		if roles[strconv.Itoa(portR1)] != "slave" {
			stillOld = false
		}
		res["stillOldAt20s"] = stillOld
	}
	res["downAfterMs"] = 30000
	pass := res["promoted"] == true && res["newPort"] == portR1 && res["stockOk"] == true && stillOld && res["agreed"] == 3
	res["pass"] = pass
	return finish("failover-30000", res)
}

func scenarioQuorum() error {
	logf("== quorum")
	cases := []struct {
		name          string
		quorum        int
		stop          int
		expectPromote bool
	}{
		{"quorum2-stop2", 2, 2, false},
		{"quorum1-stop2", 1, 2, false},
		{"quorum2-stop1", 2, 1, true},
	}
	var out []map[string]any
	pass := true
	for _, cs := range cases {
		logf("quorum %s", cs.name)
		opt := oneReplica("")
		opt.quorum = cs.quorum
		opt.downAfter = 1000
		opt.failoverTimeout = 20000
		if err := startCluster(opt); err != nil {
			return err
		}
		ports := []int{portS1, portS2, portS3}
		for i := 0; i < cs.stop; i++ {
			stopPort(ports[len(ports)-1-i])
		}
		alive := ports[:len(ports)-cs.stop]
		var ck string
		ckDeadline := time.Now().Add(8 * time.Second)
		for {
			text, err := ckquorum(alive[0])
			if err != nil {
				ck = err.Error()
			} else {
				ck = text
			}
			if cs.expectPromote {
				break
			}
			if strings.Contains(ck, "majority") || time.Now().After(ckDeadline) {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if err := setKey(portM, "stock", "1"); err != nil {
			return err
		}
		if err := waitKey(portR1, "stock", "1", 3*time.Second); err != nil {
			return err
		}
		watchFor := 15 * time.Second
		res, err := killAndWatch(alive, []int{portR1}, portM, watchFor, nil)
		if err != nil {
			return err
		}
		peers, _ := sentinelPeers(alive[0])
		var peerFlags []map[string]string
		for _, p := range peers {
			peerFlags = append(peerFlags, map[string]string{
				"port": p["port"], "flags": p["flags"],
			})
		}
		promoted := res["promoted"] == true
		casePass := promoted == cs.expectPromote
		if cs.expectPromote {
			casePass = casePass && res["newPort"] == portR1 && res["agreed"] == len(alive)
		} else {
			role, _, _, rerr := roleNow(portR1)
			res["replicaRole"] = role
			casePass = casePass && rerr == nil && role == "slave" && strings.Contains(ck, "majority")
		}
		if !casePass {
			pass = false
		}
		res["name"] = cs.name
		res["quorum"] = cs.quorum
		res["stopped"] = cs.stop
		res["ckquorum"] = ck
		res["peers"] = peerFlags
		res["casePass"] = casePass
		out = append(out, res)
	}
	return finish("quorum", map[string]any{"pass": pass, "cases": out, "watchSec": 15})
}

func scenarioPriority() error {
	logf("== priority")
	opt := clusterOpts{
		replicas: []dataOpts{
			{port: portR1, replicaOf: addr(portM), priority: 100, announce: true},
			{port: portR2, replicaOf: addr(portM), priority: 0, announce: true},
		},
		quorum: 2, downAfter: 500, failoverTimeout: 20000, sentinels: 3,
	}
	if err := startCluster(opt); err != nil {
		return err
	}
	var reps []map[string]string
	deadline := time.Now().Add(8 * time.Second)
	found := false
	for time.Now().Before(deadline) {
		var err error
		reps, err = sentinelReplicas(portS1)
		if err == nil {
			n := 0
			zero := false
			for _, r := range reps {
				if r["port"] == strconv.Itoa(portR1) || r["port"] == strconv.Itoa(portR2) {
					n++
				}
				if r["port"] == strconv.Itoa(portR2) && r["slave-priority"] == "0" {
					zero = true
				}
			}
			if n >= 2 && zero {
				found = true
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !found {
		return fmt.Errorf("priority not visible: %+v\n%s", reps, tailLogs())
	}
	if err := setKey(portM, "stock", "5"); err != nil {
		return err
	}
	if err := waitKey(portR1, "stock", "5", 3*time.Second); err != nil {
		return err
	}
	if err := waitKey(portR2, "stock", "5", 3*time.Second); err != nil {
		return err
	}
	res, err := killAndWatch([]int{portS1, portS2, portS3}, []int{portR1, portR2}, portM, 15*time.Second, nil)
	if err != nil {
		return err
	}
	reconfErr := waitReplicaOf(portR2, portR1, 10*time.Second)
	role2, mp2, state2, _ := roleNow(portR2)
	brief := []map[string]string{}
	for _, r := range reps {
		brief = append(brief, map[string]string{
			"port": r["port"], "flags": r["flags"], "slave-priority": r["slave-priority"],
		})
	}
	res["replicasBefore"] = brief
	res["r2Role"] = role2
	res["r2Master"] = mp2
	res["r2State"] = state2
	res["reconfErr"] = errString(reconfErr)
	pass := res["promoted"] == true && res["newPort"] == portR1 && role2 == "slave" && mp2 == portR1 && reconfErr == nil
	res["pass"] = pass
	return finish("priority", res)
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func scenarioSleep() error {
	logf("== sleep")
	opt := oneReplica("")
	opt.downAfter = 800
	if err := startCluster(opt); err != nil {
		return err
	}
	if err := setKey(portM, "stock", "7"); err != nil {
		return err
	}
	if err := waitKey(portR1, "stock", "7", 3*time.Second); err != nil {
		return err
	}
	pid := pidOf(portM)
	sub, err := subscribe(portS1, failoverChannels...)
	if err != nil {
		return err
	}
	c, err := dial(addr(portM), time.Second)
	if err != nil {
		return err
	}
	if err := c.write("DEBUG", "SLEEP", "4"); err != nil {
		return err
	}
	origin := time.Now()
	var sleepUs atomic.Int64
	var promotedBefore atomic.Bool
	go func() {
		_, _ = readVal(c.r)
		sleepUs.Store(time.Since(origin).Microseconds())
		c.Close()
	}()
	watch := awaitPromotion(watchOpt{
		sentinels: []int{portS1, portS2, portS3},
		replicas:  []int{portR1},
		oldPort:   portM,
		origin:    origin,
		timeout:   8 * time.Second,
		sub:       sub,
		onPromote: func() {
			if sleepUs.Load() == 0 {
				promotedBefore.Store(true)
			}
		},
	})
	promotedBeforeReturn := promotedBefore.Load()
	// 等 SLEEP 返回后再去问旧主库。问早了会卡在同一条事件循环上。
	deadline := time.Now().Add(6 * time.Second)
	for sleepUs.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	oldS := &sticky{addr: addr(portM)}
	newS := &sticky{addr: addr(portR1)}
	defer oldS.Close()
	defer newS.Close()
	var firstBoth, lastBoth time.Time
	ghostOK := 0
	demoteUs := int64(0)
	loopDeadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(loopDeadline) {
		rOld, _, _, e1 := stickyRole(oldS)
		rNew, _, _, e2 := stickyRole(newS)
		if e1 == nil && e2 == nil && rOld == "master" && rNew == "master" {
			now := time.Now()
			if firstBoth.IsZero() {
				firstBoth = now
			}
			lastBoth = now
			if _, err := oldS.cmd("SET", "ghost", "1"); err == nil {
				ghostOK++
			}
		}
		if e1 == nil && rOld == "slave" && e2 == nil && rNew == "master" {
			demoteUs = time.Since(origin).Microseconds()
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	ghost, ghostPresent, _ := getKey(portR1, "ghost")
	stock, _, _ := getKey(portR1, "stock")
	overlapUs := int64(0)
	if !firstBoth.IsZero() {
		overlapUs = lastBoth.Sub(firstBoth).Microseconds()
	}
	stillThere := pidOf(portM) == pid
	pass := watch["promoted"] == true && watch["newPort"] == portR1 &&
		promotedBeforeReturn && stillThere && demoteUs > 0 && stock == "7" &&
		(ghostOK == 0 || !ghostPresent)
	watch["pass"] = pass
	watch["sleepReturnUs"] = sleepUs.Load()
	watch["promotedBeforeSleepReturn"] = promotedBeforeReturn
	watch["oldPid"] = pid
	watch["oldPidStillThere"] = stillThere
	watch["ghostOK"] = ghostOK
	watch["ghostFinal"] = ghost
	watch["ghostPresent"] = ghostPresent
	watch["stockAfter"] = stock
	watch["overlapUs"] = overlapUs
	watch["demoteUs"] = demoteUs
	watch["downAfterMs"] = 800
	return finish("sleep", watch)
}

func scenarioZombie() error {
	logf("== zombie")
	if err := startCluster(oneReplica("")); err != nil {
		return err
	}
	if err := setKey(portM, "kept", "from-new"); err != nil {
		return err
	}
	if err := waitKey(portR1, "kept", "from-new", 3*time.Second); err != nil {
		return err
	}
	if _, err := killAndWatch([]int{portS1, portS2, portS3}, []int{portR1}, portM, 15*time.Second, nil); err != nil {
		return err
	}
	got, ok, err := getKey(portR1, "kept")
	if err != nil || !ok || got != "from-new" {
		return fmt.Errorf("kept after failover: %q %v", got, err)
	}
	var rounds []map[string]any
	pass := true
	for i := 0; i < 3; i++ {
		logf("zombie round %d", i)
		row, err := zombieOnce()
		if err != nil {
			return err
		}
		row["i"] = i
		if row["roundPass"] != true {
			pass = false
		}
		rounds = append(rounds, row)
	}
	return finish("zombie", map[string]any{"pass": pass, "rounds": rounds})
}

func zombieOnce() (map[string]any, error) {
	stopPort(portM)
	if err := os.RemoveAll(dataDir(portM)); err != nil {
		return nil, err
	}
	if err := writeDataConf(dataOpts{port: portM, priority: 100}); err != nil {
		return nil, err
	}
	var sets atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var firstOK atomic.Int64
	wg.Add(1)
	go func() {
		defer wg.Done()
		var c *Conn
		for {
			select {
			case <-stop:
				if c != nil {
					c.Close()
				}
				return
			default:
			}
			if c == nil {
				nc, err := dial(addr(portM), 200*time.Millisecond)
				if err != nil {
					time.Sleep(2 * time.Millisecond)
					continue
				}
				c = nc
			}
			_, err := c.cmd("SET", "kept", "from-zombie")
			if err != nil {
				c.Close()
				c = nil
				continue
			}
			if firstOK.Load() == 0 {
				firstOK.Store(time.Now().UnixNano())
			}
			sets.Add(1)
		}
	}()
	tLaunch := time.Now()
	if err := launch(portM, false); err != nil {
		close(stop)
		wg.Wait()
		return nil, err
	}
	var demoteUs int64
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		role, mp, _, err := roleNow(portM)
		if err == nil && role == "slave" && mp == portR1 {
			demoteUs = time.Since(tLaunch).Microseconds()
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	reconfErr := waitReplicaOf(portM, portR1, 10*time.Second)
	keptNew, _, _ := getKey(portR1, "kept")
	keptOld, _, _ := getKey(portM, "kept")
	lines := confLines(portM, "replicaof")
	firstUs := int64(0)
	if firstOK.Load() != 0 {
		firstUs = time.Unix(0, firstOK.Load()).Sub(tLaunch).Microseconds()
	}
	roundPass := demoteUs > 0 && reconfErr == nil && keptNew == "from-new" && keptOld == "from-new"
	return map[string]any{
		"roundPass":  roundPass,
		"zombieSets": sets.Load(),
		"firstSetUs": firstUs,
		"demoteUs":   demoteUs,
		"keptOnNew":  keptNew,
		"keptOnOld":  keptOld,
		"replicaof":  lines,
		"reconfErr":  errString(reconfErr),
	}, nil
}

func scenarioTail() error {
	logf("== tail")
	var direct []map[string]any
	var delayed []map[string]any
	pass := true
	for i := 0; i < 3; i++ {
		logf("tail direct %d", i)
		row, err := tailOnce(0)
		if err != nil {
			return err
		}
		row["i"] = i
		if row["samplePass"] != true {
			pass = false
		}
		direct = append(direct, row)
	}
	for i := 0; i < 3; i++ {
		logf("tail delay %d", i)
		row, err := tailOnce(2 * time.Millisecond)
		if err != nil {
			return err
		}
		row["i"] = i
		if row["samplePass"] != true {
			pass = false
		}
		delayed = append(delayed, row)
	}
	var stale []map[string]any
	for i := 0; i < 3; i++ {
		logf("tail stale %d", i)
		row, err := staleOnce()
		if err != nil {
			return err
		}
		row["i"] = i
		if row["samplePass"] != true {
			pass = false
		}
		stale = append(stale, row)
	}
	return finish("tail", map[string]any{
		"pass": pass, "direct": direct, "delay2ms": delayed, "stale": stale,
		"delayNote": "local tcp proxy sleep per chunk per direction, not a second machine",
		"staleNote": "REPLICAOF 127.0.0.1 1 freezes the replica, then 2000 more INCRs, then kill the master",
	})
}

func tailOnce(delay time.Duration) (map[string]any, error) {
	upstream := ""
	var proxy *delayProxy
	if delay > 0 {
		var err error
		proxy, err = startDelayProxy(addr(portProxy), addr(portM), delay)
		if err != nil {
			return nil, err
		}
		defer proxy.Close()
		upstream = addr(portProxy)
	}
	if err := startCluster(oneReplica(upstream)); err != nil {
		return nil, err
	}
	c, err := dial(addr(portM), time.Second)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	var killNano atomic.Int64
	go func() {
		time.Sleep(250 * time.Millisecond)
		killNano.Store(time.Now().UnixNano())
		_ = killPort(portM)
	}()
	t0 := time.Now()
	var last int64
	var n int64
	var loopErr string
	for time.Since(t0) < 5*time.Second {
		v, err := c.cmd("INCR", "stock")
		if err != nil {
			loopErr = err.Error()
			break
		}
		last = v.Int
		n++
	}
	loopUs := time.Since(t0).Microseconds()
	if killNano.Load() == 0 {
		return nil, fmt.Errorf("incr loop ended before kill: %s", loopErr)
	}
	origin := time.Unix(0, killNano.Load())
	sub, err := subscribe(portS1, failoverChannels...)
	if err != nil {
		return nil, err
	}
	// 订阅晚于 kill。事件时间仍相对 kill，但 +sdown 可能已经发过。
	watch := awaitPromotion(watchOpt{
		sentinels: []int{portS1, portS2, portS3},
		replicas:  []int{portR1},
		oldPort:   portM,
		origin:    origin,
		timeout:   15 * time.Second,
		sub:       sub,
	})
	finalStr, present, gerr := getKey(portR1, "stock")
	final := int64(0)
	if present {
		final, _ = strconv.ParseInt(finalStr, 10, 64)
	}
	loss := last - final
	// final 比 lastAck 大 1，是回复没读到、写入已经复制。这不是从库更旧。
	samplePass := watch["promoted"] == true && watch["newPort"] == portR1 && gerr == nil && last > 100 && final >= last && final <= last+1
	watch["successes"] = n
	watch["lastAck"] = last
	watch["final"] = final
	watch["loss"] = loss
	watch["loopUs"] = loopUs
	watch["loopErr"] = loopErr
	watch["delayUs"] = delay.Microseconds()
	watch["samplePass"] = samplePass
	watch["subscribedAfterKill"] = true
	return watch, nil
}

func staleOnce() (map[string]any, error) {
	if err := startCluster(oneReplica("")); err != nil {
		return nil, err
	}
	c, err := dial(addr(portM), time.Second)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	var base int64
	warmUntil := time.Now().Add(30 * time.Millisecond)
	for time.Now().Before(warmUntil) {
		v, err := c.cmd("INCR", "stock")
		if err != nil {
			return nil, err
		}
		base = v.Int
	}
	rc, err := dial(addr(portR1), time.Second)
	if err != nil {
		return nil, err
	}
	_, err = rc.cmd("REPLICAOF", "127.0.0.1", "1")
	rc.Close()
	if err != nil {
		return nil, err
	}
	state := ""
	linkDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(linkDeadline) {
		_, _, st, rerr := roleNow(portR1)
		state = st
		if rerr == nil && st != "" && st != "connected" {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if state == "connected" {
		return nil, fmt.Errorf("replica still connected after REPLICAOF 127.0.0.1 1, state=%s", state)
	}
	frozenStr, present, gerr := getKey(portR1, "stock")
	if gerr != nil || !present {
		return nil, fmt.Errorf("frozen get: %q %v", frozenStr, gerr)
	}
	frozen, _ := strconv.ParseInt(frozenStr, 10, 64)
	const extra = 2000
	var last int64
	for i := 0; i < extra; i++ {
		v, err := c.cmd("INCR", "stock")
		if err != nil {
			return nil, err
		}
		last = v.Int
	}
	origin := time.Now()
	if err := killPort(portM); err != nil {
		return nil, err
	}
	sub, err := subscribe(portS1, failoverChannels...)
	if err != nil {
		return nil, err
	}
	watch := awaitPromotion(watchOpt{
		sentinels: []int{portS1, portS2, portS3},
		replicas:  []int{portR1},
		oldPort:   portM,
		origin:    origin,
		timeout:   15 * time.Second,
		sub:       sub,
	})
	finalStr, finalPresent, ferr := getKey(portR1, "stock")
	final := int64(0)
	if finalPresent {
		final, _ = strconv.ParseInt(finalStr, 10, 64)
	}
	loss := last - final
	samplePass := watch["promoted"] == true && watch["newPort"] == portR1 && ferr == nil &&
		finalPresent && last >= frozen+int64(extra) && final < last && final <= frozen+50 && loss >= int64(extra)-50
	watch["base"] = base
	watch["frozen"] = frozen
	watch["linkState"] = state
	watch["extra"] = extra
	watch["lastAck"] = last
	watch["final"] = final
	watch["loss"] = loss
	watch["samplePass"] = samplePass
	watch["subscribedAfterKill"] = true
	return watch, nil
}

func scenarioConfig() error {
	logf("== config")
	if err := startCluster(oneReplica("")); err != nil {
		return err
	}
	if err := setKey(portM, "stock", "42"); err != nil {
		return err
	}
	if err := waitKey(portR1, "stock", "42", 3*time.Second); err != nil {
		return err
	}
	watch, err := killAndWatch([]int{portS1, portS2, portS3}, []int{portR1}, portM, 15*time.Second, nil)
	if err != nil {
		return err
	}
	if watch["promoted"] != true || watch["newPort"] != portR1 {
		return fmt.Errorf("config setup did not promote: %+v", watch["endAddrs"])
	}
	var lines []string
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		lines = nil
		ok := true
		for _, p := range []int{portS1, portS2, portS3} {
			line, err := monitorLine(p)
			if err != nil || !strings.Contains(line, "6380") {
				ok = false
			}
			lines = append(lines, line)
		}
		if ok {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	for _, p := range []int{portS1, portS2, portS3} {
		stopPort(p)
	}
	tRestart := time.Now()
	for _, p := range []int{portS1, portS2, portS3} {
		if err := launch(p, true); err != nil {
			return err
		}
	}
	var restartUs int64
	restartDeadline := time.Now().Add(8 * time.Second)
	restartOK := false
	for time.Now().Before(restartDeadline) {
		good := 0
		for _, p := range []int{portS1, portS2, portS3} {
			m, err := sentinelMaster(p)
			if err == nil && m["port"] == "6380" && !strings.Contains(m["flags"], "s_down") && !strings.Contains(m["flags"], "o_down") {
				good++
			}
		}
		if good == 3 {
			restartUs = time.Since(tRestart).Microseconds()
			restartOK = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, p := range []int{portS1, portS2, portS3} {
		stopPort(p)
	}
	for _, p := range []int{portS1, portS2, portS3} {
		if err := writeSentinelConf(sentinelOpts{
			port: p, masterPort: portM, quorum: 2, downAfter: 500, failoverTimeout: 20000,
		}); err != nil {
			return err
		}
		if err := launch(p, true); err != nil {
			return err
		}
	}
	if err := tuneSentinels([]int{portS1, portS2, portS3}); err != nil {
		return err
	}
	freshDeadline := time.Now().Add(8 * time.Second)
	sawOther := false
	for time.Now().Before(freshDeadline) {
		for _, p := range []int{portS1, portS2, portS3} {
			mp, err := masterAddr(p)
			if err == nil && mp != portM {
				sawOther = true
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	fresh := map[string]any{}
	allOld := !sawOther
	for _, p := range []int{portS1, portS2, portS3} {
		m, err := sentinelMaster(p)
		if err != nil {
			allOld = false
			fresh[strconv.Itoa(p)] = err.Error()
			continue
		}
		fresh[strconv.Itoa(p)] = map[string]string{
			"port": m["port"], "flags": m["flags"], "num-slaves": m["num-slaves"],
		}
		if m["port"] != strconv.Itoa(portM) {
			allOld = false
		}
	}
	role, _, _, rerr := roleNow(portR1)
	pass := watch["promoted"] == true && restartOK && allOld && rerr == nil && role == "master"
	for _, line := range lines {
		if !strings.Contains(line, "6380") {
			pass = false
		}
	}
	return finish("config-restart", map[string]any{
		"pass": pass, "monitorLines": lines, "restartUs": restartUs, "restartOK": restartOK,
		"fresh": fresh, "realMasterRole": role, "realMasterPort": portR1,
	})
}

func scenarioForced() error {
	logf("== forced")
	cached, err := forcedOnce("cache")
	if err != nil {
		return err
	}
	ask, err := forcedOnce("ask")
	if err != nil {
		return err
	}
	pass := cached["modePass"] == true && ask["modePass"] == true
	return finish("forced", map[string]any{"pass": pass, "cache": cached, "ask": ask})
}

func forcedOnce(mode string) (map[string]any, error) {
	logf("forced %s", mode)
	opt := oneReplica("")
	opt.downAfter = 5000
	if err := startCluster(opt); err != nil {
		return nil, err
	}
	if err := setKey(portM, "stock", "0"); err != nil {
		return nil, err
	}
	if err := waitKey(portR1, "stock", "0", 3*time.Second); err != nil {
		return nil, err
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var successOld, successNew, errors atomic.Int64
	wg.Add(1)
	go func() {
		defer wg.Done()
		var c *Conn
		sentinel := &sticky{addr: addr(portS1)}
		defer sentinel.Close()
		cur := portM
		dialTo := func(p int) {
			if c != nil {
				c.Close()
			}
			nc, err := dial(addr(p), time.Second)
			if err != nil {
				c = nil
				return
			}
			c = nc
			cur = p
		}
		dialTo(portM)
		defer func() {
			if c != nil {
				c.Close()
			}
		}()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if mode == "ask" || c == nil {
				p, err := stickyMasterPort(sentinel)
				if err != nil {
					errors.Add(1)
					time.Sleep(time.Millisecond)
					continue
				}
				if c == nil || p != cur {
					dialTo(p)
				}
			}
			if c == nil {
				errors.Add(1)
				time.Sleep(time.Millisecond)
				continue
			}
			_, err := c.cmd("INCR", "stock")
			if err != nil {
				errors.Add(1)
				c.Close()
				c = nil
				continue
			}
			if cur == portM {
				successOld.Add(1)
			} else {
				successNew.Add(1)
			}
		}
	}()
	time.Sleep(200 * time.Millisecond)
	c, err := dial(addr(portS1), time.Second)
	if err != nil {
		close(stop)
		wg.Wait()
		return nil, err
	}
	t0 := time.Now()
	_, err = c.cmd("SENTINEL", "FAILOVER", "miaosha")
	c.Close()
	if err != nil {
		close(stop)
		wg.Wait()
		return nil, err
	}
	oldS := &sticky{addr: addr(portM)}
	newS := &sticky{addr: addr(portR1)}
	defer oldS.Close()
	defer newS.Close()
	var firstBoth, lastBoth time.Time
	demoteUs := int64(0)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		rOld, _, _, e1 := stickyRole(oldS)
		rNew, _, _, e2 := stickyRole(newS)
		if e1 == nil && e2 == nil && rOld == "master" && rNew == "master" {
			now := time.Now()
			if firstBoth.IsZero() {
				firstBoth = now
			}
			lastBoth = now
		}
		if e1 == nil && rOld == "slave" && e2 == nil && rNew == "master" {
			demoteUs = time.Since(t0).Microseconds()
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	reconfErr := waitReplicaOf(portM, portR1, 8*time.Second)
	finalPort, _ := masterAddr(portS1)
	finalStr, present, gerr := getKey(finalPort, "stock")
	final := int64(0)
	if present {
		final, _ = strconv.ParseInt(finalStr, 10, 64)
	}
	oldN := successOld.Load()
	newN := successNew.Load()
	gap := oldN + newN - final
	overlapUs := int64(0)
	if !firstBoth.IsZero() {
		overlapUs = lastBoth.Sub(firstBoth).Microseconds()
	}
	modePass := demoteUs > 0 && reconfErr == nil && gerr == nil && present && final <= oldN+newN && finalPort == portR1
	return map[string]any{
		"mode": mode, "modePass": modePass,
		"successOld": oldN, "successNew": newN, "errors": errors.Load(),
		"final": final, "finalPort": finalPort, "gap": gap,
		"demoteUs": demoteUs, "overlapUs": overlapUs,
		"reconfErr": errString(reconfErr),
		"note":      "gap is acknowledged INCRs minus the value left on the elected master; not a 4s capacity run",
	}, nil
}

func runAll() error {
	steps := []struct {
		name string
		fn   func() error
	}{
		{"manual", scenarioManual},
		{"steady", scenarioSteady},
		{"failover-500", scenarioFailover500},
		{"quorum", scenarioQuorum},
		{"priority", scenarioPriority},
		{"sleep", scenarioSleep},
		{"zombie", scenarioZombie},
		{"tail", scenarioTail},
		{"config", scenarioConfig},
		{"forced", scenarioForced},
		{"failover-30000", scenarioFailover30000},
	}
	for _, step := range steps {
		t0 := time.Now()
		if err := step.fn(); err != nil {
			return err
		}
		logf("done %s in %s", step.name, time.Since(t0).Round(time.Millisecond))
	}
	return nil
}
