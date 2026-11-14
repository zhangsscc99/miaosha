package main

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type nodeStat struct {
	Addr       string  `json:"addr"`
	Port       int     `json:"port"`
	Pid        int     `json:"pid"`
	ID         string  `json:"id,omitempty"`
	Slots      int     `json:"slots,omitempty"`
	Flags      string  `json:"flags,omitempty"`
	CPUPercent float64 `json:"cpuPercent,omitempty"`
	RSSKB      int64   `json:"rssKB,omitempty"`
	UsedMemory int64   `json:"usedMemory,omitempty"`
	Ops        int64   `json:"ops,omitempty"`
	Commands   int64   `json:"commands,omitempty"`
}

type result struct {
	Scenario         string         `json:"scenario"`
	Note             string         `json:"note"`
	Pass             bool           `json:"pass"`
	Detail           string         `json:"detail,omitempty"`
	Workers          int            `json:"workers,omitempty"`
	ElapsedSec       float64        `json:"elapsedSec,omitempty"`
	Ops              int64          `json:"ops,omitempty"`
	QPS              float64        `json:"qps,omitempty"`
	Errors           int64          `json:"errors,omitempty"`
	RedisCommands    int64          `json:"redisCommands,omitempty"`
	RedisCPUPercent  float64        `json:"redisCpuPercent,omitempty"`
	ClientCPUPercent float64        `json:"clientCpuPercent,omitempty"`
	P50us            int64          `json:"p50us,omitempty"`
	P99us            int64          `json:"p99us,omitempty"`
	P999us           int64          `json:"p999us,omitempty"`
	Maxus            int64          `json:"maxus,omitempty"`
	Nodes            []nodeStat     `json:"nodes,omitempty"`
	Extra            map[string]any `json:"extra,omitempty"`
}

func (r *result) put(k string, v any) {
	if r.Extra == nil {
		r.Extra = map[string]any{}
	}
	r.Extra[k] = v
}

type runner struct {
	w *world
}

func (rn *runner) fill(s *node) nodeStat {
	return nodeStat{
		Addr:       s.Addr,
		Port:       s.Port,
		Pid:        s.Pid,
		ID:         s.ID,
		Slots:      s.Slots,
		Flags:      s.Flags,
		RSSKB:      rssKB(s.Pid),
		UsedMemory: rn.w.used(s),
	}
}

func (rn *runner) scenarioReady() (result, error) {
	w := rn.w
	deadline := time.Now().Add(15 * time.Second)
	var last error
	for {
		if err := w.refresh(); err != nil {
			last = err
		} else if len(w.masters()) == 3 {
			reps := 0
			slots := 0
			for _, m := range w.masters() {
				slots += m.Slots
				reps += len(w.replicasOf(m.ID))
			}
			if reps == 3 && slots == 16384 {
				break
			}
			last = fmt.Errorf("masters=%d replicas=%d slots=%d", len(w.masters()), reps, slots)
		}
		if time.Now().After(deadline) {
			if last == nil {
				last = fmt.Errorf("replicas not ready")
			}
			return result{}, last
		}
		time.Sleep(100 * time.Millisecond)
	}
	res := result{
		Scenario: "ready",
		Note:     "3 个主库各 1 个副本，16384 个槽。每个进程 maxmemory 64mb。没有第二台机器。",
	}
	var nodes []map[string]any
	slots := 0
	reps := 0
	for _, m := range w.masters() {
		slots += m.Slots
		rs := w.replicasOf(m.ID)
		reps += len(rs)
		st := rn.fill(m)
		info, _ := m.call("INFO", "cluster")
		enabled := ""
		if info.Err == "" {
			enabled = infoValue(info.Str, "cluster_enabled")
		}
		cfg, _ := m.call("CONFIG", "GET", "cluster-node-timeout")
		nodes = append(nodes, map[string]any{
			"addr": st.Addr, "slots": m.Slots, "epoch": m.Epoch, "flags": m.Flags,
			"rssKB": st.RSSKB, "usedMemory": st.UsedMemory, "pid": st.Pid,
			"replicas": len(rs), "clusterEnabled": enabled,
			"nodeTimeout": replyList(cfg),
		})
		for _, r := range rs {
			rt := rn.fill(r)
			nodes = append(nodes, map[string]any{
				"addr": rt.Addr, "slots": r.Slots, "epoch": r.Epoch, "flags": r.Flags,
				"rssKB": rt.RSSKB, "usedMemory": rt.UsedMemory, "pid": rt.Pid,
				"master": m.Addr,
			})
		}
	}
	ci, err := w.clusterInfo(w.masters()[0])
	if err != nil {
		return result{}, err
	}
	res.put("nodes", nodes)
	res.put("clusterState", ci["cluster_state"])
	res.put("clusterSize", ci["cluster_size"])
	res.put("knownNodes", ci["cluster_known_nodes"])
	res.put("slotsAssigned", ci["cluster_slots_assigned"])
	res.put("masterCount", len(w.masters()))
	res.put("replicaCount", reps)
	res.Detail = fmt.Sprintf("masters=%d replicas=%d slots=%d state=%s", len(w.masters()), reps, slots, ci["cluster_state"])
	res.Pass = len(w.masters()) == 3 && reps == 3 && slots == 16384 && ci["cluster_state"] == "ok" && ci["cluster_known_nodes"] == "6"
	return res, nil
}

func replyList(v Val) string {
	if v.Err != "" {
		return v.Err
	}
	parts := []string{}
	for _, el := range v.Elems {
		if el.Str != "" {
			parts = append(parts, el.Str)
		}
	}
	if len(parts) == 0 && v.Str != "" {
		return v.Str
	}
	return strings.Join(parts, " ")
}

func (rn *runner) scenarioIdle() (result, error) {
	w := rn.w
	if err := w.refresh(); err != nil {
		return result{}, err
	}
	for _, n := range w.nodes {
		if err := n.refreshMeta(); err != nil {
			return result{}, err
		}
	}
	type snap struct {
		cmd int64
		bus int64
		cpu float64
	}
	before := make([]snap, len(w.nodes))
	for i, n := range w.nodes {
		ci, _ := w.clusterInfo(n)
		bus := int64(0)
		if ci != nil {
			bus, _ = strconvParse(ci["cluster_stats_messages_sent"])
		}
		before[i] = snap{cmd: w.commands(n), bus: bus, cpu: cpuTicks(n.Pid)}
	}
	time.Sleep(4 * time.Second)
	res := result{
		Scenario:   "idle",
		Note:       "6 个进程空闲 4 秒。命令增量里含采样自己打的 INFO。总线消息不进 total_commands_processed。",
		ElapsedSec: 4,
	}
	var sumCPU float64
	var sumCmd, sumBus int64
	nodes := make([]map[string]any, 0, len(w.nodes))
	for i, n := range w.nodes {
		ci, _ := w.clusterInfo(n)
		bus := int64(0)
		if ci != nil {
			bus, _ = strconvParse(ci["cluster_stats_messages_sent"])
		}
		cmd := w.commands(n) - before[i].cmd
		cpu := cpuPercent(before[i].cpu, cpuTicks(n.Pid), 4)
		sumCPU += cpu
		sumCmd += cmd
		sumBus += bus - before[i].bus
		nodes = append(nodes, map[string]any{
			"addr": n.Addr, "cpu": round1(cpu), "commands": cmd, "busSent": bus - before[i].bus,
		})
	}
	res.RedisCPUPercent = sumCPU
	res.RedisCommands = sumCmd
	res.put("nodes", nodes)
	res.put("busSent", sumBus)
	res.Detail = fmt.Sprintf("cmd=%d busSent=%d cpuSum=%.1f", sumCmd, sumBus, sumCPU)
	res.Pass = sumCmd >= 0 && len(nodes) == 6
	return res, nil
}

func closeConns(cs []*Conn) {
	for _, c := range cs {
		if c != nil {
			c.Close()
		}
	}
}

func dialWorkers(addr string, n int) ([]*Conn, error) {
	out := make([]*Conn, n)
	for i := 0; i < n; i++ {
		c, err := dial(addr, 2*time.Second)
		if err != nil {
			closeConns(out)
			return nil, err
		}
		out[i] = c
	}
	return out, nil
}

func pipelineSet(c *Conn, pairs [][2]string) error {
	const batch = 100
	for i := 0; i < len(pairs); i += batch {
		j := i + batch
		if j > len(pairs) {
			j = len(pairs)
		}
		for _, p := range pairs[i:j] {
			if err := c.writeQueued("SET", p[0], p[1]); err != nil {
				return err
			}
		}
		if err := c.flush(); err != nil {
			return err
		}
		for n := i; n < j; n++ {
			v, err := readVal(c.r)
			if err != nil {
				return err
			}
			if v.Err != "" {
				return fmt.Errorf("set %s: %s", pairs[n][0], v.Err)
			}
		}
	}
	return nil
}

func strconvParse(s string) (int64, error) {
	var n int64
	_, err := fmt.Sscan(strings.TrimSpace(s), &n)
	return n, err
}

func (rn *runner) scenarioRoute() (result, error) {
	w := rn.w
	p, err := w.onePair("route")
	if err != nil {
		return result{}, err
	}
	if _, err := p.master.do("SET", p.key, "v1"); err != nil {
		return result{}, err
	}
	if err := w.waitCaughtUp(p.master, 3*time.Second); err != nil {
		return result{}, err
	}
	other := w.otherMaster(p.master)
	okey, _, err := w.keyFor("routeOther", other)
	if err != nil {
		return result{}, err
	}
	if _, err := other.do("SET", okey, "o"); err != nil {
		return result{}, err
	}
	c, err := dial(p.replica.Addr, 2*time.Second)
	if err != nil {
		return result{}, err
	}
	defer c.Close()
	get1, err := c.call("GET", p.key)
	if err != nil {
		return result{}, err
	}
	set1, err := c.call("SET", p.key, "nope")
	if err != nil {
		return result{}, err
	}
	ro, err := c.call("READONLY")
	if err != nil {
		return result{}, err
	}
	get2, err := c.call("GET", p.key)
	if err != nil {
		return result{}, err
	}
	getOther, err := c.call("GET", okey)
	if err != nil {
		return result{}, err
	}
	rw, err := c.call("READWRITE")
	if err != nil {
		return result{}, err
	}
	get3, err := c.call("GET", p.key)
	if err != nil {
		return result{}, err
	}
	still, err := p.master.call("GET", p.key)
	if err != nil {
		return result{}, err
	}
	res := result{Scenario: "route", Note: "副本不执行 READONLY 时，读和写都回到主库。READONLY 只服务本主库的槽。READWRITE 之后又回到 MOVED。"}
	res.put("replica", p.replica.Addr)
	res.put("master", p.master.Addr)
	res.put("other", other.Addr)
	res.put("getWithoutReadonly", get1.Err)
	res.put("setOnReplica", set1.Err)
	res.put("readonly", replyText(ro))
	res.put("getAfterReadonly", replyText(get2))
	res.put("getOtherSlot", getOther.Err)
	res.put("readwrite", replyText(rw))
	res.put("getAfterReadwrite", get3.Err)
	res.put("masterStill", replyText(still))
	res.Detail = fmt.Sprintf("get=%s set=%s after=%s other=%s back=%s", get1.Err, set1.Err, replyText(get2), getOther.Err, get3.Err)
	res.Pass = strings.HasPrefix(get1.Err, "MOVED") &&
		strings.Contains(get1.Err, p.master.Addr) &&
		strings.HasPrefix(set1.Err, "MOVED") &&
		strings.Contains(set1.Err, p.master.Addr) &&
		replyText(ro) == "OK" &&
		replyText(get2) == "v1" &&
		strings.HasPrefix(getOther.Err, "MOVED") &&
		strings.Contains(getOther.Err, other.Addr) &&
		replyText(rw) == "OK" &&
		strings.HasPrefix(get3.Err, "MOVED") &&
		replyText(still) == "v1"
	return res, nil
}

func (rn *runner) scenarioOnMaster() (result, error) {
	w := rn.w
	if err := w.refresh(); err != nil {
		return result{}, err
	}
	m := w.masters()[0]
	before, err := dialRole(m.Addr)
	if err != nil {
		return result{}, err
	}
	v, err := m.call("CLUSTER", "FAILOVER")
	if err != nil {
		return result{}, err
	}
	after, err := dialRole(m.Addr)
	if err != nil {
		return result{}, err
	}
	res := result{Scenario: "failover-on-master", Note: "CLUSTER FAILOVER 打到主库上会被拒绝，角色不变。"}
	res.put("before", before)
	res.put("reply", v.Err)
	res.put("after", after)
	res.Detail = fmt.Sprintf("%s role %s -> %s", v.Err, before, after)
	res.Pass = strings.Contains(v.Err, "You should send CLUSTER FAILOVER to a replica") && before == "master" && after == "master"
	return res, nil
}

func (rn *runner) benchAddr(name, note, addr string, workers int, setup func(*Conn) error, op func(*Conn) error, watch []*node) (result, error) {
	conns := make([]*Conn, workers)
	for i := 0; i < workers; i++ {
		c, err := dial(addr, 2*time.Second)
		if err != nil {
			closeConns(conns)
			return result{}, err
		}
		if setup != nil {
			if err := setup(c); err != nil {
				c.Close()
				closeConns(conns)
				return result{}, err
			}
		}
		conns[i] = c
	}
	defer closeConns(conns)
	warmEnd := time.Now().Add(time.Second)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(c *Conn) {
			defer wg.Done()
			for time.Now().Before(warmEnd) {
				_ = op(c)
			}
		}(conns[i])
	}
	wg.Wait()
	for _, s := range watch {
		_ = s.refreshMeta()
	}
	beforeCmd := make([]int64, len(watch))
	cpu0 := make([]float64, len(watch))
	for i, s := range watch {
		beforeCmd[i] = rn.w.commands(s)
		cpu0[i] = cpuTicks(s.Pid)
	}
	client0 := cpuTicks(os.Getpid())
	hists := make([]*hist, workers)
	errN := make([]int64, workers)
	opsN := make([]int64, workers)
	start := time.Now()
	end := start.Add(4 * time.Second)
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		hists[i] = newHist()
		go func(i int) {
			defer wg.Done()
			c := conns[i]
			h := hists[i]
			for time.Now().Before(end) {
				t0 := time.Now()
				err := op(c)
				h.add(time.Since(t0).Nanoseconds())
				opsN[i]++
				if err != nil {
					errN[i]++
				}
			}
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	client1 := cpuTicks(os.Getpid())
	merged := newHist()
	res := result{Scenario: name, Note: note, Workers: workers, ElapsedSec: elapsed}
	for i := range workers {
		res.Ops += opsN[i]
		res.Errors += errN[i]
		merged.merge(hists[i])
	}
	var redisCPU float64
	res.Nodes = make([]nodeStat, len(watch))
	for i, s := range watch {
		st := rn.fill(s)
		st.Commands = rn.w.commands(s) - beforeCmd[i]
		st.CPUPercent = cpuPercent(cpu0[i], cpuTicks(s.Pid), elapsed)
		res.Nodes[i] = st
		res.RedisCommands += st.Commands
		redisCPU += st.CPUPercent
	}
	res.RedisCPUPercent = redisCPU
	res.ClientCPUPercent = cpuPercent(client0, client1, elapsed)
	if elapsed > 0 {
		res.QPS = float64(res.Ops) / elapsed
	}
	res.P50us = merged.le(0.50) / 1000
	res.P99us = merged.le(0.99) / 1000
	res.P999us = merged.le(0.999) / 1000
	res.Maxus = merged.max / 1000
	res.Pass = res.Errors == 0 && res.Ops > 0
	res.Detail = fmt.Sprintf("qps=%.0f err=%d p99us=%d", res.QPS, res.Errors, res.P99us)
	return res, nil
}

func (rn *runner) hotPair() (pair, error) {
	return rn.w.onePair("hot")
}

func (rn *runner) scenarioIncr(workers int) (result, error) {
	p, err := rn.hotPair()
	if err != nil {
		return result{}, err
	}
	if _, err := p.master.do("SET", p.key, "0"); err != nil {
		return result{}, err
	}
	name := fmt.Sprintf("incr-w%d", workers)
	res, err := rn.benchAddr(name,
		"热 key 的 INCR 打在主库上。副本在复制这条流，不接这个命令。",
		p.master.Addr, workers, nil,
		func(c *Conn) error {
			v, err := c.call("INCR", p.key)
			if err != nil {
				return err
			}
			if v.Err != "" {
				return fmt.Errorf("%s", v.Err)
			}
			return nil
		},
		[]*node{p.master, p.replica},
	)
	if err != nil {
		return res, err
	}
	res.put("master", p.master.Addr)
	res.put("replica", p.replica.Addr)
	if len(res.Nodes) == 2 {
		res.put("masterCpu", round1(res.Nodes[0].CPUPercent))
		res.put("replicaCpu", round1(res.Nodes[1].CPUPercent))
	}
	return res, nil
}

func (rn *runner) scenarioReplicaGet(workers int) (result, error) {
	p, err := rn.hotPair()
	if err != nil {
		return result{}, err
	}
	if _, err := p.master.do("SET", p.key, "v"); err != nil {
		return result{}, err
	}
	if err := rn.w.waitCaughtUp(p.master, 3*time.Second); err != nil {
		return result{}, err
	}
	name := fmt.Sprintf("replica-get-w%d", workers)
	res, err := rn.benchAddr(name,
		"READONLY 之后读副本上的同一个热 key。主库这一段不接 GET。",
		p.replica.Addr, workers,
		func(c *Conn) error {
			v, err := c.call("READONLY")
			if err != nil {
				return err
			}
			if v.Err != "" {
				return fmt.Errorf("%s", v.Err)
			}
			return nil
		},
		func(c *Conn) error {
			v, err := c.call("GET", p.key)
			if err != nil {
				return err
			}
			if v.Err != "" || v.Str != "v" {
				return fmt.Errorf("get %s", replyText(v))
			}
			return nil
		},
		[]*node{p.replica, p.master},
	)
	if err != nil {
		return res, err
	}
	res.put("replica", p.replica.Addr)
	res.put("master", p.master.Addr)
	if len(res.Nodes) == 2 {
		res.put("replicaCpu", round1(res.Nodes[0].CPUPercent))
		res.put("masterCpu", round1(res.Nodes[1].CPUPercent))
	}
	return res, nil
}

func (rn *runner) scenarioBoth() (result, error) {
	p, err := rn.hotPair()
	if err != nil {
		return result{}, err
	}
	if _, err := p.master.do("SET", p.key, "0"); err != nil {
		return result{}, err
	}
	const n = 16
	writers, err := dialWorkers(p.master.Addr, n)
	if err != nil {
		return result{}, err
	}
	defer closeConns(writers)
	readers, err := dialWorkers(p.replica.Addr, n)
	if err != nil {
		return result{}, err
	}
	defer closeConns(readers)
	for _, c := range readers {
		v, err := c.call("READONLY")
		if err != nil {
			return result{}, err
		}
		if v.Err != "" {
			return result{}, fmt.Errorf("%s", v.Err)
		}
	}
	warmEnd := time.Now().Add(time.Second)
	var wg sync.WaitGroup
	wg.Add(n * 2)
	for i := 0; i < n; i++ {
		go func(c *Conn) {
			defer wg.Done()
			for time.Now().Before(warmEnd) {
				_, _ = c.call("INCR", p.key)
			}
		}(writers[i])
		go func(c *Conn) {
			defer wg.Done()
			for time.Now().Before(warmEnd) {
				_, _ = c.call("GET", p.key)
			}
		}(readers[i])
	}
	wg.Wait()
	for _, s := range []*node{p.master, p.replica} {
		_ = s.refreshMeta()
	}
	c0 := []float64{cpuTicks(p.master.Pid), cpuTicks(p.replica.Pid)}
	cmd0 := []int64{rn.w.commands(p.master), rn.w.commands(p.replica)}
	client0 := cpuTicks(os.Getpid())
	var incrOps, incrErr, getOps, getErr atomic.Int64
	wh, rh := newHist(), newHist()
	var mu sync.Mutex
	start := time.Now()
	end := start.Add(4 * time.Second)
	wg.Add(n * 2)
	for i := 0; i < n; i++ {
		go func(c *Conn) {
			defer wg.Done()
			h := newHist()
			for time.Now().Before(end) {
				t0 := time.Now()
				v, err := c.call("INCR", p.key)
				h.add(time.Since(t0).Nanoseconds())
				if err != nil || v.Err != "" {
					incrErr.Add(1)
				} else {
					incrOps.Add(1)
				}
			}
			mu.Lock()
			wh.merge(h)
			mu.Unlock()
		}(writers[i])
		go func(c *Conn) {
			defer wg.Done()
			h := newHist()
			for time.Now().Before(end) {
				t0 := time.Now()
				v, err := c.call("GET", p.key)
				h.add(time.Since(t0).Nanoseconds())
				if err != nil || v.Err != "" {
					getErr.Add(1)
				} else {
					getOps.Add(1)
				}
			}
			mu.Lock()
			rh.merge(h)
			mu.Unlock()
		}(readers[i])
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	res := result{
		Scenario: "incr-and-replica-get",
		Note:     "同一个热 key，16 条连接 INCR 主库，同时 16 条连接 READONLY GET 副本。4 个核是同一台机器。",
		Workers:  32, ElapsedSec: elapsed,
	}
	res.Ops = incrOps.Load() + getOps.Load()
	res.Errors = incrErr.Load() + getErr.Load()
	if elapsed > 0 {
		res.QPS = float64(res.Ops) / elapsed
	}
	res.ClientCPUPercent = cpuPercent(client0, cpuTicks(os.Getpid()), elapsed)
	mst := rn.fill(p.master)
	mst.CPUPercent = cpuPercent(c0[0], cpuTicks(p.master.Pid), elapsed)
	mst.Commands = rn.w.commands(p.master) - cmd0[0]
	rst := rn.fill(p.replica)
	rst.CPUPercent = cpuPercent(c0[1], cpuTicks(p.replica.Pid), elapsed)
	rst.Commands = rn.w.commands(p.replica) - cmd0[1]
	res.Nodes = []nodeStat{mst, rst}
	res.RedisCPUPercent = mst.CPUPercent + rst.CPUPercent
	res.RedisCommands = mst.Commands + rst.Commands
	res.put("incrOps", incrOps.Load())
	res.put("incrErr", incrErr.Load())
	res.put("incrQPS", float64(incrOps.Load())/elapsed)
	res.put("incrP99us", wh.le(0.99)/1000)
	res.put("incrMaxus", wh.max/1000)
	res.put("getOps", getOps.Load())
	res.put("getErr", getErr.Load())
	res.put("getQPS", float64(getOps.Load())/elapsed)
	res.put("getP99us", rh.le(0.99)/1000)
	res.put("getMaxus", rh.max/1000)
	res.put("masterCpu", round1(mst.CPUPercent))
	res.put("replicaCpu", round1(rst.CPUPercent))
	res.Detail = fmt.Sprintf("incr=%.0f get=%.0f masterCpu=%.1f replicaCpu=%.1f", float64(incrOps.Load())/elapsed, float64(getOps.Load())/elapsed, mst.CPUPercent, rst.CPUPercent)
	res.Pass = incrErr.Load() == 0 && getErr.Load() == 0 && incrOps.Load() > 0 && getOps.Load() > 0
	return res, nil
}

func (rn *runner) scenarioMemory() (result, error) {
	w := rn.w
	p, err := w.onePair("mem")
	if err != nil {
		return result{}, err
	}
	if _, err := p.master.do("FLUSHALL"); err != nil {
		return result{}, err
	}
	// 别的主库上的键不在这次的账里。只清这一对会把整个节点清掉，副本跟着复制。
	time.Sleep(100 * time.Millisecond)
	if err := w.waitCaughtUp(p.master, 3*time.Second); err != nil {
		return result{}, err
	}
	beforeM := w.used(p.master)
	beforeR := w.used(p.replica)
	const n = 20000
	val := strings.Repeat("x", 200)
	c, err := dial(p.master.Addr, 2*time.Second)
	if err != nil {
		return result{}, err
	}
	defer c.Close()
	tag := strings.TrimPrefix(p.key, "mem")
	// p.key 是 mem{tN}。同一对花括号里的键在同一个槽。
	body := p.key[len("mem"):]
	pairs := make([][2]string, n)
	for i := 0; i < n; i++ {
		pairs[i] = [2]string{fmt.Sprintf("k%d%s", i, body), val}
	}
	_ = tag
	if err := pipelineSet(c, pairs); err != nil {
		return result{}, err
	}
	if err := w.waitCaughtUp(p.master, 5*time.Second); err != nil {
		return result{}, err
	}
	afterM := w.used(p.master)
	afterR := w.used(p.replica)
	usage, err := p.master.call("MEMORY", "USAGE", pairs[0][0])
	if err != nil {
		return result{}, err
	}
	info, err := p.master.call("INFO", "replication")
	if err != nil {
		return result{}, err
	}
	backlog := infoInt(info.Str, "repl_backlog_size")
	hist := infoInt(info.Str, "repl_backlog_histlen")
	first := infoInt(info.Str, "repl_backlog_first_byte_offset")
	_ = first
	res := result{Scenario: "memory", Note: "2 万个 200 字节的值，用同一个 hash tag 放在一个主库上。副本是另一份。repl-backlog 是额外的环形缓冲，不摊进每个键。"}
	dm := afterM - beforeM
	dr := afterR - beforeR
	res.put("keys", n)
	res.put("valueBytes", 200)
	res.put("masterBefore", beforeM)
	res.put("masterAfter", afterM)
	res.put("masterDelta", dm)
	res.put("masterPerKey", float64(dm)/float64(n))
	res.put("replicaBefore", beforeR)
	res.put("replicaAfter", afterR)
	res.put("replicaDelta", dr)
	res.put("replicaPerKey", float64(dr)/float64(n))
	res.put("memoryUsage", usage.Int)
	res.put("replBacklogSize", backlog)
	res.put("replBacklogHistlen", hist)
	res.put("master", p.master.Addr)
	res.put("replica", p.replica.Addr)
	res.Detail = fmt.Sprintf("masterPer=%.2f replicaPer=%.2f usage=%d backlog=%d", float64(dm)/float64(n), float64(dr)/float64(n), usage.Int, backlog)
	res.Pass = dm > 0 && dr > 0 && usage.Int > 200
	for _, m := range w.masters() {
		_, _ = m.call("FLUSHALL")
	}
	return res, nil
}
