package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func (rn *runner) scenarioReady() (result, error) {
	cl := rn.cl
	if err := cl.refresh(); err != nil {
		return result{}, err
	}
	res := result{
		Scenario: "ready",
		Note:     "3 个主库、没有副本。16384 个槽要正好分完。独立的 7104 不开 cluster。",
	}
	sum := 0
	minSlots, maxSlots := 1<<30, 0
	ids := map[string]bool{}
	for _, s := range cl.masters {
		st := rn.stat(s)
		info, err := s.info("server")
		if err != nil {
			return result{}, err
		}
		clInfo, err := s.info("cluster")
		if err != nil {
			return result{}, err
		}
		mem, err := s.info("memory")
		if err != nil {
			return result{}, err
		}
		ci, err := cl.clusterInfo(s)
		if err != nil {
			return result{}, err
		}
		res.Nodes = append(res.Nodes, st)
		sum += s.Slots
		if s.Slots < minSlots {
			minSlots = s.Slots
		}
		if s.Slots > maxSlots {
			maxSlots = s.Slots
		}
		ids[s.ID] = true
		res.put("redisVersion", infoValue(info, "redis_version"))
		res.put("clusterEnabled", infoValue(clInfo, "cluster_enabled"))
		res.put("allocator", infoValue(mem, "mem_allocator"))
		res.put("state:"+s.Addr, ci["cluster_state"])
		res.put("coverage:"+s.Addr, ci["cluster_require_full_coverage"]) // may be absent
		cfg, err := s.do("CONFIG", "GET", "cluster-require-full-coverage")
		if err == nil && len(cfg.Elems) == 2 {
			res.put("coverageCfg:"+strconv.Itoa(s.Port), cfg.Elems[1].Str)
		}
		to, err := s.do("CONFIG", "GET", "cluster-node-timeout")
		if err == nil && len(to.Elems) == 2 {
			res.put("nodeTimeoutMs", to.Elems[1].Str)
		}
		mm, err := s.do("CONFIG", "GET", "maxmemory")
		if err == nil && len(mm.Elems) == 2 {
			res.put("maxmemory:"+strconv.Itoa(s.Port), mm.Elems[1].Str)
		}
	}
	alone := rn.stat(cl.alone)
	res.Nodes = append(res.Nodes, alone)
	ainfo, err := cl.alone.info("cluster")
	if err != nil {
		return result{}, err
	}
	res.put("standaloneClusterEnabled", infoValue(ainfo, "cluster_enabled"))
	res.put("slotSum", sum)
	res.put("minSlots", minSlots)
	res.put("maxSlots", maxSlots)
	res.put("masters", len(cl.masters))
	missing := 0
	for _, own := range cl.owner {
		if own == nil {
			missing++
		}
	}
	res.put("missingSlots", missing)
	enabled := ""
	if v, ok := res.Extra["clusterEnabled"].(string); ok {
		enabled = v
	}
	aloneEnabled := infoValue(ainfo, "cluster_enabled")
	res.Detail = fmt.Sprintf("slots=%d min=%d max=%d missing=%d masters=%d cluster=%s alone=%s", sum, minSlots, maxSlots, missing, len(ids), enabled, aloneEnabled)
	res.Pass = sum == 16384 && missing == 0 && maxSlots-minSlots <= 1 && len(ids) == 3 &&
		enabled == "1" && aloneEnabled == "0" && res.Extra["state:127.0.0.1:7101"] == "ok"
	return res, nil
}

func (rn *runner) scenarioHash() (result, error) {
	cl := rn.cl
	if err := cl.refresh(); err != nil {
		return result{}, err
	}
	s := cl.masters[0]
	res := result{Scenario: "hash", Note: "本地 CRC16 和 CLUSTER KEYSLOT 对照。hash tag 只取第一对非空花括号。"}
	mismatch := 0
	var samples []string
	for i := 0; i < 500; i++ {
		k := fmt.Sprintf("u:%d", i*17+3)
		v, err := s.do("CLUSTER", "KEYSLOT", k)
		if err != nil {
			return result{}, err
		}
		if int(v.Int) != slotOf(k) {
			mismatch++
			if len(samples) < 5 {
				samples = append(samples, fmt.Sprintf("%s local=%d redis=%d", k, slotOf(k), v.Int))
			}
		}
	}
	tags := []string{"foo", "foo{bar}", "foo{bar}baz", "foo{}bar", "{bar", "foo{bar", "{bar}baz", "a{b}c{d}"}
	tagRows := make([]map[string]any, 0, len(tags))
	tagBad := 0
	for _, k := range tags {
		v, err := s.do("CLUSTER", "KEYSLOT", k)
		if err != nil {
			return result{}, err
		}
		local := slotOf(k)
		row := map[string]any{"key": k, "local": local, "redis": v.Int, "match": int(v.Int) == local}
		tagRows = append(tagRows, row)
		if int(v.Int) != local {
			tagBad++
		}
	}
	const n = 100000
	perNode := map[string]int{}
	perSlotMax := 0
	slotCount := make([]int, 16384)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("u:%d", i)
		slot := slotOf(k)
		slotCount[slot]++
		if own := cl.ownerOf(slot); own != nil {
			perNode[own.Addr]++
		}
	}
	used := 0
	for _, c := range slotCount {
		if c > perSlotMax {
			perSlotMax = c
		}
		if c > 0 {
			used++
		}
	}
	res.put("checked", 500)
	res.put("mismatch", mismatch)
	res.put("mismatchSamples", samples)
	res.put("tags", tagRows)
	res.put("tagBad", tagBad)
	res.put("spreadKeys", n)
	res.put("spreadSlotsUsed", used)
	res.put("spreadMaxInOneSlot", perSlotMax)
	res.put("spreadPerNode", perNode)
	res.Detail = fmt.Sprintf("mismatch=%d tagBad=%d maxInOneSlot=%d", mismatch, tagBad, perSlotMax)
	res.Pass = mismatch == 0 && tagBad == 0
	return res, nil
}

func (rn *runner) scenarioCrossslot() (result, error) {
	cl := rn.cl
	if err := cl.refresh(); err != nil {
		return result{}, err
	}
	res := result{Scenario: "crossslot", Note: "没有同一个 hash tag 时，库存和已购集合不能放进同一次 EVAL。带 tag 的 8 个连接把 100 件卖给 100 个人。"}
	stockKey := "plain:stock"
	winKey := "plain:win"
	if slotOf(stockKey) == slotOf(winKey) {
		winKey = "plain:win:other"
	}
	if slotOf(stockKey) == slotOf(winKey) {
		return result{}, fmt.Errorf("could not find two keys in different slots")
	}
	own := cl.ownerOf(slotOf(stockKey))
	if _, err := own.do("SET", stockKey, "5"); err != nil {
		return result{}, err
	}
	v, err := own.call("EVAL", rn.lua, "2", stockKey, winKey, "u0")
	if err != nil {
		return result{}, err
	}
	cross := replyText(v)
	left, err := own.do("GET", stockKey)
	if err != nil {
		return result{}, err
	}
	mget, err := own.call("MGET", stockKey, winKey)
	mgetText := ""
	if err != nil {
		mgetText = err.Error()
	} else {
		mgetText = replyText(mget)
	}

	tag, slot, err := cl.tagOn(cl.masters[0].Addr)
	if err != nil {
		return result{}, err
	}
	tStock := "ms{" + tag + "}:stock"
	tWin := "ms{" + tag + "}:win"
	owner := cl.ownerOf(slot)
	if _, err := owner.do("SET", tStock, "100"); err != nil {
		return result{}, err
	}
	if _, err := owner.do("DEL", tWin); err != nil {
		return result{}, err
	}
	const users = 800
	const workers = 8
	conns, err := dialWorkers(owner.Addr, workers)
	if err != nil {
		return result{}, err
	}
	defer closeConns(conns)
	var okN, soldN, dupN, errN atomic.Int64
	var wg sync.WaitGroup
	var errText string
	var errOnce sync.Once
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			defer wg.Done()
			for u := w; u < users; u += workers {
				v, err := conns[w].call("EVAL", rn.lua, "2", tStock, tWin, strconv.Itoa(u))
				if err != nil || v.Err != "" {
					errN.Add(1)
					errOnce.Do(func() {
						if err != nil {
							errText = err.Error()
						} else {
							errText = v.Err
						}
					})
					continue
				}
				switch v.Int {
				case 1:
					okN.Add(1)
				case 0:
					soldN.Add(1)
				case 2:
					dupN.Add(1)
				default:
					errN.Add(1)
				}
			}
		}(w)
	}
	wg.Wait()
	final, err := owner.do("GET", tStock)
	if err != nil {
		return result{}, err
	}
	card, err := owner.do("SCARD", tWin)
	if err != nil {
		return result{}, err
	}
	res.put("plainStockSlot", slotOf(stockKey))
	res.put("plainWinSlot", slotOf(winKey))
	res.put("crossslot", cross)
	res.put("stockAfterCrossslot", replyText(left))
	res.put("mget", mgetText)
	res.put("tag", tag)
	res.put("tagSlot", slot)
	res.put("taggedSuccess", okN.Load())
	res.put("taggedSoldOut", soldN.Load())
	res.put("taggedDup", dupN.Load())
	res.put("taggedErrors", errN.Load())
	res.put("taggedError", errText)
	res.put("taggedStock", replyText(final))
	res.put("taggedWinners", card.Int)
	res.Detail = fmt.Sprintf("cross=%s stockLeft=%s tagged ok=%d sold=%d dup=%d err=%d stock=%s winners=%d", cross, replyText(left), okN.Load(), soldN.Load(), dupN.Load(), errN.Load(), replyText(final), card.Int)
	res.Pass = strings.Contains(cross, "CROSSSLOT") && replyText(left) == "5" &&
		strings.Contains(mgetText, "CROSSSLOT") &&
		okN.Load() == 100 && soldN.Load() == 700 && dupN.Load() == 0 && errN.Load() == 0 && replyText(final) == "0" && card.Int == 100
	_, _ = owner.call("DEL", stockKey, winKey)
	_, _ = owner.call("DEL", tStock, tWin)
	// plain win key may live on another node; deleting from owner returns MOVED and does not delete it.
	if other := cl.ownerOf(slotOf(winKey)); other != nil {
		_, _ = other.call("DEL", winKey)
	}
	return res, nil
}

func (rn *runner) scenarioMoved() (result, error) {
	cl := rn.cl
	if err := cl.refresh(); err != nil {
		return result{}, err
	}
	key, slot, err := cl.keyOn("moved", cl.masters[0].Addr)
	if err != nil {
		return result{}, err
	}
	src := cl.ownerOf(slot)
	dst := cl.otherThan(src)
	if _, err := src.do("SET", key, "v"); err != nil {
		return result{}, err
	}
	wrong, err := dst.call("GET", key)
	if err != nil {
		return result{}, err
	}
	kind, gotSlot, addr, ok := parseRedirect(wrong.Err)
	c, err := dial(dst.Addr, time.Second)
	if err != nil {
		return result{}, err
	}
	defer c.Close()
	direct := 0
	moved := 0
	for i := 0; i < 20; i++ {
		v, err := c.call("GET", key)
		if err != nil {
			return result{}, err
		}
		if strings.HasPrefix(v.Err, "MOVED") {
			moved++
		} else if v.Err == "" && v.Str == "v" {
			direct++
		}
	}
	// 缓存槽位：第一次走错节点，之后连到回复里的地址。
	followMoved := 0
	followOK := 0
	var cached *Conn
	defer func() {
		if cached != nil {
			cached.Close()
		}
	}()
	for i := 0; i < 20; i++ {
		var v Val
		var err error
		if cached == nil {
			v, err = c.call("GET", key)
		} else {
			v, err = cached.call("GET", key)
		}
		if err != nil {
			return result{}, err
		}
		if strings.HasPrefix(v.Err, "MOVED") {
			followMoved++
			_, _, a, ok := parseRedirect(v.Err)
			if !ok {
				break
			}
			if cached != nil {
				cached.Close()
			}
			cached, err = dial(a, time.Second)
			if err != nil {
				return result{}, err
			}
			v, err = cached.call("GET", key)
			if err != nil {
				return result{}, err
			}
		}
		if v.Err == "" && v.Str == "v" {
			followOK++
		}
	}
	res := result{Scenario: "moved", Note: "槽不在这个节点上时，每次都是 MOVED，不会因为多问几次就改口。缓存地址之后，后面的 GET 不再经过错的节点。"}
	res.put("key", key)
	res.put("slot", slot)
	res.put("owner", src.Addr)
	res.put("wrong", dst.Addr)
	res.put("first", wrong.Err)
	res.put("kind", kind)
	res.put("replySlot", gotSlot)
	res.put("replyAddr", addr)
	res.put("stickyMoved", moved)
	res.put("stickyDirect", direct)
	res.put("followMoved", followMoved)
	res.put("followOK", followOK)
	res.Detail = fmt.Sprintf("first=%s stickyMoved=%d followMoved=%d followOK=%d", wrong.Err, moved, followMoved, followOK)
	res.Pass = ok && kind == "MOVED" && gotSlot == slot && addr == src.Addr && moved == 20 && direct == 0 && followMoved == 1 && followOK == 20
	_, _ = src.call("DEL", key)
	return res, nil
}

func (rn *runner) scenarioIdle() (result, error) {
	cl := rn.cl
	before := make([]int64, len(cl.masters))
	sent0 := make([]int64, len(cl.masters))
	recv0 := make([]int64, len(cl.masters))
	cpu0 := make([]float64, len(cl.masters))
	for i, s := range cl.masters {
		if err := s.refreshMeta(); err != nil {
			return result{}, err
		}
		info, err := s.info("stats")
		if err != nil {
			return result{}, err
		}
		before[i] = infoInt(info, "total_commands_processed")
		ci, err := cl.clusterInfo(s)
		if err != nil {
			return result{}, err
		}
		sent0[i], _ = strconv.ParseInt(ci["cluster_stats_messages_sent"], 10, 64)
		recv0[i], _ = strconv.ParseInt(ci["cluster_stats_messages_received"], 10, 64)
		cpu0[i] = cpuTicks(s.Pid)
	}
	const sec = 4.0
	time.Sleep(time.Duration(sec * float64(time.Second)))
	res := result{Scenario: "idle", Note: "空闲 4 秒。命令增量是这一次采样用的 CLUSTER INFO，总线消息是节点之间的 gossip，不是库存流量。", ElapsedSec: sec}
	var cmd, sent, recv int64
	var cpu float64
	for i, s := range cl.masters {
		info, err := s.info("stats")
		if err != nil {
			return result{}, err
		}
		ci, err := cl.clusterInfo(s)
		if err != nil {
			return result{}, err
		}
		st := rn.stat(s)
		st.Commands = infoInt(info, "total_commands_processed") - before[i]
		// 采样本身会打进命令数。下面减掉这次 INFO 和 CLUSTER INFO。
		st.CPUPercent = cpuPercent(cpu0[i], cpuTicks(s.Pid), sec)
		sent1, _ := strconv.ParseInt(ci["cluster_stats_messages_sent"], 10, 64)
		recv1, _ := strconv.ParseInt(ci["cluster_stats_messages_received"], 10, 64)
		res.put("sent:"+strconv.Itoa(s.Port), sent1-sent0[i])
		res.put("recv:"+strconv.Itoa(s.Port), recv1-recv0[i])
		res.Nodes = append(res.Nodes, st)
		cmd += st.Commands
		sent += sent1 - sent0[i]
		recv += recv1 - recv0[i]
		cpu += st.CPUPercent
	}
	res.RedisCommands = cmd
	res.RedisCPUPercent = cpu
	res.put("busSent", sent)
	res.put("busRecv", recv)
	state, err := cl.stateOf(cl.masters[0])
	if err != nil {
		return result{}, err
	}
	res.put("state", state)
	res.Detail = fmt.Sprintf("commands=%d busSent=%d busRecv=%d cpuSum=%.1f state=%s", cmd, sent, recv, cpu, state)
	res.Pass = state == "ok" && sent > 0 && recv > 0
	return res, nil
}

func (rn *runner) benchGet(name, note, addr, key string, workers int, watch []*server, nodeIndex func(i int) int, op func(c *Conn) error) (result, error) {
	conns, err := dialWorkers(addr, workers)
	if err != nil {
		return result{}, err
	}
	defer closeConns(conns)
	return rn.bench(name, note, workers, time.Second, 4*time.Second, watch, func(i int) error {
		return op(conns[i])
	}, nodeIndex)
}

func (rn *runner) scenarioHotGet(workers int) (result, error) {
	cl := rn.cl
	if err := cl.refresh(); err != nil {
		return result{}, err
	}
	key := "hot:get"
	slot := slotOf(key)
	own := cl.ownerOf(slot)
	if own == nil {
		return result{}, fmt.Errorf("no owner for %s", key)
	}
	if _, err := own.do("SET", key, "1"); err != nil {
		return result{}, err
	}
	name := fmt.Sprintf("hot-get-w%d", workers)
	res, err := rn.benchGet(name, "同一个键，连接直接打到槽所在的主库。另外两个主库不应该吃到这些 GET。", own.Addr, key, workers, cl.masters, func(i int) int {
		for n, s := range cl.masters {
			if s.Addr == own.Addr {
				return n
			}
		}
		return -1
	}, func(c *Conn) error {
		v, err := c.call("GET", key)
		if err != nil {
			return err
		}
		if v.Err != "" || v.Str != "1" {
			return fmt.Errorf("%s", replyText(v))
		}
		return nil
	})
	if err != nil {
		return res, err
	}
	res.put("key", key)
	res.put("slot", slot)
	res.put("owner", own.Addr)
	return res, nil
}

func (rn *runner) scenarioStandaloneGet() (result, error) {
	cl := rn.cl
	key := "hot:get"
	if _, err := cl.alone.do("SET", key, "1"); err != nil {
		return result{}, err
	}
	return rn.benchGet("standalone-get-w16", "同一个 GET，打到没开 cluster 的 7104。用来看直连时 cluster 模式本身贵多少。", cl.alone.Addr, key, 16, []*server{cl.alone}, func(i int) int { return 0 }, func(c *Conn) error {
		v, err := c.call("GET", key)
		if err != nil {
			return err
		}
		if v.Err != "" || v.Str != "1" {
			return fmt.Errorf("%s", replyText(v))
		}
		return nil
	})
}

func (rn *runner) scenarioHotIncr() (result, error) {
	cl := rn.cl
	if err := cl.refresh(); err != nil {
		return result{}, err
	}
	key := "hot:incr"
	own := cl.ownerOf(slotOf(key))
	if _, err := own.do("SET", key, "0"); err != nil {
		return result{}, err
	}
	res, err := rn.benchGet("hot-incr-w16", "同一个键的 INCR，仍然只打到一个主库。", own.Addr, key, 16, cl.masters, func(i int) int {
		for n, s := range cl.masters {
			if s.Addr == own.Addr {
				return n
			}
		}
		return -1
	}, func(c *Conn) error {
		v, err := c.call("INCR", key)
		if err != nil {
			return err
		}
		if v.Err != "" || v.Kind != ':' {
			return fmt.Errorf("%s", replyText(v))
		}
		return nil
	})
	if err != nil {
		return res, err
	}
	res.put("owner", own.Addr)
	res.put("slot", slotOf(key))
	return res, nil
}

func (rn *runner) scenarioMovedFollow() (result, error) {
	cl := rn.cl
	if err := cl.refresh(); err != nil {
		return result{}, err
	}
	key := "hot:moved"
	own := cl.ownerOf(slotOf(key))
	wrong := cl.otherThan(own)
	if _, err := own.do("SET", key, "1"); err != nil {
		return result{}, err
	}
	n := 16
	wrongC, err := dialWorkers(wrong.Addr, n)
	if err != nil {
		return result{}, err
	}
	defer closeConns(wrongC)
	rightC, err := dialWorkers(own.Addr, n)
	if err != nil {
		return result{}, err
	}
	defer closeConns(rightC)
	res, err := rn.bench("moved-follow-w16", "每次都先打到没有这个槽的主库，读到 MOVED 再去真正的主库。槽位缓存故意不更新。", n, time.Second, 4*time.Second, cl.masters, func(i int) error {
		v, err := wrongC[i].call("GET", key)
		if err != nil {
			return err
		}
		kind, _, addr, ok := parseRedirect(v.Err)
		if !ok || kind != "MOVED" || addr != own.Addr {
			return fmt.Errorf("redirect %s", replyText(v))
		}
		v, err = rightC[i].call("GET", key)
		if err != nil {
			return err
		}
		if v.Err != "" || v.Str != "1" {
			return fmt.Errorf("get %s", replyText(v))
		}
		return nil
	}, func(i int) int { return -1 })
	if err != nil {
		return res, err
	}
	res.put("owner", own.Addr)
	res.put("wrong", wrong.Addr)
	res.put("slot", slotOf(key))
	return res, nil
}

func (rn *runner) scenarioSpread() (result, error) {
	cl := rn.cl
	if err := cl.refresh(); err != nil {
		return result{}, err
	}
	const per = 16
	const keysPer = 32
	type group struct {
		s     *server
		conns []*Conn
		keys  []string
	}
	var groups []group
	for _, s := range cl.masters {
		var keys []string
		for i := 0; len(keys) < keysPer && i < 500000; i++ {
			k := fmt.Sprintf("spread:%s:%d", strings.ReplaceAll(s.Addr, ":", "_"), i)
			if own := cl.ownerOf(slotOf(k)); own != nil && own.Addr == s.Addr {
				keys = append(keys, k)
			}
		}
		if len(keys) != keysPer {
			return result{}, fmt.Errorf("only %d keys for %s", len(keys), s.Addr)
		}
		pairs := make([][2]string, len(keys))
		for i, k := range keys {
			pairs[i] = [2]string{k, "1"}
		}
		if err := pipelineSet(s.c, pairs); err != nil {
			return result{}, err
		}
		cs, err := dialWorkers(s.Addr, per)
		if err != nil {
			return result{}, err
		}
		groups = append(groups, group{s: s, conns: cs, keys: keys})
	}
	defer func() {
		for _, g := range groups {
			closeConns(g.conns)
		}
	}()
	workers := per * len(groups)
	res, err := rn.bench("spread-get", "三个主库各 16 条连接，各自读自己槽上的键。这不是把一个库存键拆开。", workers, time.Second, 4*time.Second, cl.masters, func(i int) error {
		g := &groups[i/per]
		c := g.conns[i%per]
		k := g.keys[i%len(g.keys)]
		v, err := c.call("GET", k)
		if err != nil {
			return err
		}
		if v.Err != "" || v.Str != "1" {
			return fmt.Errorf("%s", replyText(v))
		}
		return nil
	}, func(i int) int { return i / per })
	if err != nil {
		return res, err
	}
	return res, nil
}

func (rn *runner) scenarioMemory() (result, error) {
	cl := rn.cl
	if err := cl.flushMasters(); err != nil {
		return result{}, err
	}
	if _, err := cl.alone.do("FLUSHALL"); err != nil {
		return result{}, err
	}
	const n = 30000
	val := strings.Repeat("x", 200)
	base := map[string]int64{}
	for _, s := range append([]*server{}, append(cl.masters, cl.alone)...) {
		info, err := s.info("memory")
		if err != nil {
			return result{}, err
		}
		base[s.Addr] = infoInt(info, "used_memory")
	}
	grouped := map[string][][2]string{}
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("mem:%d", i)
		own := cl.ownerOf(slotOf(k))
		grouped[own.Addr] = append(grouped[own.Addr], [2]string{k, val})
	}
	for _, s := range cl.masters {
		if err := pipelineSet(s.c, grouped[s.Addr]); err != nil {
			return result{}, err
		}
	}
	alonePairs := make([][2]string, n)
	for i := 0; i < n; i++ {
		alonePairs[i] = [2]string{fmt.Sprintf("mem:%d", i), val}
	}
	if err := pipelineSet(cl.alone.c, alonePairs); err != nil {
		return result{}, err
	}
	res := result{Scenario: "memory", Note: "3 万个 200 字节的值。used_memory 增量除以键数。Cluster 和没开 cluster 的进程各写一遍。"}
	var clusterDelta int64
	for _, s := range cl.masters {
		info, err := s.info("memory")
		if err != nil {
			return result{}, err
		}
		d := infoInt(info, "used_memory") - base[s.Addr]
		clusterDelta += d
		res.put("delta:"+strconv.Itoa(s.Port), d)
		res.put("keys:"+strconv.Itoa(s.Port), len(grouped[s.Addr]))
	}
	ainfo, err := cl.alone.info("memory")
	if err != nil {
		return result{}, err
	}
	aloneDelta := infoInt(ainfo, "used_memory") - base[cl.alone.Addr]
	sampleKey := "mem:0"
	own := cl.ownerOf(slotOf(sampleKey))
	mu, err := own.do("MEMORY", "USAGE", sampleKey)
	if err != nil {
		return result{}, err
	}
	amu, err := cl.alone.do("MEMORY", "USAGE", sampleKey)
	if err != nil {
		return result{}, err
	}
	res.put("keys", n)
	res.put("valueBytes", 200)
	res.put("clusterUsedDelta", clusterDelta)
	res.put("clusterBytesPerKey", float64(clusterDelta)/float64(n))
	res.put("standaloneUsedDelta", aloneDelta)
	res.put("standaloneBytesPerKey", float64(aloneDelta)/float64(n))
	res.put("clusterMemoryUsage", mu.Int)
	res.put("standaloneMemoryUsage", amu.Int)
	res.Detail = fmt.Sprintf("clusterDelta=%d aloneDelta=%d usage=%d/%d", clusterDelta, aloneDelta, mu.Int, amu.Int)
	res.Pass = clusterDelta > 0 && aloneDelta > 0 && mu.Int > 200 && amu.Int > 200
	if err := cl.flushMasters(); err != nil {
		return res, err
	}
	if _, err := cl.alone.do("FLUSHALL"); err != nil {
		return res, err
	}
	return res, nil
}

func (rn *runner) scenarioAskSteps() (result, error) {
	cl := rn.cl
	if err := cl.refresh(); err != nil {
		return result{}, err
	}
	tag, slot, err := cl.tagOn(cl.masters[0].Addr)
	if err != nil {
		return result{}, err
	}
	src := cl.ownerOf(slot)
	dst := cl.otherThan(src)
	original := src.ID
	key := "demo{" + tag + "}"
	defer func() { _ = cl.resetSlot(slot, original) }()
	steps := []map[string]string{}
	add := func(name, reply string) {
		steps = append(steps, map[string]string{"name": name, "reply": reply})
	}
	must := func(name string, s *server, parts ...string) error {
		v, err := s.call(parts...)
		if err != nil {
			add(name, "io:"+err.Error())
			return err
		}
		add(name, replyText(v))
		return nil
	}
	if _, err := src.do("SET", key, "100"); err != nil {
		return result{}, err
	}
	if err := must("get-src", src, "GET", key); err != nil {
		return result{}, err
	}
	if err := must("get-dst-before", dst, "GET", key); err != nil {
		return result{}, err
	}
	if err := cl.beginMigrate(slot, src, dst); err != nil {
		return result{}, err
	}
	if err := must("get-src-migrating", src, "GET", key); err != nil {
		return result{}, err
	}
	if err := must("get-dst-no-asking", dst, "GET", key); err != nil {
		return result{}, err
	}
	if _, err := dst.do("ASKING"); err != nil {
		return result{}, err
	}
	if err := must("get-dst-asking-before-migrate", dst, "GET", key); err != nil {
		return result{}, err
	}
	mv, err := migrateKeys(src, dst, []string{key}, false)
	if err != nil {
		return result{}, err
	}
	add("migrate", replyText(mv))
	if err := must("get-src-after", src, "GET", key); err != nil {
		return result{}, err
	}
	if err := must("get-dst-after-no-asking", dst, "GET", key); err != nil {
		return result{}, err
	}
	if _, err := dst.do("ASKING"); err != nil {
		return result{}, err
	}
	if _, err := dst.do("PING"); err != nil {
		return result{}, err
	}
	if err := must("get-after-asking-ping", dst, "GET", key); err != nil {
		return result{}, err
	}
	if _, err := dst.do("ASKING"); err != nil {
		return result{}, err
	}
	if err := must("get-dst-asking", dst, "GET", key); err != nil {
		return result{}, err
	}
	n, err := src.countSlot(slot)
	if err != nil {
		return result{}, err
	}
	if err := cl.assignSlot(slot, dst.ID); err != nil {
		return result{}, err
	}
	if err := must("get-old-after-setslot", src, "GET", key); err != nil {
		return result{}, err
	}
	if err := must("get-new-owner", dst, "GET", key); err != nil {
		return result{}, err
	}
	res := result{Scenario: "ask-steps", Note: "迁走之前源节点直接读。迁走之后源节点回 ASK。目标节点没有 ASKING 时回 MOVED。ASKING 只覆盖下一条命令，中间夹一个 PING 就会被用掉。"}
	res.put("slot", slot)
	res.put("tag", tag)
	res.put("source", src.Addr)
	res.put("target", dst.Addr)
	res.put("steps", steps)
	res.put("sourceKeysBeforeAssign", n)
	step := map[string]string{}
	for _, s := range steps {
		step[s["name"]] = s["reply"]
	}
	res.Detail = fmt.Sprintf("srcAfter=%s asking=%s afterPing=%s new=%s", step["get-src-after"], step["get-dst-asking"], step["get-after-asking-ping"], step["get-new-owner"])
	res.Pass = step["get-src"] == "100" &&
		strings.HasPrefix(step["get-dst-before"], "MOVED ") && strings.Contains(step["get-dst-before"], src.Addr) &&
		step["get-src-migrating"] == "100" &&
		strings.HasPrefix(step["get-dst-no-asking"], "MOVED ") &&
		step["get-dst-asking-before-migrate"] == "nil" &&
		step["migrate"] == "OK" &&
		strings.HasPrefix(step["get-src-after"], "ASK ") && strings.Contains(step["get-src-after"], dst.Addr) &&
		strings.HasPrefix(step["get-dst-after-no-asking"], "MOVED ") &&
		strings.HasPrefix(step["get-after-asking-ping"], "MOVED ") &&
		step["get-dst-asking"] == "100" &&
		n == 0 &&
		strings.HasPrefix(step["get-old-after-setslot"], "MOVED ") && strings.Contains(step["get-old-after-setslot"], dst.Addr) &&
		step["get-new-owner"] == "100"
	return res, nil
}
