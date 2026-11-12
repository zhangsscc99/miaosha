package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

func (rn *runner) scenarioSetslotThird() (result, error) {
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
	var third *server
	for _, s := range cl.masters {
		if s.Addr != src.Addr && s.Addr != dst.Addr {
			third = s
		}
	}
	original := src.ID
	key := "third{" + tag + "}"
	defer func() { _ = cl.resetSlot(slot, original) }()
	if _, err := src.do("SET", key, "1"); err != nil {
		return result{}, err
	}
	if err := cl.beginMigrate(slot, src, dst); err != nil {
		return result{}, err
	}
	mv, err := migrateKeys(src, dst, []string{key}, false)
	if err != nil {
		return result{}, err
	}
	if mv.Err != "" || mv.Str != "OK" {
		return result{}, fmt.Errorf("migrate %s", replyText(mv))
	}
	// 只告诉源节点。第三方这时还拿着旧地图。
	if _, err := src.do("CLUSTER", "SETSLOT", strconv.Itoa(slot), "NODE", dst.ID); err != nil {
		return result{}, err
	}
	v, err := third.call("GET", key)
	if err != nil {
		return result{}, err
	}
	immediate := replyText(v)
	t0 := time.Now()
	sawMs := -1
	last := ""
	for time.Since(t0) < 8*time.Second {
		if err := cl.refresh(); err != nil {
			return result{}, err
		}
		own := cl.ownerOf(slot)
		// refresh 读的是随便一个节点的 CLUSTER NODES。第三方要单独看。
		nodes, err := third.call("CLUSTER", "NODES")
		if err != nil {
			return result{}, err
		}
		last = ownerInNodes(nodes.Str, slot)
		if last == dst.ID {
			sawMs = int(time.Since(t0).Milliseconds())
			break
		}
		_ = own
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := dst.do("CLUSTER", "SETSLOT", strconv.Itoa(slot), "NODE", dst.ID); err != nil {
		return result{}, err
	}
	if _, err := third.do("CLUSTER", "SETSLOT", strconv.Itoa(slot), "NODE", dst.ID); err != nil {
		return result{}, err
	}
	if err := cl.refresh(); err != nil {
		return result{}, err
	}
	res := result{Scenario: "setslot-third", Note: "SETSLOT NODE 先只打给源节点。第三方的 CLUSTER NODES 还指着旧主库时，GET 会按它自己的地图回 MOVED。"}
	res.put("slot", slot)
	res.put("source", src.Addr)
	res.put("target", dst.Addr)
	res.put("third", third.Addr)
	res.put("getOnThird", immediate)
	res.put("thirdAgreedMs", sawMs)
	res.put("thirdOwnerAtEndOfWait", last)
	res.put("targetID", dst.ID)
	res.Detail = fmt.Sprintf("get=%s agreedMs=%d owner=%s", immediate, sawMs, last)
	res.Pass = cl.ownerOf(slot) != nil && cl.ownerOf(slot).ID == dst.ID
	return res, nil
}

func ownerInNodes(text string, slot int) string {
	for _, line := range strings.Split(text, "\n") {
		f := strings.Fields(strings.TrimSpace(line))
		if len(f) < 9 || !strings.Contains(f[2], "master") {
			continue
		}
		for _, tok := range f[8:] {
			if ownsToken(tok, slot) {
				return f[0]
			}
		}
	}
	return ""
}

func ownsToken(tok string, slot int) bool {
	if strings.HasPrefix(tok, "[") && strings.HasSuffix(tok, "]") {
		body := tok[1 : len(tok)-1]
		if i := strings.Index(body, "->-"); i > 0 {
			n, err := strconv.Atoi(body[:i])
			return err == nil && n == slot
		}
		return false
	}
	if strings.Contains(tok, "-") {
		a, b, ok := strings.Cut(tok, "-")
		if !ok {
			return false
		}
		lo, e1 := strconv.Atoi(a)
		hi, e2 := strconv.Atoi(b)
		return e1 == nil && e2 == nil && slot >= lo && slot <= hi
	}
	n, err := strconv.Atoi(tok)
	return err == nil && n == slot
}

func (rn *runner) scenarioReplace() (result, error) {
	cl := rn.cl
	if err := cl.refresh(); err != nil {
		return result{}, err
	}
	tag, slot, err := cl.tagOn(cl.masters[1].Addr)
	if err != nil {
		return result{}, err
	}
	src := cl.ownerOf(slot)
	dst := cl.otherThan(src)
	original := src.ID
	key := "repl{" + tag + "}"
	defer func() { _ = cl.resetSlot(slot, original) }()
	if _, err := src.do("SET", key, "100"); err != nil {
		return result{}, err
	}
	if err := cl.beginMigrate(slot, src, dst); err != nil {
		return result{}, err
	}
	const n = 50
	for i := 0; i < n; i++ {
		if _, err := dst.do("ASKING"); err != nil {
			return result{}, err
		}
		if _, err := dst.do("INCR", key); err != nil {
			return result{}, fmt.Errorf("incr %d: %w", i, err)
		}
	}
	before, err := func() (Val, error) {
		if _, err := dst.do("ASKING"); err != nil {
			return Val{}, err
		}
		return dst.call("GET", key)
	}()
	if err != nil {
		return result{}, err
	}
	mv, err := migrateKeys(src, dst, []string{key}, true)
	if err != nil {
		return result{}, err
	}
	if _, err := dst.do("ASKING"); err != nil {
		return result{}, err
	}
	after, err := dst.call("GET", key)
	if err != nil {
		return result{}, err
	}
	res := result{Scenario: "replace-loss", Note: "目标上先 ASKING INCR 出一份新值，再 MIGRATE REPLACE。源上的旧值盖掉目标上的 50 次递增。"}
	res.put("slot", slot)
	res.put("incrs", n)
	res.put("before", replyText(before))
	res.put("migrate", replyText(mv))
	res.put("after", replyText(after))
	res.Detail = fmt.Sprintf("before=%s migrate=%s after=%s", replyText(before), replyText(mv), replyText(after))
	res.Pass = replyText(before) == "50" && replyText(mv) == "OK" && replyText(after) == "100"
	return res, nil
}

func (rn *runner) scenarioNoreplace() (result, error) {
	cl := rn.cl
	if err := cl.refresh(); err != nil {
		return result{}, err
	}
	tag, slot, err := cl.tagOn(cl.masters[2].Addr)
	if err != nil {
		return result{}, err
	}
	src := cl.ownerOf(slot)
	dst := cl.otherThan(src)
	original := src.ID
	key := "norepl{" + tag + "}"
	defer func() { _ = cl.resetSlot(slot, original) }()
	if _, err := src.do("SET", key, "7"); err != nil {
		return result{}, err
	}
	if err := cl.beginMigrate(slot, src, dst); err != nil {
		return result{}, err
	}
	if _, err := dst.do("ASKING"); err != nil {
		return result{}, err
	}
	if _, err := dst.do("SET", key, "9"); err != nil {
		return result{}, err
	}
	mv, err := migrateKeys(src, dst, []string{key}, false)
	if err != nil {
		return result{}, err
	}
	srcGet, err := src.call("GET", key)
	if err != nil {
		return result{}, err
	}
	if _, err := dst.do("ASKING"); err != nil {
		return result{}, err
	}
	dstGet, err := dst.call("GET", key)
	if err != nil {
		return result{}, err
	}
	res := result{Scenario: "noreplace", Note: "目标上已经有这个键时，不带 REPLACE 的 MIGRATE 失败。源上的 7 还在，目标上的 9 也还在。"}
	res.put("migrate", replyText(mv))
	res.put("source", replyText(srcGet))
	res.put("target", replyText(dstGet))
	res.Detail = fmt.Sprintf("migrate=%s source=%s target=%s", replyText(mv), replyText(srcGet), replyText(dstGet))
	res.Pass = strings.Contains(replyText(mv), "BUSYKEY") && replyText(srcGet) == "7" && replyText(dstGet) == "9"
	return res, nil
}

func (rn *runner) scenarioPartial() (result, error) {
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
	defer func() { _ = cl.resetSlot(slot, original) }()
	var keys []string
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("{%s}:%d", tag, i)
		keys = append(keys, k)
		if _, err := src.do("SET", k, strconv.Itoa(i)); err != nil {
			return result{}, err
		}
	}
	if err := cl.beginMigrate(slot, src, dst); err != nil {
		return result{}, err
	}
	mv, err := migrateKeys(src, dst, keys[:40], false)
	if err != nil {
		return result{}, err
	}
	if replyText(mv) != "OK" {
		return result{}, fmt.Errorf("migrate %s", replyText(mv))
	}
	ask, okSrc, bad := 0, 0, 0
	for i := 0; i < 40; i++ {
		v, err := src.call("GET", keys[i])
		if err != nil {
			return result{}, err
		}
		if strings.HasPrefix(v.Err, "ASK") {
			ask++
		} else {
			bad++
		}
	}
	for i := 40; i < 100; i++ {
		v, err := src.call("GET", keys[i])
		if err != nil {
			return result{}, err
		}
		if v.Err == "" && v.Str == strconv.Itoa(i) {
			okSrc++
		} else {
			bad++
		}
	}
	res := result{Scenario: "partial", Note: "同一个槽先迁 40 个键。不跟着 ASK 的客户端，这 40 个读不到；还留在源上的 60 个仍然能读。"}
	res.put("ask", ask)
	res.put("stillOnSource", okSrc)
	res.put("bad", bad)
	res.Detail = fmt.Sprintf("ask=%d still=%d bad=%d", ask, okSrc, bad)
	res.Pass = ask == 40 && okSrc == 60 && bad == 0
	return res, nil
}

func (rn *runner) scenarioTryagain() (result, error) {
	cl := rn.cl
	if err := cl.refresh(); err != nil {
		return result{}, err
	}
	tag, slot, err := cl.tagOn(cl.masters[1].Addr)
	if err != nil {
		return result{}, err
	}
	src := cl.ownerOf(slot)
	dst := cl.otherThan(src)
	original := src.ID
	stock := "ms{" + tag + "}:stock"
	win := "ms{" + tag + "}:win"
	defer func() { _ = cl.resetSlot(slot, original) }()
	if _, err := src.do("SET", stock, "5"); err != nil {
		return result{}, err
	}
	if _, err := src.do("DEL", win); err != nil {
		return result{}, err
	}
	if _, err := src.do("SADD", win, "seed"); err != nil {
		return result{}, err
	}
	if err := cl.beginMigrate(slot, src, dst); err != nil {
		return result{}, err
	}
	mv, err := migrateKeys(src, dst, []string{stock}, false)
	if err != nil {
		return result{}, err
	}
	srcEval, err := src.call("EVAL", rn.lua, "2", stock, win, "u1")
	if err != nil {
		return result{}, err
	}
	if _, err := dst.do("ASKING"); err != nil {
		return result{}, err
	}
	dstEval, err := dst.call("EVAL", rn.lua, "2", stock, win, "u1")
	if err != nil {
		return result{}, err
	}
	mv2, err := migrateKeys(src, dst, []string{win}, false)
	if err != nil {
		return result{}, err
	}
	srcAfter, err := src.call("EVAL", rn.lua, "2", stock, win, "u1")
	if err != nil {
		return result{}, err
	}
	if _, err := dst.do("ASKING"); err != nil {
		return result{}, err
	}
	dstAfter, err := dst.call("EVAL", rn.lua, "2", stock, win, "u1")
	if err != nil {
		return result{}, err
	}
	if _, err := dst.do("ASKING"); err != nil {
		return result{}, err
	}
	left, err := dst.call("GET", stock)
	if err != nil {
		return result{}, err
	}
	res := result{Scenario: "tryagain", Note: "库存键已经迁走、已购集合还在源上时，同一次 EVAL 在两边都是 TRYAGAIN。两个键都到了目标上之后，ASKING 再执行才扣成 4。"}
	res.put("migrateStock", replyText(mv))
	res.put("sourceHalf", replyText(srcEval))
	res.put("targetHalf", replyText(dstEval))
	res.put("migrateWin", replyText(mv2))
	res.put("sourceBoth", replyText(srcAfter))
	res.put("targetBoth", replyText(dstAfter))
	res.put("stock", replyText(left))
	res.Detail = fmt.Sprintf("srcHalf=%s dstHalf=%s srcBoth=%s dstBoth=%s stock=%s", replyText(srcEval), replyText(dstEval), replyText(srcAfter), replyText(dstAfter), replyText(left))
	res.Pass = strings.Contains(replyText(srcEval), "TRYAGAIN") &&
		strings.Contains(replyText(dstEval), "TRYAGAIN") &&
		strings.HasPrefix(replyText(srcAfter), "ASK ") &&
		replyText(dstAfter) == "1" && replyText(left) == "4"
	return res, nil
}

func (rn *runner) scenarioAskFollow() (result, error) {
	cl := rn.cl
	if err := cl.refresh(); err != nil {
		return result{}, err
	}
	tag, slot, err := cl.tagOn(cl.masters[2].Addr)
	if err != nil {
		return result{}, err
	}
	src := cl.ownerOf(slot)
	dst := cl.otherThan(src)
	original := src.ID
	key := "askf{" + tag + "}"
	defer func() { _ = cl.resetSlot(slot, original) }()
	if _, err := src.do("SET", key, "1"); err != nil {
		return result{}, err
	}
	if err := cl.beginMigrate(slot, src, dst); err != nil {
		return result{}, err
	}
	mv, err := migrateKeys(src, dst, []string{key}, false)
	if err != nil {
		return result{}, err
	}
	if replyText(mv) != "OK" {
		return result{}, fmt.Errorf("migrate %s", replyText(mv))
	}
	n := 16
	srcC, err := dialWorkers(src.Addr, n)
	if err != nil {
		return result{}, err
	}
	defer closeConns(srcC)
	dstC, err := dialWorkers(dst.Addr, n)
	if err != nil {
		return result{}, err
	}
	defer closeConns(dstC)
	res, err := rn.bench("ask-follow-w16", "键已经在目标上，槽还没改主。每次从源节点拿到 ASK，再 ASKING 后去目标读。", n, time.Second, 4*time.Second, cl.masters, func(i int) error {
		v, err := srcC[i].call("GET", key)
		if err != nil {
			return err
		}
		kind, _, addr, ok := parseRedirect(v.Err)
		if !ok || kind != "ASK" || addr != dst.Addr {
			return fmt.Errorf("ask %s", replyText(v))
		}
		if _, err := dstC[i].call("ASKING"); err != nil {
			return err
		}
		v, err = dstC[i].call("GET", key)
		if err != nil {
			return err
		}
		if v.Err != "" || v.Str != "1" {
			return fmt.Errorf("get %s", replyText(v))
		}
		return nil
	}, nil)
	if err != nil {
		return res, err
	}
	res.put("source", src.Addr)
	res.put("target", dst.Addr)
	res.put("slot", slot)
	return res, nil
}

func (rn *runner) scenarioStall() (result, error) {
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
	defer func() { _ = cl.resetSlot(slot, original) }()
	sameKey, _, err := cl.keyOn("stall-same", src.Addr)
	if err != nil {
		return result{}, err
	}
	if slotOf(sameKey) == slot {
		return result{}, fmt.Errorf("same key landed in the migrating slot")
	}
	var other *server
	for _, s := range cl.masters {
		if s.Addr != src.Addr && s.Addr != dst.Addr {
			other = s
		}
	}
	if other == nil {
		return result{}, fmt.Errorf("no third master")
	}
	otherKey, _, err := cl.keyOn("stall-other", other.Addr)
	if err != nil {
		return result{}, err
	}
	if _, err := src.do("SET", sameKey, "1"); err != nil {
		return result{}, err
	}
	if _, err := other.do("SET", otherKey, "1"); err != nil {
		return result{}, err
	}
	val := strings.Repeat("y", 200)
	makeKeys := func(prefix string, n int) ([]string, error) {
		keys := make([]string, n)
		pairs := make([][2]string, n)
		for i := 0; i < n; i++ {
			keys[i] = fmt.Sprintf("%s{%s}:%d", prefix, tag, i)
			pairs[i] = [2]string{keys[i], val}
		}
		if err := pipelineSet(src.c, pairs); err != nil {
			return nil, err
		}
		return keys, nil
	}
	oneKeys, err := makeKeys("one", 40)
	if err != nil {
		return result{}, err
	}
	fmt.Println("stall keys-one", len(oneKeys))
	batchKeys, err := makeKeys("batch", 200)
	if err != nil {
		return result{}, err
	}
	fmt.Println("stall keys-batch", len(batchKeys))
	if err := cl.beginMigrate(slot, src, dst); err != nil {
		return result{}, err
	}
	sameC, err := dialWorkers(src.Addr, 4)
	if err != nil {
		return result{}, err
	}
	defer closeConns(sameC)
	otherC, err := dialWorkers(other.Addr, 4)
	if err != nil {
		return result{}, err
	}
	defer closeConns(otherC)
	runGets := func(during func()) (sameHist, otherHist *hist, sameN, otherN int64) {
		shs := make([]*hist, 4)
		ohs := make([]*hist, 4)
		var sn, on [4]int64
		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(8)
		for i := 0; i < 4; i++ {
			go func(i int) {
				defer wg.Done()
				h := newHist()
				shs[i] = h
				for n := 0; n < 1500; n++ {
					select {
					case <-stop:
						return
					default:
					}
					t0 := time.Now()
					v, err := sameC[i].call("GET", sameKey)
					h.add(time.Since(t0).Nanoseconds())
					sn[i]++
					if err != nil || v.Err != "" || v.Str != "1" {
						return
					}
				}
			}(i)
		}
		for i := 0; i < 4; i++ {
			go func(i int) {
				defer wg.Done()
				h := newHist()
				ohs[i] = h
				for n := 0; n < 1500; n++ {
					select {
					case <-stop:
						return
					default:
					}
					t0 := time.Now()
					v, err := otherC[i].call("GET", otherKey)
					h.add(time.Since(t0).Nanoseconds())
					on[i]++
					if err != nil || v.Err != "" || v.Str != "1" {
						return
					}
				}
			}(i)
		}
		time.Sleep(5 * time.Millisecond)
		during()
		close(stop)
		wg.Wait()
		sameHist, otherHist = newHist(), newHist()
		for i := 0; i < 4; i++ {
			sameHist.merge(shs[i])
			otherHist.merge(ohs[i])
			sameN += sn[i]
			otherN += on[i]
		}
		return sameHist, otherHist, sameN, otherN
	}
	var oneMax int64
	oneHist := newHist()
	tOne := time.Now()
	sh, oh, sn, on := runGets(func() {
		for _, k := range oneKeys {
			t0 := time.Now()
			v, err := migrateKeys(src, dst, []string{k}, false)
			oneHist.add(time.Since(t0).Nanoseconds())
			if err != nil || (v.Err != "" && v.Str != "OK" && v.Str != "NOKEY") {
				oneMax = -1
			}
		}
	})
	oneElapsed := time.Since(tOne).Seconds()
	var batchReply string
	var batchNs int64
	sh2, oh2, sn2, on2 := runGets(func() {
		t0 := time.Now()
		v, err := migrateKeys(src, dst, batchKeys, false)
		batchNs = time.Since(t0).Nanoseconds()
		if err != nil {
			batchReply = err.Error()
			return
		}
		batchReply = replyText(v)
	})
	state, err := cl.stateOf(src)
	if err != nil {
		state = err.Error()
	}
	res := result{Scenario: "stall", Note: "MIGRATE 在主线程里做完才返回。同一进程上的 GET 会排在这条命令后面。另一个主库上的 GET 不排这条队。"}
	res.put("oneElapsedSec", oneElapsed)
	res.put("oneMigrateP50us", oneHist.le(0.50)/1000)
	res.put("oneMigrateP99us", oneHist.le(0.99)/1000)
	res.put("oneMigrateMaxus", oneHist.max/1000)
	res.put("oneSameGetN", sn)
	res.put("oneSameGetP99us", sh.le(0.99)/1000)
	res.put("oneSameGetMaxus", sh.max/1000)
	res.put("oneOtherGetN", on)
	res.put("oneOtherGetP99us", oh.le(0.99)/1000)
	res.put("oneOtherGetMaxus", oh.max/1000)
	res.put("batchKeys", len(batchKeys))
	res.put("batchReply", batchReply)
	res.put("batchUs", batchNs/1000)
	res.put("batchSameGetN", sn2)
	res.put("batchSameGetP99us", sh2.le(0.99)/1000)
	res.put("batchSameGetMaxus", sh2.max/1000)
	res.put("batchOtherGetN", on2)
	res.put("batchOtherGetP99us", oh2.le(0.99)/1000)
	res.put("batchOtherGetMaxus", oh2.max/1000)
	res.put("state", state)
	res.put("oneMigrateFailed", oneMax == -1)
	res.ElapsedSec = oneElapsed
	res.Detail = fmt.Sprintf("oneMax=%d batchUs=%d sameMax=%d otherMax=%d state=%s", oneHist.max/1000, batchNs/1000, sh2.max/1000, oh2.max/1000, state)
	res.Pass = oneMax != -1 && batchReply == "OK" && state == "ok" && sh2.max > oh2.max && batchNs > 0
	return res, nil
}

func (rn *runner) scenarioSleep() (result, error) {
	cl := rn.cl
	if err := cl.refresh(); err != nil {
		return result{}, err
	}
	victim := cl.masters[1]
	watcher := cl.masters[0]
	key, _, err := cl.keyOn("sleep-watch", watcher.Addr)
	if err != nil {
		return result{}, err
	}
	if _, err := watcher.do("SET", key, "1"); err != nil {
		return result{}, err
	}
	vkey, _, err := cl.keyOn("sleep-victim", victim.Addr)
	if err != nil {
		return result{}, err
	}
	if _, err := victim.do("SET", vkey, "0"); err != nil {
		return result{}, err
	}
	wc, err := dial(watcher.Addr, time.Second)
	if err != nil {
		return result{}, err
	}
	defer wc.Close()
	sc, err := dial(victim.Addr, time.Second)
	if err != nil {
		return result{}, err
	}
	defer sc.Close()
	sc.SetDeadline(time.Now().Add(20 * time.Second))
	start := time.Now()
	type reply struct {
		v   Val
		err error
		at  time.Duration
	}
	doneSleep := make(chan reply, 1)
	go func() {
		v, err := sc.call("DEBUG", "SLEEP", "6")
		doneSleep <- reply{v, err, time.Since(start)}
	}()
	firstFail := time.Duration(-1)
	failText := ""
	var woke time.Duration
	select {
	case r := <-doneSleep:
		woke = r.at
		if r.err != nil {
			return result{}, r.err
		}
		failText = "sleep returned before fail: " + replyText(r.v)
	case <-time.After(20 * time.Millisecond):
	}
	okGets := 0
	deadline := time.Now().Add(12 * time.Second)
	for firstFail < 0 && woke == 0 && time.Now().Before(deadline) {
		v, err := wc.call("GET", key)
		if err != nil {
			failText = err.Error()
			firstFail = time.Since(start)
			break
		}
		if v.Err != "" {
			failText = v.Err
			firstFail = time.Since(start)
			break
		}
		if v.Str == "1" {
			okGets++
		}
		select {
		case r := <-doneSleep:
			woke = r.at
			if r.err != nil {
				return result{}, r.err
			}
			if replyText(r.v) != "OK" {
				return result{}, fmt.Errorf("sleep reply %s", replyText(r.v))
			}
		default:
		}
		time.Sleep(20 * time.Millisecond)
	}
	if woke == 0 {
		r := <-doneSleep
		woke = r.at
		if r.err != nil {
			return result{}, r.err
		}
		if replyText(r.v) != "OK" {
			return result{}, fmt.Errorf("sleep reply %s", replyText(r.v))
		}
	}
	final, _ := victim.call("GET", vkey)
	state, _ := cl.stateOf(watcher)
	logs := cl.logLines(watcher.Port, []string{"Marking node", "Clear FAIL", "Cluster state"}, 20)
	marked := 0
	for _, line := range logs {
		if strings.Contains(line, "Marking node") {
			marked++
		}
	}
	res := result{Scenario: "sleep", Note: "DEBUG SLEEP 6 堵住一个主库的主线程。node-timeout 写成 5000。这次 6 秒没有被标成 FAIL，观察节点上的 GET 一直成功。"}
	res.put("victim", victim.Addr)
	res.put("watcher", watcher.Addr)
	res.put("firstFailMs", ms(firstFail))
	res.put("firstFail", failText)
	res.put("wokeMs", ms(woke))
	res.put("okGets", okGets)
	res.put("final", replyText(final))
	res.put("state", state)
	res.put("marked", marked)
	res.put("logs", logs)
	res.Detail = fmt.Sprintf("failMs=%d wokeMs=%d okGets=%d marked=%d state=%s", ms(firstFail), ms(woke), okGets, marked, state)
	res.Pass = ms(woke) >= 5800 && ms(woke) <= 6500 && firstFail < 0 && okGets > 100 && state == "ok" && marked == 0 && replyText(final) == "0"
	return res, nil
}

func ms(d time.Duration) int64 {
	if d < 0 {
		return -1
	}
	return d.Milliseconds()
}

func (rn *runner) scenarioCoverage() (result, error) {
	cl := rn.cl
	if err := cl.refresh(); err != nil {
		return result{}, err
	}
	victim := cl.masters[2]
	watcher := cl.masters[0]
	if err := cl.flushMasters(); err != nil {
		return result{}, err
	}
	if err := cl.refresh(); err != nil {
		return result{}, err
	}
	wkey, _, err := cl.keyOn("cov-watch", watcher.Addr)
	if err != nil {
		return result{}, err
	}
	if _, err := watcher.do("SET", wkey, "v"); err != nil {
		return result{}, err
	}
	vkeys := make([]string, 0, 20)
	for len(vkeys) < 20 {
		k, _, err := cl.keyOn("cov-v"+strconv.Itoa(len(vkeys)), victim.Addr)
		if err != nil {
			return result{}, err
		}
		// keyOn 会返回同一个最小下标。用已经用过的前缀避开。
		if contains(vkeys, k) {
			return result{}, fmt.Errorf("key collision %s", k)
		}
		vkeys = append(vkeys, k)
		if _, err := victim.do("SET", k, "v"+strconv.Itoa(len(vkeys))); err != nil {
			return result{}, err
		}
	}
	victimSlots := victim.Slots
	wc, err := dial(watcher.Addr, time.Second)
	if err != nil {
		return result{}, err
	}
	defer wc.Close()
	start := time.Now()
	if err := killIfRedis(victim.Pid); err != nil {
		return result{}, err
	}
	firstFail := time.Duration(-1)
	failText := ""
	for time.Since(start) < 15*time.Second {
		v, err := wc.call("GET", wkey)
		if err != nil {
			failText = err.Error()
			firstFail = time.Since(start)
			break
		}
		if v.Err != "" {
			failText = v.Err
			firstFail = time.Since(start)
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	downN := 0
	for i := 0; i < 20; i++ {
		v, err := wc.call("GET", wkey)
		if err != nil || strings.Contains(replyText(v), "CLUSTERDOWN") {
			downN++
		}
	}
	foreign, err := wc.call("GET", vkeys[0])
	foreignText := ""
	if err != nil {
		foreignText = err.Error()
	} else {
		foreignText = replyText(foreign)
	}
	restartAt := time.Now()
	if err := cl.restart(victim); err != nil {
		return result{}, err
	}
	pingMs := time.Since(start).Milliseconds()
	restartMs := time.Since(restartAt).Milliseconds()
	// 进程已经应答，集群可能还是 fail。这段只打 20 次，不当成容量。
	direct, err := dial(victim.Addr, time.Second)
	if err != nil {
		return result{}, err
	}
	defer direct.Close()
	gapKey := "cov-gap"
	// 找一个属于 victim 的键。重启不改槽。
	gapKey, _, err = cl.keyOn("cov-gap", victim.Addr)
	if err != nil {
		return result{}, err
	}
	gapOK := 0
	gapErr := ""
	for i := 0; i < 20; i++ {
		v, err := direct.call("SET", gapKey, "gap")
		if err != nil {
			gapErr = err.Error()
			break
		}
		if v.Err != "" {
			if gapErr == "" {
				gapErr = v.Err
			}
			continue
		}
		gapOK++
	}
	victimOwnMs := int64(-1)
	for time.Since(restartAt) < 5*time.Second {
		m, err := cl.clusterInfo(victim)
		if err == nil && m["cluster_state"] == "ok" {
			victimOwnMs = time.Since(restartAt).Milliseconds()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	watchWhenOwn := ""
	lateSet := ""
	if victimOwnMs >= 0 {
		v, err := wc.call("GET", wkey)
		if err != nil {
			watchWhenOwn = err.Error()
		} else {
			watchWhenOwn = replyText(v)
		}
		v, err = direct.call("SET", gapKey, "gap")
		if err != nil {
			lateSet = err.Error()
		} else {
			lateSet = replyText(v)
		}
	}
	stillDown := 0
	for i := 0; i < 20; i++ {
		v, err := wc.call("GET", wkey)
		if err != nil || v.Err != "" {
			stillDown++
		}
	}
	okMs := int64(-1)
	okDeadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(okDeadline) {
		st, err := cl.stateOf(watcher)
		if err == nil && st == "ok" {
			okMs = time.Since(start).Milliseconds()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	missing := 0
	for _, k := range vkeys {
		v, err := victim.call("GET", k)
		if err != nil {
			return result{}, err
		}
		if v.Nil || v.Str == "" && v.Err == "" && v.Kind == '$' {
			missing++
		}
		if replyText(v) == "nil" {
			missing++
		}
	}
	// 上面 nil 可能数了两次。按 reply 重数。
	missing = 0
	for _, k := range vkeys {
		v, err := victim.call("GET", k)
		if err != nil {
			return result{}, err
		}
		if replyText(v) == "nil" {
			missing++
		}
	}
	watchLeft, err := watcher.call("GET", wkey)
	if err != nil {
		return result{}, err
	}
	gapLeft, err := victim.call("GET", gapKey)
	if err != nil {
		return result{}, err
	}

	// 第二次：coverage no。进程在，先改配置再杀。
	for _, s := range cl.masters {
		if _, err := s.do("CONFIG", "SET", "cluster-require-full-coverage", "no"); err != nil {
			return result{}, err
		}
	}
	if err := victim.refreshMeta(); err != nil {
		return result{}, err
	}
	start2 := time.Now()
	if err := killIfRedis(victim.Pid); err != nil {
		return result{}, err
	}
	slotsFail := int64(-1)
	state2 := ""
	for time.Since(start2) < 15*time.Second {
		ci, err := cl.clusterInfo(watcher)
		if err != nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		state2 = ci["cluster_state"]
		n, _ := strconv.ParseInt(ci["cluster_slots_fail"], 10, 64)
		if n > 0 {
			slotsFail = n
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	failSeenMs := time.Since(start2).Milliseconds()
	localOK := 0
	for i := 0; i < 20; i++ {
		v, err := wc.call("GET", wkey)
		if err == nil && v.Err == "" && v.Str == "v" {
			localOK++
		}
	}
	foreign2, err := wc.call("GET", vkeys[0])
	foreign2Text := ""
	if err != nil {
		foreign2Text = err.Error()
	} else {
		foreign2Text = replyText(foreign2)
	}
	refused := ""
	if c, err := dial(victim.Addr, 200*time.Millisecond); err != nil {
		refused = err.Error()
	} else {
		refused = "connected"
		c.Close()
	}
	res := result{Scenario: "coverage", Note: "默认要求所有槽都有主库。一个主库被 kill -9 之后，活着的节点上的键也变成 CLUSTERDOWN。关掉这项之后，活着的槽还能读，死掉的槽是 MOVED 到已经没有进程的地址。没有 AOF，重启后原来的 20 个键是空的。没有副本，所以没有故障转移。"}
	res.put("victim", victim.Addr)
	res.put("watcher", watcher.Addr)
	res.put("victimSlots", victimSlots)
	res.put("firstFailMs", ms(firstFail))
	res.put("firstFail", failText)
	res.put("downOf20", downN)
	res.put("foreignWhileDown", foreignText)
	res.put("pingMs", pingMs)
	res.put("restartMs", restartMs)
	res.put("gapSetOK", gapOK)
	res.put("gapSetErr", gapErr)
	res.put("victimOwnMs", victimOwnMs)
	res.put("watchWhenOwn", watchWhenOwn)
	res.put("lateSet", lateSet)
	res.put("watcherStillDownOf20", stillDown)
	res.put("recoveredMs", okMs)
	res.put("missingKeys", missing)
	res.put("watcherValue", replyText(watchLeft))
	res.put("gapValue", replyText(gapLeft))
	res.put("coverageNoFailMs", failSeenMs)
	res.put("coverageNoState", state2)
	res.put("coverageNoSlotsFail", slotsFail)
	res.put("coverageNoLocalOK", localOK)
	res.put("coverageNoForeign", foreign2Text)
	res.put("coverageNoDial", refused)
	res.put("logs", cl.logLines(watcher.Port, []string{"Marking node", "Clear FAIL", "Cluster state"}, 30))
	res.Detail = fmt.Sprintf("failMs=%d down=%d ownMs=%d late=%s watchWhenOwn=%s recoveredMs=%d missing=%d slotsFail=%d localOK=%d foreign=%s", ms(firstFail), downN, victimOwnMs, lateSet, watchWhenOwn, okMs, missing, slotsFail, localOK, foreign2Text)
	res.Pass = strings.Contains(failText, "CLUSTERDOWN The cluster is down") &&
		downN == 20 &&
		strings.Contains(foreignText, "CLUSTERDOWN") &&
		missing == 20 &&
		replyText(watchLeft) == "v" &&
		gapOK == 0 &&
		strings.Contains(gapErr, "CLUSTERDOWN") &&
		victimOwnMs >= 1000 &&
		lateSet == "OK" &&
		strings.Contains(watchWhenOwn, "CLUSTERDOWN") &&
		replyText(gapLeft) == "gap" &&
		stillDown == 20 &&
		okMs > pingMs &&
		slotsFail == int64(victimSlots) &&
		state2 == "ok" &&
		localOK == 20 &&
		strings.HasPrefix(foreign2Text, "MOVED ") &&
		strings.Contains(foreign2Text, victim.Addr) &&
		refused != "connected"
	return res, nil
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}
