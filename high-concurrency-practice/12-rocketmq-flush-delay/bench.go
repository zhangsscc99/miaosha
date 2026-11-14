package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

func scenarioProduce() result {
	topic := topicName()
	note := "同步发送，等 broker 返回。oneway 只表示调用返回，不是已经落盘。"
	if *oneway {
		note = "SendOneWay。调用返回不等于 broker 已经收下。不拿这个数去除机器。"
	}
	wait, err := ensureTopic(topic, *queueN)
	if err != nil {
		return fail(note, err)
	}
	p, err := newProducer(*queueN)
	if err != nil {
		return fail(note, err)
	}
	defer p.Shutdown()

	body := payloadN(*payload)
	winStart := time.Now().Add(*warmup)
	deadline := winStart.Add(*duration)
	var success, errors int64
	var queueBits [64]uint32
	h := newHist()
	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				now := time.Now()
				if now.After(deadline) {
					return
				}
				counting := !now.Before(winStart)
				msg := primitive.NewMessage(topic, body)
				t0 := time.Now()
				var qid int
				var sendErr error
				if *oneway {
					ctx, cancel := context.WithTimeout(context.Background(), *sendTimeout)
					sendErr = p.SendOneWay(ctx, msg)
					cancel()
				} else {
					ctx, cancel := context.WithTimeout(context.Background(), *sendTimeout)
					res, err := p.SendSync(ctx, msg)
					cancel()
					sendErr = err
					if err == nil && res.Status != primitive.SendOK {
						sendErr = fmt.Errorf("status %d", res.Status)
					}
					if err == nil && res != nil && res.MessageQueue != nil {
						qid = res.MessageQueue.QueueId
					}
				}
				if !counting {
					continue
				}
				if sendErr != nil {
					atomic.AddInt64(&errors, 1)
					continue
				}
				atomic.AddInt64(&success, 1)
				h.add(time.Since(t0).Nanoseconds())
				if qid >= 0 && qid < len(queueBits) {
					atomic.StoreUint32(&queueBits[qid], 1)
				}
			}
		}()
	}
	time.Sleep(time.Until(winStart))
	b0 := cpuTicks(*brokerPid)
	c0 := cpuTicks(os.Getpid())
	rss0 := rssKB(*brokerPid)
	wg.Wait()
	elapsed := time.Since(winStart).Seconds()
	b1 := cpuTicks(*brokerPid)
	c1 := cpuTicks(os.Getpid())
	var ids []int
	for i := range queueBits {
		if atomic.LoadUint32(&queueBits[i]) == 1 {
			ids = append(ids, i)
		}
	}
	ok := success > 0 && errors == 0
	res := result{
		Scenario:    *scenarioName,
		Note:        note,
		Pass:        ok,
		Topic:       topic,
		Queues:      *queueN,
		Workers:     *workers,
		Payload:     *payload,
		OneWay:      *oneway,
		Success:     success,
		Errors:      errors,
		QPS:         float64(success) / elapsed,
		ElapsedSec:  elapsed,
		P50us:       h.le(0.50) / 1000,
		P99us:       h.le(0.99) / 1000,
		Maxus:       h.le(1) / 1000,
		BrokerCPU:   cpuPercent(b0, b1, elapsed),
		ClientCPU:   cpuPercent(c0, c1, elapsed),
		BrokerRSSKB: rssKB(*brokerPid),
		RSSBeforeKB: rss0,
		RouteMs:     wait.Milliseconds(),
		QueueIDs:    ids,
	}
	res.Detail = fmt.Sprintf("ok=%d err=%d qps=%.0f elapsed=%.3fs p99us=%d maxus=%d queues=%v brokerCPU=%.1f clientCPU=%.1f",
		success, errors, res.QPS, elapsed, res.P99us, res.Maxus, ids, res.BrokerCPU, res.ClientCPU)
	return res
}

func scenarioSeed() result {
	topic := topicName()
	note := "同步发送指定条数，返回之后才允许杀进程。"
	if _, err := ensureTopic(topic, *queueN); err != nil {
		return fail(note, err)
	}
	p, err := newProducer(*queueN)
	if err != nil {
		return fail(note, err)
	}
	defer p.Shutdown()
	body := payloadN(*payload)
	var success, errors int64
	var firstErr string
	t0 := time.Now()
	for i := 0; i < *messages; i++ {
		msg := primitive.NewMessage(topic, body)
		if err := sendSync(p, msg); err != nil {
			errors++
			if firstErr == "" {
				firstErr = err.Error()
			}
		} else {
			success++
		}
	}
	elapsed := time.Since(t0).Seconds()
	res := result{
		Scenario:   "seed",
		Note:       note,
		Pass:       success == int64(*messages) && errors == 0,
		Topic:      topic,
		Messages:   *messages,
		Success:    success,
		Errors:     errors,
		ElapsedSec: elapsed,
		Payload:    *payload,
		Detail:     firstErr,
	}
	res.Detail = fmt.Sprintf("sent=%d err=%d elapsed=%.3fs first=%s", success, errors, elapsed, firstErr)
	return res
}

func scenarioCount() result {
	topic := topicName()
	note := "重启之后从最早的位点读。expect 是必须相等，expect-less 是必须更少。"
	var got int64
	var storeSum int64
	c, err := startPush(consumeOpts{
		group:    topic + "_c",
		topic:    topic,
		instance: topic + "-count",
		threads:  4,
	}, func(_ context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, m := range msgs {
			atomic.AddInt64(&got, 1)
			atomic.AddInt64(&storeSum, int64(m.StoreSize))
		}
		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		return fail(note, err)
	}
	defer c.Shutdown()
	quiet := time.Now()
	deadline := time.Now().Add(20 * time.Second)
	stable := 0
	var last int64 = -1
	for time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
		n := atomic.LoadInt64(&got)
		if n == last {
			stable++
		} else {
			stable = 0
			last = n
			quiet = time.Now()
		}
		if stable >= 4 && n > 0 {
			break
		}
		if stable >= 8 && time.Since(quiet) > 1500*time.Millisecond && time.Since(deadline.Add(-20*time.Second)) > 3*time.Second {
			break
		}
	}
	kept := atomic.LoadInt64(&got)
	pass := true
	if *expectKept >= 0 && kept != int64(*expectKept) {
		pass = false
	}
	if *expectLess >= 0 && kept >= int64(*expectLess) {
		pass = false
	}
	res := result{
		Scenario:     "count",
		Note:         note,
		Pass:         pass,
		Topic:        topic,
		Kept:         kept,
		Messages:     *messages,
		StoreSizeSum: atomic.LoadInt64(&storeSum),
	}
	res.Detail = fmt.Sprintf("kept=%d expect=%d expectLess=%d", kept, *expectKept, *expectLess)
	return res
}

func scenarioConsume() result {
	topic := topicName()
	note := "先把消息写进队列，再启动消费者。耗时含重新分配队列。"
	n := *messages
	if _, err := ensureTopic(topic, *queueN); err != nil {
		return fail(note, err)
	}
	p, err := newProducer(*queueN)
	if err != nil {
		return fail(note, err)
	}
	body := payloadN(*payload)
	var wrote, werr int64
	for i := 0; i < n; i++ {
		var raw []byte
		if *withIDs {
			raw = []byte(fmt.Sprintf("id:%d", i))
		} else {
			raw = body
		}
		msg := primitive.NewMessage(topic, raw)
		if err := sendSync(p, msg); err != nil {
			werr++
		} else {
			wrote++
		}
	}
	p.Shutdown()
	if wrote != int64(n) {
		return result{Scenario: "consume", Note: note, Pass: false, Topic: topic, Detail: fmt.Sprintf("prefill %d err %d", wrote, werr)}
	}
	var got int64
	var storeSum int64
	var first int64
	uniq := map[int]struct{}{}
	var uniqMu sync.Mutex
	start := time.Now()
	var cs []*consumerPush
	for i := 0; i < *consumerN; i++ {
		i := i
		c, err := startPush(consumeOpts{
			group:    topic + "_g",
			topic:    topic,
			instance: fmt.Sprintf("%s-%d", topic, i),
			threads:  *threads,
			batch:    32,
		}, func(_ context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
			now := time.Now().UnixNano()
			atomic.CompareAndSwapInt64(&first, 0, now)
			for _, m := range msgs {
				atomic.AddInt64(&got, 1)
				atomic.AddInt64(&storeSum, int64(m.StoreSize))
				if *withIDs {
					if id, ok := parseID(m.Body); ok {
						uniqMu.Lock()
						uniq[id] = struct{}{}
						uniqMu.Unlock()
					}
				}
			}
			return consumer.ConsumeSuccess, nil
		})
		if err != nil {
			for _, x := range cs {
				x.Shutdown()
			}
			return fail(note, err)
		}
		cs = append(cs, c)
	}
	defer func() {
		for _, c := range cs {
			c.Shutdown()
		}
	}()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if *withIDs {
			uniqMu.Lock()
			u := len(uniq)
			uniqMu.Unlock()
			if u >= n {
				break
			}
		} else if atomic.LoadInt64(&got) >= int64(n) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	elapsed := time.Since(start).Seconds()
	g := atomic.LoadInt64(&got)
	uniqMu.Lock()
	u := len(uniq)
	uniqMu.Unlock()
	var firstMs int64
	if f := atomic.LoadInt64(&first); f > 0 {
		firstMs = (f - start.UnixNano()) / int64(time.Millisecond)
	}
	pass := g == int64(n)
	if *withIDs {
		pass = u == n
	}
	res := result{
		Scenario:     "consume",
		Note:         note,
		Pass:         pass,
		Unique:       int64(u),
		Topic:        topic,
		Queues:       *queueN,
		Consumers:    *consumerN,
		Threads:      *threads,
		Messages:     n,
		Deliveries:   g,
		DrainSec:     elapsed,
		QPS:          float64(g) / elapsed,
		FirstDelayMs: firstMs,
		StoreSizeSum: atomic.LoadInt64(&storeSum),
		Payload:      *payload,
	}
	if g > 0 {
		res.BytesPerMsg = float64(res.StoreSizeSum) / float64(g)
	}
	res.Detail = fmt.Sprintf("got=%d unique=%d qps=%.0f drain=%.3fs firstMs=%d storeBytes/msg=%.2f", g, u, res.QPS, elapsed, firstMs, res.BytesPerMsg)
	return res
}

func scenarioWinners() result {
	topic := topicName()
	note := "5000 条同步发送再读完。墙钟短的话文档不写 CPU。"
	n := *messages
	if _, err := ensureTopic(topic, 1); err != nil {
		return fail(note, err)
	}
	p, err := newProducer(1)
	if err != nil {
		return fail(note, err)
	}
	body := payloadN(*payload)
	t0 := time.Now()
	var success int64
	for i := 0; i < n; i++ {
		msg := primitive.NewMessage(topic, body)
		if err := sendSync(p, msg); err != nil {
			p.Shutdown()
			return fail(note, err)
		}
		success++
	}
	sendSec := time.Since(t0).Seconds()
	p.Shutdown()
	var got int64
	c, err := startPush(consumeOpts{
		group:    topic + "_g",
		topic:    topic,
		instance: topic + "-w",
		threads:  4,
		batch:    32,
	}, func(_ context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		atomic.AddInt64(&got, int64(len(msgs)))
		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		return fail(note, err)
	}
	defer c.Shutdown()
	d0 := time.Now()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) && atomic.LoadInt64(&got) < int64(n) {
		time.Sleep(10 * time.Millisecond)
	}
	drain := time.Since(d0).Seconds()
	g := atomic.LoadInt64(&got)
	res := result{
		Scenario:   "winners",
		Note:       note,
		Pass:       success == int64(n) && g == int64(n),
		Topic:      topic,
		Messages:   n,
		Success:    success,
		Deliveries: g,
		ElapsedSec: sendSec,
		DrainSec:   drain,
		Payload:    *payload,
	}
	res.Detail = fmt.Sprintf("send=%.3fs drain=%.3fs n=%d got=%d", sendSec, drain, n, g)
	return res
}

func scenarioDisk() result {
	topic := topicName()
	note := "CommitLog 按消息总长度往前走。文件本身按 mappedFileSizeCommitLog 预分配，规划用走出的字节，不用整文件除条数。"
	if _, err := ensureTopic(topic, 1); err != nil {
		return fail(note, err)
	}
	clog := filepath.Join(*storeRoot, "commitlog")
	before, err := walkCommitLog(clog)
	if err != nil {
		return fail(note, err)
	}
	_, cq0, _ := dirUsage(filepath.Join(*storeRoot, "consumequeue"))
	_, ix0, _ := dirUsage(filepath.Join(*storeRoot, "index"))
	rss0 := rssKB(*brokerPid)
	p, err := newProducer(1)
	if err != nil {
		return fail(note, err)
	}
	body := payloadN(*payload)
	var success, errors int64
	t0 := time.Now()
	for i := 0; i < *messages; i++ {
		msg := primitive.NewMessage(topic, body)
		if err := sendSync(p, msg); err != nil {
			errors++
		} else {
			success++
		}
	}
	elapsed := time.Since(t0).Seconds()
	p.Shutdown()
	time.Sleep(300 * time.Millisecond)
	after, err := walkCommitLog(clog)
	if err != nil {
		return fail(note, err)
	}
	_, cq1, _ := dirUsage(filepath.Join(*storeRoot, "consumequeue"))
	_, ix1, _ := dirUsage(filepath.Join(*storeRoot, "index"))
	dMsg := after.messages - before.messages
	dBytes := after.bytes - before.bytes
	var per float64
	if dMsg > 0 {
		per = float64(dBytes) / float64(dMsg)
	}
	waste := (after.logical - before.logical) - dBytes
	if waste < 0 {
		waste = 0
	}
	res := result{
		Scenario:     "disk",
		Note:         note,
		Pass:         success == int64(*messages) && errors == 0 && dMsg == success,
		Topic:        topic,
		Messages:     *messages,
		Success:      success,
		Errors:       errors,
		ElapsedSec:   elapsed,
		Payload:      *payload,
		CommitLogMsg: dMsg,
		CommitBytes:  dBytes,
		BytesPerMsg:  per,
		Prealloc:     after.logical - before.logical,
		AllocBytes:   after.alloc - before.alloc,
		CQAlloc:      cq1 - cq0,
		IndexAlloc:   ix1 - ix0,
		TailWaste:    waste,
		RSSBeforeKB:  rss0,
		RSSAfterKB:   rssKB(*brokerPid),
	}
	res.Detail = fmt.Sprintf("walked=%d bytes=%d per=%.2f preallocDelta=%d allocDelta=%d cqDelta=%d indexDelta=%d tail=%d rss=%d->%d elapsed=%.3fs",
		dMsg, dBytes, per, res.Prealloc, res.AllocBytes, res.CQAlloc, res.IndexAlloc, waste, rss0, res.RSSAfterKB, elapsed)
	return res
}
