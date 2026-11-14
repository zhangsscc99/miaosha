package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type slot struct {
	t0    int64
	count bool
}

type pubOpt struct {
	names       []string
	workers     int
	payload     int
	persistent  bool
	confirms    bool
	warmup      time.Duration
	duration    time.Duration
	stopOnAlarm bool
	drain       bool
	prefetch    int
}

type pubOut struct {
	counted      int64
	nacks        int64
	errors       int64
	elapsed      time.Duration
	h            *hist
	brokerCPU    float64
	clientCPU    float64
	rssKB        int64
	alarm        bool
	alarmRaw     string
	limit        int64
	rssBytes     int64
	ready        int64
	messages     int64
	memory       int64
	messageBytes int64
	qtype        string
	flushTimeout bool
	inflightLeft int64
}

func declareNames(base string, n int, durable, quorum bool) ([]string, error) {
	if n < 1 {
		n = 1
	}
	args := classicArgs(nil)
	if quorum {
		args = amqp.Table{"x-queue-type": "quorum"}
		durable = true
	}
	names := make([]string, 0, n)
	for i := 0; i < n; i++ {
		name := base
		if n > 1 {
			name = fmt.Sprintf("%s-%d", base, i)
		}
		if err := resetQueue(name, durable, args); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, nil
}

func publishWindow(opt pubOpt) (pubOut, error) {
	var out pubOut
	out.h = newHist()
	if opt.workers < 1 {
		opt.workers = 1
	}
	body := make([]byte, opt.payload)
	for i := range body {
		body[i] = 'x'
	}
	mode := amqp.Persistent
	if !opt.persistent {
		mode = amqp.Transient
	}

	stopPublish := make(chan struct{})
	var stopOnce sync.Once
	stop := func() { stopOnce.Do(func() { close(stopPublish) }) }

	var drainConns []*amqp.Connection
	if opt.drain {
		for _, name := range opt.names {
			c := openConsumer(name, opt.prefetch)
			drainConns = append(drainConns, c.conn)
			go func(msgs <-chan amqp.Delivery) {
				for d := range msgs {
					_ = d.Ack(false)
				}
			}(c.msgs)
		}
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			n := int64(0)
			for _, name := range opt.names {
				if q, ok := findQueue(name); ok {
					n += q.Consumers
				}
			}
			if n >= int64(len(opt.names)) {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

	var record atomic.Bool
	var counted, nacks, pubErr, inflight atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < opt.workers; w++ {
		name := opt.names[w%len(opt.names)]
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			conn := dial()
			defer conn.Close()
			ch := channel(conn)
			var conf <-chan amqp.Confirmation
			pending := map[uint64]slot{}
			var mu sync.Mutex
			if opt.confirms {
				if err := ch.Confirm(false); err != nil {
					pubErr.Add(1)
					return
				}
				conf = ch.NotifyPublish(make(chan amqp.Confirmation, 4096))
				go func() {
					for c := range conf {
						mu.Lock()
						s, ok := pending[c.DeliveryTag]
						if ok {
							delete(pending, c.DeliveryTag)
						}
						mu.Unlock()
						if !ok || !s.count {
							continue
						}
						if c.Ack {
							out.h.add(time.Now().UnixNano() - s.t0)
							counted.Add(1)
						} else {
							nacks.Add(1)
						}
						inflight.Add(-1)
					}
				}()
			}
			var tag uint64
			for {
				select {
				case <-stopPublish:
					return
				default:
				}
				tag++
				now := time.Now().UnixNano()
				count := record.Load()
				if count {
					inflight.Add(1)
				}
				if opt.confirms {
					mu.Lock()
					pending[tag] = slot{t0: now, count: count}
					mu.Unlock()
				}
				err := ch.PublishWithContext(context.Background(), "", name, false, false, amqp.Publishing{
					DeliveryMode: mode,
					Body:         body,
				})
				if err != nil {
					pubErr.Add(1)
					if count {
						inflight.Add(-1)
					}
					if opt.confirms {
						mu.Lock()
						delete(pending, tag)
						mu.Unlock()
					}
					return
				}
				if !opt.confirms && count {
					out.h.add(time.Now().UnixNano() - now)
					counted.Add(1)
					inflight.Add(-1)
				}
			}
		}(name)
	}

	if opt.warmup > 0 {
		time.Sleep(opt.warmup)
	}
	record.Store(true)
	pid := brokerPID()
	b0 := cpuTicks(pid)
	c0 := cpuTicks(os.Getpid())
	t0 := time.Now()
	deadline := t0.Add(opt.duration)
	for time.Now().Before(deadline) {
		if opt.stopOnAlarm {
			if st, err := status(); err == nil && alarmOn(st.Alarms) {
				out.alarm = true
				out.alarmRaw = string(st.Alarms)
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	record.Store(false)
	stop()
	flushDeadline := time.Now().Add(1 * time.Second)
	for inflight.Load() > 0 && time.Now().Before(flushDeadline) {
		time.Sleep(10 * time.Millisecond)
	}
	out.elapsed = time.Since(t0)
	out.inflightLeft = inflight.Load()
	out.flushTimeout = out.inflightLeft > 0
	b1 := cpuTicks(pid)
	c1 := cpuTicks(os.Getpid())
	out.brokerCPU = cpuPercent(b0, b1, out.elapsed.Seconds())
	out.clientCPU = cpuPercent(c0, c1, out.elapsed.Seconds())
	out.rssKB = rssKB(pid)
	out.counted = counted.Load()
	out.nacks = nacks.Load()
	out.errors = pubErr.Load()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
	}
	for _, c := range drainConns {
		c.Close()
	}

	if st, err := status(); err == nil {
		if alarmOn(st.Alarms) {
			out.alarm = true
			out.alarmRaw = string(st.Alarms)
		}
		out.limit = st.Limit
		out.rssBytes = st.Memory.Total.RSS
	}
	for _, name := range opt.names {
		q, ok := findQueue(name)
		if !ok {
			continue
		}
		out.ready += q.Ready
		out.messages += q.Messages
		out.memory += q.Memory
		out.messageBytes += q.MessageBytes
		out.qtype = q.Type
		deleteQueue(name)
	}
	return out, nil
}

func deleteQueue(name string) {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return
	}
	_, _ = ch.QueueDelete(name, false, false, false)
}

func fillProduce(res *result, opt pubOpt, out pubOut) {
	res.Payload = opt.payload
	res.Workers = opt.workers
	res.Prefetch = opt.prefetch
	res.Queues = len(opt.names)
	res.Durable = *durable
	res.Persistent = opt.persistent
	res.ConfirmsOn = opt.confirms
	res.Quorum = *quorum
	res.Drain = opt.drain
	res.QueueType = out.qtype
	res.Confirms = out.counted
	res.Nacks = out.nacks
	res.Errors = out.errors
	res.ElapsedSec = out.elapsed.Seconds()
	if out.elapsed > 0 {
		res.QPS = float64(out.counted) / out.elapsed.Seconds()
	}
	if out.h.count() > 0 {
		res.P50us = out.h.le(0.50) / 1000
		res.P99us = out.h.le(0.99) / 1000
		res.Maxus = atomic.LoadInt64(&out.h.max) / 1000
	}
	res.BrokerCPU = out.brokerCPU
	res.ClientCPU = out.clientCPU
	res.BrokerRSSKB = out.rssKB
	res.Alarm = out.alarm
	res.AlarmRaw = out.alarmRaw
	res.VMLimit = out.limit
	res.RssBytes = out.rssBytes
	res.Ready = out.ready
	res.Messages = out.messages
	res.QueueMemory = out.memory
	res.MessageBytes = out.messageBytes
	res.Pass = out.errors == 0 && out.counted > 0 && out.nacks == 0
	res.Detail = fmt.Sprintf("qps=%.0f counted=%d p99us<=%d maxus=%d brokerCpu=%.1f clientCpu=%.1f alarm=%v ready=%d messages=%d errors=%d nacks=%d flushTimeout=%v type=%s",
		res.QPS, out.counted, res.P99us, res.Maxus, out.brokerCPU, out.clientCPU, out.alarm, out.ready, out.messages, out.errors, out.nacks, out.flushTimeout, out.qtype)
}

func scenarioProduce() result {
	res := result{
		Scenario: "produce",
		Note:     "发布窗口含预热之后的在途 confirm。drain 打开时每个队列一个消费者，prefetch 用参数。没有 confirm 的延迟是 Publish 返回，不是 broker 确认。",
	}
	names, err := declareNames(qname(), *queueN, *durable || *quorum, *quorum)
	if err != nil {
		res.Detail = err.Error()
		return res
	}
	out, err := publishWindow(pubOpt{
		names:       names,
		workers:     *workers,
		payload:     *payloadN,
		persistent:  *persistent,
		confirms:    *confirmsOn,
		warmup:      *warmup,
		duration:    *duration,
		stopOnAlarm: *stopAlarm,
		drain:       *drain,
		prefetch:    *prefetch,
	})
	if err != nil {
		res.Detail = err.Error()
		return res
	}
	fillProduce(&res, pubOpt{
		names: names, workers: *workers, payload: *payloadN, persistent: *persistent,
		confirms: *confirmsOn, drain: *drain, prefetch: *prefetch,
	}, out)
	return res
}

func scenarioConsume() result {
	name := qname()
	res := result{
		Scenario:  "consume",
		Note:      "先把持久化消息确认进一个经典队列，再计消费。计时从启动消费者开始，含建立连接。",
		Prefetch:  *prefetch,
		ConsumerN: *consumerN,
		Payload:   *payloadN,
		Messages:  int64(*messages),
	}
	if err := resetQueue(name, true, classicArgs(nil)); err != nil {
		res.Detail = err.Error()
		return res
	}
	acks, _, err := confirmExact(name, *messages, *workers, *payloadN, true)
	if err != nil || acks != int64(*messages) {
		res.Detail = fmt.Sprintf("preload acks=%d err=%v", acks, err)
		deleteQueue(name)
		return res
	}
	got, elapsed, brokerCPU, clientCPU := consumeCounted(name, *messages, *consumerN, *prefetch)
	q, _ := waitQueue(name, 2*time.Second, func(q qStat) bool { return q.Messages == 0 })
	if q.Name == "" {
		q, _ = findQueue(name)
	}
	res.Confirms = acks
	res.Deliveries = got
	res.ElapsedSec = elapsed.Seconds()
	if elapsed > 0 {
		res.QPS = float64(got) / elapsed.Seconds()
	}
	res.BrokerCPU = brokerCPU
	res.ClientCPU = clientCPU
	res.Ready = q.Ready
	res.Unacked = q.Unacked
	res.QueueType = q.Type
	res.Pass = got == int64(*messages) && q.Messages == 0
	res.Detail = fmt.Sprintf("qps=%.0f got=%d elapsedMs=%.1f ready=%d unacked=%d brokerCpu=%.1f clientCpu=%.1f prefetch=%d consumers=%d",
		res.QPS, got, elapsed.Seconds()*1000, q.Ready, q.Unacked, brokerCPU, clientCPU, *prefetch, *consumerN)
	deleteQueue(name)
	return res
}

func consumeCounted(name string, n, consumers, prefetch int) (got int64, elapsed time.Duration, brokerCPU, clientCPU float64) {
	if consumers < 1 {
		consumers = 1
	}
	var count atomic.Int64
	done := make(chan struct{})
	var once sync.Once
	var conns []*amqp.Connection
	pid := brokerPID()
	b0 := cpuTicks(pid)
	c0 := cpuTicks(os.Getpid())
	t0 := time.Now()
	for i := 0; i < consumers; i++ {
		c := openConsumer(name, prefetch)
		conns = append(conns, c.conn)
		go func(msgs <-chan amqp.Delivery) {
			for d := range msgs {
				if err := d.Ack(false); err != nil {
					return
				}
				if count.Add(1) >= int64(n) {
					once.Do(func() { close(done) })
					return
				}
			}
		}(c.msgs)
	}
	select {
	case <-done:
	case <-time.After(120 * time.Second):
	}
	elapsed = time.Since(t0)
	got = count.Load()
	brokerCPU = cpuPercent(b0, cpuTicks(pid), elapsed.Seconds())
	clientCPU = cpuPercent(c0, cpuTicks(os.Getpid()), elapsed.Seconds())
	for _, c := range conns {
		c.Close()
	}
	return got, elapsed, brokerCPU, clientCPU
}

func scenarioWinners() result {
	name := "ms11-winners"
	res := result{
		Scenario: "winners",
		Note:     "5000 条持久化消息，4 条连接，等全部 confirm，再用 1 个消费者 prefetch=250 读完。墙钟很短时不把 CPU 当容量。",
		Messages: int64(*messages),
		Payload:  *payloadN,
		Workers:  *workers,
	}
	if err := resetQueue(name, true, classicArgs(nil)); err != nil {
		res.Detail = err.Error()
		return res
	}
	pid := brokerPID()
	b0 := cpuTicks(pid)
	acks, pubElapsed, err := confirmExact(name, *messages, *workers, *payloadN, true)
	b1 := cpuTicks(pid)
	if err != nil {
		res.Detail = fmt.Sprintf("publish: %v acks=%d", err, acks)
		deleteQueue(name)
		return res
	}
	got, consElapsed, _, _ := consumeCounted(name, *messages, 1, *prefetch)
	q, _ := waitQueue(name, 2*time.Second, func(q qStat) bool { return q.Messages == 0 })
	if q.Name == "" {
		q, _ = findQueue(name)
	}
	res.Confirms = acks
	res.Deliveries = got
	res.ElapsedSec = pubElapsed.Seconds()
	res.DrainSec = consElapsed.Seconds()
	res.BrokerCPU = cpuPercent(b0, b1, pubElapsed.Seconds())
	res.Pass = acks == int64(*messages) && got == int64(*messages) && q.Messages == 0
	res.Detail = fmt.Sprintf("publishMs=%.1f consumeMs=%.1f acks=%d got=%d left=%d brokerCpuPublish=%.1f",
		pubElapsed.Seconds()*1000, consElapsed.Seconds()*1000, acks, got, q.Messages, res.BrokerCPU)
	deleteQueue(name)
	return res
}

func scenarioDisk() result {
	name := "ms11-disk"
	res := result{
		Scenario: "disk",
		Note:     "同一段 200 字节写进持久化经典队列，不压缩。message_bytes 和 mnesia 目录的前后差分开记。目录差包含段文件，不只是正文。",
		Payload:  *payloadN,
		Messages: int64(*messages),
	}
	if err := resetQueue(name, true, classicArgs(nil)); err != nil {
		res.Detail = err.Error()
		return res
	}
	app0, alloc0, err := diskUsage()
	if err != nil {
		res.Detail = err.Error()
		return res
	}
	rss0 := rssKB(brokerPID())
	acks, elapsed, err := confirmExact(name, *messages, *workers, *payloadN, true)
	app1, alloc1, err2 := diskUsage()
	q, _ := findQueue(name)
	res.Confirms = acks
	res.ElapsedSec = elapsed.Seconds()
	res.Messages = q.Messages
	res.MessageBytes = q.MessageBytes
	res.QueueMemory = q.Memory
	res.QueueType = q.Type
	res.BrokerRSSKB = rssKB(brokerPID())
	res.DiskApparentDelta = app1 - app0
	res.DiskAllocDelta = alloc1 - alloc0
	if acks > 0 {
		res.BytesPerMessage = float64(q.MessageBytes) / float64(q.Messages)
		res.MemoryPerMessage = float64(q.Memory) / float64(q.Messages)
		res.ApparentPerMessage = float64(app1-app0) / float64(acks)
		res.AllocPerMessage = float64(alloc1-alloc0) / float64(acks)
	}
	if err != nil || err2 != nil {
		res.Detail = fmt.Sprintf("publish=%v du=%v acks=%d", err, err2, acks)
		deleteQueue(name)
		return res
	}
	res.Pass = acks == int64(*messages) && q.Messages == acks
	res.Detail = fmt.Sprintf("acks=%d messages=%d messageBytes=%d bytesPer=%.2f queueMem=%d memPer=%.2f apparentDelta=%d apparentPer=%.2f allocDelta=%d allocPer=%.2f rssKB %d->%d elapsedMs=%.1f type=%s",
		acks, q.Messages, q.MessageBytes, res.BytesPerMessage, q.Memory, res.MemoryPerMessage,
		res.DiskApparentDelta, res.ApparentPerMessage, res.DiskAllocDelta, res.AllocPerMessage,
		rss0, res.BrokerRSSKB, elapsed.Seconds()*1000, q.Type)
	deleteQueue(name)
	return res
}

func scenarioAlarm() result {
	res := result{
		Scenario: "alarm",
		Note:     "水位是启动时空节点 RSS 再加 256MiB。持续发布持久化消息，直到内存告警或到点。告警按 RSS，不是 Erlang allocated。",
	}
	wipeQueues()
	names, err := declareNames("ms11-alarm", 1, true, false)
	if err != nil {
		res.Detail = err.Error()
		return res
	}
	out, err := publishWindow(pubOpt{
		names:       names,
		workers:     *workers,
		payload:     *payloadN,
		persistent:  true,
		confirms:    true,
		warmup:      0,
		duration:    *duration,
		stopOnAlarm: true,
		drain:       false,
	})
	if err != nil {
		res.Detail = err.Error()
		return res
	}
	fillProduce(&res, pubOpt{
		names: names, workers: *workers, payload: *payloadN, persistent: true, confirms: true,
	}, out)
	res.Scenario = "alarm"
	res.Note = "水位是启动时空节点 RSS 再加 256MiB。持续发布持久化消息，直到内存告警或到点。告警按 RSS，不是 Erlang allocated。"
	// 到点没告警也是结果。0 条确认才算失败。
	res.Pass = out.errors == 0 && out.counted > 0
	return res
}

func scenarioWipe() result {
	wipeQueues()
	return result{Scenario: "wipe", Note: "删掉 ms11- 前缀的队列，给重启测试一个短的恢复。", Pass: true, Detail: "wiped"}
}
