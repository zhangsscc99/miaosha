package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	rocketmq "github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

func scenarioPing() result {
	topic := topicName()
	note := "建 topic 之后发 1 条再读 1 条。routeMs 是 NameServer 上能查到队列的等待。"
	wait, err := ensureTopic(topic, 1)
	if err != nil {
		return fail(note, err)
	}
	p, err := newProducer(1)
	if err != nil {
		return fail(note, err)
	}
	defer p.Shutdown()
	var got int64
	c, err := startPush(consumeOpts{group: topic + "_g", topic: topic, instance: topic + "-c", threads: 1}, func(_ context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		atomic.AddInt64(&got, int64(len(msgs)))
		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		return fail(note, err)
	}
	defer c.Shutdown()
	time.Sleep(300 * time.Millisecond)
	if err := sendSync(p, primitive.NewMessage(topic, []byte("id:1"))); err != nil {
		return fail(note, err)
	}
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) && atomic.LoadInt64(&got) < 1 {
		time.Sleep(50 * time.Millisecond)
	}
	g := atomic.LoadInt64(&got)
	res := result{Scenario: "ping", Note: note, Pass: g == 1, Topic: topic, Deliveries: g, RouteMs: wait.Milliseconds(), Messages: 1}
	res.Detail = fmt.Sprintf("got=%d routeMs=%d", g, res.RouteMs)
	return res
}

func levelWait(level int) int64 {
	switch level {
	case 1:
		return 1000
	case 2:
		return 5000
	default:
		return 1000
	}
}

func scenarioDelay() result {
	topic := topicName()
	level := *delayLevel
	expect := levelWait(level)
	n := *messages
	note := "延迟级别 1 是 1 秒，级别 2 是 5 秒，和发行版默认的前两档一样。用出生时间算，不用第二次写入的存储时间。"
	if _, err := ensureTopic(topic, 1); err != nil {
		return fail(note, err)
	}
	var mu sync.Mutex
	var got int
	var minB, maxB, minS, maxS int64
	var early int64
	c, err := startPush(consumeOpts{group: topic + "_g", topic: topic, instance: topic + "-c", threads: 2}, func(_ context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		now := time.Now().UnixMilli()
		mu.Lock()
		defer mu.Unlock()
		for _, m := range msgs {
			got++
			b := now - m.BornTimestamp
			s := now - m.StoreTimestamp
			if got == 1 {
				minB, maxB, minS, maxS = b, b, s, s
			} else {
				if b < minB {
					minB = b
				}
				if b > maxB {
					maxB = b
				}
				if s < minS {
					minS = s
				}
				if s > maxS {
					maxS = s
				}
			}
			if b < expect-200 {
				early++
			}
		}
		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		return fail(note, err)
	}
	defer c.Shutdown()
	time.Sleep(300 * time.Millisecond)
	p, err := newProducer(1)
	if err != nil {
		return fail(note, err)
	}
	defer p.Shutdown()
	for i := 0; i < n; i++ {
		msg := primitive.NewMessage(topic, []byte(fmt.Sprintf("id:%d", i)))
		msg.WithDelayTimeLevel(level)
		if err := sendSync(p, msg); err != nil {
			return fail(note, err)
		}
	}
	deadline := time.Now().Add(time.Duration(expect+8000) * time.Millisecond)
	for time.Now().Before(deadline) {
		mu.Lock()
		g := got
		mu.Unlock()
		if g >= n {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	pass := got == n && early == 0 && minB >= expect-200 && maxB <= expect+5000
	res := result{
		Scenario: "delay", Note: note, Pass: pass, Topic: topic, Messages: n,
		Deliveries: int64(got), Early: early, MinBornMs: minB, MaxBornMs: maxB, MinStoreMs: minS, MaxStoreMs: maxS,
	}
	res.Detail = fmt.Sprintf("level=%d expectMs=%d got=%d early=%d born=%d..%d store=%d..%d", level, expect, got, early, minB, maxB, minS, maxS)
	return res
}

func scenarioTag() result {
	topic := topicName()
	note := "消费者只订 TagA。TagB 不应该被投递。"
	n := *messages
	if _, err := ensureTopic(topic, 1); err != nil {
		return fail(note, err)
	}
	var gotA, gotB int64
	c, err := startPush(consumeOpts{group: topic + "_g", topic: topic, instance: topic + "-c", tag: "TagA", threads: 2}, func(_ context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, m := range msgs {
			if m.GetTags() == "TagA" {
				atomic.AddInt64(&gotA, 1)
			} else {
				atomic.AddInt64(&gotB, 1)
			}
		}
		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		return fail(note, err)
	}
	defer c.Shutdown()
	time.Sleep(300 * time.Millisecond)
	p, err := newProducer(1)
	if err != nil {
		return fail(note, err)
	}
	defer p.Shutdown()
	for i := 0; i < n; i++ {
		tag := "TagA"
		if i%2 == 1 {
			tag = "TagB"
		}
		msg := primitive.NewMessage(topic, []byte(fmt.Sprintf("id:%d", i)))
		msg.WithTag(tag)
		if err := sendSync(p, msg); err != nil {
			return fail(note, err)
		}
	}
	deadline := time.Now().Add(8 * time.Second)
	want := int64(n / 2)
	for time.Now().Before(deadline) && atomic.LoadInt64(&gotA) < want {
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(400 * time.Millisecond)
	a, b := atomic.LoadInt64(&gotA), atomic.LoadInt64(&gotB)
	res := result{Scenario: "tag", Note: note, Pass: a == want && b == 0, Topic: topic, Messages: n, Deliveries: a, Orders: b}
	res.Detail = fmt.Sprintf("tagA=%d tagB=%d wantA=%d", a, b, want)
	return res
}

func scenarioAssign() result {
	topic := topicName()
	note := "集群消费里一个队列只分给组内一个消费者。队列比消费者少时，多出来的消费者是 0。"
	n := *messages
	if _, err := ensureTopic(topic, *queueN); err != nil {
		return fail(note, err)
	}
	type box struct {
		mu      sync.Mutex
		unique  map[int]struct{}
		deliver int
	}
	boxes := make([]*box, *consumerN)
	var cs []*consumerPush
	for i := 0; i < *consumerN; i++ {
		b := &box{unique: map[int]struct{}{}}
		boxes[i] = b
		c, err := startPush(consumeOpts{
			group:    topic + "_g",
			topic:    topic,
			instance: fmt.Sprintf("%s-%d", topic, i),
			threads:  4,
		}, func(_ context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
			b.mu.Lock()
			for _, m := range msgs {
				b.deliver++
				if id, ok := parseID(m.Body); ok {
					b.unique[id] = struct{}{}
				}
			}
			b.mu.Unlock()
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
	time.Sleep(800 * time.Millisecond)
	p, err := newProducer(*queueN)
	if err != nil {
		return fail(note, err)
	}
	for i := 0; i < n; i++ {
		msg := primitive.NewMessage(topic, []byte(fmt.Sprintf("id:%d", i)))
		if err := sendSync(p, msg); err != nil {
			p.Shutdown()
			return fail(note, err)
		}
	}
	p.Shutdown()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		sum := 0
		for _, b := range boxes {
			b.mu.Lock()
			sum += len(b.unique)
			b.mu.Unlock()
		}
		if sum >= n {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(400 * time.Millisecond)
	counts := make([]int, len(boxes))
	var deliveries int
	zeros := 0
	for i, b := range boxes {
		b.mu.Lock()
		counts[i] = len(b.unique)
		deliveries += b.deliver
		if len(b.unique) == 0 {
			zeros++
		}
		b.mu.Unlock()
	}
	sum := 0
	for _, c := range counts {
		sum += c
	}
	pass := sum == n && deliveries == n
	if *queueN < *consumerN {
		pass = pass && zeros == *consumerN-*queueN
	} else if *consumerN > 1 {
		pass = pass && zeros == 0
	}
	res := result{
		Scenario: "assign", Note: note, Pass: pass, Topic: topic, Queues: *queueN, Consumers: *consumerN,
		Messages: n, Unique: int64(sum), Deliveries: int64(deliveries), Counts: counts,
	}
	res.Detail = fmt.Sprintf("counts=%v sum=%d deliveries=%d zeros=%d", counts, sum, deliveries, zeros)
	return res
}

func scenarioOrder(orderly bool) result {
	topic := topicName()
	name := "concurrent"
	note := "消息先写进同一个队列，再启动消费者。0 号睡 400 毫秒。并发消费应该看到后面的号先完成；顺序消费不应该。"
	if orderly {
		name = "orderly"
	}
	n := *messages
	if n < 8 {
		n = 32
	}
	if _, err := ensureTopic(topic, 1); err != nil {
		return fail(note, err)
	}
	p, err := newProducer(1)
	if err != nil {
		return fail(note, err)
	}
	for i := 0; i < n; i++ {
		msg := primitive.NewMessage(topic, []byte(fmt.Sprintf("id:%d", i)))
		if err := sendSync(p, msg); err != nil {
			p.Shutdown()
			return fail(note, err)
		}
	}
	p.Shutdown()
	var mu sync.Mutex
	var order []int
	c, err := startPush(consumeOpts{
		group: topic + "_g", topic: topic, instance: topic + "-c", threads: 8, orderly: orderly,
	}, func(_ context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, m := range msgs {
			id, _ := parseID(m.Body)
			if id == 0 {
				time.Sleep(400 * time.Millisecond)
			}
			mu.Lock()
			order = append(order, id)
			mu.Unlock()
		}
		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		return fail(note, err)
	}
	defer c.Shutdown()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		g := len(order)
		mu.Unlock()
		if g >= n {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	seen := map[int]struct{}{}
	inversion := false
	saw0 := false
	orderOK := len(order) == n
	for i, id := range order {
		seen[id] = struct{}{}
		if id == 0 {
			saw0 = true
		} else if !saw0 {
			inversion = true
		}
		if id != i {
			orderOK = false
		}
	}
	pass := len(seen) == n && len(order) == n
	if orderly {
		pass = pass && orderOK && !inversion
	} else {
		pass = pass && inversion
	}
	res := result{
		Scenario: name, Note: note, Pass: pass, Topic: topic, Messages: n,
		Deliveries: int64(len(order)), Unique: int64(len(seen)), Inversion: inversion, OrderOK: orderOK,
	}
	head := order
	if len(head) > 8 {
		head = head[:8]
	}
	res.Detail = fmt.Sprintf("n=%d got=%d inversion=%v orderOk=%v head=%v", n, len(order), inversion, orderOK, head)
	return res
}

func scenarioRetry() result {
	topic := topicName()
	note := "maxReconsumeTimes=2。消费失败后 broker 在重试次数达到 2 时把消息放进 %DLQ%。级别 3 和 4 在这次的配置里是 1 秒，不是发行版的 10 秒和 30 秒。"
	n := *messages
	if _, err := ensureTopic(topic, 1); err != nil {
		return fail(note, err)
	}
	var attempts int64
	c, err := startPush(consumeOpts{
		group: topic + "_g", topic: topic, instance: topic + "-c", threads: 4, maxRetry: 2,
	}, func(_ context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		atomic.AddInt64(&attempts, int64(len(msgs)))
		return consumer.ConsumeRetryLater, nil
	})
	if err != nil {
		return fail(note, err)
	}
	defer c.Shutdown()
	time.Sleep(300 * time.Millisecond)
	p, err := newProducer(1)
	if err != nil {
		return fail(note, err)
	}
	for i := 0; i < n; i++ {
		msg := primitive.NewMessage(topic, []byte(fmt.Sprintf("id:%d", i)))
		if err := sendSync(p, msg); err != nil {
			p.Shutdown()
			return fail(note, err)
		}
	}
	p.Shutdown()
	dlqTopic := "%DLQ%" + topic + "_g"
	deadline := time.Now().Add(20 * time.Second)
	var dlq *consumerPush
	for time.Now().Before(deadline) {
		if err := waitRoute(dlqTopic, 1500*time.Millisecond); err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	var dlqN int64
	dlq, err = startPush(consumeOpts{
		group: topic + "_dlq_g", topic: dlqTopic, instance: topic + "-dlq", threads: 2,
	}, func(_ context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		atomic.AddInt64(&dlqN, int64(len(msgs)))
		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		return result{Scenario: "retry", Note: note, Pass: false, Topic: topic, Attempts: atomic.LoadInt64(&attempts), Detail: "dlq consumer: " + err.Error()}
	}
	defer dlq.Shutdown()
	deadline = time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && atomic.LoadInt64(&dlqN) < int64(n) {
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond)
	a, d := atomic.LoadInt64(&attempts), atomic.LoadInt64(&dlqN)
	res := result{
		Scenario: "retry", Note: note, Pass: d == int64(n) && a == int64(n*3),
		Topic: topic, Messages: n, Attempts: a, DLQ: d,
	}
	res.Detail = fmt.Sprintf("attempts=%d dlq=%d wantAttempts=%d wantDlq=%d", a, d, n*3, n)
	return res
}

func scenarioDedup() result {
	topic := topicName()
	note := "broker 不按业务键去重。120 次发送，应用按用户收成 100。"
	if _, err := ensureTopic(topic, 1); err != nil {
		return fail(note, err)
	}
	var mu sync.Mutex
	seen := map[int]struct{}{}
	var deliveries int64
	c, err := startPush(consumeOpts{group: topic + "_g", topic: topic, instance: topic + "-c", threads: 4}, func(_ context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		mu.Lock()
		for _, m := range msgs {
			deliveries++
			if id, ok := parseID(m.Body); ok {
				seen[id] = struct{}{}
			}
		}
		mu.Unlock()
		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		return fail(note, err)
	}
	defer c.Shutdown()
	time.Sleep(300 * time.Millisecond)
	p, err := newProducer(1)
	if err != nil {
		return fail(note, err)
	}
	sendN := 0
	for i := 0; i < 100; i++ {
		msg := primitive.NewMessage(topic, []byte(fmt.Sprintf("id:%d", i)))
		msg.WithKeys([]string{fmt.Sprintf("u%d", i)})
		if err := sendSync(p, msg); err != nil {
			p.Shutdown()
			return fail(note, err)
		}
		sendN++
	}
	for i := 0; i < 20; i++ {
		msg := primitive.NewMessage(topic, []byte(fmt.Sprintf("id:%d", i)))
		msg.WithKeys([]string{fmt.Sprintf("u%d", i)})
		if err := sendSync(p, msg); err != nil {
			p.Shutdown()
			return fail(note, err)
		}
		sendN++
	}
	p.Shutdown()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && atomic.LoadInt64(&deliveries) < int64(sendN) {
		time.Sleep(30 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond)
	mu.Lock()
	u := len(seen)
	d := deliveries
	mu.Unlock()
	res := result{Scenario: "dedup", Note: note, Pass: u == 100 && d == 120, Topic: topic, Unique: int64(u), Deliveries: d, Messages: sendN}
	res.Detail = fmt.Sprintf("unique=%d deliveries=%d sent=%d", u, d, sendN)
	return res
}

func scenarioAckEarly() result {
	topic := topicName()
	note := "先返回成功再决定记不记。id 能被 5 整除的 20 条不记。同一组再启动，第二次不应该再读到。"
	n := 100
	if _, err := ensureTopic(topic, 1); err != nil {
		return fail(note, err)
	}
	var mu sync.Mutex
	orders := map[int]struct{}{}
	var got int64
	c, err := startPush(consumeOpts{group: topic + "_g", topic: topic, instance: topic + "-c1", threads: 1}, func(_ context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, m := range msgs {
			atomic.AddInt64(&got, 1)
			id, ok := parseID(m.Body)
			if ok && id%5 != 0 {
				mu.Lock()
				orders[id] = struct{}{}
				mu.Unlock()
			}
		}
		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		return fail(note, err)
	}
	time.Sleep(300 * time.Millisecond)
	p, err := newProducer(1)
	if err != nil {
		c.Shutdown()
		return fail(note, err)
	}
	for i := 0; i < n; i++ {
		if err := sendSync(p, primitive.NewMessage(topic, []byte(fmt.Sprintf("id:%d", i)))); err != nil {
			p.Shutdown()
			c.Shutdown()
			return fail(note, err)
		}
	}
	p.Shutdown()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && atomic.LoadInt64(&got) < int64(n) {
		time.Sleep(20 * time.Millisecond)
	}
	c.Shutdown()
	time.Sleep(300 * time.Millisecond)
	var second int64
	c2, err := startPush(consumeOpts{group: topic + "_g", topic: topic, instance: topic + "-c2", threads: 1}, func(_ context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		atomic.AddInt64(&second, int64(len(msgs)))
		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		return fail(note, err)
	}
	defer c2.Shutdown()
	time.Sleep(2 * time.Second)
	mu.Lock()
	o := len(orders)
	mu.Unlock()
	res := result{
		Scenario: "ack-early", Note: note, Pass: atomic.LoadInt64(&got) == int64(n) && o == 80 && atomic.LoadInt64(&second) == 0,
		Topic: topic, Deliveries: atomic.LoadInt64(&got), Orders: int64(o), SecondFetch: atomic.LoadInt64(&second), Messages: n,
	}
	res.Detail = fmt.Sprintf("first=%d orders=%d second=%d", res.Deliveries, o, res.SecondFetch)
	return res
}

func scenarioBroadcast() result {
	topic := topicName()
	note := "广播消费。两个消费者都要拿到全部，不是对半。"
	n := *messages
	if _, err := ensureTopic(topic, 1); err != nil {
		return fail(note, err)
	}
	var a, b int64
	c1, err := startPush(consumeOpts{group: topic + "_g", topic: topic, instance: topic + "-b1", threads: 1, broadcast: true}, func(_ context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		atomic.AddInt64(&a, int64(len(msgs)))
		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		return fail(note, err)
	}
	defer c1.Shutdown()
	c2, err := startPush(consumeOpts{group: topic + "_g", topic: topic, instance: topic + "-b2", threads: 1, broadcast: true}, func(_ context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		atomic.AddInt64(&b, int64(len(msgs)))
		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		return fail(note, err)
	}
	defer c2.Shutdown()
	time.Sleep(500 * time.Millisecond)
	p, err := newProducer(1)
	if err != nil {
		return fail(note, err)
	}
	defer p.Shutdown()
	for i := 0; i < n; i++ {
		if err := sendSync(p, primitive.NewMessage(topic, []byte(fmt.Sprintf("id:%d", i)))); err != nil {
			return fail(note, err)
		}
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && (atomic.LoadInt64(&a) < int64(n) || atomic.LoadInt64(&b) < int64(n)) {
		time.Sleep(30 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond)
	av, bv := atomic.LoadInt64(&a), atomic.LoadInt64(&b)
	res := result{Scenario: "broadcast", Note: note, Pass: av == int64(n) && bv == int64(n), Topic: topic, Messages: n, Counts: []int{int(av), int(bv)}}
	res.Detail = fmt.Sprintf("c1=%d c2=%d want=%d", av, bv, n)
	return res
}

type txState struct {
	mu sync.Mutex
	m  map[string]primitive.LocalTransactionState
}

func (s *txState) ExecuteLocalTransaction(msg *primitive.Message) primitive.LocalTransactionState {
	act := msg.GetProperty("act")
	var st primitive.LocalTransactionState
	switch act {
	case "commit":
		st = primitive.CommitMessageState
	case "rollback":
		st = primitive.RollbackMessageState
	default:
		st = primitive.UnknowState
	}
	s.mu.Lock()
	s.m[msg.TransactionId] = st
	s.mu.Unlock()
	return st
}

func (s *txState) CheckLocalTransaction(msg *primitive.MessageExt) primitive.LocalTransactionState {
	s.mu.Lock()
	st, ok := s.m[msg.TransactionId]
	s.mu.Unlock()
	if !ok || st == primitive.UnknowState {
		return primitive.CommitMessageState
	}
	return st
}

func scenarioTx() result {
	topic := topicName()
	note := "6 条本地事务提交，3 条回滚，1 条未知。未知的等 broker 回查，回查把它提交。transactionTimeOut=1000，transactionCheckInterval=2000，代码默认是 6000 和 30000。"
	if _, err := ensureTopic(topic, 1); err != nil {
		return fail(note, err)
	}
	var mu sync.Mutex
	got := map[int]struct{}{}
	c, err := startPush(consumeOpts{group: topic + "_c", topic: topic, instance: topic + "-c", threads: 2}, func(_ context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		mu.Lock()
		for _, m := range msgs {
			if id, ok := parseID(m.Body); ok {
				got[id] = struct{}{}
			}
		}
		mu.Unlock()
		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		return fail(note, err)
	}
	defer c.Shutdown()
	time.Sleep(300 * time.Millisecond)
	st := &txState{m: map[string]primitive.LocalTransactionState{}}
	p, err := rocketmq.NewTransactionProducer(
		st,
		producer.WithNameServer(primitive.NamesrvAddr{*namesrv}),
		producer.WithGroupName(topic+"_pg"),
		producer.WithInstanceName(topic+"-tx"),
		producer.WithVIPChannel(false),
		producer.WithSendMsgTimeout(*sendTimeout),
		producer.WithRetry(1),
	)
	if err != nil {
		return fail(note, err)
	}
	if err := p.Start(); err != nil {
		return fail(note, err)
	}
	defer p.Shutdown()
	for i := 0; i < 10; i++ {
		act := "commit"
		if i >= 6 && i <= 8 {
			act = "rollback"
		} else if i == 9 {
			act = "unknown"
		}
		msg := primitive.NewMessage(topic, []byte(fmt.Sprintf("id:%d", i)))
		msg.WithProperty("act", act)
		if _, err := p.SendMessageInTransaction(context.Background(), msg); err != nil {
			return fail(note, err)
		}
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		g := len(got)
		mu.Unlock()
		if g >= 7 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	missing := []int{}
	extra := []int{}
	want := map[int]struct{}{0: {}, 1: {}, 2: {}, 3: {}, 4: {}, 5: {}, 9: {}}
	for id := range want {
		if _, ok := got[id]; !ok {
			missing = append(missing, id)
		}
	}
	for id := range got {
		if _, ok := want[id]; !ok {
			extra = append(extra, id)
		}
	}
	res := result{Scenario: "tx", Note: note, Pass: len(missing) == 0 && len(extra) == 0 && len(got) == 7, Topic: topic, Unique: int64(len(got)), Messages: 10}
	res.Detail = fmt.Sprintf("got=%d missing=%v extra=%v", len(got), missing, extra)
	return res
}

func scenarioNamesrvDown() result {
	topic := topicName()
	note := "先发 1 条把路由放进生产者。再 kill -9 NameServer。老生产者继续发；新生产者在超时里发不出去。"
	if *namesrvPid <= 0 {
		return result{Scenario: "namesrv-down", Note: note, Pass: false, Detail: "missing namesrv pid"}
	}
	if _, err := ensureTopic(topic, 1); err != nil {
		return fail(note, err)
	}
	p, err := newProducer(1)
	if err != nil {
		return fail(note, err)
	}
	defer p.Shutdown()
	if err := sendSync(p, primitive.NewMessage(topic, []byte("id:0"))); err != nil {
		return fail(note, err)
	}
	if err := syscall.Kill(*namesrvPid, syscall.SIGKILL); err != nil {
		return fail(note, err)
	}
	time.Sleep(200 * time.Millisecond)
	var okN, errN int64
	for i := 0; i < 50; i++ {
		msg := primitive.NewMessage(topic, []byte(fmt.Sprintf("id:%d", i+1)))
		if err := sendSync(p, msg); err != nil {
			errN++
		} else {
			okN++
		}
	}
	freshFail := false
	done := make(chan struct{})
	go func() {
		defer close(done)
		p2, err := newProducer(1)
		if err != nil {
			freshFail = true
			return
		}
		defer p2.Shutdown()
		if err := sendSync(p2, primitive.NewMessage(topic, []byte("id:fresh"))); err != nil {
			freshFail = true
		}
	}()
	select {
	case <-done:
	case <-time.After(6 * time.Second):
		freshFail = true
	}
	res := result{
		Scenario: "namesrv-down", Note: note, Pass: okN == 50 && errN == 0 && freshFail,
		Topic: topic, CachedOK: okN, Errors: errN, FreshFail: freshFail,
	}
	res.Detail = fmt.Sprintf("cachedOk=%d cachedErr=%d freshFail=%v", okN, errN, freshFail)
	return res
}
