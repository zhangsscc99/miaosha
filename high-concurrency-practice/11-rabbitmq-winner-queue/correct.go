package main

import (
	"fmt"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func scenarioUnroutable(mandatory bool) result {
	name := "ms11-unroutable"
	res := result{
		Scenario: "unroutable",
		Note:     "默认交换机上，路由键对应的队列不存在。confirm 只表示 broker 接过了这条发布，不表示进了队列。仓库的 sendMiaoshaMessage 没有 mandatory。",
	}
	if mandatory {
		res.Scenario = "unroutable-mandatory"
		res.Note = "同样的队列名加上 mandatory。basic.return 会先回来，confirm 仍然是 ack。"
	}
	// 删掉同名队列，避免上一次运行把它声明出来。
	conn := dial()
	ch := channel(conn)
	_, _ = ch.QueueDelete(name, false, false, false)
	conn.Close()

	bodies := ids(1, 100)
	acks, nacks, returns, err := publishBodies("", name, bodies, true, mandatory)
	res.Acks = int64(acks)
	res.Nacks = int64(nacks)
	res.Returns = int64(returns)
	res.Published = int64(len(bodies))
	_, exists := findQueue(name)
	absent := !exists
	if err != nil {
		res.Detail = err.Error()
		return res
	}
	res.Pass = acks == 100 && nacks == 0 && absent && ((mandatory && returns == 100) || (!mandatory && returns == 0))
	res.Detail = fmt.Sprintf("acks=%d nacks=%d returns=%d queueAbsent=%v", acks, nacks, returns, absent)
	return res
}

func scenarioManualStall() result {
	name := "ms11-manual-stall"
	res := result{
		Scenario: "manual-stall",
		Note:     "配置是 manual、prefetch=1、concurrency=10。receive(String) 没有 Channel，spring-amqp 2.4.0 在 manual 下正常返回不会 basicAck。这里用 10 个消费者各拿 1 条不确认。",
	}
	if err := resetQueue(name, true, classicArgs(nil)); err != nil {
		res.Detail = err.Error()
		return res
	}
	var hold []chan struct{}
	var conns []*amqp.Connection
	got := make(chan struct{}, 16)
	for i := 0; i < 10; i++ {
		c := openConsumer(name, 1)
		conns = append(conns, c.conn)
		h := make(chan struct{})
		hold = append(hold, h)
		go func(msgs <-chan amqp.Delivery, h chan struct{}) {
			select {
			case <-msgs:
				got <- struct{}{}
			case <-time.After(5 * time.Second):
			}
			<-h
		}(c.msgs, h)
	}
	if _, ok := waitQueue(name, 3*time.Second, func(q qStat) bool { return q.Consumers == 10 }); !ok {
		res.Detail = "consumers did not reach 10"
		release(hold, conns)
		return res
	}
	acks, nacks, _, err := publishBodies("", name, ids(1, 100), true, false)
	if err != nil {
		res.Detail = err.Error()
		release(hold, conns)
		return res
	}
	deadline := time.Now().Add(3 * time.Second)
	for len(got) < 10 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	q, ok := waitQueue(name, 3*time.Second, func(q qStat) bool {
		return q.Unacked == 10 && q.Ready == 90
	})
	res.Ready = q.Ready
	res.Unacked = q.Unacked
	res.Consumers = q.Consumers
	res.Acks = int64(acks)
	res.Nacks = int64(nacks)
	// 关掉连接，未确认的消息重新入队。
	for _, c := range conns {
		c.Close()
	}
	q2, ok2 := waitQueue(name, 3*time.Second, func(q qStat) bool {
		return q.Unacked == 0 && q.Ready == 100
	})
	c := openConsumer(name, 50)
	n, re := takeFor(c.msgs, 2*time.Second)
	c.conn.Close()
	release(hold, nil)
	res.Deliveries = int64(n)
	res.Redelivered = int64(re)
	// 只有已经被投递过的 10 条会带 redelivered。没投递过的 90 条重新可投，但标记是 false。
	res.Pass = ok && ok2 && n == 100 && re == 10 && acks == 100
	res.Detail = fmt.Sprintf("held ready=%d unacked=%d afterClose ready=%d unacked=%d second=%d redelivered=%d", q.Ready, q.Unacked, q2.Ready, q2.Unacked, n, re)
	return res
}

func release(hold []chan struct{}, conns []*amqp.Connection) {
	for _, h := range hold {
		select {
		case <-h:
		default:
			close(h)
		}
	}
	for _, c := range conns {
		if c != nil {
			c.Close()
		}
	}
}

func scenarioAutoReturn() result {
	name := "ms11-auto-return"
	res := result{
		Scenario: "auto-return",
		Note:     "如果确认模式是自动，方法正常返回就会确认。stock<=0 直接 return 的那 20 条被确认，订单没有，队列里也不再有。这不是当前 manual 配置的路径。",
	}
	if err := resetQueue(name, true, classicArgs(nil)); err != nil {
		res.Detail = err.Error()
		return res
	}
	acks, _, _, err := publishBodies("", name, ids(1, 100), true, false)
	if err != nil {
		res.Detail = err.Error()
		return res
	}
	c := openConsumer(name, 200)
	ds, err := take(c.msgs, 100, 5*time.Second)
	if err != nil {
		res.Detail = err.Error()
		c.conn.Close()
		return res
	}
	orders := 0
	for _, d := range ds {
		var id int
		fmt.Sscanf(string(d.Body), "%d", &id)
		_ = d.Ack(false)
		if id%5 != 0 {
			orders++
		}
	}
	c.conn.Close()
	c2 := openConsumer(name, 10)
	n, _ := takeFor(c2.msgs, 300*time.Millisecond)
	c2.conn.Close()
	q, _ := findQueue(name)
	res.Acks = int64(acks)
	res.Orders = int64(orders)
	res.Deliveries = int64(n)
	res.Ready = q.Ready
	res.Pass = orders == 80 && n == 0 && q.Ready == 0 && q.Unacked == 0
	res.Detail = fmt.Sprintf("orders=%d second=%d ready=%d unacked=%d", orders, n, q.Ready, q.Unacked)
	return res
}

func scenarioAckBeforeInsert() result {
	name := "ms11-ack-before"
	res := result{
		Scenario: "ack-before-insert",
		Note:     "receiveMiaoShaMessage 先 basicAck 再 insert。200 条都先确认，其中 20 条没有记入订单。这些确认不会再投递。",
	}
	if err := resetQueue(name, true, classicArgs(nil)); err != nil {
		res.Detail = err.Error()
		return res
	}
	if _, _, _, err := publishBodies("", name, ids(1, 200), true, false); err != nil {
		res.Detail = err.Error()
		return res
	}
	c := openConsumer(name, 300)
	ds, err := take(c.msgs, 200, 5*time.Second)
	if err != nil {
		res.Detail = err.Error()
		c.conn.Close()
		return res
	}
	orders := map[string]struct{}{}
	for i, d := range ds {
		_ = d.Ack(true)
		if i < 180 {
			orders[string(d.Body)] = struct{}{}
		}
	}
	c.conn.Close()
	c2 := openConsumer(name, 20)
	n, _ := takeFor(c2.msgs, 400*time.Millisecond)
	c2.conn.Close()
	q, _ := findQueue(name)
	res.Orders = int64(len(orders))
	res.Deliveries = int64(n)
	res.Ready = q.Ready
	res.Unacked = q.Unacked
	res.Pass = len(orders) == 180 && n == 0 && q.Ready == 0 && q.Unacked == 0
	res.Detail = fmt.Sprintf("orders=%d second=%d ready=%d unacked=%d", len(orders), n, q.Ready, q.Unacked)
	return res
}

func scenarioAckCurrent(prefetch int) result {
	name := fmt.Sprintf("ms11-ack-current-%d", prefetch)
	res := result{
		Scenario: fmt.Sprintf("ack-current-prefetch%d", prefetch),
		Note:     "先确认当前这一条（multiple=true）再关掉连接，后面已经投递但 tag 更大的消息不在这次确认里，断线后重新入队。prefetch=1 时窗口里只有这一条。",
		Prefetch: prefetch,
	}
	if err := resetQueue(name, true, classicArgs(nil)); err != nil {
		res.Detail = err.Error()
		return res
	}
	if _, _, _, err := publishBodies("", name, ids(1, 50), true, false); err != nil {
		res.Detail = err.Error()
		return res
	}
	c := openConsumer(name, prefetch)
	ds, err := take(c.msgs, 1, 5*time.Second)
	if err != nil {
		res.Detail = err.Error()
		c.conn.Close()
		return res
	}
	if err := ds[0].Ack(true); err != nil {
		res.Detail = err.Error()
		c.conn.Close()
		return res
	}
	c.conn.Close()
	q, ok := waitQueue(name, 3*time.Second, func(q qStat) bool {
		return q.Unacked == 0 && q.Ready == 49
	})
	res.Ready = q.Ready
	res.Unacked = q.Unacked
	res.Pass = ok && q.Ready == 49 && q.Unacked == 0
	res.Detail = fmt.Sprintf("prefetch=%d ready=%d unacked=%d", prefetch, q.Ready, q.Unacked)
	return res
}

func scenarioAckMax() result {
	name := "ms11-ack-max"
	res := result{
		Scenario: "ack-max-prefetch50",
		Note:     "prefetch=50，把窗口里最大的 delivery tag 用 multiple=true 确认，并且一条订单都不记。这是 basicAck(tag, true) 的语义，不是 receive 现在一条一条调用的形状。",
		Prefetch: 50,
	}
	if err := resetQueue(name, true, classicArgs(nil)); err != nil {
		res.Detail = err.Error()
		return res
	}
	if _, _, _, err := publishBodies("", name, ids(1, 50), true, false); err != nil {
		res.Detail = err.Error()
		return res
	}
	c := openConsumer(name, 50)
	ds, err := take(c.msgs, 50, 5*time.Second)
	if err != nil {
		res.Detail = err.Error()
		c.conn.Close()
		return res
	}
	if err := ds[len(ds)-1].Ack(true); err != nil {
		res.Detail = err.Error()
		c.conn.Close()
		return res
	}
	c.conn.Close()
	q, _ := waitQueue(name, 3*time.Second, func(q qStat) bool {
		return q.Ready == 0 && q.Unacked == 0
	})
	c2 := openConsumer(name, 10)
	n, _ := takeFor(c2.msgs, 400*time.Millisecond)
	c2.conn.Close()
	res.Ready = q.Ready
	res.Unacked = q.Unacked
	res.Deliveries = int64(n)
	res.Pass = q.Ready == 0 && q.Unacked == 0 && n == 0
	res.Detail = fmt.Sprintf("ready=%d unacked=%d second=%d", q.Ready, q.Unacked, n)
	return res
}

func scenarioRedelivery() result {
	name := "ms11-redelivery"
	res := result{
		Scenario: "redelivery",
		Note:     "200 条读完不确认就关连接。下一次仍是 200 条，redelivered，按用户收成 200 单。",
	}
	if err := resetQueue(name, true, classicArgs(nil)); err != nil {
		res.Detail = err.Error()
		return res
	}
	if _, _, _, err := publishBodies("", name, ids(1, 200), true, false); err != nil {
		res.Detail = err.Error()
		return res
	}
	c := openConsumer(name, 300)
	ds, err := take(c.msgs, 200, 5*time.Second)
	if err != nil {
		res.Detail = err.Error()
		c.conn.Close()
		return res
	}
	c.conn.Close()
	if _, ok := waitQueue(name, 3*time.Second, func(q qStat) bool { return q.Ready == 200 && q.Unacked == 0 }); !ok {
		q, _ := findQueue(name)
		res.Detail = fmt.Sprintf("after close ready=%d unacked=%d first=%d", q.Ready, q.Unacked, len(ds))
		return res
	}
	c2 := openConsumer(name, 300)
	ds2, err := take(c2.msgs, 200, 5*time.Second)
	if err != nil {
		res.Detail = err.Error()
		c2.conn.Close()
		return res
	}
	users := map[string]struct{}{}
	re := 0
	for _, d := range ds2 {
		if d.Redelivered {
			re++
		}
		users[string(d.Body)] = struct{}{}
		_ = d.Ack(false)
	}
	c2.conn.Close()
	res.Orders = int64(len(users))
	res.Deliveries = int64(len(ds2))
	res.Redelivered = int64(re)
	res.Pass = len(ds2) == 200 && re == 200 && len(users) == 200
	res.Detail = fmt.Sprintf("second=%d redelivered=%d users=%d", len(ds2), re, len(users))
	return res
}

func scenarioDedup() result {
	name := "ms11-dedup"
	res := result{
		Scenario: "dedup",
		Note:     "100 个用户再写 20 条。confirm 不去重。消费端按用户收成 100 单。",
	}
	if err := resetQueue(name, true, classicArgs(nil)); err != nil {
		res.Detail = err.Error()
		return res
	}
	bodies := append(ids(1, 100), ids(1, 20)...)
	acks, nacks, _, err := publishBodies("", name, bodies, true, false)
	if err != nil {
		res.Detail = err.Error()
		return res
	}
	c := openConsumer(name, 200)
	ds, err := take(c.msgs, len(bodies), 5*time.Second)
	if err != nil {
		res.Detail = err.Error()
		c.conn.Close()
		return res
	}
	users := map[string]int{}
	for _, d := range ds {
		users[string(d.Body)]++
		_ = d.Ack(false)
	}
	c.conn.Close()
	res.Published = int64(len(bodies))
	res.Acks = int64(acks)
	res.Nacks = int64(nacks)
	res.Orders = int64(len(users))
	res.Deliveries = int64(len(ds))
	res.Pass = acks == 120 && nacks == 0 && len(ds) == 120 && len(users) == 100
	res.Detail = fmt.Sprintf("confirms=%d nacks=%d deliveries=%d users=%d", acks, nacks, len(ds), len(users))
	return res
}

func scenarioReject() result {
	name := "ms11-reject"
	res := result{
		Scenario: "reject-publish",
		Note:     "x-max-length=10 且 overflow=reject-publish。多出来的发布被 nack，队列里留下 10 条。不看 confirm 的话，调用方会以为 30 条都发出去了。",
	}
	args := classicArgs(amqp.Table{"x-max-length": int32(10), "x-overflow": "reject-publish"})
	if err := resetQueue(name, true, args); err != nil {
		res.Detail = err.Error()
		return res
	}
	acks, nacks, _, err := publishBodies("", name, ids(1, 30), true, false)
	q, _ := findQueue(name)
	res.Acks = int64(acks)
	res.Nacks = int64(nacks)
	res.Messages = q.Messages
	if err != nil {
		res.Detail = fmt.Sprintf("%s acks=%d nacks=%d messages=%d", err.Error(), acks, nacks, q.Messages)
		return res
	}
	res.Pass = acks == 10 && nacks == 20 && q.Messages == 10
	res.Detail = fmt.Sprintf("acks=%d nacks=%d messages=%d", acks, nacks, q.Messages)
	return res
}

func scenarioDropHead() result {
	name := "ms11-drop-head"
	res := result{
		Scenario: "drop-head",
		Note:     "x-max-length=10 且 overflow=drop-head。200 条发布都会被 confirm，队头被丢掉，只留下最后 10 个用户。",
	}
	args := classicArgs(amqp.Table{"x-max-length": int32(10), "x-overflow": "drop-head"})
	if err := resetQueue(name, true, args); err != nil {
		res.Detail = err.Error()
		return res
	}
	acks, nacks, _, err := publishBodies("", name, ids(1, 200), true, false)
	if err != nil {
		res.Detail = err.Error()
		return res
	}
	c := openConsumer(name, 20)
	ds, err := take(c.msgs, 10, 3*time.Second)
	extra, _ := takeFor(c.msgs, 200*time.Millisecond)
	c.conn.Close()
	got := make([]string, 0, len(ds))
	for _, d := range ds {
		got = append(got, string(d.Body))
		_ = d.Ack(false)
	}
	want := ids(191, 200)
	same := len(got) == len(want)
	if same {
		for i := range want {
			if got[i] != want[i] {
				same = false
			}
		}
	}
	res.Acks = int64(acks)
	res.Nacks = int64(nacks)
	res.Messages = int64(len(got))
	res.Bodies = fmt.Sprint(got)
	res.Pass = err == nil && acks == 200 && nacks == 0 && same && extra == 0
	res.Detail = fmt.Sprintf("acks=%d nacks=%d left=%v extra=%d", acks, nacks, got, extra)
	return res
}

func scenarioCompeting() result {
	name := "ms11-competing"
	res := result{
		Scenario: "competing",
		Note:     "一个经典队列、两个消费者。和第 9 章一个分区两个消费者不一样，这里两边都该分到消息。",
		Prefetch: 10,
	}
	if err := resetQueue(name, true, classicArgs(nil)); err != nil {
		res.Detail = err.Error()
		return res
	}
	c1 := openConsumer(name, 10)
	c2 := openConsumer(name, 10)
	defer c1.conn.Close()
	defer c2.conn.Close()
	if _, ok := waitQueue(name, 3*time.Second, func(q qStat) bool { return q.Consumers == 2 }); !ok {
		res.Detail = "consumers != 2"
		return res
	}
	var n1, n2 int
	done := make(chan struct{})
	go func() {
		defer close(done)
		left := 1000
		t1 := time.NewTimer(10 * time.Second)
		defer t1.Stop()
		for left > 0 {
			select {
			case d, ok := <-c1.msgs:
				if !ok {
					return
				}
				_ = d.Ack(false)
				n1++
				left--
			case d, ok := <-c2.msgs:
				if !ok {
					return
				}
				_ = d.Ack(false)
				n2++
				left--
			case <-t1.C:
				return
			}
		}
	}()
	acks, _, _, err := publishBodies("", name, ids(1, 1000), true, false)
	<-done
	res.Acks = int64(acks)
	res.Split = fmt.Sprintf("%d+%d", n1, n2)
	if err != nil {
		res.Detail = err.Error() + " split=" + res.Split
		return res
	}
	res.Pass = n1 > 0 && n2 > 0 && n1+n2 == 1000 && acks == 1000
	res.Detail = fmt.Sprintf("split=%d+%d confirms=%d", n1, n2, acks)
	return res
}

func scenarioFanout() result {
	res := result{
		Scenario: "fanout",
		Note:     "MQConfig 声明了 fanout，但 sendMiaoshaMessage 没走它。一条发布进两个队列，broker 里是两份。按帮助中心的计数，这是两次接收。",
	}
	conn := dial()
	defer conn.Close()
	ch := channel(conn)
	const ex = "ms11-fanout"
	if err := ch.ExchangeDeclare(ex, "fanout", true, false, false, false, nil); err != nil {
		res.Detail = err.Error()
		return res
	}
	for _, q := range []string{"ms11-fan-1", "ms11-fan-2"} {
		if _, err := ch.QueueDeclare(q, true, false, false, false, classicArgs(nil)); err != nil {
			res.Detail = err.Error()
			return res
		}
		if err := ch.QueueBind(q, "", ex, false, nil); err != nil {
			res.Detail = err.Error()
			return res
		}
	}
	acks, nacks, _, err := publishBodies(ex, "", ids(1, 10), true, false)
	q1, _ := findQueue("ms11-fan-1")
	q2, _ := findQueue("ms11-fan-2")
	res.Acks = int64(acks)
	res.Nacks = int64(nacks)
	if err != nil {
		res.Detail = fmt.Sprintf("%s q1=%d q2=%d", err.Error(), q1.Messages, q2.Messages)
		return res
	}
	res.Pass = acks == 10 && nacks == 0 && q1.Messages == 10 && q2.Messages == 10
	res.Detail = fmt.Sprintf("acks=%d q1=%d q2=%d", acks, q1.Messages, q2.Messages)
	return res
}

func scenarioRequeue() result {
	name := "ms11-requeue"
	res := result{
		Scenario: "requeue",
		Note:     "default-requeue-rejected=true 时，nack 且 requeue=true 会把同一条立刻送回来。这里数到 5 次就确认，避免死循环把测试挂住。",
	}
	if err := resetQueue(name, true, classicArgs(nil)); err != nil {
		res.Detail = err.Error()
		return res
	}
	if _, _, _, err := publishBodies("", name, []string{"poison"}, true, false); err != nil {
		res.Detail = err.Error()
		return res
	}
	c := openConsumer(name, 1)
	n := 0
	re := 0
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for n < 5 {
		select {
		case d, ok := <-c.msgs:
			if !ok {
				res.Detail = "consumer closed"
				c.conn.Close()
				return res
			}
			n++
			if d.Redelivered {
				re++
			}
			if n < 5 {
				_ = d.Nack(false, true)
			} else {
				_ = d.Ack(false)
			}
		case <-timer.C:
			res.Detail = fmt.Sprintf("timeout deliveries=%d redelivered=%d", n, re)
			c.conn.Close()
			return res
		}
	}
	c.conn.Close()
	q, _ := waitQueue(name, 2*time.Second, func(q qStat) bool { return q.Messages == 0 })
	res.Deliveries = int64(n)
	res.Redelivered = int64(re)
	res.Messages = q.Messages
	res.Pass = n == 5 && re == 4 && q.Messages == 0
	res.Detail = fmt.Sprintf("deliveries=%d redelivered=%d left=%d", n, re, q.Messages)
	return res
}

func scenarioDLX() result {
	mainQ := "ms11-dlx-main"
	dlq := "ms11-dlx-dead"
	res := result{
		Scenario: "dlx",
		Note:     "nack 且 requeue=false，消息进死信队列。主队列空，订单是 0。",
	}
	if err := resetQueue(dlq, true, classicArgs(nil)); err != nil {
		res.Detail = err.Error()
		return res
	}
	args := classicArgs(amqp.Table{
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": dlq,
	})
	if err := resetQueue(mainQ, true, args); err != nil {
		res.Detail = err.Error()
		return res
	}
	if _, _, _, err := publishBodies("", mainQ, ids(1, 20), true, false); err != nil {
		res.Detail = err.Error()
		return res
	}
	c := openConsumer(mainQ, 20)
	ds, err := take(c.msgs, 20, 5*time.Second)
	if err != nil {
		res.Detail = err.Error()
		c.conn.Close()
		return res
	}
	for _, d := range ds {
		_ = d.Nack(false, false)
	}
	c.conn.Close()
	_, ok := waitQueue(dlq, 3*time.Second, func(q qStat) bool { return q.Messages == 20 })
	mq, _ := findQueue(mainQ)
	dq, _ := findQueue(dlq)
	res.Messages = dq.Messages
	res.Ready = mq.Messages
	res.Pass = ok && mq.Messages == 0 && dq.Messages == 20
	res.Detail = fmt.Sprintf("main=%d dead=%d", mq.Messages, dq.Messages)
	return res
}

func scenarioRetry() result {
	name := "ms11-retry"
	res := result{
		Scenario: "retry",
		Note:     "同一条消息前两次 nack 重新入队，第三次确认并记订单。次数在这个进程里，不是 broker 的投递计数器。经典队列没有 quorum 的 x-delivery-limit。",
	}
	if err := resetQueue(name, true, classicArgs(nil)); err != nil {
		res.Detail = err.Error()
		return res
	}
	if _, _, _, err := publishBodies("", name, ids(1, 20), true, false); err != nil {
		res.Detail = err.Error()
		return res
	}
	c := openConsumer(name, 20)
	attempts := map[string]int{}
	orders := 0
	deliveries := 0
	timer := time.NewTimer(8 * time.Second)
	defer timer.Stop()
	for orders < 20 {
		select {
		case d, ok := <-c.msgs:
			if !ok {
				res.Detail = "consumer closed"
				c.conn.Close()
				return res
			}
			deliveries++
			id := string(d.Body)
			attempts[id]++
			if attempts[id] < 3 {
				_ = d.Nack(false, true)
			} else {
				orders++
				_ = d.Ack(false)
			}
		case <-timer.C:
			res.Detail = fmt.Sprintf("timeout orders=%d deliveries=%d", orders, deliveries)
			c.conn.Close()
			return res
		}
	}
	c.conn.Close()
	q, _ := waitQueue(name, 2*time.Second, func(q qStat) bool { return q.Messages == 0 })
	res.Orders = int64(orders)
	res.Deliveries = int64(deliveries)
	res.Messages = q.Messages
	res.Pass = orders == 20 && deliveries == 60 && len(attempts) == 20 && q.Messages == 0
	res.Detail = fmt.Sprintf("orders=%d deliveries=%d users=%d left=%d", orders, deliveries, len(attempts), q.Messages)
	return res
}

func scenarioRestart(kind string) result {
	res := result{Scenario: "restart-" + kind}
	var (
		name       string
		durable    = true
		persistent = true
		args       = classicArgs(nil)
	)
	switch kind {
	case "persistent":
		name = "ms11-restart-persistent"
		res.Note = "持久化消息、持久队列。kill -9 再启动，看还剩多少。confirm 返回之后才杀进程。"
	case "transient":
		name = "ms11-restart-transient"
		persistent = false
		res.Note = "持久队列里的非持久化消息。kill -9 再启动。数量是这次的结果，不事先假定。"
	case "undurable":
		name = "ms11-restart-undurable"
		durable = false
		res.Note = "非持久队列。kill -9 之后队列应该不在。消息本身是持久化的，队列先没了。"
	case "quorum":
		name = "ms11-restart-quorum"
		args = amqp.Table{"x-queue-type": "quorum"}
		res.Note = "单节点 quorum，副本数就是 1。这不是仓库里的队列类型。kill -9 再启动，看 100 条还在不在。"
	default:
		res.Detail = "unknown restart kind"
		return res
	}
	res.Queue = name
	res.Persistent = persistent
	res.Durable = durable
	res.Quorum = kind == "quorum"
	if err := resetQueue(name, durable, args); err != nil {
		if kind == "undurable" && strings.Contains(err.Error(), "transient_nonexcl_queues") {
			res.Pass = true
			line := err.Error()
			if i := strings.IndexByte(line, '\n'); i > 0 {
				line = line[:i]
			}
			res.Detail = "declare rejected: " + line
			return res
		}
		res.Detail = err.Error()
		return res
	}
	acks, nacks, _, err := publishBodies("", name, ids(1, 100), persistent, false)
	if err != nil {
		res.Detail = fmt.Sprintf("publish: %s acks=%d nacks=%d", err.Error(), acks, nacks)
		return res
	}
	if err := restartBroker(); err != nil {
		res.Detail = err.Error()
		return res
	}
	time.Sleep(300 * time.Millisecond)
	q, exists := findQueue(name)
	absent := !exists
	if exists && q.Messages != 100 {
		if waited, ok := waitQueue(name, 8*time.Second, func(q qStat) bool { return q.Messages == 100 }); ok {
			q = waited
		} else if found, yes := findQueue(name); yes {
			q = found
		}
	}
	res.Acks = int64(acks)
	res.Kept = q.Messages
	res.QueueType = q.Type
	switch kind {
	case "persistent":
		res.Pass = !absent && q.Messages == 100 && acks == 100
	case "quorum":
		// kept 可以是 0。单节点 kill -9 之后 Ra 可能把成员当成没注册，另起一个 last applied 为 0 的新成员。
		res.Pass = !absent && acks == 100
	case "undurable":
		res.Pass = absent && acks == 100
		res.Kept = 0
	case "transient":
		res.Pass = acks == 100
	}
	res.Detail = fmt.Sprintf("acks=%d absent=%v kept=%d type=%s", acks, absent, res.Kept, q.Type)
	return res
}
