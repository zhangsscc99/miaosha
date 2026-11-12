package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type cons struct {
	conn *amqp.Connection
	msgs <-chan amqp.Delivery
}

func openConsumer(name string, prefetch int) *cons {
	conn := dial()
	ch := channel(conn)
	if err := ch.Qos(prefetch, 0, false); err != nil {
		panic(err)
	}
	msgs, err := ch.Consume(name, "", false, false, false, false, nil)
	if err != nil {
		panic(err)
	}
	return &cons{conn: conn, msgs: msgs}
}

func take(msgs <-chan amqp.Delivery, n int, timeout time.Duration) ([]amqp.Delivery, error) {
	out := make([]amqp.Delivery, 0, n)
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for len(out) < n {
		select {
		case d, ok := <-msgs:
			if !ok {
				return out, fmt.Errorf("consumer closed at %d", len(out))
			}
			out = append(out, d)
		case <-timer.C:
			return out, fmt.Errorf("timeout at %d/%d", len(out), n)
		}
	}
	return out, nil
}

func takeFor(msgs <-chan amqp.Delivery, timeout time.Duration) (n, redelivered int) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case d, ok := <-msgs:
			if !ok {
				return n, redelivered
			}
			n++
			if d.Redelivered {
				redelivered++
			}
			_ = d.Ack(false)
		case <-timer.C:
			return n, redelivered
		}
	}
}

func ids(from, to int) []string {
	out := make([]string, 0, to-from+1)
	for i := from; i <= to; i++ {
		out = append(out, fmt.Sprintf("%d", i))
	}
	return out
}

// publishBodies 走默认交换机或指定交换机，等每条的 confirm。
func publishBodies(exchange, key string, bodies []string, persistent, mandatory bool) (acks, nacks, returns int, err error) {
	conn := dial()
	defer conn.Close()
	ch := channel(conn)
	if err = ch.Confirm(false); err != nil {
		return 0, 0, 0, err
	}
	conf := ch.NotifyPublish(make(chan amqp.Confirmation, len(bodies)+8))
	ret := ch.NotifyReturn(make(chan amqp.Return, len(bodies)+8))
	closes := ch.NotifyClose(make(chan *amqp.Error, 1))
	mode := amqp.Persistent
	if !persistent {
		mode = amqp.Transient
	}
	for _, body := range bodies {
		err = ch.PublishWithContext(context.Background(), exchange, key, mandatory, false, amqp.Publishing{
			DeliveryMode: mode,
			Body:         []byte(body),
		})
		if err != nil {
			return acks, nacks, returns, err
		}
	}
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	got := 0
	for got < len(bodies) {
		select {
		case c := <-conf:
			got++
			if c.Ack {
				acks++
			} else {
				nacks++
			}
		case <-ret:
			returns++
		case e := <-closes:
			return acks, nacks, returns, fmt.Errorf("channel closed: %v", e)
		case <-timer.C:
			return acks, nacks, returns, fmt.Errorf("confirm timeout got=%d acks=%d nacks=%d returns=%d", got, acks, nacks, returns)
		}
	}
	// mandatory 的 basic.return 可能紧跟在最后一条 confirm 后面。
	idle := time.NewTimer(200 * time.Millisecond)
	defer idle.Stop()
	for {
		select {
		case <-ret:
			returns++
			if !idle.Stop() {
				select {
				case <-idle.C:
				default:
				}
			}
			idle.Reset(50 * time.Millisecond)
		case <-idle.C:
			return acks, nacks, returns, nil
		}
	}
}

// confirmExact 用若干连接把 n 条固定正文确认完。返回确认条数和墙钟。
func confirmExact(name string, n, workers, payload int, persistent bool) (acks int64, elapsed time.Duration, err error) {
	if workers < 1 {
		workers = 1
	}
	body := make([]byte, payload)
	for i := range body {
		body[i] = 'x'
	}
	mode := amqp.Persistent
	if !persistent {
		mode = amqp.Transient
	}
	var got atomic.Int64
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	t0 := time.Now()
	for w := 0; w < workers; w++ {
		count := n / workers
		if w < n%workers {
			count++
		}
		if count == 0 {
			continue
		}
		wg.Add(1)
		go func(count int) {
			defer wg.Done()
			conn := dial()
			defer conn.Close()
			ch := channel(conn)
			if err := ch.Confirm(false); err != nil {
				errCh <- err
				return
			}
			conf := ch.NotifyPublish(make(chan amqp.Confirmation, 4096))
			left := int64(count)
			done := make(chan struct{})
			go func() {
				for c := range conf {
					if c.Ack {
						got.Add(1)
					}
					left--
					if left == 0 {
						close(done)
						return
					}
				}
			}()
			for i := 0; i < count; i++ {
				if err := ch.PublishWithContext(context.Background(), "", name, false, false, amqp.Publishing{
					DeliveryMode: mode,
					Body:         body,
				}); err != nil {
					errCh <- err
					return
				}
			}
			select {
			case <-done:
			case <-time.After(120 * time.Second):
				errCh <- fmt.Errorf("confirm timeout name=%s", name)
			}
		}(count)
	}
	wg.Wait()
	elapsed = time.Since(t0)
	close(errCh)
	for e := range errCh {
		if err == nil {
			err = e
		}
	}
	return got.Load(), elapsed, err
}
