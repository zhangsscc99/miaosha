package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/rocketmq-client-go/v2/admin"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/apache/rocketmq-client-go/v2/rlog"
)

func init() {
	rlog.SetLogLevel("error")
}

type topicAdmin interface {
	CreateTopic(context.Context, ...admin.OptionCreate) error
	FetchPublishMessageQueues(context.Context, string) ([]*primitive.MessageQueue, error)
	Close() error
}

var (
	adOnce sync.Once
	ad     topicAdmin
	adErr  error
)

func getAdmin() (topicAdmin, error) {
	adOnce.Do(func() {
		a, err := admin.NewAdmin(admin.WithResolver(primitive.NewPassthroughResolver([]string{*namesrv})))
		if err != nil {
			adErr = err
			return
		}
		ad = a
	})
	return ad, adErr
}

func waitRoute(topic string, timeout time.Duration) error {
	a, err := getAdmin()
	if err != nil {
		return err
	}
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		qs, err := a.FetchPublishMessageQueues(ctx, topic)
		cancel()
		if err == nil && len(qs) > 0 {
			return nil
		}
		last = err
		time.Sleep(200 * time.Millisecond)
	}
	if last == nil {
		last = fmt.Errorf("no queue")
	}
	return last
}

func ensureTopic(topic string, queues int) (time.Duration, error) {
	a, err := getAdmin()
	if err != nil {
		return 0, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = a.CreateTopic(ctx,
		admin.WithTopicCreate(topic),
		admin.WithBrokerAddrCreate(*brokerAddr),
		admin.WithReadQueueNums(queues),
		admin.WithWriteQueueNums(queues),
	)
	cancel()
	if err != nil && !strings.Contains(err.Error(), "topic already exist") && !strings.Contains(strings.ToLower(err.Error()), "already") {
		// 建过的 topic 再创建，不同版本的报错不一样。下面用路由是否出现来判断。
		_ = err
	}
	start := time.Now()
	if err := waitRoute(topic, 8*time.Second); err != nil {
		return time.Since(start), fmt.Errorf("topic %s route: %w", topic, err)
	}
	return time.Since(start), nil
}

type prodAPI interface {
	Start() error
	Shutdown() error
	SendSync(ctx context.Context, msgs ...*primitive.Message) (*primitive.SendResult, error)
	SendOneWay(ctx context.Context, msgs ...*primitive.Message) error
}

func newProducer(queues int) (prodAPI, error) {
	p, err := producer.NewDefaultProducer(
		producer.WithNameServer(primitive.NamesrvAddr{*namesrv}),
		producer.WithRetry(1),
		producer.WithSendMsgTimeout(*sendTimeout),
		producer.WithVIPChannel(false),
		producer.WithDefaultTopicQueueNums(queues),
		producer.WithInstanceName(fmt.Sprintf("p-%d-%d", os.Getpid(), time.Now().UnixNano())),
	)
	if err != nil {
		return nil, err
	}
	if err := p.Start(); err != nil {
		return nil, err
	}
	return p, nil
}

func sendSync(p prodAPI, msg *primitive.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), *sendTimeout)
	defer cancel()
	res, err := p.SendSync(ctx, msg)
	if err != nil {
		return err
	}
	if res.Status != primitive.SendOK {
		return fmt.Errorf("status %d", res.Status)
	}
	return nil
}

type consumeOpts struct {
	group     string
	topic     string
	instance  string
	tag       string
	orderly   bool
	broadcast bool
	threads   int
	batch     int
	maxRetry  int32
	fromFirst bool
}

func startPush(opt consumeOpts, fn func(context.Context, ...*primitive.MessageExt) (consumer.ConsumeResult, error)) (*consumerPush, error) {
	if opt.threads <= 0 {
		opt.threads = 4
	}
	if opt.batch <= 0 {
		opt.batch = 1
	}
	if opt.tag == "" {
		opt.tag = "*"
	}
	if opt.instance == "" {
		opt.instance = opt.group + "-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	model := consumer.Clustering
	if opt.broadcast {
		model = consumer.BroadCasting
	}
	opts := []consumer.Option{
		consumer.WithNameServer(primitive.NamesrvAddr{*namesrv}),
		consumer.WithGroupName(opt.group),
		consumer.WithInstance(opt.instance),
		consumer.WithVIPChannel(false),
		consumer.WithConsumerModel(model),
		consumer.WithConsumeFromWhere(consumer.ConsumeFromFirstOffset),
		consumer.WithConsumerOrder(opt.orderly),
		consumer.WithConsumeGoroutineNums(opt.threads),
		consumer.WithConsumeMessageBatchMaxSize(opt.batch),
		consumer.WithPullBatchSize(32),
	}
	if opt.maxRetry > 0 {
		opts = append(opts, consumer.WithMaxReconsumeTimes(opt.maxRetry))
	}
	c, err := consumer.NewPushConsumer(opts...)
	if err != nil {
		return nil, err
	}
	if err := c.Subscribe(opt.topic, consumer.MessageSelector{Type: consumer.TAG, Expression: opt.tag}, fn); err != nil {
		_ = c.Shutdown()
		return nil, err
	}
	var last error
	for i := 0; i < 20; i++ {
		err = c.Start()
		if err == nil {
			return &consumerPush{c: c}, nil
		}
		last = err
		_ = c.Shutdown()
		c, err = consumer.NewPushConsumer(opts...)
		if err != nil {
			return nil, err
		}
		if err = c.Subscribe(opt.topic, consumer.MessageSelector{Type: consumer.TAG, Expression: opt.tag}, fn); err != nil {
			return nil, err
		}
		time.Sleep(300 * time.Millisecond)
	}
	return nil, last
}

type pushAPI interface {
	Start() error
	Shutdown() error
	Subscribe(topic string, selector consumer.MessageSelector, f func(context.Context, ...*primitive.MessageExt) (consumer.ConsumeResult, error)) error
}

type consumerPush struct {
	c pushAPI
}

func (p *consumerPush) Shutdown() {
	if p != nil && p.c != nil {
		_ = p.c.Shutdown()
	}
}

func payloadN(n int) []byte {
	if n < 1 {
		n = 1
	}
	return bytesRepeat(n)
}

func bytesRepeat(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = 'a'
	}
	return b
}

func parseID(body []byte) (int, bool) {
	s := string(body)
	if !strings.HasPrefix(s, "id:") {
		return 0, false
	}
	n, err := strconv.Atoi(s[3:])
	if err != nil {
		return 0, false
	}
	return n, true
}

func cpuTicks(pid int) float64 {
	if pid <= 0 {
		return 0
	}
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0
	}
	s := string(b)
	i := strings.LastIndex(s, ")")
	if i < 0 || i+2 >= len(s) {
		return 0
	}
	fields := strings.Fields(s[i+2:])
	if len(fields) < 13 {
		return 0
	}
	ut, _ := strconv.ParseFloat(fields[11], 64)
	st, _ := strconv.ParseFloat(fields[12], 64)
	return ut + st
}

func cpuPercent(before, after, elapsed float64) float64 {
	if elapsed <= 0 {
		return 0
	}
	return (after - before) / 100 / elapsed * 100
}

func rssKB(pid int) int64 {
	if pid <= 0 {
		return 0
	}
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				v, _ := strconv.ParseInt(fields[1], 10, 64)
				return v
			}
		}
	}
	return 0
}
