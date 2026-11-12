package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// jsonBody 是 MiaoshaMessage 的替身：goodsId 加整个 MiaoshaUser。
// 没有启动 Spring，所以这不是 Fastjson 某一次的原始字节，只保留同样的字段。
const jsonBody = `{"goodsId":1,"user":{"head":"","id":10000001,"lastLoginDate":1762963200000,"loginCount":3,"nickname":"10000001","password":"n/a","registerDate":1762963200000,"salt":"n/a"}}`

type result struct {
	Scenario         string  `json:"scenario"`
	Note             string  `json:"note"`
	Pass             bool    `json:"pass"`
	Detail           string  `json:"detail,omitempty"`
	Workers          int     `json:"workers,omitempty"`
	Partitions       int     `json:"partitions,omitempty"`
	Acks             string  `json:"acks,omitempty"`
	LingerMs         int     `json:"lingerMs,omitempty"`
	Compression      string  `json:"compression,omitempty"`
	Idempotent       bool    `json:"idempotent,omitempty"`
	Inflight         int     `json:"inflight,omitempty"`
	PayloadBytes     int     `json:"payloadBytes,omitempty"`
	WarmupSec        float64 `json:"warmupSec,omitempty"`
	IssueSec         float64 `json:"issueSec,omitempty"`
	FlushSec         float64 `json:"flushSec,omitempty"`
	ElapsedSec       float64 `json:"elapsedSec,omitempty"`
	QPS              float64 `json:"qps,omitempty"`
	Success          int64   `json:"success,omitempty"`
	Errors           int64   `json:"errors,omitempty"`
	BrokerCPUPercent float64 `json:"brokerCpuPercent,omitempty"`
	ClientCPUPercent float64 `json:"clientCpuPercent,omitempty"`
	BrokerRSSKB      int64   `json:"brokerRssKB,omitempty"`
	P50us            int64   `json:"p50us,omitempty"`
	P99us            int64   `json:"p99us,omitempty"`
	P999us           int64   `json:"p999us,omitempty"`
	Maxus            int64   `json:"maxus,omitempty"`
	Over1ms          int64   `json:"over1ms,omitempty"`
	Over10ms         int64   `json:"over10ms,omitempty"`
	Over50ms         int64   `json:"over50ms,omitempty"`
	Produced         int64   `json:"produced,omitempty"`
	Deliveries       int64   `json:"deliveries,omitempty"`
	Unique           int64   `json:"unique,omitempty"`
	Orders           int64   `json:"orders,omitempty"`
	SecondFetch      int64   `json:"secondFetch,omitempty"`
	Assigned         []int   `json:"assigned,omitempty"`
	Counts           []int   `json:"counts,omitempty"`
	DirBytes         int64   `json:"dirBytes,omitempty"`
	LogBytes         int64   `json:"logBytes,omitempty"`
	LogLogical       int64   `json:"logLogical,omitempty"`
	IndexBytes       int64   `json:"indexBytes,omitempty"`
	IndexLogical     int64   `json:"indexLogical,omitempty"`
	OtherBytes       int64   `json:"otherBytes,omitempty"`
	BytesPerRecord   float64 `json:"bytesPerRecord,omitempty"`
	RSSBeforeKB      int64   `json:"rssBeforeKB,omitempty"`
	RSSAfterKB       int64   `json:"rssAfterKB,omitempty"`
	UserFacingSec    float64 `json:"userFacingSec,omitempty"`
	DrainSec         float64 `json:"drainSec,omitempty"`
	DrainQPS         float64 `json:"drainQps,omitempty"`
	Consumers        int     `json:"consumers,omitempty"`
}

func main() {
	scenario := flag.String("scenario", "", "dedup|loss|redelivery|assign|produce|winners|disk")
	out := flag.String("out", "", "json path")
	broker := flag.String("broker", "127.0.0.1:9092", "bootstrap")
	kafkaBin := flag.String("kafka-bin", "", "directory with kafka-topics.sh")
	brokerPid := flag.Int("broker-pid", 0, "kafka java pid")
	logDir := flag.String("log-dir", "/tmp/miaosha-kafka-data", "broker log.dirs")
	topic := flag.String("topic", "", "topic, default depends on scenario")
	workers := flag.Int("workers", 8, "produce goroutines")
	partitions := flag.Int("partitions", 1, "topic partitions")
	acks := flag.String("acks", "1", "0, 1, or all")
	lingerMs := flag.Int("linger-ms", 5, "producer linger")
	compression := flag.String("compression", "none", "none|lz4|snappy")
	idempotent := flag.Bool("idempotent", false, "acks=all only")
	duration := flag.Duration("duration", 4*time.Second, "measure window")
	warmup := flag.Duration("warmup", time.Second, "warmup before the window")
	messages := flag.Int("messages", 1000, "exact record count")
	payloadKind := flag.String("payload", "fixed200", "fixed200|json")
	flag.Parse()
	if *scenario == "" || *out == "" || *kafkaBin == "" {
		fmt.Fprintln(os.Stderr, "need -scenario, -out, -kafka-bin")
		os.Exit(2)
	}
	if *brokerPid > 0 && !pidAlive(*brokerPid) {
		fatal(fmt.Errorf("broker pid %d is not alive", *brokerPid))
	}

	payload := fixed200()
	if *payloadKind == "json" {
		payload = []byte(jsonBody)
	}
	if *topic == "" {
		*topic = "ms-" + *scenario
	}

	var (
		res result
		err error
	)
	switch *scenario {
	case "dedup":
		res, err = runDedup(*broker, *kafkaBin, *topic)
	case "loss":
		res, err = runCommit(*broker, *kafkaBin, *topic, true)
	case "redelivery":
		res, err = runCommit(*broker, *kafkaBin, *topic, false)
	case "assign":
		res, err = runAssign(*broker, *kafkaBin, *topic, *partitions, *messages)
	case "produce":
		res, err = runProduce(*broker, *kafkaBin, *topic, *brokerPid, *workers, *partitions, *acks, *lingerMs, *compression, *idempotent, *duration, *warmup, payload)
	case "winners":
		res, err = runWinners(*broker, *kafkaBin, *topic, *brokerPid, *workers, *acks, *lingerMs, *compression, *messages, payload)
	case "disk":
		res, err = runDisk(*broker, *kafkaBin, *topic, *brokerPid, *logDir, *messages, *compression, payload)
	default:
		err = fmt.Errorf("unknown scenario %s", *scenario)
	}
	if err != nil {
		res.Scenario = *scenario
		res.Pass = false
		res.Detail = err.Error()
		_ = writeResult(*out, res)
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := writeResult(*out, res); err != nil {
		fatal(err)
	}
	if !res.Pass {
		fmt.Fprintln(os.Stderr, res.Detail)
		os.Exit(1)
	}
}

func writeResult(path string, res result) error {
	b, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

func fixed200() []byte {
	// 200 字节载荷，和第 2 章费用表里的「200 字节」对齐。不是 JSON。
	b := make([]byte, 200)
	copy(b, "miaosha-order-")
	for i := len("miaosha-order-"); i < len(b); i++ {
		b[i] = 'x'
	}
	return b
}

func createTopic(kafkaBin, topic string, partitions int) error {
	cmd := exec.Command(filepath.Join(kafkaBin, "kafka-topics.sh"),
		"--bootstrap-server", "127.0.0.1:9092",
		"--create",
		"--topic", topic,
		"--partitions", strconv.Itoa(partitions),
		"--replication-factor", "1",
	)
	cmd.Env = append(os.Environ(), "KAFKA_HEAP_OPTS=-Xms64m -Xmx128m")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("create topic %s: %v: %s", topic, err, out)
	}
	return nil
}

func newProducer(broker, acks string, linger time.Duration, compression string, idempotent, roundRobin bool) (*kgo.Client, int, error) {
	inflight := 5
	opts := []kgo.Opt{
		kgo.SeedBrokers(broker),
		kgo.ClientID("miaosha-kafkabench"),
		kgo.ProducerLinger(linger),
		kgo.ProducerBatchMaxBytes(1_000_012),
		kgo.MaxBufferedRecords(200_000),
		kgo.RecordDeliveryTimeout(30 * time.Second),
		kgo.ProduceRequestTimeout(15 * time.Second),
	}
	if roundRobin {
		// 正确性测试要把记录打进每个分区。吞吐测试不用它，默认的粘性分区器批次更大。
		opts = append(opts, kgo.RecordPartitioner(kgo.RoundRobinPartitioner()))
	}
	switch compression {
	case "none":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.NoCompression()))
	case "lz4":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.Lz4Compression()))
	case "snappy":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.SnappyCompression()))
	default:
		return nil, 0, fmt.Errorf("compression %s", compression)
	}
	switch acks {
	case "0":
		opts = append(opts, kgo.RequiredAcks(kgo.NoAck()), kgo.DisableIdempotentWrite(), kgo.MaxProduceRequestsInflightPerBroker(inflight))
	case "1":
		opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()), kgo.DisableIdempotentWrite(), kgo.MaxProduceRequestsInflightPerBroker(inflight))
	case "all":
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
		if idempotent {
			inflight = 1
		} else {
			opts = append(opts, kgo.DisableIdempotentWrite(), kgo.MaxProduceRequestsInflightPerBroker(inflight))
		}
	default:
		return nil, 0, fmt.Errorf("acks %s", acks)
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, 0, err
	}
	return cl, inflight, nil
}

func newGroup(broker, group, topic string, onAssigned func(context.Context, *kgo.Client, map[string][]int32)) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(broker),
		kgo.ClientID("miaosha-kafkabench"),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxWait(200 * time.Millisecond),
	}
	if onAssigned != nil {
		opts = append(opts, kgo.OnPartitionsAssigned(onAssigned))
	}
	return kgo.NewClient(opts...)
}

func produceRecords(cl *kgo.Client, topic string, values [][]byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	const chunk = 400
	for off := 0; off < len(values); {
		n := chunk
		if off+n > len(values) {
			n = len(values) - off
		}
		recs := make([]*kgo.Record, n)
		for i := 0; i < n; i++ {
			recs[i] = &kgo.Record{Topic: topic, Value: values[off+i]}
		}
		if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
			return err
		}
		off += n
	}
	return nil
}

func pollFor(cl *kgo.Client, want int, timeout time.Duration, collect bool) (int, map[string]int, error) {
	deadline := time.Now().Add(timeout)
	n := 0
	var vals map[string]int
	if collect {
		vals = make(map[string]int)
	}
	var hard error
	for time.Now().Before(deadline) {
		if want > 0 && n >= want {
			break
		}
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		fetches := cl.PollFetches(ctx)
		cancel()
		fetches.EachError(func(_ string, _ int32, err error) {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return
			}
			if hard == nil {
				hard = err
			}
		})
		if collect {
			fetches.EachRecord(func(r *kgo.Record) {
				n++
				vals[string(r.Value)]++
			})
		} else {
			n += fetches.NumRecords()
		}
		if hard != nil {
			return n, vals, hard
		}
	}
	return n, vals, nil
}

func runDedup(broker, kafkaBin, topic string) (result, error) {
	res := result{
		Scenario: "dedup",
		Note:     "幂等生产者只去掉同一次发送的重试。业务上把同一个用户再写一条，日志里仍是两条。消费端用用户 id 收成一单。",
	}
	if err := createTopic(kafkaBin, topic, 1); err != nil {
		return res, err
	}
	cl, _, err := newProducer(broker, "all", 0, "none", true, false)
	if err != nil {
		return res, err
	}
	values := make([][]byte, 0, 120)
	for i := 0; i < 100; i++ {
		values = append(values, []byte(fmt.Sprintf("u-%03d", i)))
	}
	for i := 0; i < 20; i++ {
		values = append(values, []byte(fmt.Sprintf("u-%03d", i)))
	}
	if err := produceRecords(cl, topic, values); err != nil {
		cl.Close()
		return res, err
	}
	cl.Close()

	g, err := newGroup(broker, "g-dedup", topic, nil)
	if err != nil {
		return res, err
	}
	defer g.Close()
	n, vals, err := pollFor(g, 120, 20*time.Second, true)
	if err != nil {
		return res, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	cerr := g.CommitUncommittedOffsets(ctx)
	cancel()
	if cerr != nil {
		return res, cerr
	}
	res.Produced = 120
	res.Deliveries = int64(n)
	res.Unique = int64(len(vals))
	res.Orders = res.Unique
	res.Pass = n == 120 && len(vals) == 100
	res.Detail = fmt.Sprintf("log=%d unique=%d", n, len(vals))
	return res, nil
}

func runCommit(broker, kafkaBin, topic string, commitBeforeWrite bool) (result, error) {
	const n = 200
	const drop = 20
	res := result{Produced: n}
	if commitBeforeWrite {
		res.Scenario = "loss"
		res.Note = "先提交位移再落库。receiveMiaoShaMessage 先 basicAck 再 insert，进程死在两步之间时，这 20 条不会再投递。"
	} else {
		res.Scenario = "redelivery"
		res.Note = "先记下订单再提交位移。进程在提交前退出，同一组的下一个消费者会再读到这些记录。用户 id 唯一，订单仍是 200。"
	}
	if err := createTopic(kafkaBin, topic, 1); err != nil {
		return res, err
	}
	cl, _, err := newProducer(broker, "all", 0, "none", true, false)
	if err != nil {
		return res, err
	}
	values := make([][]byte, n)
	for i := 0; i < n; i++ {
		values[i] = []byte(fmt.Sprintf("u-%03d", i))
	}
	if err := produceRecords(cl, topic, values); err != nil {
		cl.Close()
		return res, err
	}
	cl.Close()

	g1, err := newGroup(broker, "g-commit", topic, nil)
	if err != nil {
		return res, err
	}
	got, vals, err := pollFor(g1, n, 20*time.Second, true)
	if err != nil {
		g1.Close()
		return res, err
	}
	if got != n {
		g1.Close()
		return res, fmt.Errorf("first fetch %d want %d", got, n)
	}
	orders := len(vals)
	if commitBeforeWrite {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		cerr := g1.CommitUncommittedOffsets(ctx)
		cancel()
		g1.Close()
		if cerr != nil {
			return res, cerr
		}
		orders = n - drop
	} else {
		g1.Close()
	}

	g2, err := newGroup(broker, "g-commit", topic, nil)
	if err != nil {
		return res, err
	}
	defer g2.Close()
	want2 := 0
	wait2 := 8 * time.Second
	if !commitBeforeWrite {
		want2 = n
		wait2 = 20 * time.Second
	}
	second, vals2, err := pollFor(g2, want2, wait2, true)
	if err != nil {
		return res, err
	}
	res.Deliveries = int64(got + second)
	res.SecondFetch = int64(second)
	res.Orders = int64(orders)
	if commitBeforeWrite {
		res.Unique = int64(orders)
		res.Pass = got == n && second == 0 && orders == n-drop
		res.Detail = fmt.Sprintf("first=%d committed-then-dropped=%d second=%d", got, drop, second)
		return res, nil
	}
	unique := len(vals)
	for k := range vals2 {
		if _, ok := vals[k]; !ok {
			unique++
		}
	}
	res.Unique = int64(unique)
	res.Orders = res.Unique
	res.Pass = got == n && second == n && unique == n
	res.Detail = fmt.Sprintf("first=%d second=%d unique=%d", got, second, unique)
	return res, nil
}

type assignGate struct {
	mu    sync.Mutex
	got   [2]bool
	parts [2]int
}

func (g *assignGate) set(i int, assigned map[string][]int32) {
	n := 0
	for _, ps := range assigned {
		n += len(ps)
	}
	g.mu.Lock()
	g.got[i] = true
	g.parts[i] = n
	g.mu.Unlock()
}

func (g *assignGate) snapshot() (bool, int, int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.got[0] && g.got[1], g.parts[0], g.parts[1]
}

func runAssign(broker, kafkaBin, topic string, partitions, messages int) (result, error) {
	res := result{
		Scenario:   "assign",
		Partitions: partitions,
		Note:       "同一个消费组里，一个分区只会分给一个消费者。分区数是 1 时，第二个进程一条都读不到。",
	}
	if err := createTopic(kafkaBin, topic, partitions); err != nil {
		return res, err
	}
	var gate assignGate
	c1, err := newGroup(broker, "g-assign", topic, func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
		gate.set(0, m)
	})
	if err != nil {
		return res, err
	}
	c2, err := newGroup(broker, "g-assign", topic, func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
		gate.set(1, m)
	})
	if err != nil {
		c1.Close()
		return res, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	var n1, n2 atomic.Int64
	var wg sync.WaitGroup
	var pollErr atomic.Value
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := countLoop(ctx, c1, &n1); err != nil {
			pollErr.Store(err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := countLoop(ctx, c2, &n2); err != nil {
			pollErr.Store(err)
		}
	}()

	deadline := time.Now().Add(12 * time.Second)
	ready := false
	var p0, p1 int
	for time.Now().Before(deadline) {
		ok, a, b := gate.snapshot()
		p0, p1 = a, b
		if ok && a+b == partitions {
			if partitions == 1 || (a > 0 && b > 0) {
				ready = true
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
	}
	if !ready {
		cancel()
		wg.Wait()
		c1.Close()
		c2.Close()
		return res, fmt.Errorf("assignment not stable parts=%d got=%d+%d", partitions, p0, p1)
	}

	prod, _, err := newProducer(broker, "1", 0, "none", false, true)
	if err != nil {
		cancel()
		wg.Wait()
		c1.Close()
		c2.Close()
		return res, err
	}
	values := make([][]byte, messages)
	for i := 0; i < messages; i++ {
		values[i] = []byte(fmt.Sprintf("m-%04d", i))
	}
	err = produceRecords(prod, topic, values)
	prod.Close()
	if err != nil {
		cancel()
		wg.Wait()
		c1.Close()
		c2.Close()
		return res, err
	}

	recDeadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(recDeadline) {
		if n1.Load()+n2.Load() >= int64(messages) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	// 再留一个短暂窗口，看会不会因为再平衡多读。
	time.Sleep(400 * time.Millisecond)
	cancel()
	wg.Wait()
	c1.Close()
	c2.Close()
	if v := pollErr.Load(); v != nil {
		return res, v.(error)
	}
	_, p0, p1 = gate.snapshot()
	a, b := int(n1.Load()), int(n2.Load())
	res.Assigned = []int{p0, p1}
	res.Counts = []int{a, b}
	res.Produced = int64(messages)
	res.Deliveries = int64(a + b)
	res.Consumers = 2
	if partitions == 1 {
		res.Pass = a+b == messages && (a == 0 || b == 0) && p0+p1 == 1 && (p0 == 0 || p1 == 0)
	} else {
		res.Pass = a+b == messages && a > 0 && b > 0 && p0+p1 == partitions && p0 > 0 && p1 > 0
	}
	res.Detail = fmt.Sprintf("assigned=%d+%d counts=%d+%d", p0, p1, a, b)
	return res, nil
}

func countLoop(ctx context.Context, cl *kgo.Client, n *atomic.Int64) error {
	var hard error
	for ctx.Err() == nil {
		pctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		fetches := cl.PollFetches(pctx)
		cancel()
		fetches.EachError(func(_ string, _ int32, err error) {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return
			}
			if hard == nil {
				hard = err
			}
		})
		n.Add(int64(fetches.NumRecords()))
		if hard != nil {
			return hard
		}
	}
	return nil
}

func runProduce(broker, kafkaBin, topic string, brokerPid, workers, partitions int, acks string, lingerMs int, compression string, idempotent bool, duration, warmup time.Duration, payload []byte) (result, error) {
	res := result{
		Scenario:     "produce",
		Workers:      workers,
		Partitions:   partitions,
		Acks:         acks,
		LingerMs:     lingerMs,
		Compression:  compression,
		Idempotent:   idempotent && acks == "all",
		PayloadBytes: len(payload),
		Note:         "窗口含 Flush。acks=0 的回调发生在写入套接字时，不是 broker 落日志之后。副本数是 1，acks=all 也只等这一个副本。",
	}
	if err := createTopic(kafkaBin, topic, partitions); err != nil {
		return res, err
	}
	cl, inflight, err := newProducer(broker, acks, time.Duration(lingerMs)*time.Millisecond, compression, idempotent, false)
	if err != nil {
		return res, err
	}
	defer cl.Close()
	res.Inflight = inflight

	wh := newHist()
	if _, _, _, err := pump(cl, topic, payload, workers, warmup, wh); err != nil {
		return res, err
	}
	h := newHist()
	b0 := cpuTicks(brokerPid)
	c0 := cpuTicks(os.Getpid())
	t0 := time.Now()
	issue, flush, success, err := pump(cl, topic, payload, workers, duration, h)
	t1 := time.Now()
	if err != nil {
		return res, err
	}
	elapsed := t1.Sub(t0).Seconds()
	res.WarmupSec = warmup.Seconds()
	res.IssueSec = issue.Seconds()
	res.FlushSec = flush.Seconds()
	res.ElapsedSec = elapsed
	res.Success = success.ok
	res.Errors = success.bad
	if elapsed > 0 {
		res.QPS = float64(success.ok) / elapsed
	}
	res.BrokerCPUPercent = cpuPercent(b0, cpuTicks(brokerPid), elapsed)
	res.ClientCPUPercent = cpuPercent(c0, cpuTicks(os.Getpid()), elapsed)
	res.BrokerRSSKB = rssKB(brokerPid)
	res.P50us = h.le(0.50) / 1000
	res.P99us = h.le(0.99) / 1000
	res.P999us = h.le(0.999) / 1000
	res.Maxus = atomic.LoadInt64(&h.max) / 1000
	res.Over1ms = atomic.LoadInt64(&h.over1ms)
	res.Over10ms = atomic.LoadInt64(&h.over10)
	res.Over50ms = atomic.LoadInt64(&h.over50)
	res.Pass = success.bad == 0 && success.ok > 1000 && elapsed > 1
	res.Detail = fmt.Sprintf("qps=%.0f p99us<=%d maxus=%d brokerCpu=%.1f%% clientCpu=%.1f%% errors=%d", res.QPS, res.P99us, res.Maxus, res.BrokerCPUPercent, res.ClientCPUPercent, success.bad)
	return res, nil
}

type pumpCount struct {
	ok  int64
	bad int64
}

func pump(cl *kgo.Client, topic string, payload []byte, workers int, d time.Duration, h *hist) (time.Duration, time.Duration, pumpCount, error) {
	var ok, bad atomic.Int64
	var wg sync.WaitGroup
	deadline := time.Now().Add(d)
	issueStart := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				rec := &kgo.Record{Topic: topic, Value: payload}
				t0 := time.Now()
				cl.Produce(context.Background(), rec, func(_ *kgo.Record, err error) {
					if err != nil {
						bad.Add(1)
						return
					}
					ok.Add(1)
					h.add(time.Since(t0).Nanoseconds())
				})
			}
		}()
	}
	wg.Wait()
	issue := time.Since(issueStart)
	flushStart := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	err := cl.Flush(ctx)
	cancel()
	flush := time.Since(flushStart)
	return issue, flush, pumpCount{ok: ok.Load(), bad: bad.Load()}, err
}

func runWinners(broker, kafkaBin, topic string, brokerPid, workers int, acks string, lingerMs int, compression string, messages int, payload []byte) (result, error) {
	const warmupN = 200
	res := result{
		Scenario:     "winners",
		Note:         "先发 200 条把元数据预热掉，再计 5000 条的墙钟。这是客户端等到 ack 的时间。消费组把预热和这 5000 条一起读完。",
		Workers:      workers,
		Partitions:   1,
		Acks:         acks,
		LingerMs:     lingerMs,
		Compression:  compression,
		PayloadBytes: len(payload),
	}
	if err := createTopic(kafkaBin, topic, 1); err != nil {
		return res, err
	}
	cl, inflight, err := newProducer(broker, acks, time.Duration(lingerMs)*time.Millisecond, compression, false, false)
	if err != nil {
		return res, err
	}
	res.Inflight = inflight
	warm := bytesN(payload, warmupN)
	if err := produceRecords(cl, topic, warm); err != nil {
		cl.Close()
		return res, err
	}
	h := newHist()
	var ok, bad atomic.Int64
	start := time.Now()
	var wg sync.WaitGroup
	base := messages / workers
	extra := messages % workers
	for w := 0; w < workers; w++ {
		n := base
		if w < extra {
			n++
		}
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				rec := &kgo.Record{Topic: topic, Value: payload}
				t0 := time.Now()
				cl.Produce(context.Background(), rec, func(_ *kgo.Record, err error) {
					if err != nil {
						bad.Add(1)
						return
					}
					ok.Add(1)
					h.add(time.Since(t0).Nanoseconds())
				})
			}
		}(n)
	}
	wg.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	ferr := cl.Flush(ctx)
	cancel()
	elapsed := time.Since(start)
	cl.Close()
	if ferr != nil {
		return res, ferr
	}
	res.UserFacingSec = elapsed.Seconds()
	res.Success = ok.Load()
	res.Errors = bad.Load()
	res.P50us = h.le(0.50) / 1000
	res.P99us = h.le(0.99) / 1000
	res.P999us = h.le(0.999) / 1000
	res.Maxus = atomic.LoadInt64(&h.max) / 1000
	res.Over10ms = atomic.LoadInt64(&h.over10)
	if elapsed.Seconds() > 0 {
		res.QPS = float64(ok.Load()) / elapsed.Seconds()
	}
	// 墙钟常常不到 1 秒。CPU 百分比仍然算出来，报告里不引用短窗口。
	res.BrokerRSSKB = rssKB(brokerPid)

	g, err := newGroup(broker, "g-winners", topic, nil)
	if err != nil {
		return res, err
	}
	defer g.Close()
	drainStart := time.Now()
	got, _, err := pollFor(g, warmupN+messages, 20*time.Second, false)
	if err != nil {
		return res, err
	}
	res.DrainSec = time.Since(drainStart).Seconds()
	res.Deliveries = int64(got)
	if res.DrainSec > 0 {
		res.DrainQPS = float64(got) / res.DrainSec
	}
	res.Pass = ok.Load() == int64(messages) && bad.Load() == 0 && got == warmupN+messages
	res.Detail = fmt.Sprintf("userFacingMs=%.1f p99us<=%d drain=%d in %.1fms", res.UserFacingSec*1000, res.P99us, got, res.DrainSec*1000)
	return res, nil
}

func bytesN(payload []byte, n int) [][]byte {
	out := make([][]byte, n)
	for i := 0; i < n; i++ {
		out[i] = payload
	}
	return out
}

func runDisk(broker, kafkaBin, topic string, brokerPid int, logDir string, messages int, compression string, payload []byte) (result, error) {
	res := result{
		Scenario:     "disk",
		Note:         "logBytes 是 .log 的已分配字节（st_blocks）。indexLogical 是打开的索引文件的逻辑大小，预分配不一定占磁盘。256MB 是 JVM 堆。页缓存不算进 RSS。",
		Partitions:   1,
		Compression:  compression,
		PayloadBytes: len(payload),
		Produced:     int64(messages),
	}
	if err := createTopic(kafkaBin, topic, 1); err != nil {
		return res, err
	}
	res.RSSBeforeKB = rssKB(brokerPid)
	cl, _, err := newProducer(broker, "1", 5*time.Millisecond, compression, false, false)
	if err != nil {
		return res, err
	}
	values := bytesN(payload, messages)
	start := time.Now()
	if err := produceRecords(cl, topic, values); err != nil {
		cl.Close()
		return res, err
	}
	cl.Close()
	res.ElapsedSec = time.Since(start).Seconds()
	if res.ElapsedSec > 0 {
		res.QPS = float64(messages) / res.ElapsedSec
	}
	time.Sleep(200 * time.Millisecond)
	sizes, err := topicFileBytes(logDir, topic)
	if err != nil {
		return res, err
	}
	res.LogBytes = sizes.logAlloc
	res.LogLogical = sizes.logLogical
	res.IndexBytes = sizes.indexAlloc
	res.IndexLogical = sizes.indexLogical
	res.OtherBytes = sizes.otherAlloc
	res.DirBytes = sizes.logAlloc + sizes.indexAlloc + sizes.otherAlloc
	if messages > 0 {
		res.BytesPerRecord = float64(sizes.logAlloc) / float64(messages)
	}
	res.RSSAfterKB = rssKB(brokerPid)
	res.BrokerRSSKB = res.RSSAfterKB
	res.Success = int64(messages)
	// 压缩之后日志可以小于载荷。通过条件看的是确实写出了日志，并且 RSS 能读到。
	res.Pass = sizes.logAlloc > 0 && sizes.logLogical > 0
	res.Detail = fmt.Sprintf("logAlloc=%d logLogical=%d indexAlloc=%d indexLogical=%d bytesPerLog=%.2f rss=%d->%d", sizes.logAlloc, sizes.logLogical, sizes.indexAlloc, sizes.indexLogical, res.BytesPerRecord, res.RSSBeforeKB, res.RSSAfterKB)
	return res, nil
}

type topicSizes struct {
	logAlloc, logLogical     int64
	indexAlloc, indexLogical int64
	otherAlloc, otherLogical int64
}

func topicFileBytes(root, topic string) (topicSizes, error) {
	var sizes topicSizes
	entries, err := os.ReadDir(root)
	if err != nil {
		return sizes, err
	}
	found := false
	for _, e := range entries {
		name := e.Name()
		rest, ok := strings.CutPrefix(name, topic+"-")
		if !ok || rest == "" || rest[0] < '0' || rest[0] > '9' {
			continue
		}
		found = true
		err = filepath.Walk(filepath.Join(root, name), func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return err
			}
			logical, alloc := fileSizes(info)
			base := info.Name()
			switch {
			case strings.HasSuffix(base, ".log"):
				sizes.logLogical += logical
				sizes.logAlloc += alloc
			case strings.HasSuffix(base, ".index"), strings.HasSuffix(base, ".timeindex"):
				sizes.indexLogical += logical
				sizes.indexAlloc += alloc
			default:
				sizes.otherLogical += logical
				sizes.otherAlloc += alloc
			}
			return nil
		})
		if err != nil {
			return sizes, err
		}
	}
	if !found {
		return sizes, fmt.Errorf("no partition dir for %s under %s", topic, root)
	}
	return sizes, nil
}

func fileSizes(info os.FileInfo) (logical, alloc int64) {
	logical = info.Size()
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return logical, logical
	}
	return logical, st.Blocks * 512
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

func pidAlive(pid int) bool {
	_, err := os.Stat(fmt.Sprintf("/proc/%d", pid))
	return err == nil
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
