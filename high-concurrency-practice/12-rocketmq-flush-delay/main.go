package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"
)

type result struct {
	Scenario     string  `json:"scenario"`
	Note         string  `json:"note,omitempty"`
	Pass         bool    `json:"pass"`
	Detail       string  `json:"detail,omitempty"`
	Topic        string  `json:"topic,omitempty"`
	Queues       int     `json:"queues,omitempty"`
	Workers      int     `json:"workers,omitempty"`
	Consumers    int     `json:"consumers,omitempty"`
	Threads      int     `json:"threads,omitempty"`
	Messages     int     `json:"messages,omitempty"`
	Payload      int     `json:"payload,omitempty"`
	OneWay       bool    `json:"oneway,omitempty"`
	Success      int64   `json:"success,omitempty"`
	Errors       int64   `json:"errors,omitempty"`
	Deliveries   int64   `json:"deliveries,omitempty"`
	Unique       int64   `json:"unique,omitempty"`
	Orders       int64   `json:"orders,omitempty"`
	Kept         int64   `json:"kept,omitempty"`
	DLQ          int64   `json:"dlq,omitempty"`
	Attempts     int64   `json:"attempts,omitempty"`
	QPS          float64 `json:"qps,omitempty"`
	ElapsedSec   float64 `json:"elapsedSec,omitempty"`
	DrainSec     float64 `json:"drainSec,omitempty"`
	FirstDelayMs int64   `json:"firstDelayMs,omitempty"`
	P50us        int64   `json:"p50us,omitempty"`
	P99us        int64   `json:"p99us,omitempty"`
	Maxus        int64   `json:"maxus,omitempty"`
	BrokerCPU    float64 `json:"brokerCpuPercent,omitempty"`
	ClientCPU    float64 `json:"clientCpuPercent,omitempty"`
	BrokerRSSKB  int64   `json:"brokerRssKB,omitempty"`
	RSSBeforeKB  int64   `json:"rssBeforeKB,omitempty"`
	RSSAfterKB   int64   `json:"rssAfterKB,omitempty"`
	RouteMs      int64   `json:"routeMs,omitempty"`
	MinBornMs    int64   `json:"minBornMs,omitempty"`
	MaxBornMs    int64   `json:"maxBornMs,omitempty"`
	MinStoreMs   int64   `json:"minStoreMs,omitempty"`
	MaxStoreMs   int64   `json:"maxStoreMs,omitempty"`
	Counts       []int   `json:"counts,omitempty"`
	QueueIDs     []int   `json:"queueIds,omitempty"`
	Early        int64   `json:"early,omitempty"`
	Split        string  `json:"split,omitempty"`
	SecondFetch  int64   `json:"secondFetch,omitempty"`
	CommitLogMsg int64   `json:"commitLogMessages,omitempty"`
	CommitBytes  int64   `json:"commitBytes,omitempty"`
	BytesPerMsg  float64 `json:"bytesPerMessage,omitempty"`
	Prealloc     int64   `json:"preallocatedBytes,omitempty"`
	AllocBytes   int64   `json:"allocBytes,omitempty"`
	CQAlloc      int64   `json:"consumeQueueAllocBytes,omitempty"`
	IndexAlloc   int64   `json:"indexAllocBytes,omitempty"`
	StoreSizeSum int64   `json:"storeSizeSum,omitempty"`
	TailWaste    int64   `json:"tailWasteBytes,omitempty"`
	CachedOK     int64   `json:"cachedOk,omitempty"`
	FreshFail    bool    `json:"freshFail,omitempty"`
	Inversion    bool    `json:"inversion,omitempty"`
	OrderOK      bool    `json:"orderOk,omitempty"`
}

var (
	scenarioName = flag.String("scenario", "", "which measurement")
	outPath      = flag.String("out", "", "json path")
	namesrv      = flag.String("namesrv", "127.0.0.1:9876", "namesrv")
	brokerAddr   = flag.String("broker-addr", "127.0.0.1:10911", "broker remoting")
	brokerPid    = flag.Int("broker-pid", 0, "broker java pid")
	storeRoot    = flag.String("store", "/tmp/miaosha-rocketmq/store", "store root")
	topicFlag    = flag.String("topic", "", "topic override")
	workers      = flag.Int("workers", 8, "producer goroutines")
	queueN       = flag.Int("queues", 1, "queues to create")
	duration     = flag.Duration("duration", 4*time.Second, "measure window")
	warmup       = flag.Duration("warmup", time.Second, "warmup")
	messages     = flag.Int("messages", 1000, "exact message count")
	payload      = flag.Int("payload", 200, "payload bytes")
	delayLevel   = flag.Int("delay-level", 1, "delay level")
	sendTimeout  = flag.Duration("send-timeout", 3*time.Second, "sync send timeout")
	oneway       = flag.Bool("oneway", false, "SendOneWay, not an ack")
	namesrvPid   = flag.Int("namesrv-pid", 0, "namesrv pid to kill")
	consumerN    = flag.Int("consumers", 1, "push consumers")
	threads      = flag.Int("threads", 4, "consume goroutines per consumer")
	expectKept   = flag.Int("expect", -1, "kept must equal this, -1 skips")
	expectLess   = flag.Int("expect-less", -1, "kept must be < this, -1 skips")
	withIDs      = flag.Bool("ids", false, "number each consume message and require unique == count")
)

func topicName() string {
	if *topicFlag != "" {
		return *topicFlag
	}
	return "ms12_" + *scenarioName
}

func finish(res result) {
	if res.Scenario == "" {
		res.Scenario = *scenarioName
	}
	b, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		panic(err)
	}
	b = append(b, '\n')
	if *outPath == "" {
		os.Stdout.Write(b)
	} else if err := os.WriteFile(*outPath, b, 0o644); err != nil {
		panic(err)
	}
	fmt.Println(res.Scenario, res.Detail)
}

func fail(note string, err error) result {
	return result{Scenario: *scenarioName, Note: note, Pass: false, Detail: err.Error(), Topic: topicName()}
}

func main() {
	flag.Parse()
	if *scenarioName == "" {
		fmt.Fprintln(os.Stderr, "need -scenario")
		os.Exit(2)
	}
	var res result
	switch *scenarioName {
	case "ping":
		res = scenarioPing()
	case "delay":
		res = scenarioDelay()
	case "tag":
		res = scenarioTag()
	case "assign":
		res = scenarioAssign()
	case "orderly":
		res = scenarioOrder(true)
	case "concurrent":
		res = scenarioOrder(false)
	case "retry":
		res = scenarioRetry()
	case "dedup":
		res = scenarioDedup()
	case "ack-early":
		res = scenarioAckEarly()
	case "broadcast":
		res = scenarioBroadcast()
	case "tx":
		res = scenarioTx()
	case "namesrv-down":
		res = scenarioNamesrvDown()
	case "produce":
		res = scenarioProduce()
	case "consume":
		res = scenarioConsume()
	case "winners":
		res = scenarioWinners()
	case "disk":
		res = scenarioDisk()
	case "seed":
		res = scenarioSeed()
	case "count":
		res = scenarioCount()
	default:
		res = result{Scenario: *scenarioName, Pass: false, Detail: "unknown scenario"}
	}
	finish(res)
}
