package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"
)

type result struct {
	Scenario           string  `json:"scenario"`
	Note               string  `json:"note"`
	Pass               bool    `json:"pass"`
	Detail             string  `json:"detail"`
	Queue              string  `json:"queue,omitempty"`
	QueueType          string  `json:"queueType,omitempty"`
	Published          int64   `json:"published,omitempty"`
	Confirms           int64   `json:"confirms,omitempty"`
	Acks               int64   `json:"acks,omitempty"`
	Nacks              int64   `json:"nacks,omitempty"`
	Returns            int64   `json:"returns,omitempty"`
	Orders             int64   `json:"orders,omitempty"`
	Deliveries         int64   `json:"deliveries,omitempty"`
	Redelivered        int64   `json:"redelivered,omitempty"`
	Ready              int64   `json:"ready,omitempty"`
	Unacked            int64   `json:"unacked,omitempty"`
	Messages           int64   `json:"messages,omitempty"`
	Consumers          int64   `json:"consumers,omitempty"`
	QPS                float64 `json:"qps,omitempty"`
	ElapsedSec         float64 `json:"elapsedSec,omitempty"`
	DrainSec           float64 `json:"drainSec,omitempty"`
	P50us              int64   `json:"p50us,omitempty"`
	P99us              int64   `json:"p99us,omitempty"`
	Maxus              int64   `json:"maxus,omitempty"`
	BrokerCPU          float64 `json:"brokerCpuPercent,omitempty"`
	ClientCPU          float64 `json:"clientCpuPercent,omitempty"`
	BrokerRSSKB        int64   `json:"brokerRssKB,omitempty"`
	Alarm              bool    `json:"alarm,omitempty"`
	AlarmRaw           string  `json:"alarmRaw,omitempty"`
	MessageBytes       int64   `json:"messageBytes,omitempty"`
	QueueMemory        int64   `json:"queueMemory,omitempty"`
	BytesPerMessage    float64 `json:"bytesPerMessage,omitempty"`
	MemoryPerMessage   float64 `json:"memoryPerMessage,omitempty"`
	DiskApparentDelta  int64   `json:"diskApparentDelta,omitempty"`
	DiskAllocDelta     int64   `json:"diskAllocDelta,omitempty"`
	ApparentPerMessage float64 `json:"apparentPerMessage,omitempty"`
	AllocPerMessage    float64 `json:"allocPerMessage,omitempty"`
	Kept               int64   `json:"kept,omitempty"`
	Split              string  `json:"split,omitempty"`
	Bodies             string  `json:"bodies,omitempty"`
	Errors             int64   `json:"errors,omitempty"`
	Payload            int     `json:"payload,omitempty"`
	Workers            int     `json:"workers,omitempty"`
	Prefetch           int     `json:"prefetch,omitempty"`
	ConsumerN          int     `json:"consumerN,omitempty"`
	Queues             int     `json:"queues,omitempty"`
	Durable            bool    `json:"durable,omitempty"`
	Persistent         bool    `json:"persistent,omitempty"`
	ConfirmsOn         bool    `json:"confirmsOn,omitempty"`
	Quorum             bool    `json:"quorum,omitempty"`
	Drain              bool    `json:"drain,omitempty"`
	VMLimit            int64   `json:"vmLimit,omitempty"`
	RssBytes           int64   `json:"rssBytes,omitempty"`
}

var (
	scenarioName = flag.String("scenario", "", "which measurement to run")
	outPath      = flag.String("out", "", "json output path")
	queueFlag    = flag.String("queue", "", "queue name override")
	messages     = flag.Int("messages", 20000, "messages for consume, disk, winners")
	workers      = flag.Int("workers", 8, "publisher connections")
	consumerN    = flag.Int("consumers", 1, "consumers for the consume scenario")
	prefetch     = flag.Int("prefetch", 250, "consumer prefetch")
	duration     = flag.Duration("duration", 4*time.Second, "measured window")
	warmup       = flag.Duration("warmup", time.Second, "warmup before the window")
	payloadN     = flag.Int("payload", 200, "payload bytes")
	durable      = flag.Bool("durable", true, "durable queue")
	persistent   = flag.Bool("persistent", true, "delivery mode 2")
	confirmsOn   = flag.Bool("confirms", true, "publisher confirms")
	queueN       = flag.Int("queues", 1, "number of queues")
	quorum       = flag.Bool("quorum", false, "quorum queue")
	drain        = flag.Bool("drain", false, "consume while publishing")
	stopAlarm    = flag.Bool("stop-on-alarm", false, "stop the window when a memory alarm is set")
)

func qname() string {
	if *queueFlag != "" {
		return *queueFlag
	}
	return "ms11-" + *scenarioName
}

func finish(res result) {
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

func main() {
	flag.Parse()
	var res result
	switch *scenarioName {
	case "unroutable":
		res = scenarioUnroutable(false)
	case "unroutable-mandatory":
		res = scenarioUnroutable(true)
	case "manual-stall":
		res = scenarioManualStall()
	case "auto-return":
		res = scenarioAutoReturn()
	case "ack-before-insert":
		res = scenarioAckBeforeInsert()
	case "ack-current-prefetch1":
		res = scenarioAckCurrent(1)
	case "ack-current-prefetch50":
		res = scenarioAckCurrent(50)
	case "ack-max-prefetch50":
		res = scenarioAckMax()
	case "redelivery":
		res = scenarioRedelivery()
	case "dedup":
		res = scenarioDedup()
	case "reject-publish":
		res = scenarioReject()
	case "drop-head":
		res = scenarioDropHead()
	case "competing":
		res = scenarioCompeting()
	case "fanout":
		res = scenarioFanout()
	case "requeue":
		res = scenarioRequeue()
	case "dlx":
		res = scenarioDLX()
	case "retry":
		res = scenarioRetry()
	case "restart-persistent":
		res = scenarioRestart("persistent")
	case "restart-transient":
		res = scenarioRestart("transient")
	case "restart-undurable":
		res = scenarioRestart("undurable")
	case "restart-quorum":
		res = scenarioRestart("quorum")
	case "produce":
		res = scenarioProduce()
	case "consume":
		res = scenarioConsume()
	case "winners":
		res = scenarioWinners()
	case "disk":
		res = scenarioDisk()
	case "alarm":
		res = scenarioAlarm()
	case "wipe":
		res = scenarioWipe()
	default:
		fmt.Fprintf(os.Stderr, "unknown scenario %q\n", *scenarioName)
		os.Exit(2)
	}
	finish(res)
}
