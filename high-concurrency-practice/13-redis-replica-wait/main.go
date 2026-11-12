package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"
)

var C struct {
	master   string
	replica  string
	replica2 string
	workers  int
	duration time.Duration
	warmup   time.Duration
	payload  int
	replicas int
	waitFor  int
	waitMs   int
	from     string
	delay    time.Duration
	fsync    string
	datadir  string
	out      string
	keys     int
	scenario string
}

func main() {
	flag.StringVar(&C.scenario, "scenario", "", "see run.sh")
	flag.StringVar(&C.master, "master", "127.0.0.1:6379", "master addr")
	flag.StringVar(&C.replica, "replica", "127.0.0.1:6380", "replica addr")
	flag.StringVar(&C.replica2, "replica2", "127.0.0.1:6381", "second replica, empty to skip")
	flag.IntVar(&C.workers, "workers", 16, "connections")
	flag.DurationVar(&C.duration, "duration", 4*time.Second, "measure window")
	flag.DurationVar(&C.warmup, "warmup", time.Second, "warmup before the window")
	flag.IntVar(&C.payload, "payload", 16, "SET value bytes")
	flag.IntVar(&C.replicas, "replicas", 1, "online replicas during bench")
	flag.IntVar(&C.waitFor, "wait-for", 1, "WAIT replica count")
	flag.IntVar(&C.waitMs, "wait-ms", 1000, "WAIT timeout milliseconds")
	flag.StringVar(&C.from, "from", "master", "get target: master, replica, both")
	flag.DurationVar(&C.delay, "delay", 0, "userspace proxy delay per direction")
	flag.StringVar(&C.fsync, "fsync", "everysec", "aof fsync policy for aof-cut")
	flag.StringVar(&C.datadir, "datadir", "/tmp/miaosha-redis-repl-6379", "master dir")
	flag.StringVar(&C.out, "out", "", "json path")
	flag.IntVar(&C.keys, "keys", 10000, "key count for stale or memory")
	flag.Parse()

	var (
		res map[string]any
		err error
	)
	switch C.scenario {
	case "ready":
		res, err = scenarioReady()
	case "copy":
		res, err = scenarioCopy()
	case "readonly":
		res, err = scenarioReadonly()
	case "wait-ok":
		res, err = scenarioWaitOK()
	case "wait-down":
		res, err = scenarioWaitDown()
	case "stale":
		res, err = scenarioStale()
	case "stale-data":
		res, err = scenarioStaleData()
	case "min-one":
		res, err = scenarioMin(1)
	case "min-two":
		res, err = scenarioMin(2)
	case "lag":
		res, err = scenarioLag()
	case "partial":
		res, err = scenarioPartial()
	case "full":
		res, err = scenarioFull()
	case "promote":
		res, err = scenarioPromote()
	case "gap":
		res, err = scenarioGap()
	case "delay5":
		res, err = scenarioDelay5()
	case "memory":
		res, err = scenarioMemory()
	case "sync":
		res, err = scenarioSync()
	case "buffer":
		res, err = scenarioBuffer()
	case "bench":
		res, err = scenarioBench()
	case "wait-timeout":
		res, err = scenarioWaitTimeout()
	case "loss":
		res, err = scenarioLoss()
	case "loss-after":
		res, err = scenarioLossAfter()
	case "aof-cut":
		res, err = scenarioAOFCut()
	case "aof-after":
		res, err = scenarioAOFAfter()
	default:
		err = fmt.Errorf("unknown scenario %s", C.scenario)
	}
	if err != nil {
		fatal(err)
	}
	enc, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		fatal(err)
	}
	enc = append(enc, '\n')
	fmt.Print(string(enc))
	if C.out != "" {
		if err := os.WriteFile(C.out, enc, 0o644); err != nil {
			fatal(err)
		}
	}
	pass, _ := res["pass"].(bool)
	if !pass {
		os.Exit(2)
	}
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "error: %v\n", err)
	os.Exit(1)
}
