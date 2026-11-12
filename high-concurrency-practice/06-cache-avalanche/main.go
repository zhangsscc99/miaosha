package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

type config struct {
	Role      string
	Scenario  string
	Note      string
	Mode      string
	Addr      string
	Origin    string
	Out       string
	Barrier   string
	Instances int
	Instance  int
	Workers   int
	Keys      int
	Repeat    int
	Pool      int
	ServiceMs int
	Hz        int
	SampleMs  int
	WindowMs  int
	Duration  time.Duration
}

func main() {
	cfg := parseConfig()
	if cfg.Role == "child" {
		if err := runChild(cfg); err != nil {
			fmt.Fprintf(os.Stderr, "child %d: %v\n", cfg.Instance, err)
			os.Exit(1)
		}
		return
	}
	res, err := runSuper(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	enc, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	if cfg.Out != "" {
		if err := os.WriteFile(cfg.Out, append(enc, '\n'), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
	}
	fmt.Printf("%s pass=%v origin=%d ops=%d wall=%.3fs peak=%d missP99us=%d maxus=%d\n",
		res.Scenario, res.Pass, res.OriginLoads, res.Ops, res.WallSec, res.PeakOrigin, res.MissP99us, res.Maxus)
	if !res.Pass {
		fmt.Fprintf(os.Stderr, "%s\n", res.Detail)
		os.Exit(2)
	}
}

func parseConfig() config {
	role := flag.String("role", "super", "super or child")
	scenario := flag.String("scenario", "", "result label")
	note := flag.String("note", "", "short note stored in the result")
	mode := flag.String("mode", "", "expire-same, burst-plain, cold-fixed, local-fixed, ...")
	addr := flag.String("addr", "127.0.0.1:6379", "redis addr")
	origin := flag.String("origin", "", "origin addr, set by the parent")
	out := flag.String("out", "", "result json path")
	barrier := flag.String("barrier", "", "start barrier")
	instances := flag.Int("instances", 1, "child processes")
	instance := flag.Int("instance", 0, "this child index")
	workers := flag.Int("workers", 1, "goroutines per child")
	keys := flag.Int("keys", 200, "distinct goods keys")
	repeat := flag.Int("repeat", 1, "requests per key per process")
	pool := flag.Int("pool", 8, "sqlite connections")
	serviceMs := flag.Int("service-ms", 2, "target CPU time of one origin load")
	hz := flag.Int("hz", 10, "CONFIG SET hz for the active-expire run")
	sampleMs := flag.Int("sample-ms", 50, "expire curve sample interval")
	windowMs := flag.Int("window-ms", 1500, "expire curve window")
	duration := flag.Duration("duration", 0, "measured window after the barrier")
	flag.Parse()
	return config{
		Role: *role, Scenario: *scenario, Note: *note, Mode: *mode, Addr: *addr, Origin: *origin,
		Out: *out, Barrier: *barrier, Instances: *instances, Instance: *instance, Workers: *workers,
		Keys: *keys, Repeat: *repeat, Pool: *pool, ServiceMs: *serviceMs, Hz: *hz,
		SampleMs: *sampleMs, WindowMs: *windowMs, Duration: *duration,
	}
}

func needsOrigin(mode string) bool {
	return strings.HasPrefix(mode, "burst-") || strings.HasPrefix(mode, "gate-") || strings.HasPrefix(mode, "cold-") || strings.HasPrefix(mode, "warm-")
}

func needsChildren(mode string) bool {
	return !strings.HasPrefix(mode, "expire-")
}
