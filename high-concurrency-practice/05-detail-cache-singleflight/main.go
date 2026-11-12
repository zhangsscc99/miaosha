package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"
)

type config struct {
	Role       string
	Scenario   string
	Note       string
	Mode       string
	Addr       string
	OriginAddr string
	Fixture    string
	LuaDir     string
	Out        string
	Instances  int
	Instance   int
	Workers    int
	Attempts   int64
	Duration   time.Duration
	Follow     int
	Pool       int
	ServiceMs  int
	LockMs     int
	Barrier    string
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
	fmt.Printf("%s pass=%v ops=%d qps=%.0f origin=%d p99us=%d maxus=%d\n",
		res.Scenario, res.Pass, res.Ops, res.QPS, res.OriginLoads, res.P99us, res.Maxus)
	if !res.Pass {
		fmt.Fprintf(os.Stderr, "%s\n", res.Detail)
		os.Exit(2)
	}
}

func parseConfig() config {
	role := flag.String("role", "super", "super or child")
	scenario := flag.String("scenario", "", "result label")
	note := flag.String("note", "", "short note stored in the result")
	mode := flag.String("mode", "", "redis-get, local-hit, stampede-slow, singleflight, distlock, logical, ...")
	addr := flag.String("addr", "127.0.0.1:6379", "redis addr")
	origin := flag.String("origin", "", "origin addr, set by the parent")
	fixture := flag.String("fixture", "", "goods detail bytes")
	luaDir := flag.String("lua", "lua", "lua directory")
	out := flag.String("out", "", "result json path")
	instances := flag.Int("instances", 1, "child processes")
	instance := flag.Int("instance", 0, "this child index")
	workers := flag.Int("workers", 1, "goroutines per child")
	attempts := flag.Int64("attempts", 0, "requests per child, shared by its workers")
	duration := flag.Duration("duration", 0, "measured window after a 0.5s warmup")
	follow := flag.Int("follow", 0, "extra gets per worker after a negative-cache burst")
	pool := flag.Int("pool", 8, "sqlite connections in the origin")
	serviceMs := flag.Int("service-ms", 20, "target CPU time of one slow load")
	lockMs := flag.Int("lock-ms", 5000, "SET NX PX milliseconds; 0 means no expiry")
	barrier := flag.String("barrier", "", "start barrier")
	flag.Parse()
	return config{
		Role: *role, Scenario: *scenario, Note: *note, Mode: *mode, Addr: *addr, OriginAddr: *origin,
		Fixture: *fixture, LuaDir: *luaDir, Out: *out, Instances: *instances, Instance: *instance,
		Workers: *workers, Attempts: *attempts, Duration: *duration, Follow: *follow, Pool: *pool,
		ServiceMs: *serviceMs, LockMs: *lockMs, Barrier: *barrier,
	}
}
