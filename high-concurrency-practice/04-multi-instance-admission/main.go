package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"
)

// config 是一次压测的参数。父进程按这些参数拉起 N 个子进程，每个子进程有自己的售罄标记。
type config struct {
	Role      string
	Scenario  string
	Note      string
	Addr      string
	LuaDir    string
	Out       string
	Instances int
	Instance  int
	Workers   int
	Attempts  int64
	Duration  time.Duration
	Stock     int64
	UseFlag   bool
	Quota     float64
	Burst     float64
	Probe     time.Duration
	Gate      string
	Limit     int
	WindowMs  int
	Phases    int
	Phase2    time.Duration
	Restock   int64
	Bad       bool
	Barrier1  string
	Barrier2  string
	StockSHA  string
	AdmitSHA  string
}

type phaseSum struct {
	Evals       int64   `json:"evals"`
	Success     int64   `json:"success"`
	LocalReject int64   `json:"localReject"`
	Probes      int64   `json:"probes"`
	Errors      int64   `json:"errors"`
	ElapsedSec  float64 `json:"elapsedSec"`
	P99us       int64   `json:"p99us"`
}

type result struct {
	Scenario            string        `json:"scenario"`
	Note                string        `json:"note"`
	Addrs               string        `json:"addrs"`
	Instances           int           `json:"instances"`
	WorkersPerInstance  int           `json:"workersPerInstance"`
	AttemptsPerInstance int64         `json:"attemptsPerInstance"`
	DurationSec         float64       `json:"durationSec"`
	UseFlag             bool          `json:"useFlag"`
	QuotaPerSec         float64       `json:"quotaPerSec"`
	Burst               float64       `json:"burst"`
	ProbeMs             int64         `json:"probeMs"`
	Gate                string        `json:"gate"`
	Limit               int           `json:"limit"`
	WindowMs            int           `json:"windowMs"`
	BadInstance         bool          `json:"badInstance"`
	Ops                 int64         `json:"ops"`
	QPS                 float64       `json:"qps"`
	RedisEvals          int64         `json:"redisEvals"`
	StockEvals          int64         `json:"stockEvals"`
	RedisEvalQPS        float64       `json:"redisEvalQps"`
	RedisCommands       int64         `json:"redisCommands"`
	LocalReject         int64         `json:"localReject"`
	QuotaReject         int64         `json:"quotaReject"`
	Probes              int64         `json:"probes"`
	Success             int64         `json:"success"`
	SoldOut             int64         `json:"soldOut"`
	Duplicate           int64         `json:"duplicate"`
	Errors              int64         `json:"errors"`
	InitialStock        int64         `json:"initialStock"`
	FinalStock          int64         `json:"finalStock"`
	Winners             int64         `json:"winners"`
	RedisCPUSec         float64       `json:"redisCpuSec"`
	RedisCPUpct         float64       `json:"redisCpuPercent"`
	ClientCPUSec        float64       `json:"clientCpuSec"`
	ClientCPUpct        float64       `json:"clientCpuPercent"`
	RedisRSSKB          int64         `json:"redisRssKB"`
	UsedMemory          int64         `json:"usedMemory"`
	ElapsedSec          float64       `json:"elapsedSec"`
	WallSec             float64       `json:"wallSec"`
	FlagAtMsMin         float64       `json:"flagAtMsMin"`
	FlagAtMsMax         float64       `json:"flagAtMsMax"`
	P50us               int64         `json:"p50us"`
	P99us               int64         `json:"p99us"`
	P999us              int64         `json:"p999us"`
	Maxus               int64         `json:"maxus"`
	PerInstanceEvals    string        `json:"perInstanceEvals"`
	Phase2              *phaseSum     `json:"phase2,omitempty"`
	Pass                bool          `json:"pass"`
	Detail              string        `json:"detail"`
	Children            []childResult `json:"children,omitempty"`
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
		fatal(err)
	}
	enc, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		fatal(err)
	}
	fmt.Println(string(enc))
	if cfg.Out != "" {
		if err := os.WriteFile(cfg.Out, append(enc, '\n'), 0o644); err != nil {
			fatal(err)
		}
	}
	if !res.Pass {
		os.Exit(2)
	}
}

func parseConfig() config {
	role := flag.String("role", "super", "super or child")
	scenario := flag.String("scenario", "", "check profile")
	note := flag.String("note", "", "short note stored in the result")
	addr := flag.String("addr", "127.0.0.1:6379", "redis addr")
	luaDir := flag.String("lua", "lua", "directory of lua scripts")
	out := flag.String("out", "", "write result json here")
	instances := flag.Int("instances", 1, "child processes")
	instance := flag.Int("instance", 0, "this child index")
	workers := flag.Int("workers", 16, "connections per child")
	attempts := flag.Int64("attempts", 0, "requests per child; 0 means duration mode")
	duration := flag.Duration("duration", 0, "measured window per child")
	stock := flag.Int64("stock", 100, "initial stock")
	useFlag := flag.Bool("soldout-flag", false, "per-process sold-out flag")
	quota := flag.Float64("quota", 0, "local admits per second; 0 with gate local is unlimited")
	burst := flag.Float64("burst", 0, "token bucket capacity")
	probe := flag.Duration("probe", 0, "while sold out, one redis probe per this interval")
	gate := flag.String("gate", "none", "none, local, or redis")
	limit := flag.Int("limit", 0, "redis fixed-window admit limit")
	windowMs := flag.Int("window-ms", 60000, "redis admit window")
	phases := flag.Int("phases", 1, "1 or 2")
	phase2 := flag.Duration("phase2", 0, "second phase duration")
	restock := flag.Int64("restock", 0, "INCRBY between phases")
	bad := flag.Bool("bad", false, "instance 0 ignores the local quota")
	barrier1 := flag.String("barrier1", "", "child start address")
	barrier2 := flag.String("barrier2", "", "child phase2 address")
	stockSHA := flag.String("stock-sha", "", "EVALSHA of stock.lua")
	admitSHA := flag.String("admit-sha", "", "EVALSHA of admit.lua")
	flag.Parse()
	return config{
		Role: *role, Scenario: *scenario, Note: *note, Addr: *addr, LuaDir: *luaDir, Out: *out,
		Instances: *instances, Instance: *instance, Workers: *workers, Attempts: *attempts,
		Duration: *duration, Stock: *stock, UseFlag: *useFlag, Quota: *quota, Burst: *burst,
		Probe: *probe, Gate: *gate, Limit: *limit, WindowMs: *windowMs, Phases: *phases,
		Phase2: *phase2, Restock: *restock, Bad: *bad, Barrier1: *barrier1, Barrier2: *barrier2,
		StockSHA: *stockSHA, AdmitSHA: *admitSHA,
	}
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "error: %v\n", err)
	os.Exit(1)
}
