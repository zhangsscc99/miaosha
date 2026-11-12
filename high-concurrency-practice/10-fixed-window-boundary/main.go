package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

type phase struct {
	Name        string  `json:"name"`
	Attempts    int64   `json:"attempts,omitempty"`
	Admits      int64   `json:"admits"`
	Rejects     int64   `json:"rejects,omitempty"`
	MaxInWindow int64   `json:"maxInWindow,omitempty"`
	WallMs      float64 `json:"wallMs,omitempty"`
	Final       int64   `json:"final,omitempty"`
	Commands    int64   `json:"commands,omitempty"`
	Want        int64   `json:"want,omitempty"`
	Note        string  `json:"note,omitempty"`
}

type clockAttempt struct {
	AdmitsBefore int64   `json:"admitsBefore"`
	AdmitsAfter  int64   `json:"admitsAfter"`
	RedisMsStart int64   `json:"redisMsStart"`
	RedisMsMid   int64   `json:"redisMsMid"`
	RedisMsEnd   int64   `json:"redisMsEnd"`
	WallBeforeMs float64 `json:"wallBeforeMs"`
	WallAfterMs  float64 `json:"wallAfterMs"`
	GapMs        float64 `json:"gapMs"`
	OK           bool    `json:"ok"`
	Why          string  `json:"why,omitempty"`
}

type result struct {
	Scenario         string         `json:"scenario"`
	Algo             string         `json:"algo,omitempty"`
	Note             string         `json:"note"`
	Pass             bool           `json:"pass"`
	Detail           string         `json:"detail"`
	Limit            int64          `json:"limit,omitempty"`
	WindowMs         int64          `json:"windowMs,omitempty"`
	Workers          int            `json:"workers,omitempty"`
	Attempts         int64          `json:"attempts,omitempty"`
	Admits           int64          `json:"admits,omitempty"`
	Rejects          int64          `json:"rejects,omitempty"`
	Errors           int64          `json:"errors,omitempty"`
	AdmitsBefore     int64          `json:"admitsBefore,omitempty"`
	AdmitsAfter      int64          `json:"admitsAfter,omitempty"`
	MaxInWindow      int64          `json:"maxInWindow,omitempty"`
	LogicalGapMs     int64          `json:"logicalGapMs,omitempty"`
	WallMs           float64        `json:"wallMs,omitempty"`
	PingUs           int64          `json:"pingUs,omitempty"`
	FinalCount       int64          `json:"finalCount,omitempty"`
	Commands         int64          `json:"commands,omitempty"`
	CommandsPerOp    float64        `json:"commandsPerOp,omitempty"`
	UsedMemory       int64          `json:"usedMemory,omitempty"`
	MemoryDelta      int64          `json:"memoryDelta,omitempty"`
	KeyBytes         int64          `json:"keyBytes,omitempty"`
	BytesPerMember   float64        `json:"bytesPerMember,omitempty"`
	Members          int64          `json:"members,omitempty"`
	QPS              float64        `json:"qps,omitempty"`
	RedisCPUPercent  float64        `json:"redisCpuPercent,omitempty"`
	ClientCPUPercent float64        `json:"clientCpuPercent,omitempty"`
	RedisCPUSec      float64        `json:"redisCpuSec,omitempty"`
	ClientCPUSec     float64        `json:"clientCpuSec,omitempty"`
	RedisRSSKB       int64          `json:"redisRssKB,omitempty"`
	ElapsedSec       float64        `json:"elapsedSec,omitempty"`
	P50us            int64          `json:"p50us,omitempty"`
	P99us            int64          `json:"p99us,omitempty"`
	P999us           int64          `json:"p999us,omitempty"`
	Maxus            int64          `json:"maxus,omitempty"`
	CPUSampleValid   bool           `json:"cpuSampleValid,omitempty"`
	Phases           []phase        `json:"phases,omitempty"`
	ClockAttempts    []clockAttempt `json:"clockAttempts,omitempty"`
	Rounds           []phase        `json:"rounds,omitempty"`
	Timestamps       []int64        `json:"timestamps,omitempty"`
}

func main() {
	addr := flag.String("addr", "127.0.0.1:6379", "redis addr")
	luaDir := flag.String("lua", "lua", "lua directory")
	outDir := flag.String("out-dir", "results", "directory for json files")
	only := flag.String("only", "", "run only names containing this substring")
	flag.Parse()

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fatal(err)
	}
	c, err := dial(*addr)
	if err != nil {
		fatal(err)
	}
	defer c.Close()
	sc, err := loadScripts(c, *luaDir)
	if err != nil {
		fatal(err)
	}
	pid, err := redisPID(c)
	if err != nil {
		fatal(err)
	}
	r := &runner{
		c:    c,
		s:    sc,
		addr: *addr,
		out:  *outDir,
		only: *only,
		pid:  pid,
		clk:  loadClk(),
	}
	if err := r.runAll(); err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "error: %v\n", err)
	os.Exit(1)
}

func loadClk() float64 {
	out, err := exec.Command("getconf", "CLK_TCK").Output()
	if err != nil {
		return 100
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	if err != nil || v <= 0 {
		return 100
	}
	return v
}

func writeResult(dir, name string, res result) error {
	b, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(dir+"/"+name+".json", b, 0o644)
}

func done(res result, pass bool, detail string) result {
	res.Pass = pass
	res.Detail = detail
	return res
}

func cpuTicks(pid int) float64 {
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

func rssKB(pid int) int64 {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				n, _ := strconv.ParseInt(fields[1], 10, 64)
				return n
			}
		}
	}
	return 0
}
