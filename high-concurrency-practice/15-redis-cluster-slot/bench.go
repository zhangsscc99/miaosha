package main

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type nodeStat struct {
	Addr       string  `json:"addr"`
	Port       int     `json:"port"`
	Pid        int     `json:"pid"`
	ID         string  `json:"id,omitempty"`
	Slots      int     `json:"slots,omitempty"`
	Flags      string  `json:"flags,omitempty"`
	CPUPercent float64 `json:"cpuPercent,omitempty"`
	RSSKB      int64   `json:"rssKB,omitempty"`
	UsedMemory int64   `json:"usedMemory,omitempty"`
	Ops        int64   `json:"ops,omitempty"`
	Commands   int64   `json:"commands,omitempty"`
}

type result struct {
	Scenario         string         `json:"scenario"`
	Note             string         `json:"note"`
	Pass             bool           `json:"pass"`
	Detail           string         `json:"detail,omitempty"`
	Workers          int            `json:"workers,omitempty"`
	ElapsedSec       float64        `json:"elapsedSec,omitempty"`
	Ops              int64          `json:"ops,omitempty"`
	QPS              float64        `json:"qps,omitempty"`
	Errors           int64          `json:"errors,omitempty"`
	RedisCommands    int64          `json:"redisCommands,omitempty"`
	RedisCPUPercent  float64        `json:"redisCpuPercent,omitempty"`
	ClientCPUPercent float64        `json:"clientCpuPercent,omitempty"`
	P50us            int64          `json:"p50us,omitempty"`
	P99us            int64          `json:"p99us,omitempty"`
	P999us           int64          `json:"p999us,omitempty"`
	Maxus            int64          `json:"maxus,omitempty"`
	Nodes            []nodeStat     `json:"nodes,omitempty"`
	Extra            map[string]any `json:"extra,omitempty"`
}

func (r *result) put(k string, v any) {
	if r.Extra == nil {
		r.Extra = map[string]any{}
	}
	r.Extra[k] = v
}

type runner struct {
	cl  *cluster
	lua string
}

func (rn *runner) stat(s *server) nodeStat {
	mem := int64(0)
	if info, err := s.info("memory"); err == nil {
		mem = infoInt(info, "used_memory")
	}
	return nodeStat{
		Addr:       s.Addr,
		Port:       s.Port,
		Pid:        s.Pid,
		ID:         s.ID,
		Slots:      s.Slots,
		Flags:      s.Flags,
		RSSKB:      rssKB(s.Pid),
		UsedMemory: mem,
	}
}

func commandsOf(s *server) int64 {
	info, err := s.info("stats")
	if err != nil {
		return 0
	}
	return infoInt(info, "total_commands_processed")
}

func (rn *runner) bench(name, note string, workers int, warm, dur time.Duration, watch []*server, op func(i int) error, nodeIndex func(i int) int) (result, error) {
	if workers < 1 {
		return result{}, fmt.Errorf("workers")
	}
	warmEnd := time.Now().Add(warm)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(i int) {
			defer wg.Done()
			for time.Now().Before(warmEnd) {
				_ = op(i)
			}
		}(i)
	}
	wg.Wait()

	for _, s := range watch {
		if err := s.refreshMeta(); err != nil {
			return result{}, err
		}
	}
	beforeCmd := make([]int64, len(watch))
	cpu0 := make([]float64, len(watch))
	for i, s := range watch {
		beforeCmd[i] = commandsOf(s)
		cpu0[i] = cpuTicks(s.Pid)
	}
	client0 := cpuTicks(os.Getpid())
	hists := make([]*hist, workers)
	errN := make([]int64, workers)
	opsN := make([]int64, workers)
	nodeOps := make([]atomic.Int64, len(watch))
	start := time.Now()
	end := start.Add(dur)
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		hists[i] = newHist()
		go func(i int) {
			defer wg.Done()
			h := hists[i]
			for time.Now().Before(end) {
				t0 := time.Now()
				err := op(i)
				h.add(time.Since(t0).Nanoseconds())
				opsN[i]++
				if err != nil {
					errN[i]++
					continue
				}
				if nodeIndex != nil {
					idx := nodeIndex(i)
					if idx >= 0 && idx < len(nodeOps) {
						nodeOps[idx].Add(1)
					}
				}
			}
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	client1 := cpuTicks(os.Getpid())
	merged := newHist()
	res := result{Scenario: name, Note: note, Workers: workers, ElapsedSec: elapsed}
	var redisCPU float64
	for i := range workers {
		res.Ops += opsN[i]
		res.Errors += errN[i]
		merged.merge(hists[i])
	}
	res.Nodes = make([]nodeStat, len(watch))
	for i, s := range watch {
		st := rn.stat(s)
		st.Commands = commandsOf(s) - beforeCmd[i]
		st.CPUPercent = cpuPercent(cpu0[i], cpuTicks(s.Pid), elapsed)
		st.Ops = nodeOps[i].Load()
		res.Nodes[i] = st
		res.RedisCommands += st.Commands
		redisCPU += st.CPUPercent
	}
	res.RedisCPUPercent = redisCPU
	res.ClientCPUPercent = cpuPercent(client0, client1, elapsed)
	if elapsed > 0 {
		res.QPS = float64(res.Ops) / elapsed
	}
	res.P50us = merged.le(0.50) / 1000
	res.P99us = merged.le(0.99) / 1000
	res.P999us = merged.le(0.999) / 1000
	res.Maxus = merged.max / 1000
	res.Pass = res.Errors == 0 && res.Ops > 0
	return res, nil
}

func dialWorkers(addr string, n int) ([]*Conn, error) {
	out := make([]*Conn, n)
	for i := 0; i < n; i++ {
		c, err := dial(addr, 2*time.Second)
		if err != nil {
			for _, x := range out {
				if x != nil {
					x.Close()
				}
			}
			return nil, err
		}
		out[i] = c
	}
	return out, nil
}

func closeConns(cs []*Conn) {
	for _, c := range cs {
		if c != nil {
			c.Close()
		}
	}
}

func pipelineSet(c *Conn, pairs [][2]string) error {
	const batch = 100
	for i := 0; i < len(pairs); i += batch {
		j := i + batch
		if j > len(pairs) {
			j = len(pairs)
		}
		for _, p := range pairs[i:j] {
			if err := c.writeQueued("SET", p[0], p[1]); err != nil {
				return err
			}
		}
		if err := c.flush(); err != nil {
			return err
		}
		for n := i; n < j; n++ {
			v, err := readVal(c.r)
			if err != nil {
				return err
			}
			if v.Err != "" {
				return fmt.Errorf("set %s: %s", pairs[n][0], v.Err)
			}
		}
	}
	return nil
}
