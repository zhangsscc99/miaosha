package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func main() {
	portsFlag := flag.String("ports", "7201,7202,7203,7204,7205,7206", "cluster ports")
	redisBin := flag.String("redis", "/tmp/redis-7015/bin/redis-server", "redis-server binary")
	confdir := flag.String("confdir", "/tmp/miaosha-cluster-fo", "per-port config directory")
	outDir := flag.String("out", "results", "json directory")
	only := flag.String("only", "", "comma-separated scenario names; empty runs all")
	flag.Parse()

	var ports []int
	for _, p := range strings.Split(*portsFlag, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil {
			fatal(err)
		}
		ports = append(ports, n)
	}
	w, err := attach(ports, *redisBin, *confdir)
	if err != nil {
		fatal(err)
	}
	defer w.close()
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fatal(err)
	}
	rn := &runner{w: w}
	type job struct {
		name string
		fn   func() (result, error)
	}
	jobs := []job{
		{"ready", rn.scenarioReady},
		{"idle", rn.scenarioIdle},
		{"route", rn.scenarioRoute},
		{"failover-on-master", rn.scenarioOnMaster},
		{"incr-w16", func() (result, error) { return rn.scenarioIncr(16) }},
		{"incr-w32", func() (result, error) { return rn.scenarioIncr(32) }},
		{"replica-get-w16", func() (result, error) { return rn.scenarioReplicaGet(16) }},
		{"replica-get-w32", func() (result, error) { return rn.scenarioReplicaGet(32) }},
		{"incr-and-replica-get", rn.scenarioBoth},
		{"memory", rn.scenarioMemory},
		{"sleep-stuck", rn.scenarioSleepStuck},
		{"manual", rn.scenarioManual},
		{"force", rn.scenarioForce},
		{"takeover-alive", rn.scenarioTakeoverAlive},
		{"kill-auto", rn.scenarioKillAuto},
		{"kill-coverage-no", rn.scenarioKillCoverage},
		{"kill-behind", rn.scenarioKillBehind},
		{"no-failover", rn.scenarioNoFailover},
		{"rank", rn.scenarioRank},
		{"majority", rn.scenarioMajority},
	}
	want := map[string]bool{}
	if *only != "" {
		for _, name := range strings.Split(*only, ",") {
			want[strings.TrimSpace(name)] = true
		}
	}
	failed := 0
	for _, j := range jobs {
		if len(want) > 0 && !want[j.name] {
			continue
		}
		fmt.Println("===", j.name, "===")
		res, err := j.fn()
		if err != nil {
			res = result{Scenario: j.name, Pass: false, Detail: err.Error()}
			fmt.Println("ERR", err)
			failed++
			_ = writeResult(*outDir, res)
			break
		}
		res.Scenario = j.name
		if !res.Pass {
			fmt.Println("FAIL", res.Detail)
			failed++
		} else {
			fmt.Println("ok", res.Detail)
		}
		if err := writeResult(*outDir, res); err != nil {
			fatal(err)
		}
		if !res.Pass {
			break
		}
	}
	if failed > 0 {
		os.Exit(1)
	}
}

func writeResult(dir string, res result) error {
	b, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(filepath.Join(dir, res.Scenario+".json"), b, 0o644)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
