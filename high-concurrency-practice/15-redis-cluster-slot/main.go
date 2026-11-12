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
	portsFlag := flag.String("ports", "7101,7102,7103", "cluster master ports")
	alone := flag.Int("standalone", 7104, "standalone redis port")
	redisBin := flag.String("redis", "/tmp/redis-7015/bin/redis-server", "redis-server binary")
	confdir := flag.String("confdir", "/tmp/miaosha-cluster", "per-port config directory")
	luaPath := flag.String("lua", "lua/stock.lua", "stock script")
	outDir := flag.String("out", "results", "json directory")
	only := flag.String("only", "", "comma-separated scenario names; empty runs all")
	flag.Parse()

	lua, err := os.ReadFile(*luaPath)
	if err != nil {
		fatal(err)
	}
	var ports []int
	for _, p := range strings.Split(*portsFlag, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil {
			fatal(err)
		}
		ports = append(ports, n)
	}
	cl, err := attach(ports, *alone, *redisBin, *confdir)
	if err != nil {
		fatal(err)
	}
	defer cl.close()
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fatal(err)
	}
	rn := &runner{cl: cl, lua: string(lua)}
	type job struct {
		name string
		fn   func() (result, error)
	}
	jobs := []job{
		{"ready", rn.scenarioReady},
		{"hash", rn.scenarioHash},
		{"crossslot", rn.scenarioCrossslot},
		{"moved", rn.scenarioMoved},
		{"idle", rn.scenarioIdle},
		{"hot-get-w16", func() (result, error) { return rn.scenarioHotGet(16) }},
		{"hot-get-w32", func() (result, error) { return rn.scenarioHotGet(32) }},
		{"standalone-get-w16", rn.scenarioStandaloneGet},
		{"hot-incr-w16", rn.scenarioHotIncr},
		{"moved-follow-w16", rn.scenarioMovedFollow},
		{"spread-get", rn.scenarioSpread},
		{"memory", rn.scenarioMemory},
		{"ask-steps", rn.scenarioAskSteps},
		{"setslot-third", rn.scenarioSetslotThird},
		{"replace-loss", rn.scenarioReplace},
		{"noreplace", rn.scenarioNoreplace},
		{"partial", rn.scenarioPartial},
		{"tryagain", rn.scenarioTryagain},
		{"ask-follow-w16", rn.scenarioAskFollow},
		{"stall", rn.scenarioStall},
		{"sleep", rn.scenarioSleep},
		{"coverage", rn.scenarioCoverage},
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
