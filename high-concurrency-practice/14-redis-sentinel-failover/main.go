package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	scenario := flag.String("scenario", "all", "all, or one of the scenario names")
	flag.Parse()
	defer stopAll()
	var err error
	switch *scenario {
	case "all":
		err = runAll()
	case "stop":
		return
	case "manual":
		err = scenarioManual()
	case "steady":
		err = scenarioSteady()
	case "failover-500":
		err = scenarioFailover500()
	case "failover-30000":
		err = scenarioFailover30000()
	case "quorum":
		err = scenarioQuorum()
	case "priority":
		err = scenarioPriority()
	case "sleep":
		err = scenarioSleep()
	case "zombie":
		err = scenarioZombie()
	case "tail":
		err = scenarioTail()
	case "config":
		err = scenarioConfig()
	case "forced":
		err = scenarioForced()
	default:
		err = fmt.Errorf("unknown scenario %s", *scenario)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
