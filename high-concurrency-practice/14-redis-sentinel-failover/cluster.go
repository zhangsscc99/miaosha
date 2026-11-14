package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	redisBin = "/tmp/redis-7015/bin/redis-server"
	cliBin   = "/tmp/redis-7015/bin/redis-cli"
	root     = "/tmp/miaosha-sentinel"

	portM     = 6379
	portR1    = 6380
	portR2    = 6381
	portS1    = 26379
	portS2    = 26380
	portS3    = 26381
	portProxy = 16379
)

func dataDir(port int) string { return filepath.Join(root, strconv.Itoa(port)) }
func confPath(port int) string {
	return filepath.Join(root, strconv.Itoa(port)+".conf")
}
func pidPath(port int) string {
	return filepath.Join(root, strconv.Itoa(port)+".pid")
}
func logPath(port int) string {
	return filepath.Join(root, strconv.Itoa(port)+".log")
}

func stopAll() {
	for _, port := range []int{portM, portR1, portR2, portS1, portS2, portS3} {
		stopPort(port)
	}
}

func stopPort(port int) {
	ctxCmd := exec.Command(cliBin, "-p", strconv.Itoa(port), "shutdown", "nosave")
	done := make(chan struct{})
	go func() {
		_ = ctxCmd.Run()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(400 * time.Millisecond):
		if ctxCmd.Process != nil {
			_ = ctxCmd.Process.Kill()
		}
	}
	if pid, err := readPid(port); err == nil && pidAlive(pid) {
		_ = killIfRedis(pid)
	}
	_ = os.Remove(pidPath(port))
}

func readPid(port int) (int, error) {
	b, err := os.ReadFile(pidPath(port))
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(strings.TrimSpace(string(b)))
}

func pidOf(port int) int {
	if pid, err := readPid(port); err == nil && pidAlive(pid) {
		return pid
	}
	info, err := infoSection(port, "server")
	if err != nil {
		return 0
	}
	return int(infoInt(info, "process_id"))
}

type dataOpts struct {
	port      int
	replicaOf string // host:port, empty means master
	priority  int
	announce  bool
}

func writeDataConf(o dataOpts) error {
	if o.priority == 0 && o.replicaOf == "" {
		o.priority = 100
	}
	var b strings.Builder
	fmt.Fprintf(&b, "bind 127.0.0.1\nport %d\nprotected-mode yes\ndaemonize yes\n", o.port)
	fmt.Fprintf(&b, "pidfile %s\nlogfile %s\ndir %s\n", pidPath(o.port), logPath(o.port), dataDir(o.port))
	b.WriteString("dbfilename dump.rdb\nsave \"\"\nappendonly no\n")
	b.WriteString("maxmemory 64mb\nmaxmemory-policy noeviction\ntimeout 0\n")
	b.WriteString("tcp-backlog 1024\ndatabases 1\nhz 100\nenable-debug-command local\n")
	b.WriteString("repl-diskless-sync yes\nrepl-diskless-sync-delay 0\nrepl-backlog-size 1mb\n")
	b.WriteString("replica-serve-stale-data yes\n")
	fmt.Fprintf(&b, "replica-priority %d\n", o.priority)
	if o.replicaOf != "" {
		host, port, _ := strings.Cut(o.replicaOf, ":")
		fmt.Fprintf(&b, "replicaof %s %s\nreplica-read-only yes\n", host, port)
		if o.announce {
			fmt.Fprintf(&b, "replica-announce-ip 127.0.0.1\nreplica-announce-port %d\n", o.port)
		}
	}
	if err := os.MkdirAll(dataDir(o.port), 0o755); err != nil {
		return err
	}
	// 清掉上一次的 RDB，避免把旧值装回来。配置文件不放在数据目录里。
	_ = os.Remove(filepath.Join(dataDir(o.port), "dump.rdb"))
	return os.WriteFile(confPath(o.port), []byte(b.String()), 0o644)
}

type sentinelOpts struct {
	port            int
	masterPort      int
	quorum          int
	downAfter       int
	failoverTimeout int
}

func writeSentinelConf(o sentinelOpts) error {
	if o.failoverTimeout == 0 {
		o.failoverTimeout = 20000
	}
	var b strings.Builder
	fmt.Fprintf(&b, "bind 127.0.0.1\nport %d\nprotected-mode yes\ndaemonize yes\n", o.port)
	fmt.Fprintf(&b, "pidfile %s\nlogfile %s\ndir %s\n", pidPath(o.port), logPath(o.port), dataDir(o.port))
	fmt.Fprintf(&b, "sentinel monitor miaosha 127.0.0.1 %d %d\n", o.masterPort, o.quorum)
	fmt.Fprintf(&b, "sentinel down-after-milliseconds miaosha %d\n", o.downAfter)
	fmt.Fprintf(&b, "sentinel failover-timeout miaosha %d\n", o.failoverTimeout)
	b.WriteString("sentinel parallel-syncs miaosha 1\n")
	b.WriteString("sentinel deny-scripts-reconfig yes\n")
	b.WriteString("sentinel resolve-hostnames no\n")
	if err := os.MkdirAll(dataDir(o.port), 0o755); err != nil {
		return err
	}
	return os.WriteFile(confPath(o.port), []byte(b.String()), 0o644)
}

func launch(port int, sentinel bool) error {
	args := []string{confPath(port)}
	if sentinel {
		args = append(args, "--sentinel")
	}
	cmd := exec.Command(redisBin, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("start %d: %w: %s\n%s", port, err, strings.TrimSpace(string(out)), tailFile(logPath(port), 40))
	}
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		c, err := dial(addr(port), 200*time.Millisecond)
		if err == nil {
			_, pingErr := c.cmd("PING")
			c.Close()
			if pingErr == nil {
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("port %d did not PING\n%s", port, tailFile(logPath(port), 40))
}

func resetData(port int) error {
	stopPort(port)
	if err := os.RemoveAll(dataDir(port)); err != nil {
		return err
	}
	return os.MkdirAll(dataDir(port), 0o755)
}

func tuneSentinels(ports []int) error {
	for _, port := range ports {
		c, err := dial(addr(port), time.Second)
		if err != nil {
			return err
		}
		// 只缩短发现用的 INFO 和 hello。ping 周期保持源码里的 1000 毫秒。
		_, err = c.cmd("SENTINEL", "DEBUG", "info-period", "200", "publish-period", "200")
		c.Close()
		if err != nil {
			return fmt.Errorf("sentinel %d debug: %w", port, err)
		}
	}
	return nil
}

func sentinelMaster(port int) (map[string]string, error) {
	c, err := dial(addr(port), time.Second)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	v, err := c.cmd("SENTINEL", "master", "miaosha")
	if err != nil {
		return nil, err
	}
	return pairs(v)
}

func sentinelReplicas(port int) ([]map[string]string, error) {
	c, err := dial(addr(port), time.Second)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	v, err := c.cmd("SENTINEL", "replicas", "miaosha")
	if err != nil {
		return nil, err
	}
	return mapsOf(v)
}

func sentinelPeers(port int) ([]map[string]string, error) {
	c, err := dial(addr(port), time.Second)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	v, err := c.cmd("SENTINEL", "sentinels", "miaosha")
	if err != nil {
		return nil, err
	}
	return mapsOf(v)
}

func masterAddr(port int) (int, error) {
	c, err := dial(addr(port), 300*time.Millisecond)
	if err != nil {
		return 0, err
	}
	defer c.Close()
	v, err := c.cmd("SENTINEL", "get-master-addr-by-name", "miaosha")
	if err != nil {
		return 0, err
	}
	if v.Kind != '*' || len(v.Elems) != 2 {
		return 0, fmt.Errorf("bad addr reply")
	}
	p, err := strconv.Atoi(v.Elems[1].Str)
	if err != nil {
		return 0, err
	}
	return p, nil
}

func ckquorum(port int) (string, error) {
	c, err := dial(addr(port), time.Second)
	if err != nil {
		return "", err
	}
	defer c.Close()
	v, err := c.cmd("SENTINEL", "ckquorum", "miaosha")
	if err != nil {
		if v.Err != "" {
			return v.Err, err
		}
		return "", err
	}
	return v.Str, nil
}

func infoSection(port int, section string) (string, error) {
	c, err := dial(addr(port), 400*time.Millisecond)
	if err != nil {
		return "", err
	}
	defer c.Close()
	v, err := c.cmd("INFO", section)
	if err != nil {
		return "", err
	}
	return v.Str, nil
}

func roleNow(port int) (role string, masterPort int, state string, err error) {
	c, err := dial(addr(port), 300*time.Millisecond)
	if err != nil {
		return "", 0, "", err
	}
	defer c.Close()
	v, err := c.cmd("ROLE")
	if err != nil {
		return "", 0, "", err
	}
	if v.Kind != '*' || len(v.Elems) < 1 {
		return "", 0, "", fmt.Errorf("bad role")
	}
	role = v.Elems[0].Str
	if role == "slave" && len(v.Elems) >= 4 {
		masterPort = valInt(v.Elems[2])
		state = v.Elems[3].Str
	}
	return role, masterPort, state, nil
}

func waitRole(port int, want string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		role, _, _, err := roleNow(port)
		if err == nil && role == want {
			return nil
		}
		last = err
		if err == nil {
			last = fmt.Errorf("role %s", role)
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("port %d role: %v", port, last)
}

func waitReplicaOf(port, masterPort int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		role, mp, state, err := roleNow(port)
		if err == nil && role == "slave" && mp == masterPort && state == "connected" {
			return nil
		}
		last = fmt.Sprintf("role=%s master=%d state=%s err=%v", role, mp, state, err)
		time.Sleep(30 * time.Millisecond)
	}
	return fmt.Errorf("replica %d of %d: %s", port, masterPort, last)
}

func waitStable(sentinels []int, masterPort, replicas int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last string
	needPeers := len(sentinels) - 1
	for time.Now().Before(deadline) {
		ok := true
		var b strings.Builder
		for _, port := range sentinels {
			m, err := sentinelMaster(port)
			if err != nil {
				ok = false
				fmt.Fprintf(&b, "s%d %v\n", port, err)
				continue
			}
			others, _ := strconv.Atoi(m["num-other-sentinels"])
			slaves, _ := strconv.Atoi(m["num-slaves"])
			flags := m["flags"]
			fmt.Fprintf(&b, "s%d port=%s flags=%s others=%d slaves=%d\n", port, m["port"], flags, others, slaves)
			if m["port"] != strconv.Itoa(masterPort) || others < needPeers || slaves < replicas {
				ok = false
			}
			if strings.Contains(flags, "s_down") || strings.Contains(flags, "o_down") {
				ok = false
			}
		}
		last = b.String()
		if ok {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("sentinels not stable:\n%s%s", last, tailLogs())
}

func waitKey(port int, key, want string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var got string
	for time.Now().Before(deadline) {
		c, err := dial(addr(port), 300*time.Millisecond)
		if err == nil {
			v, err := c.cmd("GET", key)
			c.Close()
			if err == nil && !v.Nil && v.Str == want {
				return nil
			}
			if err == nil {
				got = v.Str
				if v.Nil {
					got = "<nil>"
				}
			} else {
				got = err.Error()
			}
		} else {
			got = err.Error()
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("port %d key %s = %s, want %s", port, key, got, want)
}

func setKey(port int, key, val string) error {
	c, err := dial(addr(port), time.Second)
	if err != nil {
		return err
	}
	defer c.Close()
	_, err = c.cmd("SET", key, val)
	return err
}

func getKey(port int, key string) (string, bool, error) {
	c, err := dial(addr(port), time.Second)
	if err != nil {
		return "", false, err
	}
	defer c.Close()
	v, err := c.cmd("GET", key)
	if err != nil {
		return "", false, err
	}
	if v.Nil {
		return "", false, nil
	}
	return v.Str, true, nil
}

func monitorLine(port int) (string, error) {
	b, err := os.ReadFile(confPath(port))
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "sentinel monitor ") {
			return line, nil
		}
	}
	return "", fmt.Errorf("no monitor line in %s", confPath(port))
}

func confLines(port int, prefix string) []string {
	b, err := os.ReadFile(confPath(port))
	if err != nil {
		return nil
	}
	var out []string
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, prefix) {
			out = append(out, line)
		}
	}
	return out
}

func tailFile(path string, n int) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	lines := strings.Split(string(b), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

func tailLogs() string {
	var b strings.Builder
	for _, port := range []int{portM, portR1, portR2, portS1, portS2, portS3} {
		text := tailFile(logPath(port), 25)
		if strings.TrimSpace(text) == "" {
			continue
		}
		fmt.Fprintf(&b, "\n--- %d ---\n%s\n", port, text)
	}
	return b.String()
}

type clusterOpts struct {
	replicas        []dataOpts
	quorum          int
	downAfter       int
	failoverTimeout int
	sentinels       int
}

func (o clusterOpts) sentinelPorts() []int {
	all := []int{portS1, portS2, portS3}
	if o.sentinels <= 0 || o.sentinels > len(all) {
		return all
	}
	return all[:o.sentinels]
}

func startCluster(o clusterOpts) error {
	stopAll()
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	for _, port := range []int{portM, portR1, portR2, portS1, portS2, portS3} {
		_ = os.RemoveAll(dataDir(port))
		_ = os.Remove(confPath(port))
		_ = os.Remove(logPath(port))
	}
	if err := writeDataConf(dataOpts{port: portM, priority: 100}); err != nil {
		return err
	}
	if err := launch(portM, false); err != nil {
		return err
	}
	for _, r := range o.replicas {
		if r.port == 0 {
			return fmt.Errorf("replica port is 0")
		}
		if r.priority == 0 && !r.announce && r.replicaOf == "" {
			r.priority = 100
		}
		if !r.announce {
			r.announce = true
		}
		if err := writeDataConf(r); err != nil {
			return err
		}
		if err := launch(r.port, false); err != nil {
			return err
		}
	}
	ports := o.sentinelPorts()
	for _, port := range ports {
		if err := writeSentinelConf(sentinelOpts{
			port:            port,
			masterPort:      portM,
			quorum:          o.quorum,
			downAfter:       o.downAfter,
			failoverTimeout: o.failoverTimeout,
		}); err != nil {
			return err
		}
		if err := launch(port, true); err != nil {
			return err
		}
	}
	if err := tuneSentinels(ports); err != nil {
		return err
	}
	for _, r := range o.replicas {
		host, p, _ := strings.Cut(r.replicaOf, ":")
		mp, _ := strconv.Atoi(p)
		// 从库连的是代理时，链路状态看的是代理那一端，仍然要等到 connected。
		_ = host
		if err := waitReplicaOf(r.port, mp, 8*time.Second); err != nil {
			return err
		}
	}
	if err := waitStable(ports, portM, len(o.replicas), 15*time.Second); err != nil {
		return err
	}
	return nil
}

func simpleReplicas(n int, upstream string) []dataOpts {
	if upstream == "" {
		upstream = addr(portM)
	}
	out := []dataOpts{{port: portR1, replicaOf: upstream, priority: 100, announce: true}}
	if n > 1 {
		out = append(out, dataOpts{port: portR2, replicaOf: upstream, priority: 100, announce: true})
	}
	return out
}
