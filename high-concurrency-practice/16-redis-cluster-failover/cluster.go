package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

type node struct {
	Port     int
	Addr     string
	Pid      int
	ID       string
	Flags    string
	MasterID string
	Epoch    int64
	Slots    int
	Link     string
	c        *Conn
}

func (n *node) call(parts ...string) (Val, error) {
	if n.c == nil {
		c, err := dial(n.Addr, 2*time.Second)
		if err != nil {
			return Val{}, err
		}
		n.c = c
	}
	v, err := n.c.call(parts...)
	if err != nil {
		n.c.Close()
		n.c = nil
		return Val{}, err
	}
	return v, nil
}

func (n *node) do(parts ...string) (Val, error) {
	v, err := n.call(parts...)
	if err != nil {
		return Val{}, err
	}
	if v.Err != "" {
		return v, fmt.Errorf("%s: %s", n.Addr, v.Err)
	}
	return v, nil
}

func (n *node) close() {
	if n != nil && n.c != nil {
		n.c.Close()
		n.c = nil
	}
}

func (n *node) refreshMeta() error {
	v, err := n.call("INFO", "server")
	if err != nil {
		return err
	}
	if v.Err != "" {
		return fmt.Errorf("%s: %s", n.Addr, v.Err)
	}
	n.Pid = int(infoInt(v.Str, "process_id"))
	return nil
}

type world struct {
	nodes   []*node
	byPort  map[int]*node
	owner   [16384]*node
	redis   string
	confdir string
}

func attach(ports []int, redisBin, confdir string) (*world, error) {
	w := &world{
		redis:   redisBin,
		confdir: confdir,
		byPort:  map[int]*node{},
	}
	for _, p := range ports {
		n := &node{Port: p, Addr: "127.0.0.1:" + strconv.Itoa(p)}
		if _, err := n.do("PING"); err != nil {
			return nil, fmt.Errorf("ping %s: %w", n.Addr, err)
		}
		w.nodes = append(w.nodes, n)
		w.byPort[p] = n
	}
	deadline := time.Now().Add(15 * time.Second)
	var last error
	for {
		if err := w.refresh(); err != nil {
			last = err
		} else {
			reps := 0
			slots := 0
			for _, m := range w.masters() {
				slots += m.Slots
				reps += len(w.replicasOf(m.ID))
			}
			stateOK := true
			for _, n := range w.nodes {
				st, serr := w.state(n)
				if serr != nil || st != "ok" {
					stateOK = false
					if serr != nil {
						last = serr
					} else {
						last = fmt.Errorf("%s state=%s", n.Addr, st)
					}
					break
				}
			}
			if len(w.masters()) == 3 && reps == 3 && slots == 16384 && stateOK {
				return w, nil
			}
			if stateOK {
				last = fmt.Errorf("masters=%d replicas=%d slots=%d", len(w.masters()), reps, slots)
			}
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("cluster not ready: %v", last)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (w *world) close() {
	for _, n := range w.nodes {
		n.close()
	}
}

func hasFlag(flags, want string) bool {
	for _, f := range strings.Split(flags, ",") {
		if f == want {
			return true
		}
	}
	return false
}

func countToken(tok string) int {
	if tok == "" || strings.HasPrefix(tok, "[") {
		return 0
	}
	if strings.Contains(tok, "-") {
		a, b, ok := strings.Cut(tok, "-")
		if !ok {
			return 0
		}
		lo, e1 := strconv.Atoi(a)
		hi, e2 := strconv.Atoi(b)
		if e1 != nil || e2 != nil || hi < lo {
			return 0
		}
		return hi - lo + 1
	}
	if _, err := strconv.Atoi(tok); err == nil {
		return 1
	}
	return 0
}

func applyOwner(owner *[16384]*node, n *node, tok string) {
	if tok == "" || strings.HasPrefix(tok, "[") {
		return
	}
	if strings.Contains(tok, "-") {
		a, b, ok := strings.Cut(tok, "-")
		if !ok {
			return
		}
		lo, e1 := strconv.Atoi(a)
		hi, e2 := strconv.Atoi(b)
		if e1 != nil || e2 != nil || hi < lo {
			return
		}
		for s := lo; s <= hi; s++ {
			owner[s] = n
		}
		return
	}
	slot, err := strconv.Atoi(tok)
	if err != nil || slot < 0 || slot >= 16384 {
		return
	}
	owner[slot] = n
}

func (w *world) refresh() error {
	var text string
	var last error
	for _, n := range w.nodes {
		v, err := n.call("CLUSTER", "NODES")
		if err != nil {
			last = err
			continue
		}
		if v.Err != "" {
			last = fmt.Errorf("%s", v.Err)
			continue
		}
		text = v.Str
		break
	}
	if text == "" {
		return fmt.Errorf("CLUSTER NODES: %v", last)
	}
	for i := range w.owner {
		w.owner[i] = nil
	}
	seen := map[int]bool{}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		f := strings.Fields(line)
		if len(f) < 8 {
			continue
		}
		addr := strings.Split(f[1], "@")[0]
		_, portStr, ok := strings.Cut(addr, ":")
		if !ok {
			continue
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			continue
		}
		n := w.byPort[port]
		if n == nil {
			continue
		}
		seen[port] = true
		n.ID = f[0]
		n.Flags = f[2]
		n.MasterID = f[3]
		if n.MasterID == "-" {
			n.MasterID = ""
		}
		n.Epoch, _ = strconv.ParseInt(f[6], 10, 64)
		n.Link = f[7]
		n.Slots = 0
		if hasFlag(n.Flags, "master") {
			for _, tok := range f[8:] {
				n.Slots += countToken(tok)
				if !hasFlag(n.Flags, "fail") {
					applyOwner(&w.owner, n, tok)
				}
			}
		}
	}
	for _, n := range w.nodes {
		if !seen[n.Port] {
			continue
		}
		_ = n.refreshMeta()
	}
	return nil
}

func (w *world) masters() []*node {
	var out []*node
	for _, n := range w.nodes {
		if hasFlag(n.Flags, "master") && !hasFlag(n.Flags, "fail") && n.Slots > 0 {
			out = append(out, n)
		}
	}
	return out
}

func (w *world) replicasOf(id string) []*node {
	var out []*node
	for _, n := range w.nodes {
		if n.ID == "" || id == "" {
			continue
		}
		if hasFlag(n.Flags, "slave") && n.MasterID == id && !hasFlag(n.Flags, "fail") {
			out = append(out, n)
		}
	}
	return out
}

func (w *world) otherMaster(not *node) *node {
	for _, m := range w.masters() {
		if not == nil || m.Port != not.Port {
			return m
		}
	}
	return nil
}

func (w *world) keyFor(prefix string, m *node) (string, int, error) {
	for i := 0; i < 200000; i++ {
		tag := fmt.Sprintf("t%d", i)
		slot := slotOf("{" + tag + "}")
		if w.owner[slot] == m {
			return prefix + "{" + tag + "}", slot, nil
		}
	}
	return "", 0, fmt.Errorf("no slot on %s for %s", m.Addr, prefix)
}

type pair struct {
	master  *node
	replica *node
	key     string
	slot    int
}

func (w *world) onePair(prefix string) (pair, error) {
	if err := w.refresh(); err != nil {
		return pair{}, err
	}
	for _, m := range w.masters() {
		rs := w.replicasOf(m.ID)
		if len(rs) == 0 {
			continue
		}
		key, slot, err := w.keyFor(prefix, m)
		if err != nil {
			return pair{}, err
		}
		return pair{master: m, replica: rs[0], key: key, slot: slot}, nil
	}
	return pair{}, fmt.Errorf("no master with a replica")
}

func (w *world) state(n *node) (string, error) {
	m, err := w.clusterInfo(n)
	if err != nil {
		return "", err
	}
	return m["cluster_state"], nil
}

func (w *world) clusterInfo(n *node) (map[string]string, error) {
	v, err := n.call("CLUSTER", "INFO")
	if err != nil {
		return nil, err
	}
	if v.Err != "" {
		return nil, fmt.Errorf("%s: %s", n.Addr, v.Err)
	}
	out := map[string]string{}
	for _, line := range strings.Split(v.Str, "\n") {
		line = strings.TrimSpace(strings.TrimRight(line, "\r"))
		if line == "" || !strings.Contains(line, ":") {
			continue
		}
		k, val, _ := strings.Cut(line, ":")
		out[k] = val
	}
	return out, nil
}

func (w *world) waitCaughtUp(m *node, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		v, err := m.call("INFO", "replication")
		if err != nil {
			last = err.Error()
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if v.Err != "" {
			last = v.Err
			time.Sleep(20 * time.Millisecond)
			continue
		}
		off := infoInt(v.Str, "master_repl_offset")
		found := 0
		ok := true
		for _, line := range strings.Split(v.Str, "\n") {
			line = strings.TrimSpace(strings.TrimRight(line, "\r"))
			if !strings.Contains(line, "ip=") || !strings.Contains(line, "offset=") {
				continue
			}
			_, rest, foundCut := strings.Cut(line, ":")
			if !foundCut {
				continue
			}
			found++
			state := ""
			var soff int64 = -1
			for _, p := range strings.Split(rest, ",") {
				k, val, okc := strings.Cut(p, "=")
				if !okc {
					continue
				}
				if k == "state" {
					state = val
				}
				if k == "offset" {
					soff, _ = strconv.ParseInt(val, 10, 64)
				}
			}
			if state != "online" || soff != off {
				ok = false
			}
		}
		if found > 0 && ok {
			return nil
		}
		last = fmt.Sprintf("offset=%d slaves=%d", off, found)
		time.Sleep(15 * time.Millisecond)
	}
	return fmt.Errorf("replicas of %s not caught up: %s", m.Addr, last)
}

func (w *world) kill(n *node) error {
	if err := n.refreshMeta(); err != nil {
		return err
	}
	n.close()
	return killIfRedis(n.Pid)
}

func (w *world) restart(n *node) error {
	conf := fmt.Sprintf("%s/%d/redis.conf", w.confdir, n.Port)
	if _, err := os.Stat(conf); err != nil {
		return err
	}
	cmd := exec.Command(w.redis, conf)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("start %s: %w (%s)", conf, err, strings.TrimSpace(string(out)))
	}
	deadline := time.Now().Add(8 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		n.close()
		v, err := n.call("PING")
		if err == nil && v.Str == "PONG" {
			return n.refreshMeta()
		}
		if err != nil {
			last = err
		} else if v.Err != "" {
			last = fmt.Errorf("%s", v.Err)
		}
		time.Sleep(40 * time.Millisecond)
	}
	return fmt.Errorf("restart %s: %v", n.Addr, last)
}

func (w *world) waitHealthy(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		if err := w.refresh(); err != nil {
			last = err.Error()
			time.Sleep(40 * time.Millisecond)
			continue
		}
		ms := w.masters()
		slots := 0
		reps := 0
		for _, m := range ms {
			slots += m.Slots
			reps += len(w.replicasOf(m.ID))
		}
		if len(ms) != 3 || slots != 16384 || reps < 3 {
			last = fmt.Sprintf("masters=%d slots=%d replicas=%d", len(ms), slots, reps)
			time.Sleep(40 * time.Millisecond)
			continue
		}
		bad := ""
		for _, n := range w.nodes {
			if n.Port == 7207 {
				continue
			}
			st, err := w.state(n)
			if err != nil {
				bad = err.Error()
				break
			}
			if st != "ok" {
				bad = n.Addr + " state=" + st
				break
			}
		}
		if bad == "" {
			return nil
		}
		last = bad
		time.Sleep(40 * time.Millisecond)
	}
	return fmt.Errorf("cluster not healthy: %s", last)
}

func (w *world) configSet(n *node, key, val string) error {
	_, err := n.do("CONFIG", "SET", key, val)
	return err
}

func (w *world) configSetLive(key, val string) error {
	var first error
	for _, n := range w.nodes {
		if n.Port == 7207 {
			continue
		}
		if _, err := n.call("PING"); err != nil {
			continue
		}
		if err := w.configSet(n, key, val); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (w *world) logLen(port int) int64 {
	st, err := os.Stat(fmt.Sprintf("%s/%d/redis.log", w.confdir, port))
	if err != nil {
		return 0
	}
	return st.Size()
}

func (w *world) logNew(port int, from int64, needles []string) []string {
	f, err := os.Open(fmt.Sprintf("%s/%d/redis.log", w.confdir, port))
	if err != nil {
		return nil
	}
	defer f.Close()
	if _, err := f.Seek(from, io.SeekStart); err != nil {
		return nil
	}
	b, err := io.ReadAll(io.LimitReader(f, 1<<20))
	if err != nil && len(b) == 0 {
		return nil
	}
	var hit []string
	for _, line := range strings.Split(string(b), "\n") {
		if line == "" {
			continue
		}
		if len(needles) == 0 {
			hit = append(hit, line)
			continue
		}
		for _, n := range needles {
			if strings.Contains(line, n) {
				hit = append(hit, line)
				break
			}
		}
	}
	if len(hit) > 40 {
		hit = hit[len(hit)-40:]
	}
	return hit
}

func (w *world) used(n *node) int64 {
	v, err := n.call("INFO", "memory")
	if err != nil || v.Err != "" {
		return 0
	}
	return infoInt(v.Str, "used_memory")
}

func (w *world) commands(n *node) int64 {
	v, err := n.call("INFO", "stats")
	if err != nil || v.Err != "" {
		return 0
	}
	return infoInt(v.Str, "total_commands_processed")
}

func roleName(v Val) string {
	if v.Err != "" {
		return v.Err
	}
	if len(v.Elems) == 0 {
		return ""
	}
	return v.Elems[0].Str
}

func (w *world) writeExtraConf(port int) error {
	dir := fmt.Sprintf("%s/%d", w.confdir, port)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	body := fmt.Sprintf(`bind 127.0.0.1
port %d
protected-mode yes
daemonize yes
pidfile %s/redis.pid
logfile %s/redis.log
dir %s
dbfilename dump.rdb
save ""
appendonly no
maxmemory 64mb
maxmemory-policy noeviction
timeout 0
tcp-backlog 511
loglevel notice
enable-debug-command local
cluster-enabled yes
cluster-config-file nodes.conf
cluster-node-timeout 2000
cluster-require-full-coverage yes
repl-diskless-sync yes
repl-diskless-sync-delay 0
`, port, dir, dir, dir)
	return os.WriteFile(dir+"/redis.conf", []byte(body), 0o644)
}

func (w *world) addNode(port int) (*node, error) {
	if err := w.writeExtraConf(port); err != nil {
		return nil, err
	}
	tmp := &node{Port: port, Addr: "127.0.0.1:" + strconv.Itoa(port)}
	err := w.restart(tmp)
	tmp.close()
	if err != nil {
		return nil, err
	}
	n := &node{Port: port, Addr: "127.0.0.1:" + strconv.Itoa(port)}
	if err := n.refreshMeta(); err != nil {
		return nil, err
	}
	w.nodes = append(w.nodes, n)
	w.byPort[port] = n
	return n, nil
}

func (w *world) forget(id string) []string {
	var errs []string
	if id == "" {
		return []string{"empty id"}
	}
	for _, n := range w.nodes {
		if n.ID == id {
			continue
		}
		v, err := n.call("CLUSTER", "FORGET", id)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", n.Addr, err))
			continue
		}
		if v.Err != "" && !strings.Contains(v.Err, "Unknown node") {
			errs = append(errs, n.Addr+": "+v.Err)
		}
	}
	return errs
}

func dialRole(addr string) (string, error) {
	c, err := dial(addr, time.Second)
	if err != nil {
		return "", err
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(800 * time.Millisecond))
	v, err := c.call("ROLE")
	if err != nil {
		return "", err
	}
	return roleName(v), nil
}

func (w *world) drop(port int) {
	if n := w.byPort[port]; n != nil {
		n.close()
	}
	delete(w.byPort, port)
	keep := make([]*node, 0, len(w.nodes))
	for _, n := range w.nodes {
		if n.Port != port {
			keep = append(keep, n)
		}
	}
	w.nodes = keep
}

func dialCall(addr string, parts ...string) (Val, error) {
	c, err := dial(addr, time.Second)
	if err != nil {
		return Val{}, err
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(2 * time.Second))
	return c.call(parts...)
}
