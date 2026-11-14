package main

import (
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"
)

type server struct {
	ID    string
	Addr  string
	Port  int
	Pid   int
	Slots int
	Flags string
	c     *Conn
}

func (s *server) call(parts ...string) (Val, error) {
	if s.c == nil {
		c, err := dial(s.Addr, 2*time.Second)
		if err != nil {
			return Val{}, err
		}
		s.c = c
	}
	v, err := s.c.call(parts...)
	if err != nil {
		s.c.Close()
		s.c = nil
		return Val{}, err
	}
	return v, nil
}

func (s *server) do(parts ...string) (Val, error) {
	v, err := s.call(parts...)
	if err != nil {
		return Val{}, err
	}
	if v.Err != "" {
		return v, fmt.Errorf("redis: %s", v.Err)
	}
	return v, nil
}

func (s *server) info(section string) (string, error) {
	v, err := s.do("INFO", section)
	if err != nil {
		return "", err
	}
	return v.Str, nil
}

func (s *server) refreshMeta() error {
	info, err := s.info("server")
	if err != nil {
		return err
	}
	s.Pid = int(infoInt(info, "process_id"))
	return nil
}

type cluster struct {
	masters []*server
	alone   *server
	owner   [16384]*server
	byAddr  map[string]*server
	redis   string
	confdir string
}

func attach(ports []int, alonePort int, redisBin, confdir string) (*cluster, error) {
	cl := &cluster{
		redis:   redisBin,
		confdir: confdir,
		byAddr:  map[string]*server{},
	}
	for _, p := range ports {
		s := &server{Addr: "127.0.0.1:" + strconv.Itoa(p), Port: p}
		if _, err := s.do("PING"); err != nil {
			return nil, fmt.Errorf("ping %s: %w", s.Addr, err)
		}
		cl.masters = append(cl.masters, s)
		cl.byAddr[s.Addr] = s
	}
	cl.alone = &server{Addr: "127.0.0.1:" + strconv.Itoa(alonePort), Port: alonePort}
	if _, err := cl.alone.do("PING"); err != nil {
		return nil, fmt.Errorf("ping standalone: %w", err)
	}
	if err := cl.refresh(); err != nil {
		return nil, err
	}
	return cl, nil
}

func (cl *cluster) close() {
	for _, s := range cl.masters {
		if s.c != nil {
			s.c.Close()
		}
	}
	if cl.alone != nil && cl.alone.c != nil {
		cl.alone.c.Close()
	}
}

func (cl *cluster) refresh() error {
	var text string
	var err error
	for _, s := range cl.masters {
		v, e := s.call("CLUSTER", "NODES")
		if e != nil {
			err = e
			continue
		}
		if v.Err != "" {
			err = fmt.Errorf("redis: %s", v.Err)
			continue
		}
		text = v.Str
		break
	}
	if text == "" {
		return fmt.Errorf("CLUSTER NODES: %w", err)
	}
	for i := range cl.owner {
		cl.owner[i] = nil
	}
	found := map[string]*server{}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		f := strings.Fields(line)
		if len(f) < 8 {
			continue
		}
		flags := f[2]
		if !strings.Contains(flags, "master") {
			continue
		}
		addr := strings.Split(f[1], "@")[0]
		s := cl.byAddr[addr]
		if s == nil {
			return fmt.Errorf("CLUSTER NODES has unknown addr %s", addr)
		}
		s.ID = f[0]
		s.Flags = flags
		s.Slots = 0
		found[addr] = s
		for _, tok := range f[8:] {
			if err := cl.applyToken(s, tok); err != nil {
				return err
			}
		}
	}
	if len(found) != len(cl.masters) {
		return fmt.Errorf("expected %d masters, parsed %d", len(cl.masters), len(found))
	}
	for _, s := range cl.masters {
		if err := s.refreshMeta(); err != nil {
			return err
		}
	}
	if cl.alone != nil {
		if err := cl.alone.refreshMeta(); err != nil {
			return err
		}
	}
	return nil
}

func (cl *cluster) applyToken(s *server, tok string) error {
	if strings.HasPrefix(tok, "[") && strings.HasSuffix(tok, "]") {
		body := tok[1 : len(tok)-1]
		if i := strings.Index(body, "->-"); i > 0 {
			slot, err := strconv.Atoi(body[:i])
			if err != nil {
				return fmt.Errorf("migrating token %s", tok)
			}
			cl.owner[slot] = s
			s.Slots++
		}
		return nil
	}
	if strings.Contains(tok, "-") {
		parts := strings.Split(tok, "-")
		if len(parts) != 2 {
			return fmt.Errorf("slot token %s", tok)
		}
		a, err1 := strconv.Atoi(parts[0])
		b, err2 := strconv.Atoi(parts[1])
		if err1 != nil || err2 != nil || a > b {
			return fmt.Errorf("slot range %s", tok)
		}
		for slot := a; slot <= b; slot++ {
			cl.owner[slot] = s
		}
		s.Slots += b - a + 1
		return nil
	}
	slot, err := strconv.Atoi(tok)
	if err != nil {
		return fmt.Errorf("slot token %s", tok)
	}
	cl.owner[slot] = s
	s.Slots++
	return nil
}

func (cl *cluster) slotSum() int {
	n := 0
	for _, s := range cl.masters {
		n += s.Slots
	}
	return n
}

func (cl *cluster) ownerOf(slot int) *server {
	if slot < 0 || slot >= len(cl.owner) {
		return nil
	}
	return cl.owner[slot]
}

func (cl *cluster) otherThan(s *server) *server {
	for _, m := range cl.masters {
		if m.Addr != s.Addr && !strings.Contains(m.Flags, "fail") {
			return m
		}
	}
	return nil
}

func (cl *cluster) byPort(port int) *server {
	for _, m := range cl.masters {
		if m.Port == port {
			return m
		}
	}
	return nil
}

func (cl *cluster) keyOn(prefix, addr string) (string, int, error) {
	for i := 0; i < 300000; i++ {
		k := fmt.Sprintf("%s:%d", prefix, i)
		slot := slotOf(k)
		own := cl.ownerOf(slot)
		if own != nil && own.Addr == addr {
			return k, slot, nil
		}
	}
	return "", 0, fmt.Errorf("no key on %s with prefix %s", addr, prefix)
}

func (cl *cluster) tagOn(addr string) (string, int, error) {
	for i := 0; i < 300000; i++ {
		tag := fmt.Sprintf("t%d", i)
		slot := slotOf("{" + tag + "}")
		own := cl.ownerOf(slot)
		if own != nil && own.Addr == addr {
			return tag, slot, nil
		}
	}
	return "", 0, fmt.Errorf("no tag on %s", addr)
}

func (cl *cluster) clusterInfo(s *server) (map[string]string, error) {
	v, err := s.call("CLUSTER", "INFO")
	if err != nil {
		return nil, err
	}
	if v.Err != "" {
		return nil, fmt.Errorf("redis: %s", v.Err)
	}
	m := map[string]string{}
	for _, line := range strings.Split(v.Str, "\n") {
		line = strings.TrimSpace(strings.TrimRight(line, "\r"))
		if line == "" || !strings.Contains(line, ":") {
			continue
		}
		k, val, _ := strings.Cut(line, ":")
		m[k] = val
	}
	return m, nil
}

func (cl *cluster) stateOf(s *server) (string, error) {
	m, err := cl.clusterInfo(s)
	if err != nil {
		return "", err
	}
	return m["cluster_state"], nil
}

func (s *server) countSlot(slot int) (int64, error) {
	v, err := s.do("CLUSTER", "COUNTKEYSINSLOT", strconv.Itoa(slot))
	if err != nil {
		return 0, err
	}
	if v.Kind == ':' {
		return v.Int, nil
	}
	n, _ := strconv.ParseInt(v.Str, 10, 64)
	return n, nil
}

func (s *server) keysInSlot(slot int) ([]string, error) {
	var all []string
	for {
		v, err := s.do("CLUSTER", "GETKEYSINSLOT", strconv.Itoa(slot), "512")
		if err != nil {
			return nil, err
		}
		batch := valStrings(v)
		all = append(all, batch...)
		if len(batch) < 512 {
			return all, nil
		}
	}
}

func (cl *cluster) flushMasters() error {
	for _, s := range cl.masters {
		if _, err := s.do("FLUSHALL"); err != nil {
			return fmt.Errorf("flush %s: %w", s.Addr, err)
		}
	}
	return nil
}

func (cl *cluster) beginMigrate(slot int, src, dst *server) error {
	if _, err := dst.do("CLUSTER", "SETSLOT", strconv.Itoa(slot), "IMPORTING", src.ID); err != nil {
		return fmt.Errorf("importing: %w", err)
	}
	if _, err := src.do("CLUSTER", "SETSLOT", strconv.Itoa(slot), "MIGRATING", dst.ID); err != nil {
		return fmt.Errorf("migrating: %w", err)
	}
	return nil
}

func (cl *cluster) assignSlot(slot int, id string) error {
	for _, s := range cl.masters {
		if _, err := s.do("CLUSTER", "SETSLOT", strconv.Itoa(slot), "NODE", id); err != nil {
			return fmt.Errorf("setslot %s: %w", s.Addr, err)
		}
	}
	return cl.refresh()
}

func (cl *cluster) stable(slot int) {
	for _, s := range cl.masters {
		_, _ = s.call("CLUSTER", "SETSLOT", strconv.Itoa(slot), "STABLE")
	}
}

func migrateKeys(src, dst *server, keys []string, replace bool) (Val, error) {
	if len(keys) == 0 {
		return Val{Kind: '+', Str: "NOKEY"}, nil
	}
	host, port, _ := strings.Cut(dst.Addr, ":")
	args := []string{"MIGRATE", host, port, "", "0", "15000"}
	if replace {
		args = append(args, "REPLACE")
	}
	args = append(args, "KEYS")
	args = append(args, keys...)
	return src.call(args...)
}

func (cl *cluster) deleteSlotKeys(slot int) error {
	for _, s := range cl.masters {
		keys, err := s.keysInSlot(slot)
		if err != nil {
			return err
		}
		for _, k := range keys {
			v, err := s.call("DEL", k)
			if err != nil {
				return err
			}
			if strings.HasPrefix(v.Err, "MOVED") || strings.HasPrefix(v.Err, "ASK") {
				if _, err := s.do("ASKING"); err != nil {
					return err
				}
				if _, err := s.do("DEL", k); err != nil {
					return fmt.Errorf("del %s on %s: %w", k, s.Addr, err)
				}
			} else if v.Err != "" {
				return fmt.Errorf("del %s on %s: %s", k, s.Addr, v.Err)
			}
		}
	}
	return nil
}

// resetSlot 删掉这个槽上的键，把槽还给 original，并清掉 migrating。
func (cl *cluster) resetSlot(slot int, originalID string) error {
	if err := cl.deleteSlotKeys(slot); err != nil {
		return err
	}
	for _, s := range cl.masters {
		n, err := s.countSlot(slot)
		if err != nil {
			return err
		}
		if n != 0 {
			return fmt.Errorf("%s still has %d keys in slot %d", s.Addr, n, slot)
		}
	}
	if err := cl.assignSlot(slot, originalID); err != nil {
		return err
	}
	cl.stable(slot)
	if err := cl.refresh(); err != nil {
		return err
	}
	own := cl.ownerOf(slot)
	if own == nil || own.ID != originalID {
		return fmt.Errorf("slot %d owner is %v, want %s", slot, own, originalID)
	}
	return nil
}

func (cl *cluster) restart(s *server) error {
	conf := fmt.Sprintf("%s/%d/redis.conf", cl.confdir, s.Port)
	if _, err := os.Stat(conf); err != nil {
		return err
	}
	cmd := exec.Command(cl.redis, conf)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("start %s: %w (%s)", conf, err, strings.TrimSpace(string(out)))
	}
	deadline := time.Now().Add(5 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		c, err := dial(s.Addr, 200*time.Millisecond)
		if err == nil {
			v, err := c.call("PING")
			c.Close()
			if err == nil && v.Str == "PONG" {
				if s.c != nil {
					s.c.Close()
					s.c = nil
				}
				return s.refreshMeta()
			}
			last = err
		} else {
			last = err
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("restart %s: %v", s.Addr, last)
}

func (cl *cluster) logLines(port int, needles []string, limit int) []string {
	b, err := os.ReadFile(fmt.Sprintf("%s/%d/redis.log", cl.confdir, port))
	if err != nil {
		return nil
	}
	var hit []string
	for _, line := range strings.Split(string(b), "\n") {
		for _, n := range needles {
			if strings.Contains(line, n) {
				hit = append(hit, line)
				break
			}
		}
	}
	if len(hit) > limit {
		hit = hit[len(hit)-limit:]
	}
	return hit
}

func sortedMasters(list []*server) []*server {
	out := append([]*server(nil), list...)
	sort.Slice(out, func(i, j int) bool { return out[i].Port < out[j].Port })
	return out
}
