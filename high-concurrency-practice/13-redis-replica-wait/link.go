package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

const deadPort = "6397"

type box struct {
	m, r, r2 *Conn
}

func openBox() (*box, error) {
	m, err := dial(C.master)
	if err != nil {
		return nil, err
	}
	r, err := dial(C.replica)
	if err != nil {
		m.Close()
		return nil, err
	}
	b := &box{m: m, r: r}
	if C.replica2 != "" {
		r2, err := dial(C.replica2)
		if err != nil {
			m.Close()
			r.Close()
			return nil, err
		}
		b.r2 = r2
	}
	return b, nil
}

func (b *box) close() {
	b.m.Close()
	b.r.Close()
	if b.r2 != nil {
		b.r2.Close()
	}
}

func hostPort(addr string) (string, string, error) {
	h, p, err := net.SplitHostPort(addr)
	if err != nil {
		return "", "", err
	}
	return h, p, nil
}

func replicaof(c *Conn, host, port string) error {
	_, err := c.cmd("REPLICAOF", host, port)
	return err
}

func promote(c *Conn) error {
	_, err := c.cmd("REPLICAOF", "NO", "ONE")
	return err
}

func configSet(c *Conn, key, val string) error {
	_, err := c.cmd("CONFIG", "SET", key, val)
	return err
}

func configGet(c *Conn, key string) (string, error) {
	vals, err := c.stringArray("CONFIG", "GET", key)
	if err != nil {
		return "", err
	}
	if len(vals) < 2 {
		return "", fmt.Errorf("CONFIG GET %s -> %v", key, vals)
	}
	return vals[1], nil
}

func dbsize(c *Conn) (int64, error) {
	rep, err := c.cmd("DBSIZE")
	if err != nil {
		return 0, err
	}
	return rep.Int, nil
}

func pidOf(c *Conn) (int, error) {
	info, err := c.info("server")
	if err != nil {
		return 0, err
	}
	id := int(infoInt(info, "process_id"))
	if id <= 0 {
		return 0, fmt.Errorf("no process_id")
	}
	return id, nil
}

func roleOf(c *Conn) (string, error) {
	info, err := c.info("replication")
	if err != nil {
		return "", err
	}
	return infoValue(info, "role"), nil
}

func isCode(err error, code string) bool {
	return err != nil && strings.Contains(err.Error(), code)
}

func (b *box) flush() error {
	_, err := b.m.cmd("FLUSHALL")
	return err
}

func (b *box) waitReplicas(n, ms int) (int64, error) {
	rep, err := b.m.cmd("WAIT", strconv.Itoa(n), strconv.Itoa(ms))
	if err != nil {
		return 0, err
	}
	return rep.Int, nil
}

type syncCnt struct {
	full, partialOK, partialErr, forks, forkUsec, cow int64
}

func readSync(c *Conn) (syncCnt, error) {
	st, err := c.info("stats")
	if err != nil {
		return syncCnt{}, err
	}
	ps, err := c.info("persistence")
	if err != nil {
		return syncCnt{}, err
	}
	return syncCnt{
		full:       infoInt(st, "sync_full"),
		partialOK:  infoInt(st, "sync_partial_ok"),
		partialErr: infoInt(st, "sync_partial_err"),
		forks:      infoInt(st, "total_forks"),
		forkUsec:   infoInt(st, "latest_fork_usec"),
		cow:        infoInt(ps, "rdb_last_cow_size"),
	}, nil
}

func waitSlaves(m *Conn, n int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	last := ""
	for time.Now().Before(deadline) {
		info, err := m.info("replication")
		if err == nil {
			last = infoValue(info, "connected_slaves")
			online := strings.Count(info, "state=online")
			if last == strconv.Itoa(n) && (n == 0 || online >= n) {
				return nil
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("connected_slaves=%s want %d", last, n)
}

func waitCaughtUp(m, r *Conn, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	last := ""
	for time.Now().Before(deadline) {
		mi, merr := m.info("replication")
		ri, rerr := r.info("replication")
		if merr != nil || rerr != nil {
			last = fmt.Sprintf("info %v %v", merr, rerr)
			time.Sleep(20 * time.Millisecond)
			continue
		}
		link := infoValue(ri, "master_link_status")
		syncing := infoValue(ri, "master_sync_in_progress")
		mo := infoInt(mi, "master_repl_offset")
		so := infoInt(ri, "slave_repl_offset")
		if link == "up" && syncing == "0" && mo > 0 && mo == so {
			return nil
		}
		last = fmt.Sprintf("link=%s syncing=%s master=%d replica=%d", link, syncing, mo, so)
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("not caught up: %s", last)
}

func (b *box) point(c *Conn, host, port string) error {
	return replicaof(c, host, port)
}

func (b *box) attach(c *Conn) error {
	h, p, err := hostPort(C.master)
	if err != nil {
		return err
	}
	return replicaof(c, h, p)
}

// setReplicaCount 把在线从库收成 n 个。多出来的从库改指一个没有进程的端口，数据还在那个进程里。
func (b *box) setReplicaCount(n int) error {
	h, p, err := hostPort(C.master)
	if err != nil {
		return err
	}
	switch n {
	case 0:
		if err := replicaof(b.r, "127.0.0.1", deadPort); err != nil {
			return err
		}
		if b.r2 != nil {
			if err := replicaof(b.r2, "127.0.0.1", deadPort); err != nil {
				return err
			}
		}
		return waitSlaves(b.m, 0, 5*time.Second)
	case 1:
		if err := replicaof(b.r, h, p); err != nil {
			return err
		}
		if b.r2 != nil {
			if err := replicaof(b.r2, "127.0.0.1", deadPort); err != nil {
				return err
			}
		}
		if err := waitCaughtUp(b.m, b.r, 15*time.Second); err != nil {
			return err
		}
		return waitSlaves(b.m, 1, 5*time.Second)
	case 2:
		if b.r2 == nil {
			return fmt.Errorf("replicas=2 but replica2 is empty")
		}
		if err := replicaof(b.r, h, p); err != nil {
			return err
		}
		if err := replicaof(b.r2, h, p); err != nil {
			return err
		}
		if err := waitCaughtUp(b.m, b.r, 15*time.Second); err != nil {
			return err
		}
		if err := waitCaughtUp(b.m, b.r2, 15*time.Second); err != nil {
			return err
		}
		return waitSlaves(b.m, 2, 5*time.Second)
	default:
		return fmt.Errorf("replicas %d", n)
	}
}

func (b *box) restore() error {
	var first error
	keep := func(err error) {
		if err != nil && first == nil {
			first = err
		}
	}
	keep(configSet(b.m, "min-replicas-to-write", "0"))
	keep(configSet(b.m, "min-replicas-max-lag", "10"))
	keep(configSet(b.m, "maxmemory", "64mb"))
	keep(configSet(b.m, "repl-backlog-size", "1mb"))
	keep(configSet(b.m, "repl-diskless-sync", "yes"))
	keep(configSet(b.m, "repl-diskless-sync-delay", "0"))
	h, p, err := hostPort(C.master)
	keep(err)
	if err == nil {
		keep(replicaof(b.r, h, p))
		if b.r2 != nil {
			keep(replicaof(b.r2, h, p))
		}
		keep(waitCaughtUp(b.m, b.r, 15*time.Second))
		if b.r2 != nil {
			keep(waitCaughtUp(b.m, b.r2, 15*time.Second))
		}
	}
	return first
}

func slaveLag(info string) (maxLag int64, online int) {
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if !strings.Contains(line, "state=") {
			continue
		}
		if strings.Contains(line, "state=online") {
			online++
		}
		for _, part := range strings.Split(line, ",") {
			if strings.HasPrefix(part, "lag=") {
				v, _ := strconv.ParseInt(strings.TrimPrefix(part, "lag="), 10, 64)
				if v > maxLag {
					maxLag = v
				}
			}
		}
	}
	return maxLag, online
}

func finish(f map[string]any, pass bool, detail string) map[string]any {
	f["pass"] = pass
	f["detail"] = detail
	return f
}
