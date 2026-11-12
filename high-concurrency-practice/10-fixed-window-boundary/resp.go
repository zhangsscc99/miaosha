package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

// Reply 是一次 RESP 响应。
type Reply struct {
	Kind  byte
	Int   int64
	Str   string
	Nil   bool
	Items []Reply
}

// Conn 是一条阻塞式 Redis 连接。每个 worker 独占一条。
type Conn struct {
	nc  net.Conn
	r   *bufio.Reader
	w   *bufio.Writer
	buf []byte
}

func dial(addr string) (*Conn, error) {
	nc, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	if tc, ok := nc.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
		_ = tc.SetKeepAlive(true)
		_ = tc.SetWriteBuffer(4 * 1024 * 1024)
		_ = tc.SetReadBuffer(4 * 1024 * 1024)
	}
	c := &Conn{
		nc:  nc,
		r:   bufio.NewReaderSize(nc, 1024*1024),
		w:   bufio.NewWriterSize(nc, 1024*1024),
		buf: make([]byte, 0, 1024),
	}
	_ = c.nc.SetDeadline(time.Now().Add(15 * time.Minute))
	return c, nil
}

func (c *Conn) Close() {
	_ = c.nc.Close()
}

func (c *Conn) extend(d time.Duration) {
	_ = c.nc.SetDeadline(time.Now().Add(d))
}

func (c *Conn) cmd(parts ...string) (Reply, error) {
	if err := c.writeCmd(parts...); err != nil {
		return Reply{}, err
	}
	if err := c.w.Flush(); err != nil {
		return Reply{}, err
	}
	return readReply(c.r)
}

func (c *Conn) writeCmd(parts ...string) error {
	b := c.buf[:0]
	b = append(b, '*')
	b = strconv.AppendInt(b, int64(len(parts)), 10)
	b = append(b, '\r', '\n')
	for _, p := range parts {
		b = append(b, '$')
		b = strconv.AppendInt(b, int64(len(p)), 10)
		b = append(b, '\r', '\n')
		b = append(b, p...)
		b = append(b, '\r', '\n')
	}
	c.buf = b
	_, err := c.w.Write(b)
	return err
}

func (c *Conn) flush() error {
	return c.w.Flush()
}

func readReply(r *bufio.Reader) (Reply, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return Reply{}, err
	}
	line, err := r.ReadString('\n')
	if err != nil {
		return Reply{}, err
	}
	line = strings.TrimRight(line, "\r\n")
	switch prefix {
	case '+':
		return Reply{Kind: '+', Str: line}, nil
	case '-':
		return Reply{}, fmt.Errorf("redis: %s", line)
	case ':':
		n, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			return Reply{}, err
		}
		return Reply{Kind: ':', Int: n}, nil
	case '$':
		n, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			return Reply{}, err
		}
		if n < 0 {
			return Reply{Kind: '$', Nil: true}, nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return Reply{}, err
		}
		return Reply{Kind: '$', Str: string(buf[:n])}, nil
	case '*':
		n, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			return Reply{}, err
		}
		if n < 0 {
			return Reply{Kind: '*', Nil: true}, nil
		}
		items := make([]Reply, 0, n)
		for i := int64(0); i < n; i++ {
			it, err := readReply(r)
			if err != nil {
				return Reply{}, err
			}
			items = append(items, it)
		}
		return Reply{Kind: '*', Items: items}, nil
	default:
		return Reply{}, fmt.Errorf("unknown resp prefix %q", prefix)
	}
}

func (c *Conn) scriptLoad(body string) (string, error) {
	rep, err := c.cmd("SCRIPT", "LOAD", body)
	if err != nil {
		return "", err
	}
	if rep.Str == "" {
		return "", fmt.Errorf("empty script sha")
	}
	return rep.Str, nil
}

func (c *Conn) evalShaRaw(numKeys int, sha string, args ...string) (Reply, error) {
	parts := make([]string, 0, 3+len(args))
	parts = append(parts, "EVALSHA", sha, strconv.Itoa(numKeys))
	parts = append(parts, args...)
	return c.cmd(parts...)
}

func (c *Conn) evalInt(sha string, key string, args ...int64) (int64, error) {
	parts := make([]string, 0, 4+len(args))
	parts = append(parts, "EVALSHA", sha, "1", key)
	for _, a := range args {
		parts = append(parts, strconv.FormatInt(a, 10))
	}
	rep, err := c.cmd(parts...)
	if err != nil {
		return 0, err
	}
	return rep.Int, nil
}

func (c *Conn) writeEval(sha, key string, args ...int64) error {
	parts := make([]string, 0, 4+len(args))
	parts = append(parts, "EVALSHA", sha, "1", key)
	for _, a := range args {
		parts = append(parts, strconv.FormatInt(a, 10))
	}
	return c.writeCmd(parts...)
}

func (c *Conn) timeMS() (int64, error) {
	rep, err := c.cmd("TIME")
	if err != nil {
		return 0, err
	}
	if len(rep.Items) != 2 {
		return 0, fmt.Errorf("TIME items %d", len(rep.Items))
	}
	sec, err := strconv.ParseInt(rep.Items[0].Str, 10, 64)
	if err != nil {
		return 0, err
	}
	micro, err := strconv.ParseInt(rep.Items[1].Str, 10, 64)
	if err != nil {
		return 0, err
	}
	return sec*1000 + micro/1000, nil
}

func (c *Conn) info(section string) (string, error) {
	rep, err := c.cmd("INFO", section)
	if err != nil {
		return "", err
	}
	return rep.Str, nil
}

func infoValue(info, key string) string {
	prefix := key + ":"
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimRight(line, "\r")
		if strings.HasPrefix(line, prefix) {
			return strings.TrimPrefix(line, prefix)
		}
	}
	return ""
}

func (c *Conn) commands() (int64, error) {
	info, err := c.info("stats")
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(infoValue(info, "total_commands_processed"), 10, 64)
}

func (c *Conn) usedMemory() (int64, error) {
	info, err := c.info("memory")
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(infoValue(info, "used_memory"), 10, 64)
}

func (c *Conn) memoryUsage(key string) (int64, bool, error) {
	rep, err := c.cmd("MEMORY", "USAGE", key)
	if err != nil {
		return 0, false, err
	}
	if rep.Nil {
		return 0, false, nil
	}
	return rep.Int, true, nil
}

func (c *Conn) getInt(key string) (int64, bool, error) {
	rep, err := c.cmd("GET", key)
	if err != nil {
		return 0, false, err
	}
	if rep.Nil || rep.Str == "" {
		return 0, false, nil
	}
	n, err := strconv.ParseInt(rep.Str, 10, 64)
	if err != nil {
		return 0, false, err
	}
	return n, true, nil
}

func (c *Conn) flushDB() error {
	_, err := c.cmd("FLUSHDB")
	return err
}

func redisPID(c *Conn) (int, error) {
	info, err := c.info("server")
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(strings.TrimSpace(infoValue(info, "process_id")))
}
