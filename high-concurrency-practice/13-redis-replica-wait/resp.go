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
	Kind byte
	Int  int64
	Str  string
	Nil  bool
}

// Conn 是一条阻塞式 Redis 连接。每个压测 worker 独占一条，避免加锁。
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
	}
	return &Conn{
		nc:  nc,
		r:   bufio.NewReaderSize(nc, 64*1024),
		w:   bufio.NewWriterSize(nc, 16*1024),
		buf: make([]byte, 0, 512),
	}, nil
}

func (c *Conn) Close() {
	if c != nil && c.nc != nil {
		_ = c.nc.Close()
	}
}

func (c *Conn) cmd(parts ...string) (Reply, error) {
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
	if _, err := c.w.Write(b); err != nil {
		return Reply{}, err
	}
	if err := c.w.Flush(); err != nil {
		return Reply{}, err
	}
	return readReply(c.r)
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
		for i := int64(0); i < n; i++ {
			if _, err := readReply(r); err != nil {
				return Reply{}, err
			}
		}
		return Reply{Kind: '*', Int: n}, nil
	default:
		return Reply{}, fmt.Errorf("unknown resp prefix %q", prefix)
	}
}

func (c *Conn) stringArray(parts ...string) ([]string, error) {
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
	if _, err := c.w.Write(b); err != nil {
		return nil, err
	}
	if err := c.w.Flush(); err != nil {
		return nil, err
	}
	prefix, err := c.r.ReadByte()
	if err != nil {
		return nil, err
	}
	line, err := c.r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if prefix != '*' {
		return nil, fmt.Errorf("expected array, got %q%s", prefix, strings.TrimRight(line, "\r\n"))
	}
	n, err := strconv.ParseInt(strings.TrimRight(line, "\r\n"), 10, 64)
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, nil
	}
	out := make([]string, 0, n)
	for i := int64(0); i < n; i++ {
		rep, err := readReply(c.r)
		if err != nil {
			return nil, err
		}
		if rep.Kind != '$' || rep.Nil {
			return nil, fmt.Errorf("expected bulk string in array")
		}
		out = append(out, rep.Str)
	}
	return out, nil
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

func infoInt(info, key string) int64 {
	v, _ := strconv.ParseInt(strings.TrimSpace(infoValue(info, key)), 10, 64)
	return v
}
