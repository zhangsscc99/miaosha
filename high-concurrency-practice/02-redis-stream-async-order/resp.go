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

// Reply 是一次 RESP 响应。数组留在 Elems 里，Stream 的读取要靠它。
type Reply struct {
	Kind  byte
	Int   int64
	Str   string
	Nil   bool
	Elems []Reply
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
	}
	return &Conn{
		nc:  nc,
		r:   bufio.NewReaderSize(nc, 64*1024),
		w:   bufio.NewWriterSize(nc, 64*1024),
		buf: make([]byte, 0, 512),
	}, nil
}

func (c *Conn) Close() {
	_ = c.nc.Close()
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
		elems := make([]Reply, 0, n)
		for i := int64(0); i < n; i++ {
			el, err := readReply(r)
			if err != nil {
				return Reply{}, err
			}
			elems = append(elems, el)
		}
		return Reply{Kind: '*', Elems: elems}, nil
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

func (c *Conn) evalSHA(sha string, numKeys int, rest ...string) (int64, error) {
	parts := make([]string, 0, 3+len(rest))
	parts = append(parts, "EVALSHA", sha, strconv.Itoa(numKeys))
	parts = append(parts, rest...)
	rep, err := c.cmd(parts...)
	if err != nil {
		return 0, err
	}
	return rep.Int, nil
}

func (c *Conn) getInt(key string) (int64, error) {
	rep, err := c.cmd("GET", key)
	if err != nil {
		return 0, err
	}
	if rep.Nil || rep.Str == "" {
		return 0, nil
	}
	return strconv.ParseInt(rep.Str, 10, 64)
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
