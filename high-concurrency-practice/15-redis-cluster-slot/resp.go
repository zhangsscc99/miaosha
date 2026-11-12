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

// Val 是一次 RESP 值。Redis 的错误回复留在 Err，连接不关。
type Val struct {
	Kind  byte
	Str   string
	Int   int64
	Elems []Val
	Nil   bool
	Err   string
}

type Conn struct {
	nc  net.Conn
	r   *bufio.Reader
	w   *bufio.Writer
	buf []byte
}

func dial(addr string, timeout time.Duration) (*Conn, error) {
	nc, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	if tc, ok := nc.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	return &Conn{
		nc:  nc,
		r:   bufio.NewReaderSize(nc, 256*1024),
		w:   bufio.NewWriterSize(nc, 64*1024),
		buf: make([]byte, 0, 512),
	}, nil
}

func (c *Conn) Close() {
	if c != nil && c.nc != nil {
		_ = c.nc.Close()
		c.nc = nil
	}
}

func (c *Conn) SetDeadline(t time.Time) {
	if c != nil && c.nc != nil {
		_ = c.nc.SetDeadline(t)
	}
}

func (c *Conn) encode(parts ...string) []byte {
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
	return b
}

func (c *Conn) writeQueued(parts ...string) error {
	_, err := c.w.Write(c.encode(parts...))
	return err
}

func (c *Conn) flush() error {
	return c.w.Flush()
}

func (c *Conn) call(parts ...string) (Val, error) {
	if err := c.writeQueued(parts...); err != nil {
		return Val{}, err
	}
	if err := c.flush(); err != nil {
		return Val{}, err
	}
	return readVal(c.r)
}

func readVal(r *bufio.Reader) (Val, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return Val{}, err
	}
	line, err := r.ReadString('\n')
	if err != nil {
		return Val{}, err
	}
	line = strings.TrimRight(line, "\r\n")
	switch prefix {
	case '+':
		return Val{Kind: '+', Str: line}, nil
	case '-':
		return Val{Kind: '-', Err: line}, nil
	case ':':
		n, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			return Val{}, err
		}
		return Val{Kind: ':', Int: n}, nil
	case '$':
		n, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			return Val{}, err
		}
		if n < 0 {
			return Val{Kind: '$', Nil: true}, nil
		}
		if n > 1<<20 {
			return Val{}, fmt.Errorf("refuse bulk length %d", n)
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return Val{}, err
		}
		return Val{Kind: '$', Str: string(buf[:n])}, nil
	case '*':
		n, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			return Val{}, err
		}
		if n < 0 {
			return Val{Kind: '*', Nil: true}, nil
		}
		elems := make([]Val, 0, n)
		for i := int64(0); i < n; i++ {
			el, err := readVal(r)
			if err != nil {
				return Val{}, err
			}
			elems = append(elems, el)
		}
		return Val{Kind: '*', Elems: elems}, nil
	default:
		return Val{}, fmt.Errorf("unknown resp prefix %q", prefix)
	}
}

func replyText(v Val) string {
	if v.Err != "" {
		return v.Err
	}
	if v.Nil {
		return "nil"
	}
	switch v.Kind {
	case '+', '$':
		return v.Str
	case ':':
		return strconv.FormatInt(v.Int, 10)
	default:
		if v.Kind == 0 {
			return ""
		}
		return fmt.Sprintf("kind=%c", v.Kind)
	}
}

func parseRedirect(msg string) (kind string, slot int, addr string, ok bool) {
	f := strings.Fields(msg)
	if len(f) != 3 || (f[0] != "MOVED" && f[0] != "ASK") {
		return "", 0, "", false
	}
	n, err := strconv.Atoi(f[1])
	if err != nil {
		return "", 0, "", false
	}
	return f[0], n, f[2], true
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

func valStrings(v Val) []string {
	out := make([]string, 0, len(v.Elems))
	for _, el := range v.Elems {
		if el.Kind == '$' || el.Kind == '+' {
			out = append(out, el.Str)
		}
	}
	return out
}
