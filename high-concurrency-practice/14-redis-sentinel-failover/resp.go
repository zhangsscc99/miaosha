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

// Val 是一次 RESP 值。错误回复留在 Err 里，由调用方决定要不要当成失败。
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
	setNoDelay(nc)
	return &Conn{
		nc:  nc,
		r:   bufio.NewReaderSize(nc, 256*1024),
		w:   bufio.NewWriterSize(nc, 16*1024),
		buf: make([]byte, 0, 512),
	}, nil
}

func (c *Conn) Close() {
	if c != nil && c.nc != nil {
		_ = c.nc.Close()
	}
}

func (c *Conn) write(parts ...string) error {
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
		return err
	}
	return c.w.Flush()
}

func (c *Conn) cmd(parts ...string) (Val, error) {
	if err := c.write(parts...); err != nil {
		return Val{}, err
	}
	v, err := readVal(c.r)
	if err != nil {
		return Val{}, err
	}
	if v.Kind == '-' {
		return v, fmt.Errorf("redis: %s", v.Err)
	}
	return v, nil
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

func pairs(v Val) (map[string]string, error) {
	if v.Kind != '*' {
		return nil, fmt.Errorf("expected array, got %q", v.Kind)
	}
	if len(v.Elems)%2 != 0 {
		return nil, fmt.Errorf("odd field count %d", len(v.Elems))
	}
	m := make(map[string]string, len(v.Elems)/2)
	for i := 0; i < len(v.Elems); i += 2 {
		k := v.Elems[i]
		val := v.Elems[i+1]
		if k.Kind != '$' && k.Kind != '+' {
			return nil, fmt.Errorf("field name kind %q", k.Kind)
		}
		switch val.Kind {
		case '$', '+':
			m[k.Str] = val.Str
		case ':':
			m[k.Str] = strconv.FormatInt(val.Int, 10)
		default:
			return nil, fmt.Errorf("field %s kind %q", k.Str, val.Kind)
		}
	}
	return m, nil
}

func mapsOf(v Val) ([]map[string]string, error) {
	if v.Kind != '*' {
		return nil, fmt.Errorf("expected array, got %q", v.Kind)
	}
	out := make([]map[string]string, 0, len(v.Elems))
	for _, el := range v.Elems {
		m, err := pairs(el)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, nil
}

func valInt(v Val) int {
	if v.Kind == ':' {
		return int(v.Int)
	}
	n, _ := strconv.Atoi(v.Str)
	return n
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

type sticky struct {
	addr string
	c    *Conn
}

func (s *sticky) Close() {
	if s != nil && s.c != nil {
		s.c.Close()
		s.c = nil
	}
}

func (s *sticky) ensure() error {
	if s.c != nil {
		return nil
	}
	c, err := dial(s.addr, 300*time.Millisecond)
	if err != nil {
		return err
	}
	s.c = c
	return nil
}

func (s *sticky) cmd(parts ...string) (Val, error) {
	if err := s.ensure(); err != nil {
		return Val{}, err
	}
	v, err := s.c.cmd(parts...)
	if err != nil {
		s.c.Close()
		s.c = nil
		return Val{}, err
	}
	return v, nil
}

func addr(port int) string {
	return "127.0.0.1:" + strconv.Itoa(port)
}
