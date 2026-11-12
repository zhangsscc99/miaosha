package main

import (
	"net"
	"sync"
	"time"
)

// delayProxy 把复制连接的每个方向按块 sleep。这是本机用户态延迟，不是第二台机器。
type delayProxy struct {
	ln     net.Listener
	target string
	delay  time.Duration
}

func startDelayProxy(listen, target string, delay time.Duration) (*delayProxy, error) {
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}
	p := &delayProxy{ln: ln, target: target, delay: delay}
	go p.loop()
	return p, nil
}

func (p *delayProxy) Close() {
	if p != nil && p.ln != nil {
		_ = p.ln.Close()
	}
}

func (p *delayProxy) loop() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		go p.bridge(c)
	}
}

func (p *delayProxy) bridge(down net.Conn) {
	up, err := net.DialTimeout("tcp", p.target, 2*time.Second)
	if err != nil {
		_ = down.Close()
		return
	}
	setNoDelay(down)
	setNoDelay(up)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		pump(up, down, p.delay)
		_ = up.Close()
	}()
	go func() {
		defer wg.Done()
		pump(down, up, p.delay)
		_ = down.Close()
	}()
	wg.Wait()
}

func setNoDelay(c net.Conn) {
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
}

func pump(dst, src net.Conn, delay time.Duration) {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if delay > 0 {
				time.Sleep(delay)
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}
