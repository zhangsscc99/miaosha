package main

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// runHTTP 把「本地售罄」和「每个请求打一次 Redis」放进真正的 HTTP 往返里。
// 客户端和服务端挤在同一台 4 核机器上，数字是这台机器的上限，不是带网关、鉴权和日志的线上值。
func runHTTP(withRedis bool, script string, workers int, duration, warmup time.Duration) (result, error) {
	var nodes []*node
	var sha string
	pool := make(chan *Conn, 16)
	if withRedis {
		var err error
		nodes, err = prepare([]string{"127.0.0.1:6379"}, script, 16)
		if err != nil {
			return result{}, err
		}
		defer closeNodes(nodes)
		if _, err := nodes[0].conns[0].cmd("SET", "sec:stock:1", "0"); err != nil {
			return result{}, err
		}
		sha = nodes[0].sha
		for _, c := range nodes[0].conns {
			pool <- c
		}
	}

	var sold atomic.Bool
	sold.Store(true)
	mux := http.NewServeMux()
	mux.HandleFunc("/miaosha", func(w http.ResponseWriter, r *http.Request) {
		if !withRedis {
			if sold.Load() {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"code":0}`))
				return
			}
		}
		c := <-pool
		code, err := c.evalSHA(sha, 1, "sec:stock:1")
		pool <- c
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fmt.Sprintf(`{"code":%d}`, code)))
	})

	ln, err := net.Listen("tcp", "127.0.0.1:18080")
	if err != nil {
		return result{}, err
	}
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 2 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	defer func() {
		_ = srv.Close()
		_ = ln.Close()
	}()

	transport := &http.Transport{
		MaxIdleConns:        workers,
		MaxIdleConnsPerHost: workers,
		MaxConnsPerHost:     workers,
		DisableCompression:  true,
		IdleConnTimeout:     30 * time.Second,
	}
	client := &http.Client{Transport: transport, Timeout: 2 * time.Second}

	one := func() error {
		resp, err := client.Get("http://127.0.0.1:18080/miaosha")
		if err != nil {
			return err
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("status %d", resp.StatusCode)
		}
		return nil
	}

	warmEnd := time.Now().Add(warmup)
	var warmWG sync.WaitGroup
	warmWG.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer warmWG.Done()
			for time.Now().Before(warmEnd) {
				_ = one()
			}
		}()
	}
	warmWG.Wait()

	var ops int64
	var errs int64
	hists := make([]*hist, workers)
	start := time.Now()
	end := start.Add(duration)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		hists[i] = newHist()
		go func(i int) {
			defer wg.Done()
			var local int64
			var localErr int64
			for time.Now().Before(end) {
				t0 := time.Now()
				err := one()
				hists[i].add(time.Since(t0).Nanoseconds())
				local++
				if err != nil {
					localErr++
				}
			}
			atomic.AddInt64(&ops, local)
			atomic.AddInt64(&errs, localErr)
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)
	merged := newHist()
	for _, h := range hists {
		merged.merge(h)
	}
	name := "http-local"
	note := "HTTP + 进程内售罄标记。没有 Redis、没有鉴权、没有 access log。"
	if withRedis {
		name = "http-redis"
		note = "每个 HTTP 请求在服务端同步执行一次售罄 Lua。服务端使用 16 条 Redis 连接。"
	}
	res := result{
		Scenario:   name,
		Note:       note,
		Addrs:      "127.0.0.1:18080",
		Workers:    workers,
		ElapsedSec: elapsed.Seconds(),
		Ops:        ops,
		SoldOut:    ops - errs,
		Errors:     errs,
		P50us:      merged.le(0.50) / 1000,
		P99us:      merged.le(0.99) / 1000,
		P999us:     merged.le(0.999) / 1000,
		Maxus:      merged.max / 1000,
		Pass:       errs == 0 && ops > 0,
	}
	if elapsed > 0 {
		res.QPS = float64(ops) / elapsed.Seconds()
	}
	return res, nil
}
