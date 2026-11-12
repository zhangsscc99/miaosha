package main

import "fmt"

// 固定 TTL 和抖动用同一个中心：700 毫秒。抖动是 350 到 1047 毫秒，按 key 取模，均值仍约 700。
// 本地缓存的基准是 400 毫秒。进程偏移 0/200/400/600 毫秒。key 偏移 0 到 399 毫秒。
func ttlFor(mode string, key int) int {
	switch mode {
	case "cold-jitter", "warm-jitter":
		return 350 + (key*97)%701
	case "cold-fixed", "warm-fixed":
		return 700
	default:
		return 60_000
	}
}

func localOffset(mode string, instance, key int) int64 {
	switch mode {
	case "local-proc":
		return int64(instance) * 200
	case "local-key":
		return int64((key * 97) % 400)
	default:
		return 0
	}
}

func keyName(id int) string {
	return fmt.Sprintf("g:%d", id)
}

func buildJobs(keys, repeat int) []int {
	if keys < 1 {
		keys = 1
	}
	if repeat < 1 {
		repeat = 1
	}
	jobs := make([]int, 0, keys*repeat)
	for k := 0; k < keys; k++ {
		for r := 0; r < repeat; r++ {
			jobs = append(jobs, k)
		}
	}
	return jobs
}

func slideMax(b []int64, width int) int64 {
	if len(b) == 0 || width < 1 {
		return 0
	}
	var best, sum int64
	for i := 0; i < len(b); i++ {
		sum += b[i]
		if i >= width {
			sum -= b[i-width]
		}
		if i >= width-1 && sum > best {
			best = sum
		}
	}
	return best
}

const payloadLen = 128

func goodsPayload() string {
	const head = "miaosha-goods-v1|"
	buf := make([]byte, payloadLen)
	copy(buf, head)
	for i := len(head); i < len(buf); i++ {
		buf[i] = 'x'
	}
	return string(buf)
}
