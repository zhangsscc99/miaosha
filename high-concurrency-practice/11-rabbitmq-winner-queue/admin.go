package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const amqpURL = "amqp://guest:guest@127.0.0.1:5672/"

type qStat struct {
	Name         string
	Type         string
	Messages     int64
	Ready        int64
	Unacked      int64
	Memory       int64
	MessageBytes int64
	Consumers    int64
}

type nodeStat struct {
	Alarms json.RawMessage `json:"alarms"`
	Limit  int64           `json:"vm_memory_high_watermark_limit"`
	Memory struct {
		Total struct {
			RSS       int64 `json:"rss"`
			Allocated int64 `json:"allocated"`
			Erlang    int64 `json:"erlang"`
		} `json:"total"`
	} `json:"memory"`
}

func dial() *amqp.Connection {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		panic(err)
	}
	return conn
}

func dialRetry(d time.Duration) (*amqp.Connection, error) {
	deadline := time.Now().Add(d)
	var err error
	for time.Now().Before(deadline) {
		var conn *amqp.Connection
		conn, err = amqp.Dial(amqpURL)
		if err == nil {
			return conn, nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return nil, err
}

func channel(conn *amqp.Connection) *amqp.Channel {
	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	return ch
}

func classicArgs(extra amqp.Table) amqp.Table {
	args := amqp.Table{"x-queue-type": "classic"}
	for k, v := range extra {
		args[k] = v
	}
	return args
}

func resetQueue(name string, durable bool, args amqp.Table) error {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return err
	}
	ch, err := conn.Channel()
	if err == nil {
		_, _ = ch.QueueDelete(name, false, false, false)
	}
	conn.Close()

	conn, err = amqp.Dial(amqpURL)
	if err != nil {
		return err
	}
	defer conn.Close()
	ch, err = conn.Channel()
	if err != nil {
		return err
	}
	_, err = ch.QueueDeclare(name, durable, false, false, false, args)
	return err
}

func queueAbsent(name string) bool {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return false
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return false
	}
	_, err = ch.QueueDeclarePassive(name, true, false, false, false, nil)
	return err != nil
}

func ctl(args ...string) ([]byte, error) {
	argv := make([]string, 0, 2+len(args))
	argv = append(argv, "broker.sh", "ctl")
	argv = append(argv, args...)
	cmd := exec.Command("bash", argv...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return out, fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return out, nil
}

func listQueues() ([]qStat, error) {
	out, err := ctl("--formatter", "json", "list_queues", "name", "type", "messages", "messages_ready", "messages_unacknowledged", "memory", "message_bytes", "consumers")
	if err != nil {
		return nil, err
	}
	i := bytes.IndexByte(out, '[')
	if i < 0 {
		return nil, fmt.Errorf("list_queues: %s", out)
	}
	var rows []map[string]any
	if err := json.Unmarshal(out[i:], &rows); err != nil {
		return nil, err
	}
	qs := make([]qStat, 0, len(rows))
	for _, row := range rows {
		qs = append(qs, qStat{
			Name:         asString(row["name"]),
			Type:         asString(row["type"]),
			Messages:     asInt(row["messages"]),
			Ready:        asInt(row["messages_ready"]),
			Unacked:      asInt(row["messages_unacknowledged"]),
			Memory:       asInt(row["memory"]),
			MessageBytes: asInt(row["message_bytes"]),
			Consumers:    asInt(row["consumers"]),
		})
	}
	return qs, nil
}

func findQueue(name string) (qStat, bool) {
	qs, err := listQueues()
	if err != nil {
		return qStat{}, false
	}
	for _, q := range qs {
		if q.Name == name {
			return q, true
		}
	}
	return qStat{}, false
}

func waitQueue(name string, timeout time.Duration, pred func(qStat) bool) (qStat, bool) {
	deadline := time.Now().Add(timeout)
	var last qStat
	for {
		if q, ok := findQueue(name); ok {
			last = q
			if pred(q) {
				return q, true
			}
		}
		if time.Now().After(deadline) {
			return last, false
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func status() (nodeStat, error) {
	out, err := ctl("--formatter", "json", "status")
	if err != nil {
		return nodeStat{}, err
	}
	i := bytes.IndexByte(out, '{')
	if i < 0 {
		return nodeStat{}, fmt.Errorf("status: %s", out)
	}
	var st nodeStat
	err = json.Unmarshal(out[i:], &st)
	return st, err
}

func alarmOn(raw json.RawMessage) bool {
	s := strings.TrimSpace(string(raw))
	return s != "" && s != "[]" && s != "null"
}

func diskUsage() (apparent, allocated int64, err error) {
	cmd := exec.Command("bash", "broker.sh", "du")
	out, err := cmd.Output()
	if err != nil {
		return 0, 0, err
	}
	for _, line := range strings.Split(string(out), "\n") {
		if v, ok := strings.CutPrefix(line, "apparent="); ok {
			apparent, _ = strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		}
		if v, ok := strings.CutPrefix(line, "allocated="); ok {
			allocated, _ = strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		}
	}
	return apparent, allocated, nil
}

func brokerPID() int {
	out, err := exec.Command("bash", "broker.sh", "pid").Output()
	if err != nil {
		return 0
	}
	pid, _ := strconv.Atoi(strings.TrimSpace(string(out)))
	return pid
}

func restartBroker() error {
	if out, err := exec.Command("bash", "broker.sh", "kill").CombinedOutput(); err != nil {
		return fmt.Errorf("kill: %w %s", err, out)
	}
	if out, err := exec.Command("bash", "broker.sh", "start").CombinedOutput(); err != nil {
		return fmt.Errorf("start: %w %s", err, out)
	}
	conn, err := dialRetry(30 * time.Second)
	if err != nil {
		return err
	}
	conn.Close()
	return nil
}

func wipeQueues() {
	qs, err := listQueues()
	if err != nil {
		return
	}
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return
	}
	defer conn.Close()
	for _, q := range qs {
		if !strings.HasPrefix(q.Name, "ms11-") {
			continue
		}
		ch, err := conn.Channel()
		if err != nil {
			return
		}
		_, _ = ch.QueueDelete(q.Name, false, false, false)
		_ = ch.Close()
	}
}

func cpuTicks(pid int) float64 {
	if pid <= 0 {
		return 0
	}
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0
	}
	s := string(b)
	i := strings.LastIndex(s, ")")
	if i < 0 || i+2 >= len(s) {
		return 0
	}
	fields := strings.Fields(s[i+2:])
	if len(fields) < 13 {
		return 0
	}
	ut, _ := strconv.ParseFloat(fields[11], 64)
	st, _ := strconv.ParseFloat(fields[12], 64)
	return ut + st
}

func cpuPercent(before, after, elapsed float64) float64 {
	if elapsed <= 0 {
		return 0
	}
	return (after - before) / 100 / elapsed * 100
}

func rssKB(pid int) int64 {
	if pid <= 0 {
		return 0
	}
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				v, _ := strconv.ParseInt(fields[1], 10, 64)
				return v
			}
		}
	}
	return 0
}

func asString(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprint(v)
}

func asInt(v any) int64 {
	switch t := v.(type) {
	case float64:
		return int64(t)
	case int64:
		return t
	case json.Number:
		i, _ := t.Int64()
		return i
	case string:
		i, _ := strconv.ParseInt(t, 10, 64)
		return i
	default:
		return 0
	}
}
