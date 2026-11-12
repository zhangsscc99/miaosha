package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"sync/atomic"
	"time"
	"unsafe"
)

/*
#cgo LDFLAGS: -L/usr/lib/x86_64-linux-gnu -l:libsqlite3.so.0
#include <stdlib.h>
typedef struct sqlite3 sqlite3;
typedef struct sqlite3_stmt sqlite3_stmt;
int sqlite3_open_v2(const char *filename, sqlite3 **ppDb, int flags, const char *zVfs);
int sqlite3_close(sqlite3*);
int sqlite3_exec(sqlite3*, const char *sql, int (*cb)(void*,int,char**,char**), void*, char **errmsg);
const char *sqlite3_errmsg(sqlite3*);
int sqlite3_prepare_v2(sqlite3*, const char *sql, int nByte, sqlite3_stmt **ppStmt, const char **pzTail);
int sqlite3_step(sqlite3_stmt*);
int sqlite3_reset(sqlite3_stmt*);
int sqlite3_finalize(sqlite3_stmt*);
int sqlite3_bind_blob(sqlite3_stmt*, int, const void*, int n, void (*)(void*));
const void *sqlite3_column_blob(sqlite3_stmt*, int iCol);
int sqlite3_column_bytes(sqlite3_stmt*, int iCol);

static int bind_blob_copy(sqlite3_stmt *s, int i, const void *p, int n) {
  return sqlite3_bind_blob(s, i, p, n, (void (*)(void *))-1);
}
*/
import "C"

const (
	sqliteOpenReadWrite = 0x00000002
	sqliteOpenCreate    = 0x00000004
	sqliteOpenFullMutex = 0x00010000
	sqliteRow           = 100
	sqliteDone          = 101
	originBucketsN      = 80
	originBucket        = 100 * time.Millisecond
)

// origin 是这一章的数据库。每次回源都做一次真实的 SQLite SELECT，
// 然后占住连接做一段 CPU 运算。时长由启动探针记下来，不是 sleep。
type origin struct {
	ln    net.Listener
	addr  string
	slots chan *sqlSlot
	loops int
	body  string

	loads      atomic.Int64
	serviceSum atomic.Int64
	serviceMax atomic.Int64
	startNs    atomic.Int64
	buckets    [originBucketsN]atomic.Int64
	sink       atomic.Uint64

	ProbeSQLNs  int64
	ProbeSlowNs int64
	BurnLoops   int
}

type sqlSlot struct {
	db   *C.sqlite3
	stmt *C.sqlite3_stmt
}

func startOrigin(dbPath, body string, pool int, service time.Duration) (*origin, error) {
	if pool < 1 {
		return nil, fmt.Errorf("pool < 1")
	}
	_ = os.Remove(dbPath)
	_ = os.Remove(dbPath + "-wal")
	_ = os.Remove(dbPath + "-shm")
	if err := initDB(dbPath, body); err != nil {
		return nil, err
	}
	o := &origin{slots: make(chan *sqlSlot, pool), body: body}
	for i := 0; i < pool; i++ {
		sl, err := openSlot(dbPath)
		if err != nil {
			return nil, err
		}
		o.slots <- sl
	}
	loops, err := calibrate(body, service)
	if err != nil {
		return nil, err
	}
	o.loops = loops
	o.BurnLoops = loops
	if err := o.probe(); err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	o.ln = ln
	o.addr = ln.Addr().String()
	go o.serve()
	return o, nil
}

func (o *origin) markStart() {
	for i := range o.buckets {
		o.buckets[i].Store(0)
	}
	o.loads.Store(0)
	o.serviceSum.Store(0)
	o.serviceMax.Store(0)
	o.startNs.Store(time.Now().UnixNano())
}

func (o *origin) bucketSnap() []int64 {
	out := make([]int64, originBucketsN)
	for i := range out {
		out[i] = o.buckets[i].Load()
	}
	for len(out) > 0 && out[len(out)-1] == 0 {
		out = out[:len(out)-1]
	}
	return out
}

func initDB(path, body string) error {
	db, err := openDB(path)
	if err != nil {
		return err
	}
	defer C.sqlite3_close(db)
	for _, s := range []string{
		`PRAGMA journal_mode=WAL`,
		`PRAGMA synchronous=NORMAL`,
		`CREATE TABLE goods (id INTEGER PRIMARY KEY, html BLOB)`,
	} {
		if err := execSQL(db, s); err != nil {
			return err
		}
	}
	csql := C.CString(`INSERT INTO goods(id, html) VALUES(1, ?)`)
	defer C.free(unsafe.Pointer(csql))
	var stmt *C.sqlite3_stmt
	if rc := C.sqlite3_prepare_v2(db, csql, -1, &stmt, nil); rc != 0 {
		return fmt.Errorf("prepare insert: %s", C.GoString(C.sqlite3_errmsg(db)))
	}
	defer C.sqlite3_finalize(stmt)
	raw := []byte(body)
	if rc := C.bind_blob_copy(stmt, 1, unsafe.Pointer(&raw[0]), C.int(len(raw))); rc != 0 {
		return fmt.Errorf("bind: %s", C.GoString(C.sqlite3_errmsg(db)))
	}
	if rc := C.sqlite3_step(stmt); rc != sqliteDone {
		return fmt.Errorf("insert step %d: %s", int(rc), C.GoString(C.sqlite3_errmsg(db)))
	}
	return execSQL(db, `PRAGMA wal_checkpoint(FULL)`)
}

func openDB(path string) (*C.sqlite3, error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	var db *C.sqlite3
	flags := C.int(sqliteOpenReadWrite | sqliteOpenCreate | sqliteOpenFullMutex)
	if rc := C.sqlite3_open_v2(cpath, &db, flags, nil); rc != 0 {
		msg := ""
		if db != nil {
			msg = C.GoString(C.sqlite3_errmsg(db))
			C.sqlite3_close(db)
		}
		return nil, fmt.Errorf("open sqlite: %s", msg)
	}
	return db, nil
}

func openSlot(path string) (*sqlSlot, error) {
	db, err := openDB(path)
	if err != nil {
		return nil, err
	}
	if err := execSQL(db, `PRAGMA query_only=ON`); err != nil {
		C.sqlite3_close(db)
		return nil, err
	}
	stmt, err := prepare(db, `SELECT html FROM goods WHERE id=1`)
	if err != nil {
		C.sqlite3_close(db)
		return nil, err
	}
	return &sqlSlot{db: db, stmt: stmt}, nil
}

func execSQL(db *C.sqlite3, sql string) error {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	if rc := C.sqlite3_exec(db, cs, nil, nil, nil); rc != 0 {
		return fmt.Errorf("%s: %s", sql, C.GoString(C.sqlite3_errmsg(db)))
	}
	return nil
}

func prepare(db *C.sqlite3, sql string) (*C.sqlite3_stmt, error) {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	var stmt *C.sqlite3_stmt
	if rc := C.sqlite3_prepare_v2(db, cs, -1, &stmt, nil); rc != 0 {
		return nil, fmt.Errorf("prepare: %s", C.GoString(C.sqlite3_errmsg(db)))
	}
	return stmt, nil
}

func (s *sqlSlot) query() ([]byte, error) {
	if rc := C.sqlite3_reset(s.stmt); rc != 0 {
		return nil, fmt.Errorf("reset: %s", C.GoString(C.sqlite3_errmsg(s.db)))
	}
	rc := C.sqlite3_step(s.stmt)
	if rc != sqliteRow {
		return nil, fmt.Errorf("step %d: %s", int(rc), C.GoString(C.sqlite3_errmsg(s.db)))
	}
	n := int(C.sqlite3_column_bytes(s.stmt, 0))
	p := C.sqlite3_column_blob(s.stmt, 0)
	if n == 0 || p == nil {
		return nil, fmt.Errorf("empty goods row")
	}
	return C.GoBytes(p, C.int(n)), nil
}

func calibrate(body string, target time.Duration) (int, error) {
	n := 200000
	var got time.Duration
	for i := 0; i < 24; i++ {
		t0 := time.Now()
		burnSink.Add(burn(n, body))
		got = time.Since(t0)
		if got >= target/2 && got <= target*2 {
			scaled := int(float64(n) * float64(target) / float64(got))
			if scaled < 1 {
				scaled = 1
			}
			t0 = time.Now()
			burnSink.Add(burn(scaled, body))
			got = time.Since(t0)
			if got > 0 && (got < target*7/10 || got > target*13/10) {
				scaled = int(float64(scaled) * float64(target) / float64(got))
				if scaled < 1 {
					scaled = 1
				}
			}
			return scaled, nil
		}
		if got <= 0 || got < target/2 {
			n *= 2
			if n < 0 || n > 2_000_000_000 {
				return 0, fmt.Errorf("burn did not reach %s (last %s)", target, got)
			}
			continue
		}
		n /= 2
		if n < 1 {
			n = 1
		}
	}
	return 0, fmt.Errorf("burn calibrate failed, last %s n=%d", got, n)
}

var burnSink atomic.Uint64

func burn(n int, body string) uint64 {
	var x uint64 = 1469598103934665603
	for i := 0; i < n; i++ {
		x ^= uint64(body[i%len(body)])
		x *= 1099511628211
	}
	return x
}

func (o *origin) probe() error {
	t0 := time.Now()
	b, err := o.queryOne(false)
	if err != nil {
		return err
	}
	o.ProbeSQLNs = time.Since(t0).Nanoseconds()
	if string(b) != o.body {
		return fmt.Errorf("probe sql mismatch")
	}
	t0 = time.Now()
	b, err = o.queryOne(true)
	if err != nil {
		return err
	}
	o.ProbeSlowNs = time.Since(t0).Nanoseconds()
	if string(b) != o.body {
		return fmt.Errorf("probe slow mismatch")
	}
	o.loads.Store(0)
	o.serviceSum.Store(0)
	o.serviceMax.Store(0)
	return nil
}

func (o *origin) queryOne(slow bool) ([]byte, error) {
	sl := <-o.slots
	t0 := time.Now()
	b, err := sl.query()
	if err == nil && slow && o.loops > 0 {
		o.sink.Add(burn(o.loops, o.body))
	}
	el := time.Since(t0).Nanoseconds()
	o.slots <- sl
	if err != nil {
		return nil, err
	}
	o.loads.Add(1)
	o.serviceSum.Add(el)
	for {
		old := o.serviceMax.Load()
		if el <= old || o.serviceMax.CompareAndSwap(old, el) {
			break
		}
	}
	if start := o.startNs.Load(); start > 0 {
		idx := int(time.Duration(time.Now().UnixNano()-start) / originBucket)
		if idx >= 0 && idx < originBucketsN {
			o.buckets[idx].Add(1)
		}
	}
	return b, nil
}

func (o *origin) serve() {
	for {
		c, err := o.ln.Accept()
		if err != nil {
			return
		}
		go o.handle(c)
	}
}

func (o *origin) handle(c net.Conn) {
	defer c.Close()
	r := bufio.NewReaderSize(c, 1024)
	w := bufio.NewWriterSize(c, 4096)
	hdr := make([]byte, 5)
	for {
		op, err := r.ReadByte()
		if err != nil {
			return
		}
		if op != 'L' {
			return
		}
		body, err := o.queryOne(true)
		status := byte('B')
		if err != nil {
			status = 'E'
			body = []byte(err.Error())
		}
		hdr[0] = status
		binary.BigEndian.PutUint32(hdr[1:], uint32(len(body)))
		if _, err := w.Write(hdr); err != nil {
			return
		}
		if len(body) > 0 {
			if _, err := w.Write(body); err != nil {
				return
			}
		}
		if err := w.Flush(); err != nil {
			return
		}
	}
}

type originClient struct {
	nc net.Conn
	r  *bufio.Reader
	w  *bufio.Writer
}

func dialOrigin(addr string) (*originClient, error) {
	nc, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return nil, err
	}
	if tc, ok := nc.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	return &originClient{nc: nc, r: bufio.NewReaderSize(nc, 1024), w: bufio.NewWriterSize(nc, 64)}, nil
}

func (o *originClient) Close() { _ = o.nc.Close() }

func (o *originClient) load() ([]byte, error) {
	if err := o.w.WriteByte('L'); err != nil {
		return nil, err
	}
	if err := o.w.Flush(); err != nil {
		return nil, err
	}
	var hdr [5]byte
	if _, err := io.ReadFull(o.r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[1:])
	if n > 2_000_000 {
		return nil, fmt.Errorf("origin body %d", n)
	}
	buf := make([]byte, n)
	if n > 0 {
		if _, err := io.ReadFull(o.r, buf); err != nil {
			return nil, err
		}
	}
	if hdr[0] != 'B' {
		return nil, fmt.Errorf("origin: %s", buf)
	}
	return buf, nil
}
