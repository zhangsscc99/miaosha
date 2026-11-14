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
)

// origin 是这一章的「数据库」。每个回源都是一次真实的 SQLite SELECT。
// 慢路径另外占住连接做一段 CPU 运算，时长由启动时的探针记下来，不是 sleep。
type origin struct {
	ln    net.Listener
	addr  string
	slots chan *sqlSlot
	loops int
	body  string

	loads      atomic.Int64
	fastLoads  atomic.Int64
	slowLoads  atomic.Int64
	missLoads  atomic.Int64
	serviceSum atomic.Int64
	serviceMax atomic.Int64
	serviceN   atomic.Int64
	slowSum    atomic.Int64
	slowMax    atomic.Int64
	sink       atomic.Uint64

	ProbeSQLNs  int64
	ProbeSlowNs int64
	BurnLoops   int
}

type sqlSlot struct {
	db   *C.sqlite3
	one  *C.sqlite3_stmt
	miss *C.sqlite3_stmt
}

func startOrigin(dbPath string, body []byte, pool int, service time.Duration) (*origin, error) {
	if pool < 1 {
		return nil, fmt.Errorf("pool < 1")
	}
	if err := os.Remove(dbPath); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	_ = os.Remove(dbPath + "-wal")
	_ = os.Remove(dbPath + "-shm")
	if err := initDB(dbPath, body); err != nil {
		return nil, err
	}
	o := &origin{
		slots: make(chan *sqlSlot, pool),
		body:  string(body),
	}
	for i := 0; i < pool; i++ {
		sl, err := openSlot(dbPath)
		if err != nil {
			return nil, err
		}
		o.slots <- sl
	}
	if service > 0 {
		loops, err := calibrate(o.body, service)
		if err != nil {
			return nil, err
		}
		o.loops = loops
		o.BurnLoops = loops
	}
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

func initDB(path string, body []byte) error {
	db, err := openDB(path)
	if err != nil {
		return err
	}
	defer C.sqlite3_close(db)
	stmts := []string{
		`PRAGMA journal_mode=WAL`,
		`PRAGMA synchronous=NORMAL`,
		`CREATE TABLE goods (id INTEGER PRIMARY KEY, html BLOB)`,
	}
	for _, s := range stmts {
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
	if len(body) > 0 {
		rc := C.bind_blob_copy(stmt, 1, unsafe.Pointer(&body[0]), C.int(len(body)))
		if rc != 0 {
			return fmt.Errorf("bind: %s", C.GoString(C.sqlite3_errmsg(db)))
		}
	}
	rc := C.sqlite3_step(stmt)
	if rc != sqliteDone {
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
	one, err := prepare(db, `SELECT html FROM goods WHERE id=1`)
	if err != nil {
		C.sqlite3_close(db)
		return nil, err
	}
	miss, err := prepare(db, `SELECT html FROM goods WHERE id=999999`)
	if err != nil {
		C.sqlite3_close(db)
		return nil, err
	}
	return &sqlSlot{db: db, one: one, miss: miss}, nil
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

func (s *sqlSlot) query(stmt *C.sqlite3_stmt) ([]byte, bool, error) {
	if rc := C.sqlite3_reset(stmt); rc != 0 {
		return nil, false, fmt.Errorf("reset: %s", C.GoString(C.sqlite3_errmsg(s.db)))
	}
	rc := C.sqlite3_step(stmt)
	if rc == sqliteDone {
		return nil, false, nil
	}
	if rc != sqliteRow {
		return nil, false, fmt.Errorf("step %d: %s", int(rc), C.GoString(C.sqlite3_errmsg(s.db)))
	}
	n := int(C.sqlite3_column_bytes(stmt, 0))
	if n == 0 {
		return []byte{}, true, nil
	}
	p := C.sqlite3_column_blob(stmt, 0)
	if p == nil {
		return nil, false, fmt.Errorf("null blob")
	}
	return C.GoBytes(p, C.int(n)), true, nil
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
	if body == "" {
		body = "x"
	}
	for i := 0; i < n; i++ {
		x ^= uint64(body[i%len(body)])
		x *= 1099511628211
	}
	return x
}

func (o *origin) probe() error {
	t0 := time.Now()
	b, ok, err := o.query(false, false)
	if err != nil {
		return err
	}
	if !ok || string(b) != o.body {
		return fmt.Errorf("probe sql mismatch len %d", len(b))
	}
	o.ProbeSQLNs = time.Since(t0).Nanoseconds()
	t0 = time.Now()
	b, ok, err = o.query(true, false)
	if err != nil {
		return err
	}
	if !ok || string(b) != o.body {
		return fmt.Errorf("probe slow mismatch")
	}
	o.ProbeSlowNs = time.Since(t0).Nanoseconds()
	_, _, err = o.query(false, true)
	if err != nil {
		return err
	}
	o.reset()
	return nil
}

func (o *origin) reset() {
	o.loads.Store(0)
	o.fastLoads.Store(0)
	o.slowLoads.Store(0)
	o.missLoads.Store(0)
	o.serviceSum.Store(0)
	o.serviceMax.Store(0)
	o.serviceN.Store(0)
	o.slowSum.Store(0)
	o.slowMax.Store(0)
}

func (o *origin) query(slow, missing bool) ([]byte, bool, error) {
	sl := <-o.slots
	t0 := time.Now()
	var (
		b   []byte
		ok  bool
		err error
	)
	if missing {
		b, ok, err = sl.query(sl.miss)
	} else {
		b, ok, err = sl.query(sl.one)
	}
	if err == nil && slow && o.loops > 0 && ok {
		o.sink.Add(burn(o.loops, o.body))
	}
	el := time.Since(t0).Nanoseconds()
	o.slots <- sl
	if err != nil {
		return nil, false, err
	}
	o.loads.Add(1)
	o.serviceN.Add(1)
	o.serviceSum.Add(el)
	bumpMax(&o.serviceMax, el)
	switch {
	case missing:
		o.missLoads.Add(1)
	case slow:
		o.slowLoads.Add(1)
		o.slowSum.Add(el)
		bumpMax(&o.slowMax, el)
	default:
		o.fastLoads.Add(1)
	}
	return b, ok, nil
}

func bumpMax(a *atomic.Int64, v int64) {
	for {
		old := a.Load()
		if v <= old || a.CompareAndSwap(old, v) {
			return
		}
	}
}

type originSnap struct {
	Loads      int64
	Fast       int64
	Slow       int64
	Miss       int64
	ServiceSum int64
	ServiceMax int64
	ServiceN   int64
	SlowSum    int64
	SlowMax    int64
}

func (o *origin) snap() originSnap {
	return originSnap{
		Loads:      o.loads.Load(),
		Fast:       o.fastLoads.Load(),
		Slow:       o.slowLoads.Load(),
		Miss:       o.missLoads.Load(),
		ServiceSum: o.serviceSum.Load(),
		ServiceMax: o.serviceMax.Load(),
		ServiceN:   o.serviceN.Load(),
		SlowSum:    o.slowSum.Load(),
		SlowMax:    o.slowMax.Load(),
	}
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
	w := bufio.NewWriterSize(c, 16*1024)
	hdr := make([]byte, 5)
	for {
		op, err := r.ReadByte()
		if err != nil {
			return
		}
		var (
			status = byte('E')
			body   []byte
		)
		switch op {
		case 'F', 'L':
			b, ok, err := o.query(op == 'L', false)
			if err != nil || !ok {
				status = 'E'
				if err != nil {
					body = []byte(err.Error())
				}
				break
			}
			status = 'B'
			body = b
		case 'N':
			_, ok, err := o.query(false, true)
			if err != nil {
				status = 'E'
				body = []byte(err.Error())
				break
			}
			if ok {
				status = 'E'
				body = []byte("missing id returned a row")
				break
			}
			status = 'M'
		default:
			body = []byte("bad op")
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
	return &originClient{nc: nc, r: bufio.NewReaderSize(nc, 16*1024), w: bufio.NewWriterSize(nc, 64)}, nil
}

func (o *originClient) Close() { _ = o.nc.Close() }

func (o *originClient) call(op byte) (byte, []byte, error) {
	if err := o.w.WriteByte(op); err != nil {
		return 0, nil, err
	}
	if err := o.w.Flush(); err != nil {
		return 0, nil, err
	}
	var hdr [5]byte
	if _, err := io.ReadFull(o.r, hdr[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(hdr[1:])
	if n > 2_000_000 {
		return 0, nil, fmt.Errorf("origin body %d", n)
	}
	buf := make([]byte, n)
	if n > 0 {
		if _, err := io.ReadFull(o.r, buf); err != nil {
			return 0, nil, err
		}
	}
	if hdr[0] == 'E' {
		return hdr[0], buf, fmt.Errorf("origin: %s", buf)
	}
	return hdr[0], buf, nil
}
