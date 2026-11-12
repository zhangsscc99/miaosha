package main

/*
#cgo LDFLAGS: -L/usr/lib/x86_64-linux-gnu -l:libsqlite3.so.0
#include <stdlib.h>
typedef struct sqlite3 sqlite3;
typedef struct sqlite3_stmt sqlite3_stmt;
typedef long long sqlite3_int64;
int sqlite3_open_v2(const char *filename, sqlite3 **ppDb, int flags, const char *zVfs);
int sqlite3_close(sqlite3*);
int sqlite3_exec(sqlite3*, const char *sql, int (*cb)(void*,int,char**,char**), void*, char **errmsg);
const char *sqlite3_errmsg(sqlite3*);
int sqlite3_prepare_v2(sqlite3*, const char *sql, int nByte, sqlite3_stmt **ppStmt, const char **pzTail);
int sqlite3_step(sqlite3_stmt*);
int sqlite3_reset(sqlite3_stmt*);
int sqlite3_finalize(sqlite3_stmt*);
int sqlite3_bind_int64(sqlite3_stmt*, int, sqlite3_int64);
sqlite3_int64 sqlite3_column_int64(sqlite3_stmt*, int iCol);
const unsigned char *sqlite3_column_text(sqlite3_stmt*, int iCol);
int sqlite3_changes(sqlite3*);
*/
import "C"
import (
	"fmt"
	"os"
	"time"
	"unsafe"
)

const (
	sqliteOK         = 0
	sqliteRow        = 100
	sqliteDone       = 101
	sqliteOpenRead   = 0x00000002
	sqliteOpenCreate = 0x00000004
)

type sqlRun struct {
	Rows     int     `json:"rows"`
	Due      int     `json:"due"`
	NoIndex  []int64 `json:"selectNoIndexUs"`
	Index    []int64 `json:"selectIndexUs"`
	Plan     string  `json:"plan"`
	UpdateUs int64   `json:"updateUs"`
	Changed  int     `json:"changed"`
	Sync     string  `json:"synchronous"`
}

func sqlExec(db *C.sqlite3, query string) error {
	csql := C.CString(query)
	defer C.free(unsafe.Pointer(csql))
	rc := C.sqlite3_exec(db, csql, nil, nil, nil)
	if rc != sqliteOK {
		return fmt.Errorf("sqlite exec %q: %s", query, C.GoString(C.sqlite3_errmsg(db)))
	}
	return nil
}

func sqlOpen(path string) (*C.sqlite3, error) {
	_ = os.Remove(path)
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	var db *C.sqlite3
	rc := C.sqlite3_open_v2(cpath, &db, sqliteOpenRead|sqliteOpenCreate, nil)
	if rc != sqliteOK {
		if db != nil {
			msg := C.GoString(C.sqlite3_errmsg(db))
			C.sqlite3_close(db)
			return nil, fmt.Errorf("sqlite open: %s", msg)
		}
		return nil, fmt.Errorf("sqlite open rc %d", rc)
	}
	return db, nil
}

func sqlPrepare(db *C.sqlite3, query string) (*C.sqlite3_stmt, error) {
	csql := C.CString(query)
	defer C.free(unsafe.Pointer(csql))
	var stmt *C.sqlite3_stmt
	rc := C.sqlite3_prepare_v2(db, csql, -1, &stmt, nil)
	if rc != sqliteOK {
		return nil, fmt.Errorf("prepare %q: %s", query, C.GoString(C.sqlite3_errmsg(db)))
	}
	return stmt, nil
}

func sqlSelectIDs(stmt *C.sqlite3_stmt, deadline int64) (int, time.Duration, error) {
	if rc := C.sqlite3_reset(stmt); rc != sqliteOK {
		return 0, 0, fmt.Errorf("reset %d", int(rc))
	}
	if rc := C.sqlite3_bind_int64(stmt, 1, C.sqlite3_int64(deadline)); rc != sqliteOK {
		return 0, 0, fmt.Errorf("bind %d", int(rc))
	}
	start := time.Now()
	n := 0
	for {
		rc := C.sqlite3_step(stmt)
		if rc == sqliteRow {
			n++
			continue
		}
		if rc == sqliteDone {
			return n, time.Since(start), nil
		}
		return n, time.Since(start), fmt.Errorf("step %d", int(rc))
	}
}

func sqlPlan(db *C.sqlite3) (string, error) {
	stmt, err := sqlPrepare(db, "EXPLAIN QUERY PLAN SELECT id FROM orders WHERE status=0 AND deadline<=?")
	if err != nil {
		return "", err
	}
	defer C.sqlite3_finalize(stmt)
	var parts []string
	for {
		rc := C.sqlite3_step(stmt)
		if rc == sqliteDone {
			break
		}
		if rc != sqliteRow {
			return "", fmt.Errorf("plan step %d", int(rc))
		}
		// detail 在第 4 列（0 起）
		txt := C.sqlite3_column_text(stmt, 3)
		if txt != nil {
			parts = append(parts, C.GoString((*C.char)(unsafe.Pointer(txt))))
		}
	}
	if len(parts) == 0 {
		return "", nil
	}
	out := parts[0]
	for i := 1; i < len(parts); i++ {
		out += " | " + parts[i]
	}
	return out, nil
}

func runSQL(rows, due int) (sqlRun, error) {
	var out sqlRun
	out.Rows = rows
	out.Due = due
	out.Sync = "FULL"
	if due > rows || due < 0 {
		return out, fmt.Errorf("due %d rows %d", due, rows)
	}
	db, err := sqlOpen("/tmp/miaosha-delay-orders.db")
	if err != nil {
		return out, err
	}
	defer C.sqlite3_close(db)
	for _, q := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=FULL",
		"CREATE TABLE orders(id INTEGER PRIMARY KEY, status INTEGER NOT NULL, deadline INTEGER NOT NULL)",
	} {
		if err := sqlExec(db, q); err != nil {
			return out, err
		}
	}
	if err := sqlExec(db, "BEGIN"); err != nil {
		return out, err
	}
	ins, err := sqlPrepare(db, "INSERT INTO orders(id, status, deadline) VALUES(?,?,?)")
	if err != nil {
		return out, err
	}
	far := int64(9_000_000_000_000)
	for i := 1; i <= rows; i++ {
		deadline := far
		if i <= due {
			deadline = 1
		}
		if rc := C.sqlite3_bind_int64(ins, 1, C.sqlite3_int64(i)); rc != sqliteOK {
			C.sqlite3_finalize(ins)
			return out, fmt.Errorf("bind id %d", int(rc))
		}
		if rc := C.sqlite3_bind_int64(ins, 2, 0); rc != sqliteOK {
			C.sqlite3_finalize(ins)
			return out, fmt.Errorf("bind status %d", int(rc))
		}
		if rc := C.sqlite3_bind_int64(ins, 3, C.sqlite3_int64(deadline)); rc != sqliteOK {
			C.sqlite3_finalize(ins)
			return out, fmt.Errorf("bind deadline %d", int(rc))
		}
		rc := C.sqlite3_step(ins)
		if rc != sqliteDone {
			C.sqlite3_finalize(ins)
			return out, fmt.Errorf("insert step %d", int(rc))
		}
		if rc := C.sqlite3_reset(ins); rc != sqliteOK {
			C.sqlite3_finalize(ins)
			return out, fmt.Errorf("insert reset %d", int(rc))
		}
	}
	C.sqlite3_finalize(ins)
	if err := sqlExec(db, "COMMIT"); err != nil {
		return out, err
	}

	sel, err := sqlPrepare(db, "SELECT id FROM orders WHERE status=0 AND deadline<=?")
	if err != nil {
		return out, err
	}
	for i := 0; i < 5; i++ {
		n, d, err := sqlSelectIDs(sel, 1_000)
		if err != nil {
			C.sqlite3_finalize(sel)
			return out, err
		}
		if n != due {
			C.sqlite3_finalize(sel)
			return out, fmt.Errorf("no-index rows %d want %d", n, due)
		}
		out.NoIndex = append(out.NoIndex, d.Microseconds())
	}
	C.sqlite3_finalize(sel)

	if err := sqlExec(db, "CREATE INDEX idx_due ON orders(status, deadline)"); err != nil {
		return out, err
	}
	plan, err := sqlPlan(db)
	if err != nil {
		return out, err
	}
	out.Plan = plan
	sel, err = sqlPrepare(db, "SELECT id FROM orders WHERE status=0 AND deadline<=?")
	if err != nil {
		return out, err
	}
	for i := 0; i < 5; i++ {
		n, d, err := sqlSelectIDs(sel, 1_000)
		if err != nil {
			C.sqlite3_finalize(sel)
			return out, err
		}
		if n != due {
			C.sqlite3_finalize(sel)
			return out, fmt.Errorf("index rows %d want %d", n, due)
		}
		out.Index = append(out.Index, d.Microseconds())
	}
	C.sqlite3_finalize(sel)

	upd, err := sqlPrepare(db, "UPDATE orders SET status=2 WHERE status=0 AND deadline<=?")
	if err != nil {
		return out, err
	}
	if rc := C.sqlite3_bind_int64(upd, 1, 1_000); rc != sqliteOK {
		C.sqlite3_finalize(upd)
		return out, fmt.Errorf("update bind %d", int(rc))
	}
	start := time.Now()
	rc := C.sqlite3_step(upd)
	out.UpdateUs = time.Since(start).Microseconds()
	C.sqlite3_finalize(upd)
	if rc != sqliteDone {
		return out, fmt.Errorf("update step %d", int(rc))
	}
	out.Changed = int(C.sqlite3_changes(db))
	if out.Changed != due {
		return out, fmt.Errorf("changed %d want %d", out.Changed, due)
	}
	return out, nil
}
