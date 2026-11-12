package main

/*
#cgo LDFLAGS: -lsqlite3
#include <sqlite3.h>
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"os"
	"unsafe"
)

// DB 是一条 SQLite 连接。每个 goroutine 自己持有，不跨线程用。
// 表上的主键是 (user_id, goods_id)，对应 miaosha_order 上已有的 u_uid_gid。
type DB struct {
	db   *C.sqlite3
	stmt *C.sqlite3_stmt
}

func removeDB(path string) {
	_ = os.Remove(path)
	_ = os.Remove(path + "-wal")
	_ = os.Remove(path + "-shm")
}

func openDB(path, syncMode string, create bool) (*DB, error) {
	switch syncMode {
	case "OFF", "NORMAL", "FULL":
	default:
		return nil, fmt.Errorf("bad synchronous %q", syncMode)
	}
	if create {
		removeDB(path)
	}
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	var db *C.sqlite3
	if rc := C.sqlite3_open(cpath, &db); rc != C.SQLITE_OK {
		return nil, fmt.Errorf("sqlite open %d", int(rc))
	}
	d := &DB{db: db}
	C.sqlite3_busy_timeout(db, 30000)
	if err := d.exec("PRAGMA journal_mode=WAL"); err != nil {
		d.Close()
		return nil, err
	}
	if err := d.exec("PRAGMA synchronous=" + syncMode); err != nil {
		d.Close()
		return nil, err
	}
	if create {
		if err := d.exec(`CREATE TABLE orders (
			user_id INTEGER NOT NULL,
			goods_id INTEGER NOT NULL,
			PRIMARY KEY (user_id, goods_id)
		)`); err != nil {
			d.Close()
			return nil, err
		}
	}
	sql := C.CString("INSERT INTO orders(user_id, goods_id) VALUES(?, ?)")
	defer C.free(unsafe.Pointer(sql))
	var stmt *C.sqlite3_stmt
	if rc := C.sqlite3_prepare_v2(db, sql, -1, &stmt, nil); rc != C.SQLITE_OK {
		err := fmt.Errorf("prepare: %s", C.GoString(C.sqlite3_errmsg(db)))
		d.Close()
		return nil, err
	}
	d.stmt = stmt
	return d, nil
}

func (d *DB) Close() {
	if d == nil || d.db == nil {
		return
	}
	if d.stmt != nil {
		C.sqlite3_finalize(d.stmt)
		d.stmt = nil
	}
	C.sqlite3_close(d.db)
	d.db = nil
}

func (d *DB) exec(sql string) error {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	var errMsg *C.char
	rc := C.sqlite3_exec(d.db, cs, nil, nil, &errMsg)
	if rc != C.SQLITE_OK {
		msg := C.GoString(errMsg)
		if errMsg != nil {
			C.sqlite3_free(unsafe.Pointer(errMsg))
		}
		if msg == "" {
			msg = C.GoString(C.sqlite3_errmsg(d.db))
		}
		return fmt.Errorf("%s", msg)
	}
	return nil
}

func (d *DB) begin() error {
	return d.exec("BEGIN IMMEDIATE")
}

func (d *DB) commit() error {
	return d.exec("COMMIT")
}

// insert 返回是否新写入一行。主键冲突算重复，不是错误。
func (d *DB) insert(user, goods int64) (bool, error) {
	C.sqlite3_reset(d.stmt)
	C.sqlite3_clear_bindings(d.stmt)
	if rc := C.sqlite3_bind_int64(d.stmt, 1, C.sqlite3_int64(user)); rc != C.SQLITE_OK {
		return false, fmt.Errorf("bind user %d", int(rc))
	}
	if rc := C.sqlite3_bind_int64(d.stmt, 2, C.sqlite3_int64(goods)); rc != C.SQLITE_OK {
		return false, fmt.Errorf("bind goods %d", int(rc))
	}
	rc := C.sqlite3_step(d.stmt)
	switch rc {
	case C.SQLITE_DONE:
		return C.sqlite3_changes(d.db) == 1, nil
	case C.SQLITE_CONSTRAINT:
		return false, nil
	default:
		return false, fmt.Errorf("step %d: %s", int(rc), C.GoString(C.sqlite3_errmsg(d.db)))
	}
}

func (d *DB) count() (int64, error) {
	sql := C.CString("SELECT COUNT(*) FROM orders")
	defer C.free(unsafe.Pointer(sql))
	var stmt *C.sqlite3_stmt
	if rc := C.sqlite3_prepare_v2(d.db, sql, -1, &stmt, nil); rc != C.SQLITE_OK {
		return 0, fmt.Errorf("count prepare: %s", C.GoString(C.sqlite3_errmsg(d.db)))
	}
	defer C.sqlite3_finalize(stmt)
	rc := C.sqlite3_step(stmt)
	if rc != C.SQLITE_ROW {
		return 0, fmt.Errorf("count step %d", int(rc))
	}
	return int64(C.sqlite3_column_int64(stmt, 0)), nil
}
