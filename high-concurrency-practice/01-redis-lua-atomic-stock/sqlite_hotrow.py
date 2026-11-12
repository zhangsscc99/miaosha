#!/usr/bin/env python3
"""单行库存扣减。用 SQLite WAL 模拟「所有请求打到同一行」的数据库热行。

没有 binlog，也没有从库，所以同样的机器上它通常比线上 MySQL 更快。
用它做对比时，数据库这边是被高估的。
"""

import argparse
import json
import os
import sqlite3
import threading
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--stock", type=int, default=500)
    parser.add_argument("--attempts", type=int, default=8000)
    parser.add_argument("--sync", default="NORMAL", choices=["OFF", "NORMAL", "FULL"])
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    path = f"/tmp/miaosha-hotrow-{args.sync}.db"
    for suffix in ("", "-wal", "-shm"):
        try:
            os.remove(path + suffix)
        except FileNotFoundError:
            pass

    init = sqlite3.connect(path)
    init.execute("PRAGMA journal_mode=WAL")
    init.execute(f"PRAGMA synchronous={args.sync}")
    init.execute("CREATE TABLE goods(id INTEGER PRIMARY KEY, stock INTEGER NOT NULL)")
    init.execute("INSERT INTO goods(id, stock) VALUES(1, ?)", (args.stock,))
    init.commit()
    init.close()

    ok = 0
    sold = 0
    errors = 0
    lock = threading.Lock()

    def worker(n):
        nonlocal ok, sold, errors
        con = sqlite3.connect(path, timeout=60, isolation_level=None)
        con.execute("PRAGMA journal_mode=WAL")
        con.execute(f"PRAGMA synchronous={args.sync}")
        local_ok = 0
        local_sold = 0
        local_err = 0
        for _ in range(n):
            try:
                con.execute("BEGIN IMMEDIATE")
                cur = con.execute(
                    "UPDATE goods SET stock = stock - 1 WHERE id = 1 AND stock > 0"
                )
                con.execute("COMMIT")
                if cur.rowcount == 1:
                    local_ok += 1
                else:
                    local_sold += 1
            except sqlite3.Error:
                local_err += 1
                try:
                    con.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
        con.close()
        with lock:
            ok += local_ok
            sold += local_sold
            errors += local_err

    base, rem = divmod(args.attempts, args.threads)
    counts = [base + (1 if i < rem else 0) for i in range(args.threads)]
    threads = [
        threading.Thread(target=worker, args=(counts[i],)) for i in range(args.threads)
    ]
    started = time.perf_counter()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    elapsed = time.perf_counter() - started

    con = sqlite3.connect(path)
    final = con.execute("SELECT stock FROM goods WHERE id = 1").fetchone()[0]
    con.close()
    result = {
        "scenario": "sqlite-hotrow",
        "sync": args.sync,
        "threads": args.threads,
        "attempts": args.attempts,
        "initialStock": args.stock,
        "finalStock": final,
        "success": ok,
        "soldOut": sold,
        "errors": errors,
        "elapsedSec": round(elapsed, 4),
        "qps": round(args.attempts / elapsed, 1) if elapsed else 0,
        "successQps": round(ok / elapsed, 1) if elapsed else 0,
        "pass": errors == 0
        and ok == args.stock
        and final == 0
        and ok + sold == args.attempts,
        "note": "SQLite WAL 单行条件更新。无 binlog、无从库，乐观于线上 MySQL 热行。",
    }
    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(result, fh, ensure_ascii=False, indent=2)
        fh.write("\n")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if not result["pass"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
