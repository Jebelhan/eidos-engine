"""数据库连接、事务与迁移执行。

- 单连接、单写者（WAL）；
- 所有业务写入走 transaction()，BEGIN IMMEDIATE 串行化；
- 迁移按版本号顺序执行，每个迁移一个事务，失败整段回滚。
"""

import os
import sqlite3
import time
from contextlib import contextmanager

from .migrations import MIGRATIONS


class Database:
    def __init__(self, path):
        self.path = path
        parent = os.path.dirname(os.path.abspath(path))
        os.makedirs(parent, exist_ok=True)
        self.conn = sqlite3.connect(path, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=5000")

    @contextmanager
    def transaction(self):
        """BEGIN IMMEDIATE ... COMMIT / ROLLBACK。嵌套调用会死锁，调用方自己保证不嵌套。"""
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield self.conn
            self.conn.execute("COMMIT")
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise

    def migrate(self):
        """执行未应用的迁移。每段迁移独立事务，失败即回滚且不记录版本。"""
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations ("
            " version INTEGER PRIMARY KEY,"
            " name TEXT NOT NULL,"
            " applied_at INTEGER NOT NULL)"
        )
        applied = {row["version"] for row in self.conn.execute("SELECT version FROM schema_migrations")}
        executed = []
        for version, name, apply in MIGRATIONS:
            if version in applied:
                continue
            with self.transaction() as conn:
                apply(conn)
                conn.execute(
                    "INSERT INTO schema_migrations(version, name, applied_at) VALUES(?, ?, ?)",
                    (version, name, int(time.time() * 1000)),
                )
            executed.append({"version": version, "name": name})
        return executed

    def schema_version(self):
        try:
            row = self.conn.execute("SELECT COALESCE(MAX(version), 0) AS v FROM schema_migrations").fetchone()
            return int(row["v"])
        except sqlite3.OperationalError:
            return 0

    def tables(self):
        rows = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
        return [row["name"] for row in rows]

    def migration_records(self):
        try:
            rows = self.conn.execute("SELECT version, name, applied_at FROM schema_migrations ORDER BY version").fetchall()
            return [dict(row) for row in rows]
        except sqlite3.OperationalError:
            return []

    def integrity_check(self):
        row = self.conn.execute("PRAGMA integrity_check").fetchone()
        return row[0] if row else "unknown"

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass
