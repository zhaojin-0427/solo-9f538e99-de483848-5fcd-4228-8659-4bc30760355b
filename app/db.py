"""SQLite 连接与表结构。

- batches: 每个批号的当前版本、行数、过滤器指针（filter_version/building_version）
- rows:    明细行，按 (batch_id, line_no) 存放；batch_version 记录该行最后一次
           被写入时的批次版本——未参与更正的行保留旧版本号，更正只写增/改/删的行。
           查询时按批次当前版本过滤，行内版号用于复用判断与写入核对。
- filters: 布隆过滤器，按 (batch_id, version) 存放；只有与批次当前版本同版才被使用

连接统一开 WAL：读写互不阻塞，读事务拿到一致快照。

旧库（v1：rows 主键为 (batch_id, batch_version, line_no)，整份替换式更正）
可由 init_db 在一个事务内直接升级到 v2，批次版本号、明细与可用过滤器全部保留。
"""
from __future__ import annotations

import sqlite3

SCHEMA_VERSION = 2

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    id               TEXT PRIMARY KEY,
    version          INTEGER NOT NULL,
    row_count        INTEGER NOT NULL,
    uploaded_at      TEXT NOT NULL,
    filter_version   INTEGER,                   -- 当前可用过滤器对应的批次版本
    filter_status    TEXT NOT NULL DEFAULT 'none',  -- none/building/ready/failed
    building_version INTEGER,                   -- 正在构建过滤器的版本
    filter_error     TEXT
);

CREATE TABLE IF NOT EXISTS rows (
    batch_id      TEXT NOT NULL,
    line_no       INTEGER NOT NULL,
    batch_version INTEGER NOT NULL,            -- 该行最后一次写入时的批次版本
    container     TEXT NOT NULL,
    gate_time_utc TEXT NOT NULL,
    PRIMARY KEY (batch_id, line_no)
);

CREATE INDEX IF NOT EXISTS idx_rows_container ON rows (container);

CREATE TABLE IF NOT EXISTS filters (
    batch_id  TEXT NOT NULL,
    version   INTEGER NOT NULL,
    m_bits    INTEGER NOT NULL,
    k_hashes  INTEGER NOT NULL,
    items     INTEGER NOT NULL,
    blob      BLOB NOT NULL,
    built_at  TEXT NOT NULL,
    status    TEXT NOT NULL,                    -- ready / failed
    error     TEXT,
    PRIMARY KEY (batch_id, version)
);
"""


def connect(db_path: str) -> sqlite3.Connection:
    # isolation_level=None：显式管理事务，保证“一次查询一个快照”可控
    conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def _table_names(conn: sqlite3.Connection) -> set[str]:
    return {
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
    }


def _is_legacy_rows(conn: sqlite3.Connection) -> bool:
    """v1 库的 rows 主键含 batch_version；v2 主键为 (batch_id, line_no)。"""
    pk_cols = {
        r[1] for r in conn.execute("PRAGMA table_info(rows)") if r[5] > 0  # pk 标志位
    }
    return "batch_version" in pk_cols


def _migrate_legacy_rows(conn: sqlite3.Connection) -> None:
    """把 v1 的 rows 整表重建为 v2 结构；数据逐行保留，版号不变。

    单事务内完成：建临时表 → 搬数据 → 删旧表 → 改名 → 建索引 → 升 user_version，
    任一步失败整体回滚，旧库保持可用。注意不用 executescript——它会先隐式提交，
    会破坏这里的原子性。
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute(
            """
            CREATE TABLE rows_new (
                batch_id      TEXT NOT NULL,
                line_no       INTEGER NOT NULL,
                batch_version INTEGER NOT NULL,
                container     TEXT NOT NULL,
                gate_time_utc TEXT NOT NULL,
                PRIMARY KEY (batch_id, line_no)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO rows_new (batch_id, line_no, batch_version, container, gate_time_utc)
            SELECT batch_id, line_no, batch_version, container, gate_time_utc FROM rows
            """
        )
        conn.execute("DROP TABLE rows")  # 连带删除旧索引 idx_rows_container
        conn.execute("ALTER TABLE rows_new RENAME TO rows")
        conn.execute("CREATE INDEX idx_rows_container ON rows (container)")
        conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def init_db(db_path: str) -> None:
    conn = connect(db_path)
    try:
        user_ver = conn.execute("PRAGMA user_version").fetchone()[0]
        if user_ver < SCHEMA_VERSION and "rows" in _table_names(conn) and _is_legacy_rows(conn):
            # 旧库在线升级：batches/filters 结构未变，仅 rows 需要换主键
            _migrate_legacy_rows(conn)
        # 新库或已升级库：幂等补齐全部表/索引（batches、filters 对旧库原样保留）
        conn.executescript(SCHEMA)
        if conn.execute("PRAGMA user_version").fetchone()[0] < SCHEMA_VERSION:
            conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
    finally:
        conn.close()
