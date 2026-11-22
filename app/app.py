"""港区集装箱过闸记录查询网页（Flask + SQLite）。

并发与一致性设计：
- 一次查询 = 一个读事务：批次指针、布隆过滤器、明细行来自同一已提交快照。
- 更正 = 增量写入：文件仍整体校验、批次照常升版；归一化后同一行号上
  箱号与 UTC 时刻都相同的明细行直接复用，SQLite 只写新增/改动/删去的行；
  未改动的行保留其最初写入时的版号，但查询一律按批次当前版号呈现。
- 过滤器后台构建：构建期间同批照常更正/查询；提交时仅当批次仍停留在
  构建时的版本才启用（条件更新），迟到的旧索引直接丢弃，不覆盖新版。
- 更正成功即作废旧过滤器；写入失败整体回滚，原版明细与原索引照常可查。
- 构建失败只标记状态，查询自动回退为逐行扫描原件。
"""
from __future__ import annotations

import os
import re
import threading
import time
from datetime import datetime, timezone

from flask import Flask, flash, g, jsonify, redirect, render_template, request, url_for

from bloom import BloomFilter
from csvio import MAX_ROWS, CsvError, normalize_container, parse_csv
from db import connect, init_db

DATABASE = os.environ.get(
    "DATABASE_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "gate.db"),
)
MAX_FILTER_BYTES = 2048      # 布隆过滤器上限 2 KiB
MAX_UPLOAD_BYTES = 1_000_000 # 上传文件体积上限（5000 行 CSV 远低于此）
MAX_BUILD_DELAY = 10.0       # 测试钩子：构建前人为延迟上限（秒）

BATCH_RE = re.compile(r"[A-Za-z0-9_-]{1,64}")

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-only-secret-change-me")


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def get_db():
    if "db" not in g:
        g.db = connect(DATABASE)
    return g.db


@app.teardown_appcontext
def close_db(_exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()


# ---------------------------------------------------------------- 页面

@app.get("/")
def index():
    rows = get_db().execute(
        """
        SELECT b.*, f.m_bits, f.k_hashes, f.items, length(f.blob) AS nbytes
        FROM batches b
        LEFT JOIN filters f
               ON f.batch_id = b.id AND f.version = b.filter_version AND f.status = 'ready'
        ORDER BY b.id
        """
    ).fetchall()
    return render_template(
        "index.html", batches=rows, max_rows=MAX_ROWS, max_filter=MAX_FILTER_BYTES
    )


@app.post("/batches")
def upload_batch():
    """按批号上传 CSV；同批号即更正：整份校验、批次升版、明细增量写入。

    归一化后同一行号上箱号与 UTC 时刻都相同的行原样复用（不产生任何写入）；
    只有新增、改动、删去的明细行走 SQLite 写入。整个过程在一个写事务内，
    任一步失败即回滚，旧版明细与旧过滤器保持可用。
    """
    batch_id = (request.form.get("batch") or "").strip()
    if not BATCH_RE.fullmatch(batch_id):
        flash(f"批号非法（允许字母/数字/_/-，1~64 字符）: {batch_id!r}", "error")
        return redirect(url_for("index"))
    f = request.files.get("file")
    if f is None:
        flash("缺少 CSV 文件", "error")
        return redirect(url_for("index"))
    data = f.read(MAX_UPLOAD_BYTES + 1)
    if len(data) > MAX_UPLOAD_BYTES:
        flash(f"批次 {batch_id} 上传被拒绝：文件超过 {MAX_UPLOAD_BYTES} 字节", "error")
        return redirect(url_for("index"))
    try:
        rows = parse_csv(data)  # 先整体校验（规则不变），通过后才动库
    except CsvError as e:
        flash(f"批次 {batch_id} 上传被拒绝：{e}", "error")
        return redirect(url_for("index"))

    db = get_db()
    try:
        version, reused, inserted, updated, deleted = apply_csv_correction(db, batch_id, rows)
    except Exception:
        if db.in_transaction:
            db.execute("ROLLBACK")
        raise
    if version == 1:
        flash(
            f"批次 {batch_id} 已上传：v1，{len(rows)} 行"
            f"（新增 {inserted} 行，复用 {reused} 行）",
            "ok",
        )
    else:
        flash(
            f"批次 {batch_id} 更正完成：升版至 v{version}，{len(rows)} 行；"
            f"明细复用 {reused} 行，仅写入新增 {inserted} / 改动 {updated} / 删除 {deleted} 行，"
            f"旧过滤器已失效",
            "ok",
        )
    return redirect(url_for("index"))


def apply_csv_correction(db, batch_id: str, rows: list[tuple[str, str]]) -> tuple[int, int, int, int, int]:
    """在单个写事务内完成增量更正，返回 (新版号, 复用, 新增, 改动, 删除行数)。"""
    db.execute("BEGIN IMMEDIATE")
    # 1) 批次照常升版（新批号插 v1；老批号 version+1 并作废旧过滤器指针）
    db.execute(
        """
        INSERT INTO batches (id, version, row_count, uploaded_at,
                             filter_version, filter_status, building_version, filter_error)
        VALUES (?, 1, ?, ?, NULL, 'none', NULL, NULL)
        ON CONFLICT(id) DO UPDATE SET
            version          = batches.version + 1,
            row_count        = excluded.row_count,
            uploaded_at      = excluded.uploaded_at,
            filter_version   = NULL,
            filter_status    = 'none',
            building_version = NULL,
            filter_error     = NULL
        """,
        (batch_id, len(rows), utcnow()),
    )
    version = db.execute(
        "SELECT version FROM batches WHERE id = ?", (batch_id,)
    ).fetchone()["version"]

    # 2) 取现有明细（含每行最后写入时的版号），按行号做三向 diff
    old = {
        r["line_no"]: (r["container"], r["gate_time_utc"])
        for r in db.execute(
            "SELECT line_no, container, gate_time_utc FROM rows WHERE batch_id = ?",
            (batch_id,),
        )
    }
    new = dict(enumerate(rows, start=1))
    reused = inserted = updated = deleted = 0
    to_insert: list[tuple] = []
    to_update: list[tuple] = []
    for line_no, (container, gate_time) in new.items():
        if line_no not in old:
            to_insert.append((batch_id, line_no, version, container, gate_time))
            inserted += 1
        elif old[line_no] != (container, gate_time):
            to_update.append((container, gate_time, version, batch_id, line_no))
            updated += 1
        else:
            reused += 1  # 行号+箱号+UTC 时刻一致 → 整行复用，不产生写入
    to_delete = sorted(set(old) - set(new))
    deleted = len(to_delete)

    # 3) SQLite 只对新增、改动、删去的明细行执行写入
    if to_delete:
        db.executemany(
            "DELETE FROM rows WHERE batch_id = ? AND line_no = ?",
            [(batch_id, ln) for ln in to_delete],
        )
    if to_update:
        db.executemany(
            "UPDATE rows SET container = ?, gate_time_utc = ?, batch_version = ?"
            " WHERE batch_id = ? AND line_no = ?",
            to_update,
        )
    if to_insert:
        db.executemany(
            "INSERT INTO rows (batch_id, line_no, batch_version, container, gate_time_utc)"
            " VALUES (?, ?, ?, ?, ?)",
            to_insert,
        )

    # 4) 旧版本过滤器一律清除（若有正在进行的旧构建，其提交会被条件更新拒绝）
    db.execute(
        "DELETE FROM filters WHERE batch_id = ? AND version <> ?", (batch_id, version)
    )
    db.execute("COMMIT")
    return version, reused, inserted, updated, deleted


@app.post("/batches/<batch_id>/filter")
def build_filter(batch_id: str):
    """为当前版本明细后台构建布隆过滤器（≤2 KiB）。

    表单字段 delay 为测试钩子：构建前人为延迟秒数，用于演练
    “建索引与更正交错”的场景，上限 MAX_BUILD_DELAY。
    """
    delay = 0.0
    raw_delay = (request.form.get("delay") or "").strip()
    if raw_delay:
        try:
            delay = min(MAX_BUILD_DELAY, max(0.0, float(raw_delay)))
        except ValueError:
            delay = 0.0

    db = get_db()
    db.execute("BEGIN IMMEDIATE")
    try:
        row = db.execute(
            "SELECT version, building_version FROM batches WHERE id = ?", (batch_id,)
        ).fetchone()
        if row is None:
            db.execute("ROLLBACK")
            flash(f"批次 {batch_id} 不存在", "error")
            return redirect(url_for("index"))
        version = row["version"]
        if row["building_version"] is not None:
            db.execute("ROLLBACK")
            flash(f"批次 {batch_id} 已有构建任务在进行（v{row['building_version']}）", "error")
            return redirect(url_for("index"))
        db.execute(
            "UPDATE batches SET building_version = ?, filter_status = 'building',"
            " filter_error = NULL WHERE id = ?",
            (version, batch_id),
        )
        db.execute("COMMIT")
    except Exception:
        db.execute("ROLLBACK")
        raise

    t = threading.Thread(
        target=_build_worker,
        args=(DATABASE, batch_id, version, delay),
        daemon=True,
        name=f"build-{batch_id}-v{version}",
    )
    t.start()
    flash(f"批次 {batch_id} v{version} 布隆过滤器开始后台构建", "ok")
    return redirect(url_for("index"))


@app.get("/query")
def query():
    raw = request.args.get("container", "")
    wants_json = request.args.get("format") == "json"
    try:
        container = normalize_container(raw)
    except CsvError as e:
        if wants_json:
            return jsonify({"error": str(e)}), 400
        flash(str(e), "error")
        return redirect(url_for("index"))
    result = run_query(get_db(), container)
    if wants_json:
        return jsonify(result)
    return render_template("query.html", r=result)


@app.get("/api/batches")
def api_batches():
    rows = get_db().execute(
        "SELECT id, version, row_count, uploaded_at, filter_status, filter_version,"
        " building_version, filter_error FROM batches ORDER BY id"
    ).fetchall()
    return jsonify([dict(r) for r in rows])


# ---------------------------------------------------------------- 查询核心

def run_query(db, container: str) -> dict:
    """按箱号查询：列出命中行与实际扫描过的批次。

    整个查询在一个读事务内完成 —— 批次版本、过滤器、明细行均来自
    同一已提交快照；期间发生的更正/建索引对本查询不可见。
    """
    db.execute("BEGIN")
    try:
        batches = db.execute(
            "SELECT id, version, row_count, filter_version FROM batches ORDER BY id"
        ).fetchall()
        scanned, skipped, matches = [], [], []
        for b in batches:
            bid, ver = b["id"], b["version"]
            entry = {"batch": bid, "version": ver}
            bf = None
            if b["filter_version"] == ver:
                frow = db.execute(
                    "SELECT m_bits, k_hashes, blob FROM filters"
                    " WHERE batch_id = ? AND version = ? AND status = 'ready'",
                    (bid, ver),
                ).fetchone()
                if frow is not None:
                    bf = BloomFilter(frow["m_bits"], frow["k_hashes"], bytes(frow["blob"]))
            if bf is not None and container not in bf:
                # 同版过滤器判定不存在 → 跳过该批明细
                skipped.append(entry)
                continue
            # 无同版过滤器（未建/构建中/已失败）或过滤器不能排除 → 逐行核对明细。
            # rows 只保留当前版批次的明细（未改行的行内版号可能较旧），因此这里
            # 按 batch_id 取行；呈现给用户的版号统一用快照内的批次当前版号。
            hits = db.execute(
                "SELECT line_no, gate_time_utc FROM rows"
                " WHERE batch_id = ? AND container = ?"
                " ORDER BY line_no",
                (bid, container),
            ).fetchall()
            entry["rows_scanned"] = b["row_count"]
            scanned.append(entry)
            for h in hits:
                matches.append(
                    {
                        "batch": bid,
                        "version": ver,
                        "line_no": h["line_no"],
                        "container": container,
                        "gate_time_utc": h["gate_time_utc"],
                    }
                )
        db.execute("COMMIT")
    except Exception:
        db.execute("ROLLBACK")
        raise
    return {
        "container": container,
        "matches": matches,
        "scanned": scanned,
        "skipped": skipped,
    }


# ---------------------------------------------------------------- 后台构建

def _build_worker(db_path: str, batch_id: str, version: int, delay: float) -> None:
    try:
        if delay > 0:
            time.sleep(delay)  # 测试钩子：模拟慢构建
        conn = connect(db_path)
        try:
            # 1) 同一已提交快照内读出该批次当前全部箱号（rows 只保留现版明细）
            conn.execute("BEGIN")
            try:
                containers = [
                    r["container"]
                    for r in conn.execute(
                        "SELECT container FROM rows"
                        " WHERE batch_id = ? ORDER BY line_no",
                        (batch_id,),
                    )
                ]
            finally:
                conn.execute("COMMIT")
            if not containers:
                raise RuntimeError("该批次没有可索引的行（可能已被更正替换）")
            # 2) 纯内存构建，不占用数据库
            bf = BloomFilter.build(containers)
            if bf.nbytes > MAX_FILTER_BYTES:
                raise RuntimeError(f"过滤器 {bf.nbytes} 字节超过 2 KiB 上限")
            # 3) 条件提交：批次仍停留在该版本才启用；迟到的旧索引直接丢弃
            conn.execute("BEGIN IMMEDIATE")
            try:
                cur = conn.execute(
                    "UPDATE batches SET filter_version = ?, filter_status = 'ready',"
                    " building_version = NULL, filter_error = NULL"
                    " WHERE id = ? AND version = ? AND building_version = ?",
                    (version, batch_id, version, version),
                )
                if cur.rowcount == 1:
                    conn.execute(
                        "INSERT OR REPLACE INTO filters"
                        " (batch_id, version, m_bits, k_hashes, items, blob, built_at, status, error)"
                        " VALUES (?, ?, ?, ?, ?, ?, ?, 'ready', NULL)",
                        (batch_id, version, bf.m_bits, bf.k, len(containers), bf.bits, utcnow()),
                    )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()
    except Exception as exc:  # 构建失败不影响原件查询，仅标记状态
        _mark_build_failed(db_path, batch_id, version, exc)


def _mark_build_failed(db_path: str, batch_id: str, version: int, exc: Exception) -> None:
    try:
        conn = connect(db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                cur = conn.execute(
                    "UPDATE batches SET filter_status = 'failed', building_version = NULL,"
                    " filter_error = ? WHERE id = ? AND version = ?",
                    (f"{type(exc).__name__}: {exc}", batch_id, version),
                )
                if cur.rowcount == 1:
                    conn.execute(
                        "INSERT OR REPLACE INTO filters"
                        " (batch_id, version, m_bits, k_hashes, items, blob, built_at, status, error)"
                        " VALUES (?, ?, 0, 0, 0, X'', ?, 'failed', ?)",
                        (batch_id, version, utcnow(), str(exc)),
                    )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()
    except Exception:
        app.logger.exception("标记构建失败时出错（批次 %s v%s）", batch_id, version)


# ---------------------------------------------------------------- 启动

def main() -> None:
    init_db(DATABASE)
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8000")), threaded=True)


init_db(DATABASE)  # 覆盖 `python app.py` 与 WSGI 容器两种启动方式

if __name__ == "__main__":
    main()
