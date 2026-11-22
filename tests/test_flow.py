#!/usr/bin/env python3
"""端到端验证脚本：直接以 Flask test_client 驱动应用（后台构建线程真实并发）。

运行：.venv/bin/python tests/test_flow.py
"""
from __future__ import annotations

import io
import os
import sqlite3
import sys
import tempfile
import threading
import time

APP_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "app"))
sys.path.insert(0, APP_DIR)

TMP = tempfile.mkdtemp(prefix="gate-test-")
os.environ["DATABASE_PATH"] = os.path.join(TMP, "test.db")

import app as gate  # noqa: E402
from bloom import BloomFilter  # noqa: E402

client = gate.app.test_client()
CHECKS = []


def check(name, cond, detail=""):
    CHECKS.append((name, bool(cond)))
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f" -- {detail}" if detail and not cond else ""))


def csv_bytes(pairs, header=None):
    lines = ([header] if header else []) + [f"{c},{t}" for c, t in pairs]
    return ("\n".join(lines) + "\n").encode()


def upload(batch, pairs=None, header=None, raw=None):
    data = raw if raw is not None else csv_bytes(pairs, header)
    return client.post(
        "/batches",
        data={"batch": batch, "file": (io.BytesIO(data), "f.csv")},
        content_type="multipart/form-data",
        follow_redirects=True,
    )


def build(batch, delay=0.0):
    return client.post(
        f"/batches/{batch}/filter", data={"delay": str(delay)}, follow_redirects=True
    )


def query(container):
    return client.get(f"/query?container={container}&format=json").get_json()


def batch_info(batch):
    for b in client.get("/api/batches").get_json():
        if b["id"] == batch:
            return b
    return None


def wait_for(pred, timeout=10.0, what=""):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(0.05)
    raise AssertionError(f"超时等待: {what}")


def wait_build_done(batch):
    wait_for(
        lambda: batch_info(batch)["filter_status"] in ("ready", "failed"),
        what=f"{batch} 构建结束",
    )


def db_one(sql, args=()):
    conn = sqlite3.connect(os.environ["DATABASE_PATH"])
    try:
        return conn.execute(sql, args).fetchone()
    finally:
        conn.close()


T = "2026-11-22T08:30:00Z"

# ---------------------------------------------------------------- 1. 基本上传与查询
r = upload("B1", [(f"C{i:04d}", T) for i in range(1, 51)], header="箱号,过闸时刻(UTC)")
check("上传 B1（50 行，带中文表头）", "已上传" in r.get_data(as_text=True) and "v1" in r.get_data(as_text=True))
r = upload("B2", [(f"C{i:04d}", T) for i in range(40, 91)], header="container,gate_time_utc")
check("上传 B2（51 行，带英文表头）", "已上传" in r.get_data(as_text=True))

q = query("C0045")
check("无过滤器时查询：两批都扫描", {s["batch"] for s in q["scanned"]} == {"B1", "B2"} and not q["skipped"])
check("命中 B1、B2 各一行", {(m["batch"], m["line_no"]) for m in q["matches"]} == {("B1", 45), ("B2", 6)})
q = query("C9999")
check("查询不存在箱号：无命中、全部扫描", q["matches"] == [] and len(q["scanned"]) == 2)

# ---------------------------------------------------------------- 2. 建过滤器后查询
build("B1")
build("B2")
wait_build_done("B1")
wait_build_done("B2")
check("两批过滤器就绪", batch_info("B1")["filter_status"] == "ready" and batch_info("B2")["filter_status"] == "ready")

q = query("C9999")
check("过滤器判定不存在 → 两批都跳过、零命中", q["matches"] == [] and {s["batch"] for s in q["skipped"]} == {"B1", "B2"} and not q["scanned"])

# C0001 只在 B1；用同一算法预判 B2 过滤器是否放行（避免误报导致断言偶然失败）
bf_b2 = BloomFilter.build([f"C{i:04d}" for i in range(40, 91)])
expect_b2_scanned = "C0001" in bf_b2
q = query("C0001")
scanned = {s["batch"] for s in q["scanned"]}
check("命中行来自 B1 且只有一行", [(m["batch"], m["line_no"]) for m in q["matches"]] == [("B1", 1)])
check("B1 被扫描", "B1" in scanned)
check("B2 按过滤器结果跳过或扫描", ("B2" in scanned) == expect_b2_scanned)

# ---------------------------------------------------------------- 3. 整份替换更正
r = upload("B1", [(f"D{i:04d}", T) for i in range(1, 11)])
info = batch_info("B1")
check("更正后版本 v2、10 行", info["version"] == 2 and info["row_count"] == 10)
check("更正后旧过滤器失效", info["filter_status"] == "none" and info["filter_version"] is None)
q = query("C0001")
check("旧箱号查不到", q["matches"] == [])
q = query("D0005")
check("新箱号命中 B1 v2 第 5 行", [(m["batch"], m["version"], m["line_no"]) for m in q["matches"]] == [("B1", 2, 5)])
check("B1 无同版过滤器 → 扫描原件", any(s["batch"] == "B1" for s in q["scanned"]))

# ---------------------------------------------------------------- 4. 交错：建索引期间更正，迟到旧索引不得覆盖新版
r = upload("B4", [(f"E{i:04d}", T) for i in range(1, 31)])
build("B4", delay=1.2)  # 慢构建（测试钩子）
check("构建状态为 building", batch_info("B4")["filter_status"] == "building")
time.sleep(0.2)
r = upload("B4", [(f"F{i:04d}", T) for i in range(1, 21)])  # 构建期间更正 → v2
info = batch_info("B4")
check("建索引期间照常更正（v2 生效）", info["version"] == 2 and info["row_count"] == 20)
q = query("F0010")
check("建索引期间照常查询（命中新行）", [(m["batch"], m["version"]) for m in q["matches"]] == [("B4", 2)])
time.sleep(2.0)  # 等迟到的 v1 构建落地
info = batch_info("B4")
check("迟到旧索引未覆盖新版（filter_version 仍为空）",
      info["filter_version"] is None and info["filter_status"] == "none", str(info))
check("迟到的旧过滤器未入库", db_one("SELECT COUNT(*) FROM filters WHERE batch_id='B4'")[0] == 0)
q = query("E0001")
check("旧版本数据查不到", q["matches"] == [])
q = query("F0010")
check("更正后数据仍可查", len(q["matches"]) == 1)
build("B4")  # 为 v2 重新构建
wait_build_done("B4")
info = batch_info("B4")
check("v2 过滤器就绪且同版", info["filter_status"] == "ready" and info["filter_version"] == 2)
q = query("F9999")
check("v2 过滤器跳过不存在的箱号", any(s["batch"] == "B4" for s in q["skipped"]))

# ---------------------------------------------------------------- 5. 构建失败仍可查原件
orig_build = gate.BloomFilter.build
def boom(cls, items):
    raise RuntimeError("模拟构建失败")
gate.BloomFilter.build = classmethod(boom)
try:
    build("B1")
    wait_for(lambda: batch_info("B1")["filter_status"] == "failed", what="B1 构建失败标记")
finally:
    gate.BloomFilter.build = orig_build
info = batch_info("B1")
check("B1 构建标记为失败", info["filter_status"] == "failed" and info["filter_error"])
q = query("D0003")
check("构建失败仍可查原件（回退逐行扫描）",
      [(m["batch"], m["line_no"]) for m in q["matches"]] == [("B1", 3)]
      and any(s["batch"] == "B1" for s in q["scanned"]))
build("B1")  # 失败后可重试
wait_build_done("B1")
check("失败后重试成功", batch_info("B1")["filter_status"] == "ready")

# ---------------------------------------------------------------- 6. 行数上限与 CSV 校验
big = [(f"G{i:05d}", T) for i in range(1, 5001)]
r = upload("B6", big)
check("5000 行整批接受", "5000 行" in r.get_data(as_text=True))
r = upload("B7", big + [("G5001", T)])
check("5001 行整份拒绝", "超过单批上限" in r.get_data(as_text=True) and batch_info("B7") is None)
r = upload("B7", raw=b"ONLYONE,2026-11-22T08:30:00Z,EXTRA\n")
check("列数错误整份拒绝", "应为 2 列" in r.get_data(as_text=True))
r = upload("B7", raw="CSQU3054383,不是时间\n".encode())
check("时刻非法整份拒绝", "无法解析" in r.get_data(as_text=True))
r = upload("B7", raw=b"\n\n\n")
check("空文件拒绝", "没有数据行" in r.get_data(as_text=True))
r = client.post("/batches", data={"batch": "bad batch!", "file": (io.BytesIO(b"A,2026-11-22T00:00:00Z\n"), "x.csv")},
                content_type="multipart/form-data", follow_redirects=True)
check("批号非法拒绝", "批号非法" in r.get_data(as_text=True))

# ---------------------------------------------------------------- 7. 时区归一与过滤器体积
upload("B8", [("TZ0001", "2026-11-22T18:30:00+08:00"), ("TZ0002", "2026-11-22 10:30:00")])
q = query("TZ0001")
check("带时区时刻归一为 UTC", q["matches"][0]["gate_time_utc"] == "2026-11-22T10:30:00Z", str(q["matches"]))
q = query("TZ0002")
check("无时区时刻按 UTC", q["matches"][0]["gate_time_utc"] == "2026-11-22T10:30:00Z")

build("B6")
wait_build_done("B6")
nbytes = db_one("SELECT length(blob) FROM filters WHERE batch_id='B6'")[0]
check("5000 行批次过滤器 ≤ 2 KiB", 0 < nbytes <= 2048, f"nbytes={nbytes}")

# 布隆过滤器单元性质：无假阴性；误报率有界
bf = BloomFilter.build([f"G{i:05d}" for i in range(1, 5001)])
check("过滤器无假阴性（5000 项全命中）", all(f"G{i:05d}" in bf for i in range(1, 5001)))
fp = sum(1 for i in range(5001, 5201) if f"G{i:05d}" in bf)
check("误报率有界（抽 200 个非成员 < 50%）", fp < 100, f"fp={fp}/200")

# ---------------------------------------------------------------- 8. 并发压力：更正×建索引×查询交错，校验快照一致性
upload("B9", [(f"S{i:04d}", T) for i in range(1, 81)])
stop = threading.Event()
errors = []

def corrector():
    n = 0
    c = gate.app.test_client()
    while not stop.is_set():
        n += 1
        try:
            data = csv_bytes([(f"S{(i + n) % 80:04d}", T) for i in range(80)])
            c.post("/batches", data={"batch": "B9", "file": (io.BytesIO(data), "f.csv")},
                   content_type="multipart/form-data")
        except Exception as e:  # noqa: BLE001
            errors.append(f"corrector: {e}")
        time.sleep(0.02)

def builder():
    c = gate.app.test_client()
    while not stop.is_set():
        try:
            c.post("/batches/B9/filter", data={})
        except Exception as e:  # noqa: BLE001
            errors.append(f"builder: {e}")
        time.sleep(0.05)

threads = [threading.Thread(target=corrector), threading.Thread(target=builder)]
for t in threads:
    t.start()

consistent = True
try:
    for _ in range(60):
        q = query("S0005")
        seen = {}
        for s in q["scanned"] + q["skipped"]:
            if s["batch"] in seen:  # 同一批在一次查询中出现两次 → 快照不一致
                consistent = False
            seen[s["batch"]] = s["version"]
        scanned_v = {s["batch"]: s["version"] for s in q["scanned"]}
        skipped_b = {s["batch"] for s in q["skipped"]}
        for m in q["matches"]:
            # 命中只能来自被扫描的批次，且版本必须与扫描条目一致
            if m["batch"] not in scanned_v or m["version"] != scanned_v[m["batch"]]:
                consistent = False
            if m["batch"] in skipped_b:
                consistent = False
        time.sleep(0.03)
finally:
    stop.set()
    for t in threads:
        t.join()

check("交错压力下无内部异常", not errors, "; ".join(errors[:3]))
check("60 次查询均满足快照一致性（命中⊆已扫描、版本一致、批次不重复）", consistent)
info = batch_info("B9")
check("压力后 B9 状态收敛（非 building）", info["filter_status"] != "building", str(info))

# ---------------------------------------------------------------- 9. 增量更正：复用/单行写入/增删
def row_dump(batch):
    conn = sqlite3.connect(os.environ["DATABASE_PATH"])
    try:
        conn.row_factory = sqlite3.Row
        return {
            r["line_no"]: (r["batch_version"], r["container"], r["gate_time_utc"])
            for r in conn.execute(
                "SELECT line_no, batch_version, container, gate_time_utc"
                " FROM rows WHERE batch_id = ? ORDER BY line_no",
                (batch,),
            )
        }
    finally:
        conn.close()


base = [(f"INC{i:03d}", T) for i in range(1, 101)]  # 100 行
upload("BINC", base)
before = row_dump("BINC")
check("首次上传 100 行全部写为 v1", all(v[0] == 1 for v in before.values()) and len(before) == 100)

# 只改一笔（第 37 行箱号）
one = base.copy()
one[36] = ("INC037X", T)
r = upload("BINC", one)
check("单行更正提示复用 99 / 改动 1", "复用 99" in r.get_data(as_text=True) and "改动 1" in r.get_data(as_text=True))
after = row_dump("BINC")
info = batch_info("BINC")
check("单行更正后批次升版 v2", info["version"] == 2 and info["row_count"] == 100)
check("仅第 37 行被写入（版号 v2、内容更新）",
      after[37] == (2, "INC037X", T), str(after[37]))
check("其余 99 行原样复用、版号仍为 v1",
      all(after[ln][0] == 1 and after[ln][1:] == base[ln - 1] for ln in range(1, 101) if ln != 37))

# 完全相同的文件再传一次：批次升版但明细零写入
r = upload("BINC", one)
check("无变化再传提示复用 100 / 零写入", "复用 100" in r.get_data(as_text=True) and "新增 0" in r.get_data(as_text=True)
      and "改动 0" in r.get_data(as_text=True) and "删除 0" in r.get_data(as_text=True))
check("无变化再传批次仍升版（v3）", batch_info("BINC")["version"] == 3)
check("无变化再传明细版号分布不变", row_dump("BINC") == after)

# 同时覆盖新增与删除：末行（行号 100）删除，行号 99 改为新箱号，行号 99 位置即“改动”
mix = one[:99]
mix[98] = ("INC999", T)
upload("BINC", mix)
mixdump = row_dump("BINC")
info = batch_info("BINC")
check("增删后行数与批次一致", info["version"] == 4 and info["row_count"] == 99 and len(mixdump) == 99)
check("被删行号 100 已不存在", 100 not in mixdump and 99 in mixdump)
check("行号 99 改动后写为 v4", mixdump[99] == (4, "INC999", T), str(mixdump[99]))
check("未改行继续保留旧版号", mixdump[1][0] == 1 and mixdump[37][0] == 2)

# 未改的行查询时显示所查批次的当前版号
q = query("INC001")
check("未改明细行查询显示批次当前版号 v4",
      q["matches"] == [{"batch": "BINC", "version": 4, "line_no": 1,
                        "container": "INC001", "gate_time_utc": T}], str(q["matches"]))
q = query("INC037X")
check("改动行查询同样显示当前版号 v4",
      len(q["matches"]) == 1 and q["matches"][0]["version"] == 4 and q["matches"][0]["line_no"] == 37)

# ---------------------------------------------------------------- 10. 重复箱号按各自行号列出
dups = [("DUPX", T), ("OTHER", T), ("DUPX", "2026-11-22T09:00:00Z"), ("dupx", T)]
upload("BDUP", dups)  # 第 1、3、4 行归一化后同为 DUPX
q = query("dupx")
check("重复箱号按行号 1/3/4 列出三笔",
      [(m["line_no"], m["gate_time_utc"]) for m in q["matches"]]
      == [(1, T), (3, "2026-11-22T09:00:00Z"), (4, T)]
      and all(m["version"] == 1 and m["batch"] == "BDUP" for m in q["matches"]),
      str(q["matches"]))
# 更正只动第 3 行，重复命中仍完整且版号显示当前版
dups2 = dups.copy()
dups2[2] = ("DUPX", "2026-11-22T10:00:00Z")
upload("BDUP", dups2)
q = query("DUPX")
check("更正后重复箱号仍按行号列出且统一显示 v2",
      [(m["line_no"], m["gate_time_utc"]) for m in q["matches"]]
      == [(1, T), (3, "2026-11-22T10:00:00Z"), (4, T)]
      and all(m["version"] == 2 for m in q["matches"]),
      str(q["matches"]))
check("第 3 行是唯一被写入的行", row_dump("BDUP")[3][0] == 2
      and row_dump("BDUP")[1][0] == 1 and row_dump("BDUP")[4][0] == 1)

# ---------------------------------------------------------------- 11. 写入失败：回滚后原版与原索引照常可查
upload("BFB", [(f"FB{i:03d}", T) for i in range(1, 21)])
build("BFB")
wait_build_done("BFB")
check("失败回滚用例：先备好就绪过滤器", batch_info("BFB")["filter_status"] == "ready")
q_before = query("FB010")
check("回滚前查询基线命中一行", [(m["batch"], m["line_no"]) for m in q_before["matches"]] == [("BFB", 10)])


class FailingConn:
    """测试包装连接：当 execute/executemany 的 SQL 含指定片段时抛 OperationalError。"""

    def __init__(self, real, bad_sql):
        self._real = real
        self._bad = bad_sql

    def execute(self, sql, *a, **kw):
        if self._bad and self._bad in sql:
            raise sqlite3.OperationalError(f"模拟写入失败: {sql[:40]}")
        return self._real.execute(sql, *a, **kw)

    def executemany(self, sql, *a, **kw):
        if self._bad and self._bad in sql:
            raise sqlite3.OperationalError(f"模拟批量写入失败: {sql[:40]}")
        return self._real.executemany(sql, *a, **kw)

    def __getattr__(self, name):
        return getattr(self._real, name)


orig_connect = gate.connect
# 每种写入语句单独演练：各用例会触发对应语句，且失败都发生在 COMMIT 之前
failure_cases = [
    ("UPDATE rows", [(f"FB{i:03d}", T) for i in range(1, 21)]),                 # 改第 10 行
    ("DELETE FROM rows", [(f"FB{i:03d}", T) for i in range(1, 20)]),            # 删掉第 20 行
    ("INSERT INTO rows", [(f"FB{i:03d}", T) for i in range(1, 21)]
     + [("FB021", T)]),                                                         # 新增第 21 行
]
for bad_fragment, bad_rows in failure_cases:
    def patched(path, _frag=bad_fragment):
        return FailingConn(orig_connect(path), _frag)
    gate.connect = patched
    try:
        if bad_fragment == "UPDATE rows":
            bad_rows[9] = ("FB010X", T)
        resp = client.post(
            "/batches",
            data={"batch": "BFB", "file": (io.BytesIO(csv_bytes(bad_rows)), "f.csv")},
            content_type="multipart/form-data",
        )
    finally:
        gate.connect = orig_connect
    check(f"明细写入失败返回 500（{bad_fragment}）", resp.status_code == 500, str(resp.status_code))
    # 每次失败后立即核对回滚效果（基线始终是 v1 的 20 行 + 就绪过滤器）
    info = batch_info("BFB")
    check(f"失败后批次仍停留在 v1、过滤器仍就绪（{bad_fragment}）",
          info["version"] == 1 and info["filter_status"] == "ready" and info["filter_version"] == 1, str(info))
    dump = row_dump("BFB")
    check(f"失败后明细原样未动（{bad_fragment}）",
          len(dump) == 20 and dump[10] == (1, "FB010", T)
          and all(v[1] != "FB010X" and v[1] != "FB021" for v in dump.values()))

info = batch_info("BFB")
check("失败后批次仍停留在 v1、过滤器仍就绪",
      info["version"] == 1 and info["filter_status"] == "ready" and info["filter_version"] == 1, str(info))
check("失败后明细原样未动（无 FB010X、FB010 仍在第 10 行）",
      row_dump("BFB")[10] == (1, "FB010", T) and all(v[1] != "FB010X" for v in row_dump("BFB").values()))
q = query("FB010")
check("失败后仍可查原版明细", [(m["batch"], m["line_no"], m["version"]) for m in q["matches"]] == [("BFB", 10, 1)])
q = query("ZZNOPE")
check("失败后原过滤器仍可跳过不命中批次", any(s["batch"] == "BFB" for s in q["skipped"]))
# 回滚后再做一次正常更正，确认库仍可写
upload("BFB", [(f"FB{i:03d}", T) for i in range(1, 21)])
check("回滚后库恢复可写（正常更正升到 v2）", batch_info("BFB")["version"] == 2)

# ---------------------------------------------------------------- 12. 单行更正 × 交错查箱：快照一致性
upload("BIL", [(f"IL{i:03d}", T) for i in range(1, 61)])
build("BIL")
wait_build_done("BIL")
stop2 = threading.Event()
interleave_errors = []


def single_line_corrector():
    n = 0
    c = gate.app.test_client()
    while not stop2.is_set():  # 每轮只改第 30 行一笔，版本持续前进
        n += 1
        data_rows = [(f"IL{i:03d}", T) for i in range(1, 61)]
        data_rows[29] = (f"IL030M{n}", T)
        try:
            c.post("/batches", data={"batch": "BIL", "file": (io.BytesIO(csv_bytes(data_rows)), "f.csv")},
                   content_type="multipart/form-data")
        except Exception as e:  # noqa: BLE001
            interleave_errors.append(str(e))
        time.sleep(0.01)


t = threading.Thread(target=single_line_corrector)
t.start()
interleave_ok = True
seen_versions = set()
try:
    for _ in range(60):
        q = query("IL001")  # 从未被改动的行；每轮更正只写 1 行，它应始终保留 v1 行内版号
        if len(q["matches"]) != 1 or q["matches"][0]["line_no"] != 1:
            interleave_ok = False
        ver = q["matches"][0]["version"]
        seen_versions.add(ver)  # 呈现版号必须是查询瞬间的批次当前版
        # 一次查询内：扫描/跳过条目与命中行版号一致，且批次不重复
        entries = {s["batch"]: s["version"] for s in q["scanned"]}
        if ver != entries.get("BIL"):
            interleave_ok = False
        if any(s["batch"] == "BIL" for s in q["skipped"]) and any(s["batch"] == "BIL" for s in q["scanned"]):
            interleave_ok = False
        # 当前版箱号（最新一轮的改名行）至多命中一行，且与查询快照版号一致
        cur = batch_info("BIL")
        q2 = query(f"IL030M{cur['version'] - 1}")
        if q2["matches"] and (q2["matches"][0]["version"] != cur["version"]
                              or q2["matches"][0]["line_no"] != 30):
            interleave_ok = False
        time.sleep(0.02)
finally:
    stop2.set()
    t.join()
check("单行更正交错查询无内部异常", not interleave_errors, "; ".join(interleave_errors[:3]))
check("交错期间观察到多个已提交版号（证明确实持续升版）", len(seen_versions) > 1, str(sorted(seen_versions)))
check("60 次交错查询快照一致（未改行始终可查、版号随批次当前版、行号稳定）", interleave_ok)
dump = row_dump("BIL")
check("交错结束后只有第 30 行被反复写入，其余行版号仍为 v1",
      all(dump[ln][0] == 1 for ln in dump if ln != 30) and dump[30][0] == batch_info("BIL")["version"],
      str({ln: dump[ln][0] for ln in list(dump)[:3] + [30]}))

# ---------------------------------------------------------------- 13. 旧库直接升级：版本号、明细、可用过滤器全保留
legacy_dir = tempfile.mkdtemp(prefix="gate-legacy-")
legacy_db = os.path.join(legacy_dir, "legacy.db")
lc = sqlite3.connect(legacy_db)
lc.executescript(
    """
    CREATE TABLE batches (
        id TEXT PRIMARY KEY, version INTEGER NOT NULL, row_count INTEGER NOT NULL,
        uploaded_at TEXT NOT NULL, filter_version INTEGER, filter_status TEXT NOT NULL DEFAULT 'none',
        building_version INTEGER, filter_error TEXT);
    CREATE TABLE rows (
        batch_id TEXT NOT NULL, batch_version INTEGER NOT NULL, line_no INTEGER NOT NULL,
        container TEXT NOT NULL, gate_time_utc TEXT NOT NULL,
        PRIMARY KEY (batch_id, batch_version, line_no));
    CREATE INDEX idx_rows_container ON rows (container);
    CREATE TABLE filters (
        batch_id TEXT NOT NULL, version INTEGER NOT NULL, m_bits INTEGER NOT NULL,
        k_hashes INTEGER NOT NULL, items INTEGER NOT NULL, blob BLOB NOT NULL,
        built_at TEXT NOT NULL, status TEXT NOT NULL, error TEXT,
        PRIMARY KEY (batch_id, version));
    INSERT INTO batches VALUES ('OLD', 3, 5, '2026-11-01T00:00:00Z', 3, 'ready', NULL, NULL);
    INSERT INTO rows VALUES ('OLD', 1, 1, 'OLDA', '2026-11-01T08:00:00Z');
    INSERT INTO rows VALUES ('OLD', 2, 2, 'OLDB', '2026-11-02T08:00:00Z');
    INSERT INTO rows VALUES ('OLD', 3, 3, 'OLDC', '2026-11-03T08:00:00Z');
    INSERT INTO rows VALUES ('OLD', 3, 4, 'OLDD', '2026-11-03T09:00:00Z');
    INSERT INTO rows VALUES ('OLD', 3, 5, 'OLDE', '2026-11-03T10:00:00Z');
    """
)
legacy_bf = BloomFilter.build(["OLDA", "OLDB", "OLDC", "OLDD", "OLDE"])
lc.execute("INSERT INTO filters VALUES ('OLD', 3, ?, ?, 5, ?, '2026-11-03T11:00:00Z', 'ready', NULL)",
           (legacy_bf.m_bits, legacy_bf.k, legacy_bf.bits))
lc.commit()
lc.close()
check("旧库构造完成（v1 结构、user_version=0）",
      sqlite3.connect(legacy_db).execute("PRAGMA user_version").fetchone()[0] == 0)

saved_db = gate.DATABASE
gate.DATABASE = legacy_db
try:
    gate.init_db(legacy_db)  # 显式跑一次迁移
    migrated = sqlite3.connect(legacy_db)
    check("迁移后 user_version 升到 2", migrated.execute("PRAGMA user_version").fetchone()[0] == 2)
    pk = [r[1] for r in migrated.execute("PRAGMA table_info(rows)") if r[5] > 0]
    check("迁移后 rows 主键为 (batch_id, line_no)", pk == ["batch_id", "line_no"], str(pk))
    legacy_rows = migrated.execute(
        "SELECT line_no, batch_version, container FROM rows ORDER BY line_no"
    ).fetchall()
    check("迁移后 5 行明细与各行版号原样保留",
          legacy_rows == [(1, 1, "OLDA"), (2, 2, "OLDB"), (3, 3, "OLDC"), (4, 3, "OLDD"), (5, 3, "OLDE")],
          str(legacy_rows))
    b = migrated.execute("SELECT version, row_count, filter_version, filter_status FROM batches WHERE id='OLD'").fetchone()
    check("迁移后批次版本号 v3 与过滤器指针保留", tuple(b) == (3, 5, 3, "ready"), str(tuple(b)))
    f = migrated.execute("SELECT status, length(blob) FROM filters WHERE batch_id='OLD' AND version=3").fetchone()
    check("迁移后可用过滤器 blob 保留", f[0] == "ready" and 0 < f[1] <= 2048, str(tuple(f)))
    migrated.close()

    # 迁移后应用直接可用：查询走旧过滤器跳过、命中行显示当前版 v3
    lclient = gate.app.test_client()
    q = lclient.get("/query?container=ZZMISS&format=json").get_json()
    check("迁移后旧过滤器仍生效（不命中被跳过）", any(s["batch"] == "OLD" for s in q["skipped"]))
    q = lclient.get("/query?container=olda&format=json").get_json()
    check("迁移后明细可查且显示批次当前版 v3",
          q["matches"] == [{"batch": "OLD", "version": 3, "line_no": 1,
                            "container": "OLDA", "gate_time_utc": "2026-11-01T08:00:00Z"}], str(q["matches"]))

    # 迁移后的库上做增量更正：只改一行，旧行复用、版本续升到 v4
    data = csv_bytes([
        ("OLDA", "2026-11-01T08:00:00Z"),
        ("OLDB", "2026-11-02T08:00:00Z"),
        ("OLDC", "2026-11-03T08:00:00Z"),
        ("OLDD2", "2026-11-03T09:00:00Z"),
        ("OLDE", "2026-11-03T10:00:00Z"),
    ])
    resp = lclient.post("/batches", data={"batch": "OLD", "file": (io.BytesIO(data), "f.csv")},
                        content_type="multipart/form-data", follow_redirects=True)
    check("迁移库上单行更正成功（v4、复用 4 改动 1、旧过滤器作废）",
          "v4" in resp.get_data(as_text=True) and "复用 4" in resp.get_data(as_text=True)
          and "旧过滤器已失效" in resp.get_data(as_text=True))
    q = lclient.get("/query?container=OLDD2&format=json").get_json()
    check("迁移库更正后新行可查、显示 v4",
          [(m["line_no"], m["version"]) for m in q["matches"]] == [(4, 4)])
    info = lclient.get("/api/batches").get_json()
    old_info = next(x for x in info if x["id"] == "OLD")
    check("迁移库更正后旧过滤器指针已清空", old_info["filter_version"] is None and old_info["filter_status"] == "none")
    gate.init_db(legacy_db)  # 再跑一次：幂等，不应报错也不应重复迁移
    check("init_db 幂等（重复执行不报错、版号仍为 2）",
          sqlite3.connect(legacy_db).execute("PRAGMA user_version").fetchone()[0] == 2)
finally:
    gate.DATABASE = saved_db

# 全新库初始化仍为 v2 结构
fresh_dir = tempfile.mkdtemp(prefix="gate-fresh-")
fresh_db = os.path.join(fresh_dir, "fresh.db")
gate.init_db(fresh_db)
check("全新库初始化为 v2", sqlite3.connect(fresh_db).execute("PRAGMA user_version").fetchone()[0] == 2)

# ---------------------------------------------------------------- 汇总
failed = [n for n, ok in CHECKS if not ok]
print(f"\n{len(CHECKS) - len(failed)}/{len(CHECKS)} 项通过")
if failed:
    print("失败项:", *failed, sep="\n  - ")
    sys.exit(1)
print("全部通过")
