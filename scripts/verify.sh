#!/bin/sh
# verify.sh —— 对处理结果做断言。
set -eu
cd "$(dirname "$0")/.."

./bin/walgen -out /tmp/tapdance/verify.bin -txns 20 >/dev/null 2>&1
./bin/tapdance -wal /tmp/tapdance/verify.bin > /tmp/tapdance/report.json

# 复制视图：同一批记录、不同消费进度
LAST=$(python3 -c 'import json; print(json.load(open("/tmp/tapdance/report.json"))["last_lsn"])')
HALF=$((LAST / 2))
./bin/tapdance -wal /tmp/tapdance/verify.bin -consumer "c@0" -fallback-bps 1000 > /tmp/tapdance/r_none.json
./bin/tapdance -wal /tmp/tapdance/verify.bin -consumer "c@$HALF" -fallback-bps 1000 > /tmp/tapdance/r_half.json
./bin/tapdance -wal /tmp/tapdance/verify.bin -consumer "c@$LAST" -fallback-bps 1000 > /tmp/tapdance/r_full.json
./bin/tapdance -wal /tmp/tapdance/verify.bin -consumer "fast@$LAST" -consumer "slow@$HALF,down" -fallback-bps 1000 > /tmp/tapdance/r_two.json
./bin/tapdance -wal /tmp/tapdance/verify.bin -consumer "c@0" \
  -checkpoint "0@1000" -checkpoint "$HALF@1010" -checkpoint "$HALF@1020" -checkpoint "$LAST@1030" \
  > /tmp/tapdance/r_cps.json

LAST_LSN=$LAST HALF_LSN=$HALF python3 - <<'PY'
import json, os, sys

last = int(os.environ["LAST_LSN"])
half = int(os.environ["HALF_LSN"])
SEG = 16 * 1024 * 1024

def load(name):
    return json.load(open(f"/tmp/tapdance/{name}.json"))

r = load("report")
checks = []
def check(n, ok, d): checks.append((n, ok, d))

check("有记录被解析", r["records"] > 0, f"records={r['records']}")
check("有已提交事务", r["committed"] > 0, f"committed={r['committed']}")
check("回滚事务被识别", r["aborted"] == 1, f"aborted={r['aborted']}")
check("未提交事务被识别", r["pending"] == 1, f"pending={r['pending']}")
check("行变更已应用", r["applied"] > 0, f"applied={r['applied']}")
check("orders 表有数据", r["tables"].get("orders", 0) > 0, f"tables={r['tables']}")
check("回滚记录未落库", "ROLLBACK-1" not in json.dumps(r), "rollback row must not appear")
check("积压可计算", r["backlog_bytes"] >= 0, f"backlog={r['backlog_bytes']}")
check("无消费者时如实报告", r["lag_basis"] == "no_slots" and r["slots"] == [] and r["retain_from_lsn"] is None,
      f"basis={r['lag_basis']} slots={r['slots']}")

# ---- 复制视图：同一批记录，不同消费进度 ----
none, rhalf, full = load("r_none"), load("r_half"), load("r_full")

check("进度0: 积压=全部WAL", none["backlog_bytes"] == last, f"backlog={none['backlog_bytes']} want={last}")
check("进度0: 段数=向上取整", none["backlog_segments"] == (last + SEG - 1) // SEG,
      f"segs={none['backlog_segments']}")
check("进度0: lag=积压/速率", abs(none["lag_seconds"] - last / 1000) < 1e-6,
      f"lag={none['lag_seconds']} want={last/1000}")
check("进度0: 用兜底速率", none["lag_basis"] == "fallback", f"basis={none['lag_basis']}")

check("进度半: 积压=last-half", rhalf["backlog_bytes"] == last - half,
      f"backlog={rhalf['backlog_bytes']} want={last-half}")
check("进度半: lag 减半", abs(rhalf["lag_seconds"] - (last - half) / 1000) < 1e-6,
      f"lag={rhalf['lag_seconds']}")
check("进度半: 槽位状态 active", rhalf["slots"][0]["state"] == "active" and rhalf["slots"][0]["lagging"],
      f"slot={rhalf['slots'][0]}")

check("追平: 积压为 0", full["backlog_bytes"] == 0 and full["lag_seconds"] == 0,
      f"backlog={full['backlog_bytes']} lag={full['lag_seconds']}")
check("追平: lag 基准 caught_up", full["lag_basis"] == "caught_up", f"basis={full['lag_basis']}")
check("追平: 保留边界推进到 last", full["retain_from_lsn"] == last, f"retain={full['retain_from_lsn']}")

check("进度不影响落库", none["checksum"] == rhalf["checksum"] == full["checksum"] == r["checksum"],
      "checksum drifted across consumer progress")

# ---- 多槽位：断连的槽位钉住保留边界 ----
two = load("r_two")
slots = {s["name"]: s for s in two["slots"]}
check("断连槽位状态可见", slots["slow"]["state"] == "disconnected", f"slow={slots['slow']}")
check("活跃槽位状态可见", slots["fast"]["state"] == "active", f"fast={slots['fast']}")
check("保留边界由最落后槽位决定", two["retain_from_lsn"] == half, f"retain={two['retain_from_lsn']}")
check("顶层积压取最差槽位", two["backlog_bytes"] == last - half, f"backlog={two['backlog_bytes']}")
check("顶层段数与积压一致", two["backlog_segments"] == (last - half + SEG - 1) // SEG,
      f"segs={two['backlog_segments']}")

# ---- checkpoint 采样：停顿不稀释速率 ----
cps = load("r_cps")
# 有效区间 0→half (10s) 与 half→last (10s)，中间 10s 停顿不计：rate = last/20，lag = 20s
check("采样速率优先于兜底", cps["lag_basis"] == "checkpoints", f"basis={cps['lag_basis']}")
check("停顿不稀释速率", abs(cps["slots"][0]["lag_rate_bps"] - last / 20) < 1e-6,
      f"rate={cps['slots'][0]['lag_rate_bps']} want={last/20}")
check("lag 用有效速率换算", abs(cps["lag_seconds"] - 20) < 1e-6, f"lag={cps['lag_seconds']}")

failed = 0
print("== 断言 ==")
for n, ok, d in checks:
    if not ok: failed += 1
    print(f"  [{'PASS' if ok else 'FAIL'}] {n}: {d}")
print(f"\n结果: {len(checks)-failed}/{len(checks)} 通过")
sys.exit(0 if failed == 0 else 1)
PY
