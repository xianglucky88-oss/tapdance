#!/bin/sh
# verify.sh —— 对处理结果做断言，包含乱序+重放场景与对账恒等式校验。
set -eu
cd "$(dirname "$0")/.."

./bin/walgen -out /tmp/tapdance/verify.bin -txns 20 >/dev/null 2>&1

# 正常链路
./bin/tapdance -wal /tmp/tapdance/verify.bin > /tmp/tapdance/report.json
# 乱序到达 + 每条记录额外重放 2 次（原始记录数膨胀到 3 倍）
./bin/tapdance -wal /tmp/tapdance/verify.bin -shuffle -replay 2 -seed 7 \
  > /tmp/tapdance/replay-report.json

python3 - <<'PY'
import json, sys

r = json.load(open("/tmp/tapdance/report.json"))
rr = json.load(open("/tmp/tapdance/replay-report.json"))
checks = []
def check(n, ok, d): checks.append((n, ok, d))

check("有记录被解析", r["records"] > 0, f"records={r['records']}")
check("有已提交事务", r["committed_txns"] > 0, f"committed_txns={r['committed_txns']}")
check("回滚事务被识别", r["aborted"] == 1, f"aborted={r['aborted']}")
check("未提交事务被识别", r["pending"] == 1, f"pending={r['pending']}")
check("行变更已应用", r["applied"] > 0, f"applied={r['applied']}")
check("orders 表有数据", r["tables"].get("orders", 0) > 0, f"tables={r['tables']}")
check("回滚记录未落库", "ROLLBACK-1" not in json.dumps(r), "rollback row must not appear")
check("积压可计算", r["backlog_bytes"] >= 0, f"backlog={r['backlog_bytes']}")

# ---- 对账恒等式 1：原始 = 去重后 + 重复 ----
ok = rr["records"] == rr["effective_records"] + rr["duplicates"]
check("重放：records = effective + duplicates", ok,
      f"{rr['records']} = {rr['effective_records']} + {rr['duplicates']}")

# ---- 对账恒等式 2：去重后每条记录有且仅有一个去向 ----
dest = rr["committed"] + rr["aborted_records"] + rr["orphans"] + rr["pending_records"]
check("重放：effective 记录去向闭合", rr["effective_records"] == dest,
      f"effective={rr['effective_records']} committed+aborted+orphans+pending={dest}")

# ---- 对账恒等式 3：committed = 2×提交事务 + 行变更 + DDL ----
ok = rr["committed"] == 2*rr["committed_txns"] + rr["applied"] + rr["ddl_applied"]
check("重放：committed 与落库动作闭合", ok,
      f"{rr['committed']} == 2*{rr['committed_txns']} + {rr['applied']} + {rr['ddl_applied']}")

# ---- 类型直方图解释全部重复 ----
delta = sum(rr["raw_types"].get(t, 0) - rr["effective_types"].get(t, 0)
            for t in rr["raw_types"])
check("重放：类型直方图差值 == duplicates", delta == rr["duplicates"],
      f"histogram_delta={delta} duplicates={rr['duplicates']}")

# ---- 重放不能改变最终结果：与正常链路逐字段一致 ----
for f in ("applied", "ddl_applied", "committed", "committed_txns",
          "aborted", "aborted_records", "pending", "checksum", "tables"):
    check(f"重放不改变 {f}", rr[f] == r[f], f"normal={r[f]} replay={rr[f]}")
check("重放后无冲突", rr["duplicate_conflicts"] == 0,
      f"conflicts={rr['duplicate_conflicts']}")
check("重放重复数 = 2 倍原始记录", rr["duplicates"] == 2 * r["records"],
      f"dups={rr['duplicates']} expected={2*r['records']}")

failed = 0
print("== 断言 ==")
for n, ok, d in checks:
    if not ok: failed += 1
    print(f"  [{'PASS' if ok else 'FAIL'}] {n}: {d}")
print(f"\n结果: {len(checks)-failed}/{len(checks)} 通过")
sys.exit(0 if failed == 0 else 1)
PY
