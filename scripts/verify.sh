#!/bin/sh
# verify.sh —— 对处理结果做断言。
set -eu
cd "$(dirname "$0")/.."

./bin/walgen -out /tmp/tapdance/verify.bin -txns 20 >/dev/null 2>&1
./bin/tapdance -wal /tmp/tapdance/verify.bin > /tmp/tapdance/report.json

python3 - <<'PY'
import json, sys

r = json.load(open("/tmp/tapdance/report.json"))
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

failed = 0
print("== 断言 ==")
for n, ok, d in checks:
    if not ok: failed += 1
    print(f"  [{'PASS' if ok else 'FAIL'}] {n}: {d}")
print(f"\n结果: {len(checks)-failed}/{len(checks)} 通过")
sys.exit(0 if failed == 0 else 1)
PY
