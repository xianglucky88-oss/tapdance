#!/bin/sh
# verify.sh —— 对处理结果做断言。
set -eu
cd "$(dirname "$0")/.."

mkdir -p /tmp/tapdance

run_case() {
	# $1 = 名称，$2 = walgen 额外参数
	name=$1
	shift
	./bin/walgen -out "/tmp/tapdance/$name.bin" "$@" >/dev/null 2>&1
	./bin/tapdance -wal "/tmp/tapdance/$name.bin" > "/tmp/tapdance/$name.json" 2>"/tmp/tapdance/$name.err" || true
}

# 正常多页
run_case base -txns 20
# 跨页记录（6 KiB，跨 7 页）
run_case big -txns 20 -big 6144
# 尾页补齐/截断
BASE_SIZE=$(stat -c%s /tmp/tapdance/base.bin)
run_case torn -txns 20 -torn-tail $((BASE_SIZE - 500))
# 校验失败后继续读取
run_case corrupt -txns 20 -corrupt

python3 - <<'PY'
import json, os, sys

def load(name):
    return json.load(open(f"/tmp/tapdance/{name}.json"))

checks = []
def check(n, ok, d): checks.append((n, ok, d))

base = load("base")
check("正常文件完整", base["complete"] is True, f"complete={base['complete']}")
check("有记录被解析", base["records"] > 0, f"records={base['records']}")
check("有已提交事务", base["committed"] > 0, f"committed={base['committed']}")
check("回滚事务被识别", base["aborted"] == 1, f"aborted={base['aborted']}")
check("未提交事务被识别", base["pending"] == 1, f"pending={base['pending']}")
check("行变更已应用", base["applied"] > 0, f"applied={base['applied']}")
check("orders 表有数据", base["tables"].get("orders", 0) > 0, f"tables={base['tables']}")
check("回滚记录未落库", "ROLLBACK-1" not in json.dumps(base), "rollback row must not appear")
check("积压可计算", base["backlog_bytes"] >= 0, f"backlog={base['backlog_bytes']}")
check("正常文件无尾部", base["tail"]["kind"] == 0, f"tail={base['tail']}")

big = load("big")
check("跨页文件完整", big["complete"] is True, f"complete={big['complete']}")
check("跨页记录只算一条", big["records"] == base["records"] + 1,
      f"big={big['records']} base={base['records']}")
check("跨页记录改变了行数", big["tables"]["orders"] > base["tables"]["orders"],
      f"tables={big['tables']}")
check("跨页后记录数量不变", big["tables"]["orders"] == base["tables"]["orders"] + 1,
      f"big={big['tables']} base={base['tables']}")

torn = load("torn")
check("截断文件被识别", torn["complete"] is False, f"complete={torn['complete']}")
check("截断尾部类型", torn["tail"]["kind"] == 1, f"tail={torn['tail']}")
check("尾页前记录保留", 0 < torn["records"] < base["records"],
      f"torn={torn['records']} base={base['records']}")
check("续写位点页对齐", torn["resume"]["append_lsn"] % 1024 == 0, f"resume={torn['resume']}")
check("安全位点不超过尾部", torn["resume"]["safe_lsn"] <= torn["tail"]["lsn"],
      f"safe={torn['resume']['safe_lsn']} tail={torn['tail']['lsn']}")

corrupt = load("corrupt")
check("坏页被单独报告", len(corrupt.get("page_problems", [])) == 1,
      f"problems={corrupt.get('page_problems')}")
check("坏页不终止读取", corrupt["records"] > 0, f"records={corrupt['records']}")
check("坏页影响的事务不提交", corrupt["incomplete"] >= 1, f"incomplete={corrupt['incomplete']}")
check("坏页不产生孤儿", corrupt["orphans"] == 0, f"orphans={corrupt['orphans']}")
check("缺头记录单独归类", corrupt["unattributed"] >= 0 and corrupt["unattributed"] == 3,
      f"unattributed={corrupt['unattributed']}")
check("坏页后仍有记录读出", corrupt["records"] != base["records"],
      f"corrupt={corrupt['records']} base={base['records']}")
check("坏页不会假装完整", corrupt["complete"] is False and corrupt["tail"]["kind"] == 0,
      f"complete={corrupt['complete']} tail={corrupt['tail']}")

failed = 0
print("== 断言 ==")
for n, ok, d in checks:
    if not ok: failed += 1
    print(f"  [{'PASS' if ok else 'FAIL'}] {n}: {d}")
print(f"\n结果: {len(checks)-failed}/{len(checks)} 通过")
sys.exit(0 if failed == 0 else 1)
PY
