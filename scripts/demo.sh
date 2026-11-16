#!/bin/sh
# demo.sh —— 生成 WAL，跑一次变更捕获，并验证快照与增量的衔接。
set -eu
cd "$(dirname "$0")/.."

rm -rf /tmp/tapdance
mkdir -p /tmp/tapdance

echo "== 1. 生成 WAL（40 个事务 + DDL + 回滚 + 未提交）=="
./bin/walgen -out /tmp/tapdance/wal.bin -txns 40

echo
echo "== 2. 解析并落库（无消费者：复制视图如实报告 no_slots）=="
./bin/tapdance -wal /tmp/tapdance/wal.bin

echo
echo "== 3. 快照与增量衔接验证 =="
# 取文件中段的一条记录 LSN 作为快照位点
LSN=$(./bin/tapdance -wal /tmp/tapdance/wal.bin 2>/dev/null | grep last_lsn | grep -o '[0-9]*' | head -1)
MID=$((LSN / 2))
./bin/tapdance -wal /tmp/tapdance/wal.bin -snapshot-at "$MID" || true

echo
echo "== 4. 复制视图：同一批 WAL，两个消费者不同进度 =="
echo "   analytics 追平到 $LSN；bi 断连在 $MID（仍钉住保留边界）"
./bin/tapdance -wal /tmp/tapdance/wal.bin \
  -consumer "analytics@$LSN" \
  -consumer "bi@$MID,down" \
  -checkpoint "0@1000" -checkpoint "$MID@1010" -checkpoint "$MID@1020" -checkpoint "$LSN@1030" \
  > /tmp/tapdance/replication.json
python3 - <<'PY'
import json
r = json.load(open("/tmp/tapdance/replication.json"))
print("  last_lsn={last_lsn} retain_from={retain_from_lsn} "
      "backlog={backlog_bytes}B/{backlog_segments}seg "
      "lag={lag_seconds}s ({lag_basis})".format(**r))
for s in r["slots"]:
    print("    {name:<10} {state:<12} confirmed={confirmed_lsn} "
          "backlog={backlog_bytes}B lag={lag_seconds}s "
          "rate={lag_rate_bps}B/s".format(**s))
PY
