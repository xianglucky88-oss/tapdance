#!/bin/sh
# demo.sh —— 生成 WAL，跑一次变更捕获，验证快照衔接与恢复路径。
set -eu
cd "$(dirname "$0")/.."

rm -rf /tmp/tapdance
mkdir -p /tmp/tapdance

echo "== 1. 生成 WAL（40 个事务 + DDL + 回滚 + 未提交）=="
./bin/walgen -out /tmp/tapdance/wal.bin -txns 40

echo
echo "== 2. 解析并落库 =="
./bin/tapdance -wal /tmp/tapdance/wal.bin

echo
echo "== 3. 快照与增量衔接验证 =="
LSN=$(./bin/tapdance -wal /tmp/tapdance/wal.bin 2>/dev/null | grep last_lsn | grep -o '[0-9]*' | head -1)
MID=$((LSN / 2))
./bin/tapdance -wal /tmp/tapdance/wal.bin -snapshot-at "$MID" || true

echo
echo "== 4. 跨页大记录 =="
./bin/walgen -out /tmp/tapdance/big.bin -txns 10 -big 4096
./bin/tapdance -wal /tmp/tapdance/big.bin 2>&1 | grep -E '"records"|"pages"|"complete"'

echo
echo "== 5. 写入中途崩溃后恢复 =="
SIZE=$(stat -c%s /tmp/tapdance/wal.bin)
./bin/walgen -out /tmp/tapdance/torn.bin -txns 40 -torn-tail $((SIZE - 700))
echo "-- 恢复前 --"
./bin/tapdance -wal /tmp/tapdance/torn.bin 2>&1 | grep -E '"records"|"complete"|^wal incomplete' || true
echo "-- 截掉未确认尾部 --"
./bin/tapdance -wal /tmp/tapdance/torn.bin -recover 2>&1 | grep -E '^recovered|^resume point'
./bin/tapdance -wal /tmp/tapdance/torn.bin -strict >/dev/null && echo "恢复后可严格读取"
