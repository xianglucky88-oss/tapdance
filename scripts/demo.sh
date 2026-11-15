#!/bin/sh
# demo.sh —— 生成 WAL，跑一次变更捕获，并验证快照与增量的衔接。
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
# 取文件中段的一条记录 LSN 作为快照位点
LSN=$(./bin/tapdance -wal /tmp/tapdance/wal.bin 2>/dev/null | grep last_lsn | grep -o '[0-9]*' | head -1)
MID=$((LSN / 2))
./bin/tapdance -wal /tmp/tapdance/wal.bin -snapshot-at "$MID" || true
