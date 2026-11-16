# tapdance

数据库变更捕获：解析简化版 WAL，组装事务，按 LSN 排序后落库，并支持快照与增量的衔接验证。

## 快速开始

```bash
make build
make demo     # 生成 WAL、解析落库、验证快照衔接
make verify   # 断言处理结果（含乱序 + 重放场景与对账恒等式）
make test     # 单元测试
```

## 结构

```
cmd/tapdance      解析入口（-shuffle/-replay 可模拟乱序到达与上游重放）
cmd/walgen        WAL 生成器（测试用）
internal/wal      页格式、编解码、文件读写
internal/txn      事务组装与可见性（支持跨批次保留未结束事务）
internal/schema   DDL 跟踪与列改名映射
internal/ordering LSN 排序与 (XID, LSN) 去重（有状态 Deduper，跨批次有效）
internal/retention 复制槽位点与积压
internal/lag      LSN → 时间延迟换算
internal/store    内存表与快照校验和
internal/pipeline 链路编排（Processor + Report.Reconcile 对账）
```

## 排序、去重与事务可见性规则

处理顺序固定为：**排序 → 去重 → 事务组装 → 按 LSN 落库**。

- 去重发生在事务组装**之前**。重复 BEGIN 会清空汇编器已收集的事务记录，
  重复 COMMIT 会被误判成孤儿或导致双重落库，乱序到达的 COMMIT 会因找不到
  BEGIN 而丢数据；先排序去重后，汇编器面对的始终是良构流。
- 排序按 LSN（平局按 XID、类型）恢复页读取顺序，乱序到达不影响结果。
- 去重以 map 按 `(XID, LSN)` 判重，**不依赖相邻位置**，重复记录可以相隔
  任意距离、跨越任意批次；`Deduper` 跨批次记住已见 Key，旧批次重放不会
  重新打开任何事务。
- 同 Key 但 payload/type 不一致属于冲突：保留首见版本继续处理，矛盾版本
  计入 `duplicate_conflicts`（是 `duplicates` 的子集）并通过 error 上报，不静默二选一。
- 事务语义异常（重复 BEGIN、无主 COMMIT/ABORT、无主变更）计入 `orphans`，
  已收集的事务记录不会被覆盖。未结束事务跨批次保留，COMMIT 到达后才可见。
- `Report.Reconcile()` 强制三条对账恒等式：
  - `records = effective_records + duplicates`
  - `effective_records = committed + aborted_records + orphans + pending_records`
  - `committed = 2 × committed_txns + applied + ddl_applied`
  另附原始/有效两套类型直方图，逐类型之差即可解释全部重复来源。

## WAL 格式

页大小 1024 字节，页头 16 字节（magic / 页号 / 长度 / CRC32），
记录格式 `[type:1][xid:4][len:2][payload]`。当前记录不跨页。

## 扩展方式

- 替换真实 WAL：只改 `internal/wal` 的解码，上层不用动
- 新增变更类型：在 `wal.Type` 里加枚举，在 `store.Apply` 里加分支
- 新增输出目标：`pipeline.Run` 目前返回统计，可在此挂接 sink
