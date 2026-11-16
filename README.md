# tapdance

数据库变更捕获：解析简化版 WAL，组装事务，按 LSN 排序后落库，并支持快照与增量的衔接验证。

## 快速开始

```bash
make build
make demo     # 生成 WAL、解析落库、验证快照衔接
make verify   # 断言处理结果
make test     # 单元测试
```

## 结构

```
cmd/tapdance      解析入口（-consumer / -checkpoint / -fallback-bps 注入复制视图输入）
cmd/walgen        WAL 生成器（测试用）
internal/wal      页格式、编解码、文件读写
internal/txn      事务组装与可见性
internal/schema   DDL 跟踪与列改名映射
internal/ordering LSN 排序与去重
internal/retention 复制槽状态机、位点与多槽保留边界
internal/lag      LSN → 时间延迟换算（checkpoint 采样速率）
internal/store    内存表与快照校验和
internal/pipeline 链路编排与复制视图报告
```

## 复制视图（backlog / segments / lag）

管道只负责"处理到哪里"（`last_lsn` = 本批记录的最高 LSN），
消费者的确认进度由 `-consumer name@confirmedLSN[,down]` 显式注入，
两者绝不混为一谈：

- `slots[]`：每个槽位的状态（`active` / `disconnected` / `reconnected`）、
  确认位点、积压、段数、延迟。断连的槽位位点冻结，但仍钉住保留边界。
- `retain_from_lsn`：所有槽位中最小的 restart_lsn，即 WAL 保留边界；
  没有槽位时为 `null`（没有保留需求，`lag_basis=no_slots`）。
- 顶层 `backlog_bytes` / `backlog_segments` / `lag_seconds` 取最落后的槽位。
- `lag_basis` 区分延迟的依据：`caught_up`（已追平）、`checkpoints`
  （采样速率，停顿区间不计入）、`fallback`（兜底速率）、`unknown`
  （无法估算，`lag_seconds=-1`）。
- 确认位点倒退会被拒绝并计入 `slots[].regressions`；
  断连期间的推进被忽略；`current` 倒挂时积压为 0 而不会回绕。

## WAL 格式

页大小 1024 字节，页头 16 字节（magic / 页号 / 长度 / CRC32），
记录格式 `[type:1][xid:4][len:2][payload]`。当前记录不跨页。

## 扩展方式

- 替换真实 WAL：只改 `internal/wal` 的解码，上层不用动
- 新增变更类型：在 `wal.Type` 里加枚举，在 `store.Apply` 里加分支
- 新增输出目标：`pipeline.Run` 目前返回统计，可在此挂接 sink
