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
cmd/tapdance      解析入口
cmd/walgen        WAL 生成器（测试用）
internal/wal      页格式、编解码、文件读写
internal/txn      事务组装与可见性
internal/schema   DDL 跟踪与列改名映射
internal/ordering LSN 排序与去重
internal/retention 复制槽位点与积压
internal/lag      LSN → 时间延迟换算
internal/store    内存表与快照校验和
internal/pipeline 链路编排
```

## WAL 格式

页大小 1024 字节，页头 16 字节（magic / 页号 / 长度 / CRC32），
记录格式 `[type:1][xid:4][len:2][payload]`。当前记录不跨页。

## 扩展方式

- 替换真实 WAL：只改 `internal/wal` 的解码，上层不用动
- 新增变更类型：在 `wal.Type` 里加枚举，在 `store.Apply` 里加分支
- 新增输出目标：`pipeline.Run` 目前返回统计，可在此挂接 sink
