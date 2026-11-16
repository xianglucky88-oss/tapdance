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
记录格式 `[type:1][xid:4][len:2][payload]`，LSN 等于记录的绝对字节偏移
（`页号 * PageSize + 页内偏移`）。

记录现在可以跨页：单页放不下时，记录头里的 `len` 写成哨兵 `0xFFFF`，
随后 4 字节总长度，起始页剩余空间全给这条记录；续页以 `[0xFF][frag_len:2]`
开头。页头布局、页 CRC 覆盖范围（`[16:16+used]`）与 LSN 语义都没有变化，
因此旧文件仍可读取，单页记录的文件也与旧格式逐字节一致。

### 可恢复读取

`wal.ScanFile` / `wal.Scan` 是流式、可恢复的读取入口，明确区分三类问题：

| 类别 | 含义 | 处理 |
| --- | --- | --- |
| `PageProblem` | magic / 页号 / 长度 / CRC 校验失败 | 整页跳过，后续页继续读 |
| `RecordProblem` | 页完好但记录没写完、孤儿续页、超长、跨页记录途经坏页 | 只丢弃该记录，`Records` 中不会出现半条记录 |
| `Tail` | 文件不足一页、最后一条记录/片段没写完、末页整页损坏 | 视为未确认尾部，可安全截断后重新追加 |

结果里同时给出：

- `Records`：可以可靠读出并交付的记录；
- `PageProblems` / `RecordProblems`：没有交付的数据及原因；
- `Tail.Offset`：未确认尾部起点；
- `Resume.TruncateOffset / NextPageNo / AppendLSN / SafeLSN`：重启后从哪继续。

跨页记录在读到声明的总长度、且所有片段所在页 CRC 都通过之前不会交付。
重组缓冲受 `ScanOptions.MaxRecordSize` 限制（默认 16 MiB）：声明长度超限时
只报告 `RecordOversized` 并按片段长度跳过，可继续重新对齐，不会按不可信的
4 GiB 长度分配内存。

写入端使用 `wal.Writer` 流式追加，`wal.Resume` 会在重启时截掉未确认尾部并
校验重放记录的 LSN 不漂移；文件中段的损坏不会被静默掩盖，`Resume` 会拒绝
继续追加。`wal.ReadFile` 保持严格语义：只有完全干净时才返回成功。

```bash
tapdance -wal file.bin              # 宽松读取：报告已读记录 + 损坏 + 尾部
tapdance -wal file.bin -strict      # 有任何损坏/未确认尾部即退出码 3
tapdance -wal file.bin -recover     # 截掉未确认尾部，打印续写位点
tapdance -wal file.bin -max-record 1048576
```

正常多页、跨页写入、尾页补齐、校验失败后继续读、半条记录不进入事务这些
边界都有单元测试覆盖（`internal/wal/scan_test.go`、
`internal/txn/assembler_scan_test.go`）。`make verify` 也会跑端到端的
损坏/截断场景。

## 扩展方式

- 替换真实 WAL：只改 `internal/wal` 的解码，上层不用动
- 新增变更类型：在 `wal.Type` 里加枚举，在 `store.Apply` 里加分支
- 新增输出目标：`pipeline.Run` 目前返回统计，可在此挂接 sink
