// Package ordering 负责 LSN 排序与重复记录处理。
//
// 流水线约定：排序 → 去重 → 事务组装。去重必须发生在事务组装之前：
// 重复 BEGIN 会清空汇编器里已收集的事务记录，重复 COMMIT 会被误判成
// orphan 或导致双重落库，乱序到达的 COMMIT 也会因找不到 BEGIN 而丢数据。
// 先恢复 LSN 全局序、再按 (XID, LSN) 去重后，汇编器面对的始终是良构流。
package ordering

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/internal-platform/tapdance/internal/wal"
)

// Key 唯一标识一条物理 WAL 记录。
//
// 上游重放时 LSN 保持不变，因此 (XID, LSN) 相同的两条记录必然是
// 同一条记录的重复投递，而不是两条不同的记录。
type Key struct {
	XID uint32
	LSN uint64
}

func keyOf(r wal.Record) Key { return Key{XID: r.XID, LSN: r.LSN} }

// Conflict 描述同一个 Key 被投递了两次、但内容不一致的情况。
//
// LSN 唯一对应一个物理 WAL 位置，内容不一致意味着页损坏或两个上游
// 给出了互相矛盾的重放，不能静默二选一。
type Conflict struct {
	XID      uint32
	LSN      uint64
	Kept     wal.Record // 被保留、对下游生效的版本
	Rejected wal.Record // 被丢弃的矛盾版本
}

// ConflictError 是一批记录中全部 payload/type 冲突的集合。
type ConflictError struct {
	Items []Conflict
}

func (e *ConflictError) Error() string {
	if e == nil || len(e.Items) == 0 {
		return "wal conflict: none"
	}
	c := e.Items[0]
	return fmt.Sprintf(
		"wal conflict: %d duplicate key(s) with inconsistent payload, first at (xid=%d, lsn=%d)",
		len(e.Items), c.XID, c.LSN)
}

// Sort 按 LSN 升序恢复页读取顺序。
//
// LSN 相同时依次按 XID、记录类型排序，保证输出完全确定；
// 使用稳定排序，其余到达次序保持原样。
func Sort(recs []wal.Record) []wal.Record {
	out := make([]wal.Record, len(recs))
	copy(out, recs)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].LSN != out[j].LSN {
			return out[i].LSN < out[j].LSN
		}
		if out[i].XID != out[j].XID {
			return out[i].XID < out[j].XID
		}
		return out[i].Type < out[j].Type
	})
	return out
}

// sameContent 判断两条同 Key 记录是否为完全相同的重复投递。
func sameContent(a, b wal.Record) bool {
	return a.Type == b.Type && bytes.Equal(a.Payload, b.Payload)
}

// Deduper 是跨批次保留已见 Key 的有状态去重器。
//
// 零值不可用，必须用 NewDeduper 构造。判重基于 map 而不是相邻比较：
// 重复记录可能与首次投递相隔任意距离、甚至跨越不同批次。
type Deduper struct {
	seen map[Key]wal.Record

	// Duplicates 是累计被拦下的重复记录数（含冲突记录）。
	Duplicates int
	// Conflicts 是累计发现的同 Key 内容冲突数，是 Duplicates 的子集。
	Conflicts int

	conflicts []Conflict
}

// NewDeduper 构造去重器。
func NewDeduper() *Deduper {
	return &Deduper{seen: map[Key]wal.Record{}}
}

// Feed 接收一批记录（调用方应先经 Sort 排序），返回本批次首次出现的记录。
//
// 重复记录不会出现在返回值里，只累加计数；同 Key 内容冲突时保留第一次
// 见到的版本（因为输入已按 LSN 排序，这就是规范 WAL 序里最早的版本），
// 矛盾版本一律丢弃并记入返回的 conflicts。conflicts 只包含本批次新发现
// 的冲突，累计值见 Conflicts 字段。
func (d *Deduper) Feed(recs []wal.Record) (unique []wal.Record, batchDups int, batchConflicts []Conflict) {
	for _, r := range recs {
		k := keyOf(r)
		first, ok := d.seen[k]
		if !ok {
			d.seen[k] = r
			unique = append(unique, r)
			continue
		}
		// 同一个 Key 再次出现：无论内容是否一致，都只能对下游生效一次。
		d.Duplicates++
		batchDups++
		if sameContent(first, r) {
			continue
		}
		// Key 内含 LSN，同 Key 的两条记录 LSN 必然相同，不存在"换版本"问题，
		// 首见版本就是确定的保留版本。
		c := Conflict{XID: k.XID, LSN: k.LSN, Kept: first, Rejected: r}
		d.Conflicts++
		d.conflicts = append(d.conflicts, c)
		batchConflicts = append(batchConflicts, c)
	}
	return unique, batchDups, batchConflicts
}

// ConflictsDetected 返回累计发现的全部冲突快照。
func (d *Deduper) ConflictsDetected() []Conflict {
	out := make([]Conflict, len(d.conflicts))
	copy(out, d.conflicts)
	return out
}

// Seen 返回去重器已经登记过的 Key 数量。
func (d *Deduper) Seen() int { return len(d.seen) }

// Dedupe 是无状态便捷封装：基于 map 判重，绝不依赖相邻位置。
//
// 内部会先排序，使保留的首见版本就是 LSN 最小的版本。跨批次重放
// 去重请使用 Deduper。
func Dedupe(recs []wal.Record) ([]wal.Record, int) {
	unique, dups, _ := NewDeduper().Feed(Sort(recs))
	return unique, dups
}

// Stats 统计记录类型分布，便于对账。
func Stats(recs []wal.Record) map[string]int {
	out := map[string]int{}
	for _, r := range recs {
		out[r.Type.String()]++
	}
	return out
}
