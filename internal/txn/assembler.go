// Package txn 负责把记录组装成事务，并过滤不可见的变更。
package txn

import "github.com/internal-platform/tapdance/internal/wal"

// Assembler 收集未提交事务的记录，提交时才对外可见。
//
// 组装器只接受“完整记录”。wal 扫描器保证这一点：半条记录、未完成的跨页
// 片段和坏页里的记录都不会出现在 ScanResult.Records 里，而是出现在
// ScanResult.RecordProblems / PageProblems 中。FeedScan 会把这些损坏登记
// 下来，并保守地拒绝提交 LSN 范围与损坏重叠的事务，从而避免把“半条记录”
// 当成孤儿变更继续向下游传递。
type Assembler struct {
	open         map[uint32][]wal.Record
	beginLSN     map[uint32]uint64
	committed    []wal.Record
	aborted      int
	orphans      int
	unattributed int
	incomplete   int

	// damagedXID 是已知属于哪个事务的损坏记录。
	damagedXID map[uint32]bool
	// damagedRanges 是无法归属事务的损坏（坏页、孤儿片段等）。任何
	// [beginLSN, commitLSN] 与之重叠的事务都视为不完整。
	damagedRanges []lsnRange
	tailTruncated bool
	safeLSN       uint64
}

type lsnRange struct{ lo, hi uint64 }

// New 构造组装器。
func New() *Assembler {
	return &Assembler{
		open:       map[uint32][]wal.Record{},
		beginLSN:   map[uint32]uint64{},
		damagedXID: map[uint32]bool{},
	}
}

// IngestStats 是 FeedScan 的结果摘要。
type IngestStats struct {
	// Records 是接收到的完整记录数。
	Records int `json:"records"`
	// SafeLSN 是最后一条完整记录的 LSN。
	SafeLSN uint64 `json:"safe_lsn"`
	// Incomplete 是因为 LSN 范围与损坏重叠而被拒绝提交的事务数。
	Incomplete int `json:"incomplete"`
	// Orphans 是完整记录里没有 BEGIN、也没有可解释损坏的变更数。
	Orphans int `json:"orphans"`
	// Unattributed 是没有 BEGIN 的完整变更，且其 LSN 与已知损坏重叠：
	// 很可能是 BEGIN 落在坏页里。与 Orphans 分开报告，避免把“损坏导致
	// 的缺头事务”误当成重复投递的尾巴。
	Unattributed int `json:"unattributed"`
	// TailTruncated 表示文件尾部未确认，后续记录可能缺失。
	TailTruncated bool `json:"tail_truncated"`
}

// FeedScan 先登记扫描层的损坏信息，再按顺序送入完整记录。
//
// 损坏登记是保守的：一个坏页里可能有任意事务的记录，因此跨越该页的事务
// 都会被拒绝提交。这样做的代价是可能少提交一个本来完整的事务，但不会把
// 缺失了变更的事务当成提交成功。
func (a *Assembler) FeedScan(res *wal.ScanResult) IngestStats {
	a.tailTruncated = res.Truncated()
	a.safeLSN = res.Resume.SafeLSN
	for _, p := range res.PageProblems {
		lo := uint64(p.Offset)
		a.damagedRanges = append(a.damagedRanges, lsnRange{lo, lo + wal.PageSize})
	}
	for _, p := range res.RecordProblems {
		if p.HasXID {
			a.damagedXID[p.XID] = true
		} else {
			a.damagedRanges = append(a.damagedRanges, lsnRange{p.LSN, p.LSN + 1})
		}
	}
	for _, r := range res.Records {
		a.Feed(r)
	}
	// 退出时仍未提交的事务里，如果 LSN 范围碰到损坏，说明它可能缺了
	// COMMIT 之外的记录；把它们从 open 中剔除并计入 incomplete，避免
	// 上层误以为“只是还没提交”。
	for xid := range a.open {
		if a.isDamaged(xid, a.beginLSN[xid], a.safeLSN) {
			delete(a.open, xid)
			delete(a.beginLSN, xid)
			a.incomplete++
		}
	}
	return IngestStats{
		Records:       len(res.Records),
		SafeLSN:       a.safeLSN,
		Incomplete:    a.incomplete,
		Orphans:       a.orphans,
		Unattributed:  a.unattributed,
		TailTruncated: a.tailTruncated,
	}
}

// Feed 送入一条完整记录。
func (a *Assembler) Feed(r wal.Record) {
	switch r.Type {
	case wal.TypeBegin:
		a.open[r.XID] = []wal.Record{r}
		a.beginLSN[r.XID] = r.LSN
	case wal.TypeCommit:
		recs, ok := a.open[r.XID]
		if !ok {
			// 没有 BEGIN 的 COMMIT。若损坏发生在它之前，很可能是 BEGIN
			// 落在坏页/坏记录里；否则视为重复投递的尾巴。
			if a.damagedXID[r.XID] || a.damageBefore(r.LSN) {
				a.unattributed++
			} else {
				a.orphans++
			}
			return
		}
		delete(a.open, r.XID)
		if a.isDamaged(r.XID, a.beginLSN[r.XID], r.LSN) {
			// 事务范围内有记录没有完整读出：整笔事务不对外可见，
			// 但不计入孤儿（它不是“半条记录”，而是已知缺数据的事务）。
			a.incomplete++
			return
		}
		a.committed = append(a.committed, recs...)
		a.committed = append(a.committed, r)
		delete(a.beginLSN, r.XID)
	case wal.TypeAbort:
		if _, ok := a.open[r.XID]; ok {
			delete(a.open, r.XID)
			delete(a.beginLSN, r.XID)
			a.aborted++
		}
	default:
		if recs, ok := a.open[r.XID]; ok {
			a.open[r.XID] = append(recs, r)
			return
		}
		// 未开启事务的完整变更：直接丢弃。若它的 LSN 落在已知损坏范围内，
		// 更可能是 BEGIN（或整段前缀）落在坏页/坏记录里，而不是重复投递。
		if a.damagedXID[r.XID] || a.damageBefore(r.LSN) {
			a.unattributed++
		} else {
			a.orphans++
		}
	}
}

func (a *Assembler) isDamaged(xid uint32, lo, hi uint64) bool {
	if a.damagedXID[xid] {
		return true
	}
	for _, r := range a.damagedRanges {
		if r.lo <= hi && lo <= r.hi {
			return true
		}
	}
	return false
}

// damageBefore 报告在 lsn 之前是否存在无法归属的损坏。BEGIN 必然早于同一
// 事务的变更记录，因此“找不到 BEGIN 但前面有坏页/坏记录”更可能是缺头，
// 而不是重复投递的尾巴。
func (a *Assembler) damageBefore(lsn uint64) bool {
	for _, r := range a.damagedRanges {
		if r.lo < lsn {
			return true
		}
	}
	return false
}

// Committed 返回已提交事务的全部记录（含 BEGIN/COMMIT）。
func (a *Assembler) Committed() []wal.Record { return a.committed }

// Pending 返回退出时仍未提交的事务数。
func (a *Assembler) Pending() int { return len(a.open) }

// Aborted 返回已回滚事务数。
func (a *Assembler) Aborted() int { return a.aborted }

// Orphans 返回被丢弃的孤儿记录数（只统计完整记录）。
func (a *Assembler) Orphans() int { return a.orphans }

// Incomplete 返回因范围与损坏重叠而未提交的事务数。
func (a *Assembler) Incomplete() int { return a.incomplete }

// Unattributed 返回因缺少 BEGIN 且与损坏范围重叠而未归属的完整变更数。
func (a *Assembler) Unattributed() int { return a.unattributed }

// TailTruncated 报告最近一次 FeedScan 是否遇到未确认尾部。
func (a *Assembler) TailTruncated() bool { return a.tailTruncated }

// SafeLSN 返回最后一条完整读出的记录 LSN。
func (a *Assembler) SafeLSN() uint64 { return a.safeLSN }
