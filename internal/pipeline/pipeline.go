// Package pipeline 串起 WAL → 事务 → 排序 → 落库 的整条链路。
package pipeline

import (
	"github.com/internal-platform/tapdance/internal/lag"
	"github.com/internal-platform/tapdance/internal/ordering"
	"github.com/internal-platform/tapdance/internal/retention"
	"github.com/internal-platform/tapdance/internal/schema"
	"github.com/internal-platform/tapdance/internal/store"
	"github.com/internal-platform/tapdance/internal/txn"
	"github.com/internal-platform/tapdance/internal/wal"
)

// Report 是一次处理的统计。
type Report struct {
	Records   int `json:"records"`
	Committed int `json:"committed"`
	Aborted   int `json:"aborted"`
	Orphans   int `json:"orphans"`
	// Unattributed 是没有 BEGIN 的完整变更，且 LSN 与已知损坏重叠。
	Unattributed int            `json:"unattributed"`
	Pending      int            `json:"pending"`
	Incomplete   int            `json:"incomplete"`
	Duplicates   int            `json:"duplicates"`
	Applied      int            `json:"applied"`
	Tables       map[string]int `json:"tables"`
	Checksum     string         `json:"checksum"`
	BacklogByte  uint64         `json:"backlog_bytes"`
	BacklogSeg   int            `json:"backlog_segments"`
	LagSeconds   float64        `json:"lag_seconds"`
	LastLSN      uint64         `json:"last_lsn"`

	// WAL 层的完整性信息：已读记录与未确认尾部如何向上报告。
	Pages          int                 `json:"pages"`
	PageProblems   []wal.PageProblem   `json:"page_problems,omitempty"`
	RecordProblems []wal.RecordProblem `json:"record_problems,omitempty"`
	Tail           wal.Tail            `json:"tail"`
	Resume         wal.Recovery        `json:"resume"`
	Complete       bool                `json:"complete"`
}

// Run 处理一个 WAL 文件里的全部记录（调用方已保证记录完整）。
func Run(recs []wal.Record) Report {
	return RunScan(&wal.ScanResult{
		Records: recs,
		Resume:  wal.Recovery{SafeLSN: wal.LastLSN(recs)},
	})
}

// RunScan 处理一次扫描结果，并把页级损坏、记录级不完整和未确认尾部一并
// 反映在报告中。Records 只包含扫描器确认完整的记录。
func RunScan(res *wal.ScanResult) Report {
	asm := txn.New()
	ing := asm.FeedScan(res)

	committed := ordering.Sort(asm.Committed())
	committed, dups := ordering.Dedupe(committed)

	st := store.New()
	tr := schema.New()
	for _, r := range committed {
		_ = st.Apply(r, tr)
	}

	last := wal.LastLSN(res.Records)
	slot := retention.NewSlot("tapdance", 0)
	slot.Advance(last)

	tables := map[string]int{}
	for _, name := range tr.Tables() {
		tables[name] = st.RowCount(name)
	}

	return Report{
		Records:        ing.Records,
		Committed:      len(asm.Committed()),
		Aborted:        asm.Aborted(),
		Orphans:        asm.Orphans(),
		Unattributed:   asm.Unattributed(),
		Pending:        asm.Pending(),
		Incomplete:     asm.Incomplete(),
		Duplicates:     dups,
		Applied:        st.Applied,
		Tables:         tables,
		Checksum:       st.Checksum(),
		BacklogByte:    slot.Backlog(last),
		BacklogSeg:     retention.Segments(slot.Backlog(last)),
		LagSeconds:     lag.Estimate(last, slot.ConfirmedLSN, nil, 1_000_000).Seconds(),
		LastLSN:        last,
		Pages:          res.Pages,
		PageProblems:   res.PageProblems,
		RecordProblems: res.RecordProblems,
		Tail:           res.Tail,
		Resume:         res.Resume,
		Complete:       !res.HasDamage() && !res.Truncated(),
	}
}

// SnapshotHandoff 验证"快照 + 增量"与"全量重放"是否一致。
//
// at 是快照位点：快照包含 LSN <= at 的全部变更，增量应当只补 at 之后的变更。
func SnapshotHandoff(recs []wal.Record, at uint64) (full, handoff string, err error) {
	applyRange := func(pred func(wal.Record) bool) (*store.Store, error) {
		asm := txn.New()
		for _, r := range recs {
			asm.Feed(r)
		}
		st := store.New()
		tr := schema.New()
		for _, r := range ordering.Sort(asm.Committed()) {
			if !pred(r) {
				continue
			}
			if err := st.Apply(r, tr); err != nil {
				return nil, err
			}
		}
		return st, nil
	}

	fullStore, err := applyRange(func(wal.Record) bool { return true })
	if err != nil {
		return "", "", err
	}

	// 快照：LSN <= at
	snap, err := applyRange(func(r wal.Record) bool { return r.LSN <= at })
	if err != nil {
		return "", "", err
	}
	// 增量：在快照基础上补 LSN >= at 的记录。
	tr := schema.New()
	for _, r := range ordering.Sort(mustCommitted(recs)) {
		if r.LSN >= at {
			if err := snap.Apply(r, tr); err != nil {
				return "", "", err
			}
		}
	}

	return fullStore.Checksum(), snap.Checksum(), nil
}

func mustCommitted(recs []wal.Record) []wal.Record {
	asm := txn.New()
	for _, r := range recs {
		asm.Feed(r)
	}
	return asm.Committed()
}
