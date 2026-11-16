// Package pipeline 串起 WAL → 排序去重 → 事务 → 落库 的整条链路。
//
// 处理顺序固定为：
//
//	原始记录 → LSN 排序 → (XID, LSN) 去重 → 事务组装 → 提交记录按 LSN 落库
//
// 去重发生在事务组装之前（理由见 internal/ordering 包注释）。Processor
// 是有状态的，可以连续处理多个批次：去重器跨批次记住已见 Key，汇编器
// 跨批次保留未结束事务，store/schema 持续累积变更。
package pipeline

import (
	"errors"
	"fmt"

	"github.com/internal-platform/tapdance/internal/lag"
	"github.com/internal-platform/tapdance/internal/ordering"
	"github.com/internal-platform/tapdance/internal/retention"
	"github.com/internal-platform/tapdance/internal/schema"
	"github.com/internal-platform/tapdance/internal/store"
	"github.com/internal-platform/tapdance/internal/txn"
	"github.com/internal-platform/tapdance/internal/wal"
)

// Report 是一次（或截至当前批次累计）处理的统计。
type Report struct {
	// Records 是收到的原始记录总数（含重复、乱序记录）。
	Records int `json:"records"`
	// Duplicates 是被去重器拦下的重复记录数（含冲突记录）。
	Duplicates int `json:"duplicates"`
	// DuplicateConflicts 是同 (XID, LSN) 但内容不一致的记录数，是 Duplicates 的子集。
	DuplicateConflicts int `json:"duplicate_conflicts"`
	// EffectiveRecords 是去重后真正进入事务组装的记录数。
	EffectiveRecords int `json:"effective_records"`

	// Committed 是已提交事务输出的记录数（含 BEGIN/COMMIT 控制记录）。
	Committed int `json:"committed"`
	// CommittedTxns 是已提交事务数。
	CommittedTxns int `json:"committed_txns"`
	// Aborted 是已回滚事务数（不含无主 ABORT）。
	Aborted int `json:"aborted"`
	// AbortedRecords 是属于回滚事务、因此不可见的记录数（含 BEGIN/ABORT）。
	AbortedRecords int `json:"aborted_records"`
	// Orphans 是被丢弃的孤儿记录数（重复 BEGIN、无主 COMMIT/ABORT、无主变更）。
	Orphans int `json:"orphans"`
	// Pending 是截至当前仍未结束的事务数。
	Pending int `json:"pending"`
	// PendingRecords 是未结束事务当前持有的记录数（含 BEGIN）。
	PendingRecords int `json:"pending_records"`

	// Applied 是实际落库的行变更数（INSERT/UPDATE/DELETE，不含 DDL 与控制记录）。
	Applied int `json:"applied"`
	// DDLApplied 是实际应用的 DDL 记录数。
	DDLApplied int `json:"ddl_applied"`

	// RawTypes 是原始输入按类型的直方图；EffectiveTypes 是去重后记录的直方图。
	// 两者逐类型之差就是该类型被拦下的重复数，可用于发现"排序后统计与原始统计不一致"。
	RawTypes       map[string]int `json:"raw_types"`
	EffectiveTypes map[string]int `json:"effective_types"`

	Tables      map[string]int `json:"tables"`
	Checksum    string         `json:"checksum"`
	BacklogByte uint64         `json:"backlog_bytes"`
	BacklogSeg  int            `json:"backlog_segments"`
	LagSeconds  float64        `json:"lag_seconds"`
	LastLSN     uint64         `json:"last_lsn"`
}

// Processor 是跨批次保持状态的处理器，零值不可用，请用 NewProcessor 构造。
type Processor struct {
	dedup      *ordering.Deduper
	asm        *txn.Assembler
	store      *store.Store
	track      *schema.Tracker
	records    int
	ddlApplied int
	rawTypes   map[string]int
	effTypes   map[string]int
	lastLSN    uint64
}

// NewProcessor 构造处理器。
func NewProcessor() *Processor {
	return &Processor{
		dedup:    ordering.NewDeduper(),
		asm:      txn.New(),
		store:    store.New(),
		track:    schema.New(),
		rawTypes: map[string]int{},
		effTypes: map[string]int{},
	}
}

// Process 处理一个批次的记录（允许乱序、含重复、含跨批次重放），
// 返回截至当前批次的累计报告。
//
// 返回的 error 非空时处理仍然完成（冲突时保留首见版本继续组装），
// 调用方可以同时使用 Report 做对账，并依据 error 决定是否中止下游。
func (p *Processor) Process(recs []wal.Record) (Report, error) {
	// 1. 原始口径统计：在任何重排、删除之前取样。
	p.records += len(recs)
	for _, r := range recs {
		p.rawTypes[r.Type.String()]++
	}

	// 2. 恢复 LSN 全局序，消除"按页读取顺序之外到达"的影响。
	sorted := ordering.Sort(recs)

	// 3. 去重在事务组装之前：map 判重，跨批次仍然记得已见 Key。
	unique, _, batchConflicts := p.dedup.Feed(sorted)
	for _, r := range unique {
		p.effTypes[r.Type.String()]++
	}

	// 4. 事务组装：输入已是良构流，重复 BEGIN/COMMIT 不会再出现。
	prevCommitted := len(p.asm.Committed())
	for _, r := range unique {
		p.asm.Feed(r)
	}

	// 5. 本批次新提交的记录按 LSN 排序后落库，保证跨事务也满足物理因果序。
	newCommitted := p.asm.Committed()[prevCommitted:]
	var applyErr error
	for _, r := range ordering.Sort(newCommitted) {
		if err := p.store.Apply(r, p.track); err != nil {
			applyErr = errors.Join(applyErr, fmt.Errorf("apply lsn=%d xid=%d: %w", r.LSN, r.XID, err))
			continue
		}
		if r.Type == wal.TypeDDL {
			p.ddlApplied++
		}
	}

	if max := maxLSN(recs); max > p.lastLSN {
		p.lastLSN = max
	}

	report := p.snapshot()

	var conflictErr error
	if len(batchConflicts) > 0 {
		conflictErr = &ordering.ConflictError{Items: batchConflicts}
	}
	return report, errors.Join(conflictErr, applyErr)
}

// Checksum 返回当前落库内容的校验和。
func (p *Processor) Checksum() string { return p.store.Checksum() }

func (p *Processor) snapshot() Report {
	committed := p.asm.Committed()

	slot := retention.NewSlot("tapdance", 0)
	slot.Advance(p.lastLSN)

	tables := map[string]int{}
	for _, name := range p.track.Tables() {
		tables[name] = p.store.RowCount(name)
	}

	return Report{
		Records:            p.records,
		Duplicates:         p.dedup.Duplicates,
		DuplicateConflicts: p.dedup.Conflicts,
		EffectiveRecords:   p.dedup.Seen(),

		Committed:      len(committed),
		CommittedTxns:  p.asm.CommittedTxns(),
		Aborted:        p.asm.Aborted(),
		AbortedRecords: p.asm.AbortedRecords(),
		Orphans:        p.asm.Orphans(),
		Pending:        p.asm.Pending(),
		PendingRecords: p.asm.PendingRecords(),

		Applied:    p.store.Applied,
		DDLApplied: p.ddlApplied,

		RawTypes:       copyMap(p.rawTypes),
		EffectiveTypes: copyMap(p.effTypes),

		Tables:      tables,
		Checksum:    p.store.Checksum(),
		BacklogByte: slot.Backlog(p.lastLSN),
		BacklogSeg:  retention.Segments(slot.Backlog(p.lastLSN)),
		LagSeconds:  lag.Estimate(p.lastLSN, slot.ConfirmedLSN, nil, 1_000_000).Seconds(),
		LastLSN:     p.lastLSN,
	}
}

// Reconcile 校验报告内部的对账恒等式。任何一条不成立都返回说明全部问题的错误。
//
// 恒等式：
//  1. 原始记录 = 去重后记录 + 重复记录（含冲突）。
//  2. 去重后记录 = 已提交 + 回滚持有 + 孤儿 + 未结束持有，每条唯一记录有且仅有一个去向。
//  3. 已提交记录 = 2×提交事务数（BEGIN/COMMIT）+ 实际落库的行变更 + 实际应用的 DDL。
//  4. 原始/有效类型直方图之和分别等于 Records / EffectiveRecords，且逐类型原始数 >= 有效数。
func (r Report) Reconcile() error {
	var errs error
	check := func(cond bool, format string, args ...any) {
		if !cond {
			errs = errors.Join(errs, fmt.Errorf(format, args...))
		}
	}

	check(r.Records == r.EffectiveRecords+r.Duplicates,
		"records=%d != effective=%d + duplicates=%d",
		r.Records, r.EffectiveRecords, r.Duplicates)

	check(r.EffectiveRecords == r.Committed+r.AbortedRecords+r.Orphans+r.PendingRecords,
		"effective=%d != committed=%d + aborted_records=%d + orphans=%d + pending_records=%d",
		r.EffectiveRecords, r.Committed, r.AbortedRecords, r.Orphans, r.PendingRecords)

	check(r.Committed == 2*r.CommittedTxns+r.Applied+r.DDLApplied,
		"committed=%d != 2*committed_txns=%d + applied=%d + ddl=%d",
		r.Committed, 2*r.CommittedTxns, r.Applied, r.DDLApplied)

	check(r.DuplicateConflicts <= r.Duplicates,
		"conflicts=%d exceed duplicates=%d", r.DuplicateConflicts, r.Duplicates)

	rawSum, effSum := 0, 0
	for _, n := range r.RawTypes {
		rawSum += n
	}
	for _, n := range r.EffectiveTypes {
		effSum += n
	}
	check(rawSum == r.Records, "raw type histogram sum=%d != records=%d", rawSum, r.Records)
	check(effSum == r.EffectiveRecords, "effective type histogram sum=%d != effective=%d", effSum, r.EffectiveRecords)
	for t, rawN := range r.RawTypes {
		check(rawN >= r.EffectiveTypes[t], "type %s: raw=%d < effective=%d", t, rawN, r.EffectiveTypes[t])
	}
	return errs
}

// Run 一次性处理一个 WAL 文件里的全部记录。
//
// 多批次重放场景请直接使用 NewProcessor + Process。
func Run(recs []wal.Record) (Report, error) {
	return NewProcessor().Process(recs)
}

// SnapshotHandoff 验证"快照 + 增量"与"全量重放"是否一致。
//
// at 是快照位点：快照包含 LSN <= at 的全部已提交变更，增量在同一库上
// 补 LSN >= at 的变更（边界重叠靠落库幂等性自然吸收）。两边都经过相同的
// 排序/去重/组装流程，重复与乱序投递不会破坏一致性。
func SnapshotHandoff(recs []wal.Record, at uint64) (full, handoff string, err error) {
	proc := NewProcessor()
	_, perr := proc.Process(recs)
	if perr != nil {
		var conflictErr *ordering.ConflictError
		if !errors.As(perr, &conflictErr) {
			return "", "", perr
		}
		// 仅冲突时：保留版本已经继续完成组装，可以接着对账，最后把冲突抛回。
		err = conflictErr
	}
	committed := ordering.Sort(proc.asm.Committed())

	// 全量重放。
	fullStore := store.New()
	fullTrack := schema.New()
	for _, r := range committed {
		if err = fullStore.Apply(r, fullTrack); err != nil {
			return "", "", err
		}
	}

	// 快照 + 增量：先放 <= at，再在同一个库上补 >= at。
	snapStore := store.New()
	snapTrack := schema.New()
	for _, r := range committed {
		if r.LSN <= at {
			if err = snapStore.Apply(r, snapTrack); err != nil {
				return "", "", err
			}
		}
	}
	for _, r := range committed {
		if r.LSN >= at {
			if err = snapStore.Apply(r, snapTrack); err != nil {
				return "", "", err
			}
		}
	}
	return fullStore.Checksum(), snapStore.Checksum(), nil
}

func maxLSN(recs []wal.Record) uint64 {
	var max uint64
	for _, r := range recs {
		if r.LSN > max {
			max = r.LSN
		}
	}
	return max
}

func copyMap(in map[string]int) map[string]int {
	out := make(map[string]int, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
