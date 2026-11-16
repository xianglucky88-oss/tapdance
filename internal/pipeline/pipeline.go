// Package pipeline 串起 WAL → 事务 → 排序 → 落库 的整条链路。
package pipeline

import (
	"sort"

	"github.com/internal-platform/tapdance/internal/lag"
	"github.com/internal-platform/tapdance/internal/ordering"
	"github.com/internal-platform/tapdance/internal/retention"
	"github.com/internal-platform/tapdance/internal/schema"
	"github.com/internal-platform/tapdance/internal/store"
	"github.com/internal-platform/tapdance/internal/txn"
	"github.com/internal-platform/tapdance/internal/wal"
)

// Options 是 RunWith 的复制视图输入。
type Options struct {
	// Slots 是复制槽，各自携带消费者的确认位点与连接状态。
	// 管道只读取它们，绝不代为推进——"处理到哪里"不等于"确认到哪里"。
	Slots []*retention.Slot
	// Checkpoints 是 WAL 写入进度采样，用于估算消费速率。
	Checkpoints []lag.Checkpoint
	// FallbackBPS 是采样不足时的兜底速率（字节/秒）；<= 0 表示没有兜底。
	FallbackBPS float64
}

// SlotReport 是单个槽位的复制视图。
type SlotReport struct {
	Name         string  `json:"name"`
	State        string  `json:"state"` // active | disconnected | reconnected
	Lagging      bool    `json:"lagging"`
	ConfirmedLSN uint64  `json:"confirmed_lsn"`
	RestartLSN   uint64  `json:"restart_lsn"`
	BacklogBytes uint64  `json:"backlog_bytes"`
	BacklogSegs  int     `json:"backlog_segments"`
	LagSeconds   float64 `json:"lag_seconds"` // -1 表示无法估算
	LagBasis     string  `json:"lag_basis"`   // caught_up | checkpoints | fallback | unknown
	LagRateBPS   float64 `json:"lag_rate_bps"`
	Reconnects   int     `json:"reconnects"`
	Regressions  int     `json:"regressions"`
}

// Report 是一次处理的统计。
type Report struct {
	Records    int            `json:"records"`
	Committed  int            `json:"committed"`
	Aborted    int            `json:"aborted"`
	Orphans    int            `json:"orphans"`
	Pending    int            `json:"pending"`
	Duplicates int            `json:"duplicates"`
	Applied    int            `json:"applied"`
	Tables     map[string]int `json:"tables"`
	Checksum   string         `json:"checksum"`

	// LastLSN 是本批记录的最高 LSN，即"处理到达点"。
	// 它不是任何消费者的确认位点。
	LastLSN uint64 `json:"last_lsn"`
	// Slots 是各槽位的复制视图，按名字排序。
	Slots []SlotReport `json:"slots"`
	// RetainFrom 是所有槽位共同钉住的 WAL 保留边界；没有槽位时为 null。
	RetainFrom *uint64 `json:"retain_from_lsn"`
	// 顶层积压/延迟取最落后的槽位（保留边界由它决定）。
	BacklogByte uint64  `json:"backlog_bytes"`
	BacklogSeg  int     `json:"backlog_segments"`
	LagSeconds  float64 `json:"lag_seconds"` // -1 表示无法估算
	LagBasis    string  `json:"lag_basis"`   // no_slots | caught_up | checkpoints | fallback | unknown
}

// Run 处理一个 WAL 文件里的全部记录。没有任何消费者信息时，
// 复制视图如实报告"没有槽位"，而不是虚构一个已追平的消费者。
func Run(recs []wal.Record) Report { return RunWith(recs, Options{}) }

// RunWith 处理记录，并按给定的槽位与速率采样生成复制视图。
func RunWith(recs []wal.Record, opts Options) Report {
	asm := txn.New()
	for _, r := range recs {
		asm.Feed(r)
	}

	committed := ordering.Sort(asm.Committed())
	committed, dups := ordering.Dedupe(committed)

	st := store.New()
	tr := schema.New()
	for _, r := range committed {
		_ = st.Apply(r, tr)
	}

	// 处理到达点取最高 LSN：记录可能乱序，最后一条不代表最远。
	current := wal.MaxLSN(recs)

	tables := map[string]int{}
	for _, name := range tr.Tables() {
		tables[name] = st.RowCount(name)
	}

	rep := Report{
		Records:    len(recs),
		Committed:  len(asm.Committed()),
		Aborted:    asm.Aborted(),
		Orphans:    asm.Orphans(),
		Pending:    asm.Pending(),
		Duplicates: dups,
		Applied:    st.Applied,
		Tables:     tables,
		Checksum:   st.Checksum(),
		LastLSN:    current,
		Slots:      buildSlotReports(opts.Slots, current, opts),
	}

	sum := retention.Summarize(opts.Slots, current)
	if sum.Slots == 0 {
		// 没有槽位 = 没有保留需求。如实报告，而不是假设消费已追平。
		rep.LagBasis = "no_slots"
		return rep
	}
	retainFrom := sum.RetainFrom
	rep.RetainFrom = &retainFrom
	rep.BacklogByte = sum.Backlog
	rep.BacklogSeg = retention.Segments(sum.Backlog)
	est := lag.Estimate(current, sum.MinConfirmed, opts.Checkpoints, opts.FallbackBPS)
	rep.LagSeconds = est.Seconds()
	rep.LagBasis = string(est.Basis)
	return rep
}

func buildSlotReports(slots []*retention.Slot, current uint64, opts Options) []SlotReport {
	out := make([]SlotReport, 0, len(slots))
	for _, s := range slots {
		if s == nil {
			continue
		}
		backlog := s.Backlog(current)
		est := lag.Estimate(current, s.ConfirmedLSN, opts.Checkpoints, opts.FallbackBPS)
		out = append(out, SlotReport{
			Name:         s.Name,
			State:        string(s.State()),
			Lagging:      backlog > 0,
			ConfirmedLSN: s.ConfirmedLSN,
			RestartLSN:   s.RestartLSN,
			BacklogBytes: backlog,
			BacklogSegs:  retention.Segments(backlog),
			LagSeconds:   est.Seconds(),
			LagBasis:     string(est.Basis),
			LagRateBPS:   est.Rate,
			Reconnects:   s.Reconnects,
			Regressions:  s.Regressions,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
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
