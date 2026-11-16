package pipeline

import (
	"errors"
	"math/rand"
	"testing"

	"github.com/internal-platform/tapdance/internal/ordering"
	"github.com/internal-platform/tapdance/internal/wal"
)

func change(table, pk string) []byte {
	return []byte(`{"table":"` + table + `","pk":"` + pk + `","row":{"id":"` + pk + `"}}`)
}

// buildStream 构造一份含建表 DDL、提交、回滚、未完成事务的基础记录流，LSN 唯一递增。
//
// 共 15 条唯一记录：
//   - XID 10 建表事务（90-92，3 条，含 1 条 DDL）
//   - XID 1 提交事务（100-103，4 条，2 条行变更）
//   - XID 2 回滚事务（110-112，3 条）
//   - XID 3 提交事务（120-122，3 条，1 条行变更）
//   - XID 4 未完成事务（130-131，2 条）
func buildStream() []wal.Record {
	return []wal.Record{
		{XID: 10, LSN: 90, Type: wal.TypeBegin},
		{XID: 10, LSN: 91, Type: wal.TypeDDL, Payload: []byte(`{"table":"t","op":"create"}`)},
		{XID: 10, LSN: 92, Type: wal.TypeCommit},

		{XID: 1, LSN: 100, Type: wal.TypeBegin},
		{XID: 1, LSN: 101, Type: wal.TypeInsert, Payload: change("t", "a")},
		{XID: 1, LSN: 102, Type: wal.TypeInsert, Payload: change("t", "b")},
		{XID: 1, LSN: 103, Type: wal.TypeCommit},

		{XID: 2, LSN: 110, Type: wal.TypeBegin},
		{XID: 2, LSN: 111, Type: wal.TypeInsert, Payload: change("t", "rollback-row")},
		{XID: 2, LSN: 112, Type: wal.TypeAbort},

		{XID: 3, LSN: 120, Type: wal.TypeBegin},
		{XID: 3, LSN: 121, Type: wal.TypeUpdate, Payload: change("t", "a")},
		{XID: 3, LSN: 122, Type: wal.TypeCommit},

		{XID: 4, LSN: 130, Type: wal.TypeBegin},
		{XID: 4, LSN: 131, Type: wal.TypeInsert, Payload: change("t", "pending-row")},
		// XID 4 故意不结束：跨批次保留。
	}
}

func shuffledCopy(in []wal.Record, seed int64) []wal.Record {
	out := append([]wal.Record(nil), in...)
	rnd := rand.New(rand.NewSource(seed))
	rnd.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

// 唯一记录基线：13 条 = 2 个提交事务（8 条）+ 1 个回滚（3 条）+ 1 个未完成（2 条）。

func TestShuffledReplayAppliesEachRecordOnce(t *testing.T) {
	base := buildStream()

	// 输入 = 乱序的原始流 + 乱序的整批重放 + 单条重复投递。
	feed := append(shuffledCopy(base, 42), shuffledCopy(base, 99)...)
	feed = append(feed, base[4]) // XID 1 的第一条 INSERT 再来一次

	proc := NewProcessor()
	report, err := proc.Process(feed)
	if err != nil {
		t.Fatalf("clean replay must not error: %v", err)
	}
	if err := report.Reconcile(); err != nil {
		t.Fatalf("reconcile: %v\nreport=%+v", err, report)
	}

	// 原始 31 = 唯一 15 + 重复 16（整批 15 + 单条 1）。
	if report.Records != 31 || report.EffectiveRecords != 15 || report.Duplicates != 16 {
		t.Fatalf("dedupe counts: records=%d effective=%d dups=%d",
			report.Records, report.EffectiveRecords, report.Duplicates)
	}
	// 3 个提交事务：建表 3 条 + XID1 4 条 + XID3 3 条 = 10 条。
	if report.CommittedTxns != 3 || report.Committed != 10 {
		t.Fatalf("committed: txns=%d records=%d", report.CommittedTxns, report.Committed)
	}
	if report.Aborted != 1 || report.AbortedRecords != 3 {
		t.Fatalf("aborted: txns=%d records=%d", report.Aborted, report.AbortedRecords)
	}
	if report.Pending != 1 || report.PendingRecords != 2 {
		t.Fatalf("pending: txns=%d records=%d", report.Pending, report.PendingRecords)
	}
	// 行变更：INSERT a, INSERT b, UPDATE a = 3 次；回滚/未完成的行不算，DDL 单独计。
	if report.Applied != 3 || report.DDLApplied != 1 {
		t.Fatalf("applied=%d ddl=%d", report.Applied, report.DDLApplied)
	}
	if report.Tables["t"] != 2 { // 只剩 a、b 两行
		t.Fatalf("table t rows=%d want 2: %+v", report.Tables["t"], report.Tables)
	}
}

func TestFullReplayIsIdempotent(t *testing.T) {
	base := buildStream()

	first, err := NewProcessor().Process(base)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Reconcile(); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}

	// 另一个 Processor 一次吃下"原始流 + 完整重放"：结果必须与只跑一次完全一致。
	second, err := NewProcessor().Process(append(base, base...))
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Reconcile(); err != nil {
		t.Fatalf("replay reconcile: %v", err)
	}
	if second.Checksum != first.Checksum {
		t.Fatalf("checksum drift after replay: first=%s second=%s", first.Checksum, second.Checksum)
	}
	if second.Tables["t"] != first.Tables["t"] || second.Applied != first.Applied {
		t.Fatalf("side effects doubled: first applied=%d second applied=%d", first.Applied, second.Applied)
	}
	if second.Duplicates != len(base) {
		t.Fatalf("replayed batch must be all duplicates: dups=%d", second.Duplicates)
	}
}

func TestPayloadConflictReportedButReconciles(t *testing.T) {
	ddl := []wal.Record{
		{XID: 9, LSN: 90, Type: wal.TypeBegin},
		{XID: 9, LSN: 91, Type: wal.TypeDDL, Payload: []byte(`{"table":"t","op":"create"}`)},
		{XID: 9, LSN: 92, Type: wal.TypeCommit},
	}
	good := []wal.Record{
		{XID: 1, LSN: 100, Type: wal.TypeBegin},
		{XID: 1, LSN: 101, Type: wal.TypeInsert, Payload: change("t", "k")},
		{XID: 1, LSN: 102, Type: wal.TypeCommit},
	}
	bad := wal.Record{XID: 1, LSN: 101, Type: wal.TypeInsert, Payload: []byte(`{"table":"t","pk":"k","row":{"id":"TAMPERED"}}`)}

	proc := NewProcessor()
	report, err := proc.Process(append(append(ddl, good...), bad))
	var conflictErr *ordering.ConflictError
	if !errors.As(err, &conflictErr) {
		t.Fatalf("want ConflictError, got %v", err)
	}
	if len(conflictErr.Items) != 1 {
		t.Fatalf("conflict items=%d", len(conflictErr.Items))
	}
	if err := report.Reconcile(); err != nil {
		t.Fatalf("conflict must still reconcile: %v", err)
	}
	// 6 条唯一 + 1 条冲突重复。
	if report.DuplicateConflicts != 1 || report.Duplicates != 1 || report.EffectiveRecords != 6 {
		t.Fatalf("conflict accounting: dups=%d conflicts=%d effective=%d",
			report.Duplicates, report.DuplicateConflicts, report.EffectiveRecords)
	}
	if report.Applied != 1 || report.Tables["t"] != 1 {
		t.Fatalf("kept version should apply once: applied=%d rows=%d", report.Applied, report.Tables["t"])
	}
}

func TestPendingTxnCarriesAcrossBatches(t *testing.T) {
	base := buildStream()
	proc := NewProcessor()

	// 第一批只给到 XID 4 的 INSERT（索引 0..14，共 15 条；XID 4 的 COMMIT 还没到）。
	r1, err := proc.Process(base[:15])
	if err != nil {
		t.Fatal(err)
	}
	if r1.Pending != 1 || r1.PendingRecords != 2 {
		t.Fatalf("mid-stream state: pending=%d/%d", r1.Pending, r1.PendingRecords)
	}
	// 已提交的部分先落库：3 条行变更 + 1 条 DDL。
	if r1.Applied != 3 || r1.DDLApplied != 1 {
		t.Fatalf("applied before close=%d ddl=%d", r1.Applied, r1.DDLApplied)
	}

	// 第二批补上 COMMIT，事务在同一 Processor 内关闭并可见。
	r2, err := proc.Process([]wal.Record{
		{XID: 4, LSN: 132, Type: wal.TypeCommit},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r2.Reconcile(); err != nil {
		t.Fatalf("final reconcile: %v", err)
	}
	// 此前 3 个提交事务共 10 条，XID 4 再补 3 条。
	if r2.Pending != 0 || r2.CommittedTxns != 4 || r2.Committed != 10+3 {
		t.Fatalf("after close: pending=%d txns=%d committed=%d", r2.Pending, r2.CommittedTxns, r2.Committed)
	}
	if r2.Applied != 4 { // 多了 pending-row 这条 INSERT
		t.Fatalf("applied=%d want 4", r2.Applied)
	}
	if r2.Tables["t"] != 3 {
		t.Fatalf("rows=%d want 3", r2.Tables["t"])
	}
}

func TestReplayOldBatchDoesNotReopenAnything(t *testing.T) {
	base := buildStream()
	proc := NewProcessor()

	if _, err := proc.Process(base); err != nil {
		t.Fatal(err)
	}
	// 重放 XID 1 事务的旧片段，且乱序（索引 3..6：COMMIT/BEGIN/INSERT/INSERT）。
	old := shuffledCopy(base[3:7], 7)
	r, err := proc.Process(old)
	if err != nil {
		t.Fatalf("replay must stay clean: %v", err)
	}
	if err := r.Reconcile(); err != nil {
		t.Fatalf("replay reconcile: %v", err)
	}
	if r.Duplicates != 4 {
		t.Fatalf("old batch duplicates=%d want 4", r.Duplicates)
	}
	if r.Orphans != 0 {
		t.Fatalf("replayed control records must be deduped, not become orphans: %d", r.Orphans)
	}
	if r.Pending != 1 { // XID 4 仍然挂着，不受重放影响
		t.Fatalf("pending=%d want 1", r.Pending)
	}
}

func TestRawAndEffectiveHistogramsExplainDuplicates(t *testing.T) {
	base := buildStream()
	feed := append(shuffledCopy(base, 1), shuffledCopy(base[:5], 2)...) // 前 5 条多投一次

	r, err := NewProcessor().Process(feed)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Reconcile(); err != nil {
		t.Fatal(err)
	}
	explained := 0
	for typ, rawN := range r.RawTypes {
		effN := r.EffectiveTypes[typ]
		if rawN < effN {
			t.Fatalf("type %s raw=%d < effective=%d", typ, rawN, effN)
		}
		explained += rawN - effN
	}
	if explained != r.Duplicates {
		t.Fatalf("histogram delta=%d != duplicates=%d", explained, r.Duplicates)
	}
}

func TestSnapshotHandoffSurvivesReorderAndReplay(t *testing.T) {
	base := buildStream()
	feed := append(shuffledCopy(base, 11), base...)

	full, handoff, err := SnapshotHandoff(feed, 115)
	if err != nil {
		t.Fatal(err)
	}
	if full != handoff {
		t.Fatalf("handoff checksum mismatch: full=%s handoff=%s", full, handoff)
	}
	if full == "" || full == "0000000000000000" {
		t.Fatalf("suspicious checksum %q", full)
	}
}

func TestOneShotRunReconciles(t *testing.T) {
	r, err := Run(buildStream())
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Reconcile(); err != nil {
		t.Fatalf("Run report must reconcile: %v\n%+v", err, r)
	}
	if r.Orphans != 0 {
		t.Fatalf("well-formed stream has no orphans: %d", r.Orphans)
	}
	if r.EffectiveRecords != 15 || r.CommittedTxns != 3 {
		t.Fatalf("baseline counts wrong: effective=%d txns=%d", r.EffectiveRecords, r.CommittedTxns)
	}
}
