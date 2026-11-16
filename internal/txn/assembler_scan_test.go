package txn

import (
	"bytes"
	"testing"

	"github.com/internal-platform/tapdance/internal/wal"
)

// buildWal 用真实写入器生成 WAL，便于构造跨页/损坏场景。
func buildWal(t *testing.T, recs ...wal.Record) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := wal.NewBufferWriter(&buf, wal.WriterOptions{})
	if err := w.Append(recs...); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func scanBytes(t *testing.T, raw []byte) *wal.ScanResult {
	t.Helper()
	res, err := wal.Scan(bytes.NewReader(raw), wal.ScanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// TestCorruptPageDoesNotCommitPartialTxn 确认坏页覆盖事务中段时,该事务不会
// 被当成提交成功,也不会把幸存记录当成孤儿。
func TestCorruptPageDoesNotCommitPartialTxn(t *testing.T) {
	big := bytes.Repeat([]byte("p"), wal.PageSize*2)
	recs := []wal.Record{
		{XID: 10, Type: wal.TypeBegin},
		{XID: 10, Type: wal.TypeInsert, Payload: big}, // 跨页，使事务横跨损坏页
		{XID: 10, Type: wal.TypeInsert, Payload: []byte(`{"table":"t","pk":"2"}`)},
		{XID: 10, Type: wal.TypeCommit},
		{XID: 11, Type: wal.TypeBegin},
		{XID: 11, Type: wal.TypeInsert, Payload: []byte(`{"table":"t","pk":"3"}`)},
		{XID: 11, Type: wal.TypeCommit},
	}
	raw := buildWal(t, recs...)

	// 破坏跨页记录中间的页。
	page := 1
	raw[page*wal.PageSize+wal.HeaderSize+2] ^= 0xFF
	res := scanBytes(t, raw)

	asm := New()
	stats := asm.FeedScan(res)

	for _, r := range asm.Committed() {
		if r.XID == 10 {
			t.Fatalf("transaction 10 must not be committed after damage, got %v", r.Type)
		}
	}
	if stats.Incomplete == 0 {
		t.Fatalf("expected an incomplete transaction, stats=%+v", stats)
	}
	if asm.Orphans() != 0 {
		t.Fatalf("records of a damaged txn must not be reported as orphans, got %d", asm.Orphans())
	}
	// 未受影响的事务仍然提交。
	var saw11 bool
	for _, r := range asm.Committed() {
		if r.XID == 11 && r.Type == wal.TypeCommit {
			saw11 = true
		}
	}
	if !saw11 {
		t.Fatal("unaffected transaction 11 should still commit")
	}
}

// TestIncompleteTailTxnNotCommitted 确认尾部截断导致的半条事务不会被提交。
func TestIncompleteTailTxnNotCommitted(t *testing.T) {
	big := bytes.Repeat([]byte("q"), wal.PageSize*2)
	recs := []wal.Record{
		{XID: 1, Type: wal.TypeBegin},
		{XID: 1, Type: wal.TypeInsert, Payload: []byte(`{"table":"t","pk":"1"}`)},
		{XID: 1, Type: wal.TypeCommit},
		{XID: 2, Type: wal.TypeBegin},
		{XID: 2, Type: wal.TypeInsert, Payload: big}, // 写到一半被截断
		{XID: 2, Type: wal.TypeCommit},
	}
	raw := buildWal(t, recs...)
	torn := raw[:len(raw)-wal.PageSize-wal.PageSize/2]
	res := scanBytes(t, torn)

	asm := New()
	stats := asm.FeedScan(res)
	if !stats.TailTruncated {
		t.Fatal("tail should be reported as truncated")
	}
	for _, r := range asm.Committed() {
		if r.XID == 2 {
			t.Fatalf("truncated transaction 2 must not commit, got %v", r.Type)
		}
	}
	if asm.Pending() != 0 {
		t.Fatalf("truncated txn 2 must be reclassified as incomplete, pending=%d", asm.Pending())
	}
	if stats.Incomplete != 1 {
		t.Fatalf("expected txn 2 reported as incomplete, stats=%+v", stats)
	}
	if asm.Orphans() != 0 {
		t.Fatalf("orphans %d", asm.Orphans())
	}
}

// TestCompleteRecordsStillFlow 确认健康文件的行为与旧实现一致。
func TestCompleteRecordsStillFlow(t *testing.T) {
	recs := []wal.Record{
		{XID: 1, Type: wal.TypeBegin},
		{XID: 1, Type: wal.TypeInsert, Payload: []byte(`{"table":"t","pk":"1"}`)},
		{XID: 1, Type: wal.TypeCommit},
	}
	res := scanBytes(t, buildWal(t, recs...))
	asm := New()
	stats := asm.FeedScan(res)
	if stats.Incomplete != 0 || stats.Orphans != 0 {
		t.Fatalf("stats %+v", stats)
	}
	if len(asm.Committed()) != 3 {
		t.Fatalf("committed %d", len(asm.Committed()))
	}
}

// TestOrphanIsStillCounted 确认完整但无 BEGIN 的记录仍算孤儿（区别于损坏）。
func TestOrphanIsStillCounted(t *testing.T) {
	res := scanBytes(t, buildWal(t, wal.Record{XID: 9, Type: wal.TypeInsert, Payload: []byte(`{"table":"t","pk":"9"}`)}))
	asm := New()
	asm.FeedScan(res)
	if asm.Orphans() != 1 {
		t.Fatalf("orphans %d", asm.Orphans())
	}
	if asm.Incomplete() != 0 {
		t.Fatalf("complete orphan must not be counted as incomplete: %d", asm.Incomplete())
	}
}

// TestMissingBeginAfterDamageIsUnattributed 确认坏页吞掉 BEGIN 时，后续的
// 完整记录不会被当成“重复投递的孤儿”，而是单独归类为缺头记录。
func TestMissingBeginAfterDamageIsUnattributed(t *testing.T) {
	// 第 0 页（将被破坏）含 XID 2 的 BEGIN。第 1 页完好，含 XID 2 的变更与
	// COMMIT：它们看起来就像“没有 BEGIN 的孤儿”，但前面确实有损坏。
	page0, rest := wal.EncodePage(0, []wal.Record{
		{XID: 1, Type: wal.TypeBegin},
		{XID: 1, Type: wal.TypeCommit},
		{XID: 2, Type: wal.TypeBegin},
		{XID: 2, Type: wal.TypeInsert, Payload: []byte(`{"table":"t","pk":"2"}`)},
	})
	if len(rest) != 0 {
		t.Fatal("unexpected overflow")
	}
	page1, rest := wal.EncodePage(1, []wal.Record{
		{XID: 2, Type: wal.TypeInsert, Payload: []byte(`{"table":"t","pk":"3"}`)},
		{XID: 2, Type: wal.TypeCommit},
	})
	if len(rest) != 0 {
		t.Fatal("unexpected overflow")
	}
	raw := append(page0, page1...)
	raw[wal.HeaderSize+2] ^= 0xFF // 破坏第 0 页

	res := scanBytes(t, raw)
	asm := New()
	stats := asm.FeedScan(res)
	if stats.Orphans != 0 {
		t.Fatalf("missing-BEGIN records must not count as orphans, got %d", stats.Orphans)
	}
	if stats.Unattributed != 2 {
		t.Fatalf("expected change+commit to be unattributed, stats=%+v", stats)
	}
	for _, r := range asm.Committed() {
		if r.XID == 2 {
			t.Fatal("transaction with a damaged BEGIN must not commit")
		}
	}
}
