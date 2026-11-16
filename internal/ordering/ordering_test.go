package ordering

import (
	"testing"

	"github.com/internal-platform/tapdance/internal/wal"
)

func rec(xid uint32, lsn uint64, t wal.Type, payload string) wal.Record {
	return wal.Record{XID: xid, LSN: lsn, Type: t, Payload: []byte(payload)}
}

func TestSortRestoresLSNOrder(t *testing.T) {
	in := []wal.Record{
		rec(1, 30, wal.TypeCommit, ""),
		rec(2, 10, wal.TypeBegin, ""),
		rec(1, 20, wal.TypeInsert, "a"),
		rec(1, 10, wal.TypeInsert, "b"),
	}
	out := Sort(in)
	want := []uint64{10, 10, 20, 30}
	for i, r := range out {
		if r.LSN != want[i] {
			t.Fatalf("pos %d: lsn=%d want %d (full: %+v)", i, r.LSN, want[i], out)
		}
	}
	// 同 LSN 时 XID 小的在前，输出确定。
	if out[0].XID != 1 || out[1].XID != 2 {
		t.Fatalf("tie-break by xid broken: %+v", out[:2])
	}
	// 不修改入参。
	if in[0].LSN != 30 {
		t.Fatal("Sort must not mutate input")
	}
}

func TestDedupeDropsByKeyNotAdjacency(t *testing.T) {
	in := []wal.Record{
		rec(7, 100, wal.TypeBegin, ""),
		rec(8, 200, wal.TypeBegin, ""),
		rec(7, 100, wal.TypeBegin, ""), // 与上一条相隔一条，仍须识别
		rec(7, 100, wal.TypeBegin, ""), // 第三次投递
	}
	out, dups := Dedupe(in)
	if dups != 2 {
		t.Fatalf("dups=%d want 2", dups)
	}
	if len(out) != 2 {
		t.Fatalf("unique records=%d want 2: %+v", len(out), out)
	}
	keys := map[Key]bool{}
	for _, r := range out {
		k := keyOf(r)
		if keys[k] {
			t.Fatalf("key %v survived twice", k)
		}
		keys[k] = true
	}
}

func TestDeduperSpansBatches(t *testing.T) {
	d := NewDeduper()

	batch1 := []wal.Record{rec(1, 10, wal.TypeBegin, ""), rec(1, 20, wal.TypeCommit, "")}
	unique1, dups1, _ := d.Feed(batch1)
	if len(unique1) != 2 || dups1 != 0 {
		t.Fatalf("batch1: unique=%d dups=%d", len(unique1), dups1)
	}

	// 第二批：整批都是上一批的重放，外加一条新记录。
	batch2 := []wal.Record{
		rec(1, 10, wal.TypeBegin, ""),
		rec(1, 20, wal.TypeCommit, ""),
		rec(2, 30, wal.TypeBegin, ""),
	}
	unique2, dups2, _ := d.Feed(batch2)
	if len(unique2) != 1 || dups2 != 2 {
		t.Fatalf("batch2: unique=%d dups=%d", len(unique2), dups2)
	}
	if d.Duplicates != 2 || d.Seen() != 3 {
		t.Fatalf("cumulative: dups=%d seen=%d", d.Duplicates, d.Seen())
	}
}

func TestDeduperConflictKeepsFirstSeenVersion(t *testing.T) {
	d := NewDeduper()

	first := []wal.Record{rec(5, 100, wal.TypeInsert, `{"v":1}`)}
	if _, _, c := d.Feed(first); len(c) != 0 {
		t.Fatalf("unexpected first-batch conflict: %+v", c)
	}

	// 同 Key 但 payload 不一致。
	conflicting := []wal.Record{rec(5, 100, wal.TypeInsert, `{"v":999}`)}
	unique, _, batchConflicts := d.Feed(conflicting)
	if len(unique) != 0 {
		t.Fatalf("conflicting record must not pass: %+v", unique)
	}
	if len(batchConflicts) != 1 || d.Conflicts != 1 {
		t.Fatalf("conflicts: batch=%d total=%d", len(batchConflicts), d.Conflicts)
	}
	if d.Duplicates != 1 {
		t.Fatalf("conflict must also count as duplicate, got %d", d.Duplicates)
	}
	got := d.ConflictsDetected()
	if string(got[0].Kept.Payload) != `{"v":1}` || string(got[0].Rejected.Payload) != `{"v":999}` {
		t.Fatalf("kept/rejected wrong: kept=%s rejected=%s", got[0].Kept.Payload, got[0].Rejected.Payload)
	}

	// 再投一次首版本：内容一致，只算普通重复，不算冲突。
	_, dups, c := d.Feed([]wal.Record{rec(5, 100, wal.TypeInsert, `{"v":1}`)})
	if dups != 1 || len(c) != 0 || d.Conflicts != 1 {
		t.Fatalf("identical redelivery: dups=%d newConflicts=%d totalConflicts=%d", dups, len(c), d.Conflicts)
	}
}

func TestDeduperTypeMismatchIsConflict(t *testing.T) {
	d := NewDeduper()
	_, _, _ = d.Feed([]wal.Record{rec(9, 10, wal.TypeBegin, "")})
	_, _, c := d.Feed([]wal.Record{rec(9, 10, wal.TypeCommit, "")})
	if len(c) != 1 {
		t.Fatalf("type mismatch must conflict, got %+v", c)
	}
	ce := &ConflictError{Items: c}
	if ce.Error() == "" || ce.Error() == "wal conflict: none" {
		t.Fatalf("ConflictError must render meaningfully, got %q", ce.Error())
	}
}

func TestStatsCountsTypes(t *testing.T) {
	in := []wal.Record{
		rec(1, 1, wal.TypeBegin, ""),
		rec(1, 2, wal.TypeInsert, "a"),
		rec(1, 3, wal.TypeInsert, "b"),
		rec(1, 4, wal.TypeCommit, ""),
	}
	got := Stats(in)
	if got["BEGIN"] != 1 || got["INSERT"] != 2 || got["COMMIT"] != 1 {
		t.Fatalf("stats=%v", got)
	}
}
