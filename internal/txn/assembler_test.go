package txn

import (
	"testing"

	"github.com/internal-platform/tapdance/internal/wal"
)

func TestAbortDropsChanges(t *testing.T) {
	a := New()
	a.Feed(wal.Record{XID: 1, Type: wal.TypeBegin})
	a.Feed(wal.Record{XID: 1, Type: wal.TypeInsert, Payload: []byte(`{"table":"t","pk":"1"}`)})
	a.Feed(wal.Record{XID: 1, Type: wal.TypeAbort})

	if len(a.Committed()) != 0 {
		t.Fatalf("aborted txn should not be committed")
	}
	if a.Aborted() != 1 {
		t.Fatalf("aborted count %d", a.Aborted())
	}
	if a.AbortedRecords() != 3 { // BEGIN + INSERT + ABORT
		t.Fatalf("aborted records %d want 3", a.AbortedRecords())
	}
}

func TestPendingTxnNotVisible(t *testing.T) {
	a := New()
	a.Feed(wal.Record{XID: 2, Type: wal.TypeBegin})
	a.Feed(wal.Record{XID: 2, Type: wal.TypeInsert, Payload: []byte(`{"table":"t","pk":"2"}`)})

	if len(a.Committed()) != 0 {
		t.Fatal("pending txn must not be visible")
	}
	if a.Pending() != 1 {
		t.Fatalf("pending %d", a.Pending())
	}
	if a.PendingRecords() != 2 {
		t.Fatalf("pending records %d want 2", a.PendingRecords())
	}
	if xids := a.PendingXIDs(); len(xids) != 1 || xids[0] != 2 {
		t.Fatalf("pending xids %v", xids)
	}
}

func TestCommitFlushesInOrder(t *testing.T) {
	a := New()
	a.Feed(wal.Record{XID: 3, Type: wal.TypeBegin})
	a.Feed(wal.Record{XID: 3, Type: wal.TypeInsert})
	a.Feed(wal.Record{XID: 3, Type: wal.TypeCommit})

	got := a.Committed()
	if len(got) != 3 {
		t.Fatalf("want 3 records, got %d", len(got))
	}
	if got[0].Type != wal.TypeBegin || got[2].Type != wal.TypeCommit {
		t.Fatalf("order broken: %v %v", got[0].Type, got[2].Type)
	}
	if a.CommittedTxns() != 1 {
		t.Fatalf("committed txns %d", a.CommittedTxns())
	}
}

func TestOrphanDropped(t *testing.T) {
	a := New()
	a.Feed(wal.Record{XID: 9, Type: wal.TypeInsert})
	if a.Orphans() != 1 {
		t.Fatalf("orphan count %d", a.Orphans())
	}
}

func TestOrphanCommitAndAbortCounted(t *testing.T) {
	a := New()
	// 无主 COMMIT / ABORT：不能凭空开关事务，也不能静默吞掉。
	a.Feed(wal.Record{XID: 10, Type: wal.TypeCommit})
	a.Feed(wal.Record{XID: 11, Type: wal.TypeAbort})
	if a.Orphans() != 2 {
		t.Fatalf("orphans=%d want 2", a.Orphans())
	}
	if a.CommittedTxns() != 0 || a.Aborted() != 0 {
		t.Fatal("ownerless terminal records must not open/close transactions")
	}
	if a.Pending() != 0 {
		t.Fatal("no txn may remain open")
	}
}

func TestDuplicateBeginDoesNotWipeTxn(t *testing.T) {
	a := New()
	a.Feed(wal.Record{XID: 4, LSN: 10, Type: wal.TypeBegin})
	a.Feed(wal.Record{XID: 4, LSN: 11, Type: wal.TypeInsert, Payload: []byte("first")})
	// 语义异常的第二条 BEGIN（LSN 不同，所以不会被上游去重器消掉）。
	a.Feed(wal.Record{XID: 4, LSN: 12, Type: wal.TypeBegin})
	a.Feed(wal.Record{XID: 4, LSN: 13, Type: wal.TypeInsert, Payload: []byte("second")})
	a.Feed(wal.Record{XID: 4, LSN: 14, Type: wal.TypeCommit})

	if a.Orphans() != 1 {
		t.Fatalf("duplicate BEGIN should be one orphan, got %d", a.Orphans())
	}
	got := a.Committed()
	// BEGIN + 两条 INSERT + COMMIT：首条 BEGIN 之前收集的记录不能被清掉。
	if len(got) != 4 {
		t.Fatalf("committed=%d want 4: %+v", len(got), got)
	}
	if got[1].Type != wal.TypeInsert || string(got[1].Payload) != "first" {
		t.Fatalf("first insert lost: %+v", got)
	}
	if a.CommittedTxns() != 1 {
		t.Fatalf("one txn should commit, got %d", a.CommittedTxns())
	}
}

func TestPendingTxnSurvivesAcrossBatches(t *testing.T) {
	a := New()

	// 第一批：事务只开了头。
	a.Feed(wal.Record{XID: 7, LSN: 1, Type: wal.TypeBegin})
	a.Feed(wal.Record{XID: 7, LSN: 2, Type: wal.TypeInsert})
	if a.Pending() != 1 || len(a.Committed()) != 0 {
		t.Fatal("txn should stay open across batch boundary")
	}

	// 第二批：COMMIT 到达后才可见。
	a.Feed(wal.Record{XID: 7, LSN: 3, Type: wal.TypeCommit})
	if a.Pending() != 0 {
		t.Fatalf("pending=%d want 0", a.Pending())
	}
	if a.CommittedTxns() != 1 || len(a.Committed()) != 3 {
		t.Fatalf("txn=%d committed records=%d", a.CommittedTxns(), len(a.Committed()))
	}
}

func TestRepeatedCommitAfterCloseIsOrphan(t *testing.T) {
	a := New()
	a.Feed(wal.Record{XID: 5, LSN: 1, Type: wal.TypeBegin})
	a.Feed(wal.Record{XID: 5, LSN: 2, Type: wal.TypeCommit})
	// 事务已关闭，再来一条 COMMIT（不同 LSN 的重放）只能是孤儿。
	a.Feed(wal.Record{XID: 5, LSN: 3, Type: wal.TypeCommit})

	if a.CommittedTxns() != 1 {
		t.Fatalf("txns=%d want 1", a.CommittedTxns())
	}
	if len(a.Committed()) != 2 {
		t.Fatalf("committed records=%d want 2", len(a.Committed()))
	}
	if a.Orphans() != 1 {
		t.Fatalf("orphans=%d want 1", a.Orphans())
	}
}
