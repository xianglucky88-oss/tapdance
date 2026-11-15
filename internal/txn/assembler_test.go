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
}

func TestOrphanDropped(t *testing.T) {
	a := New()
	a.Feed(wal.Record{XID: 9, Type: wal.TypeInsert})
	if a.Orphans() != 1 {
		t.Fatalf("orphan count %d", a.Orphans())
	}
}
