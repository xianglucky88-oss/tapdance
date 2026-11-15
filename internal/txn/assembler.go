// Package txn 负责把记录组装成事务，并过滤不可见的变更。
package txn

import "github.com/internal-platform/tapdance/internal/wal"

// Assembler 收集未提交事务的记录，提交时才对外可见。
type Assembler struct {
	open      map[uint32][]wal.Record
	committed []wal.Record
	aborted   int
	orphans   int
}

// New 构造组装器。
func New() *Assembler {
	return &Assembler{open: map[uint32][]wal.Record{}}
}

// Feed 送入一条记录。
func (a *Assembler) Feed(r wal.Record) {
	switch r.Type {
	case wal.TypeBegin:
		a.open[r.XID] = []wal.Record{r}
	case wal.TypeCommit:
		recs, ok := a.open[r.XID]
		if !ok {
			// 没有 BEGIN 的 COMMIT：说明是重复投递的尾巴，忽略。
			a.orphans++
			return
		}
		a.committed = append(a.committed, recs...)
		a.committed = append(a.committed, r)
		delete(a.open, r.XID)
	case wal.TypeAbort:
		if _, ok := a.open[r.XID]; ok {
			delete(a.open, r.XID)
			a.aborted++
		}
	default:
		if recs, ok := a.open[r.XID]; ok {
			a.open[r.XID] = append(recs, r)
		} else {
			// 未开启事务的变更：直接丢弃。
			a.orphans++
		}
	}
}

// Committed 返回已提交事务的全部记录（含 BEGIN/COMMIT）。
func (a *Assembler) Committed() []wal.Record { return a.committed }

// Pending 返回退出时仍未提交的事务数。
func (a *Assembler) Pending() int { return len(a.open) }

// Aborted 返回已回滚事务数。
func (a *Assembler) Aborted() int { return a.aborted }

// Orphans 返回被丢弃的孤儿记录数。
func (a *Assembler) Orphans() int { return a.orphans }
