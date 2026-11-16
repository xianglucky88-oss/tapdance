// Package txn 负责把记录组装成事务，并过滤不可见的变更。
//
// Assembler 的输入约定：调用方已经完成 LSN 排序与 (XID, LSN) 去重
// （见 internal/ordering）。因此同 Key 重复记录不会到达这里，
// 汇编器只需处理事务语义层面的异常：重复 BEGIN、无主 COMMIT/ABORT、
// 无主变更记录，以及跨批次仍未结束的事务。
package txn

import "github.com/internal-platform/tapdance/internal/wal"

// txnState 是一个尚未结束的事务。
type txnState struct {
	begin   wal.Record
	records []wal.Record // BEGIN 之后、COMMIT/ABORT 之前的全部记录
}

// Assembler 收集未提交事务的记录，提交时才对外可见。
//
// 同一个 Assembler 可以连续消费多个批次：未结束的事务保留在 open 里，
// 等后续批次的 COMMIT/ABORT 到达后再决定可见性。
type Assembler struct {
	open map[uint32]*txnState

	committed []wal.Record

	committedTxns  int
	abortedTxns    int
	abortedRecords int // 属于回滚事务、因此不可见的记录数（含 BEGIN/ABORT）
	orphans        int
}

// New 构造组装器。
func New() *Assembler {
	return &Assembler{open: map[uint32]*txnState{}}
}

// Feed 送入一条已排序、已去重的记录。
func (a *Assembler) Feed(r wal.Record) {
	switch r.Type {
	case wal.TypeBegin:
		if _, exists := a.open[r.XID]; exists {
			// 同一 XID 在事务未结束前又出现 BEGIN：属于语义异常。
			// 保留原事务，绝不覆盖已收集的记录，第二条 BEGIN 计孤儿丢弃。
			a.orphans++
			return
		}
		a.open[r.XID] = &txnState{begin: r}

	case wal.TypeCommit:
		st, ok := a.open[r.XID]
		if !ok {
			// 没有活动事务的 COMMIT：可能是乱序/残缺投递，不能凭空造事务。
			a.orphans++
			return
		}
		a.committed = append(a.committed, st.begin)
		a.committed = append(a.committed, st.records...)
		a.committed = append(a.committed, r)
		delete(a.open, r.XID)
		a.committedTxns++

	case wal.TypeAbort:
		if _, ok := a.open[r.XID]; !ok {
			// 无主 ABORT 与无主 COMMIT 同样按孤儿处理。
			a.orphans++
			return
		}
		// 回滚事务持有的全部记录都不可见，计入对账的 aborted 去向。
		a.abortedRecords += 1 + len(a.open[r.XID].records) + 1
		delete(a.open, r.XID)
		a.abortedTxns++

	default:
		// INSERT/UPDATE/DELETE/DDL
		st, ok := a.open[r.XID]
		if !ok {
			// 未开启事务的变更：没有可见性归属，直接丢弃。
			a.orphans++
			return
		}
		st.records = append(st.records, r)
	}
}

// Committed 返回已提交事务的全部记录（含 BEGIN/COMMIT），按提交到达顺序累积。
func (a *Assembler) Committed() []wal.Record { return a.committed }

// Pending 返回退出时仍未提交的事务数。
func (a *Assembler) Pending() int { return len(a.open) }

// PendingRecords 返回未结束事务当前持有的记录数（含各自的 BEGIN）。
func (a *Assembler) PendingRecords() int {
	n := 0
	for _, st := range a.open {
		n += 1 + len(st.records)
	}
	return n
}

// PendingXIDs 返回仍未结束事务的 XID，按升序，便于对账与测试。
func (a *Assembler) PendingXIDs() []uint32 {
	out := make([]uint32, 0, len(a.open))
	for xid := range a.open {
		out = append(out, xid)
	}
	// 简单插入排序：open 的规模是活动事务数，通常很小。
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1] > out[j]; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}

// CommittedTxns 返回已提交事务数。
func (a *Assembler) CommittedTxns() int { return a.committedTxns }

// Aborted 返回已回滚事务数（不含无主 ABORT，后者计入孤儿）。
func (a *Assembler) Aborted() int { return a.abortedTxns }

// AbortedRecords 返回回滚事务持有的记录数（含各自的 BEGIN/ABORT）。
func (a *Assembler) AbortedRecords() int { return a.abortedRecords }

// Orphans 返回被丢弃的孤儿记录数：
// 重复 BEGIN、无主 COMMIT/ABORT、无活动事务的变更记录。
func (a *Assembler) Orphans() int { return a.orphans }
