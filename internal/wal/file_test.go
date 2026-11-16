package wal

import "testing"

func TestLastLSNIsMaxNotTail(t *testing.T) {
	// 乱序/重放拼接后，最后一条不一定是位点最大的记录。
	recs := []Record{
		{LSN: 100, XID: 1, Type: TypeBegin},
		{LSN: 300, XID: 1, Type: TypeCommit},
		{LSN: 200, XID: 1, Type: TypeInsert}, // 尾条 LSN 反而更小
	}
	if got := LastLSN(recs); got != 300 {
		t.Fatalf("LastLSN=%d want 300 (max, not tail)", got)
	}
	if got := LastLSN(nil); got != 0 {
		t.Fatalf("LastLSN(empty)=%d want 0", got)
	}
}
