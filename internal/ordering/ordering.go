// Package ordering 负责 LSN 排序与重复记录处理。
package ordering

import (
	"sort"

	"github.com/internal-platform/tapdance/internal/wal"
)

// Sort 按 LSN 升序排序。
func Sort(recs []wal.Record) []wal.Record {
	out := make([]wal.Record, len(recs))
	copy(out, recs)
	sort.SliceStable(out, func(i, j int) bool { return out[i].LSN < out[j].LSN })
	return out
}

// Dedupe 去掉重复投递的记录。
//
// 重复的判定依据是 (XID, LSN) 相同——上游重放时 LSN 不变。
// 当前实现原样返回，没有做任何去重。
func Dedupe(recs []wal.Record) ([]wal.Record, int) {
	return recs, 0
}

// Stats 统计记录类型分布，便于对账。
func Stats(recs []wal.Record) map[string]int {
	out := map[string]int{}
	for _, r := range recs {
		out[r.Type.String()]++
	}
	return out
}
