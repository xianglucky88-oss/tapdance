// Package store 是变更落地的内存表，并负责快照导出。
package store

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/internal-platform/tapdance/internal/schema"
	"github.com/internal-platform/tapdance/internal/wal"
)

// Store 是 table → pk → row 的两级映射。
type Store struct {
	Tables map[string]map[string]map[string]any
	// Applied 记录已应用的行变更数（不含 DDL）。
	Applied int
}

// New 构造空库。
func New() *Store { return &Store{Tables: map[string]map[string]map[string]any{}} }

type change struct {
	Table string         `json:"table"`
	PK    string         `json:"pk"`
	Row   map[string]any `json:"row"`
}

// Apply 应用一条已提交的变更记录。DDL 交给 tracker，行变更写入表。
func (s *Store) Apply(r wal.Record, tracker *schema.Tracker) error {
	switch r.Type {
	case wal.TypeInsert, wal.TypeUpdate, wal.TypeDelete:
		var c change
		if err := json.Unmarshal(r.Payload, &c); err != nil {
			return fmt.Errorf("bad change payload: %w", err)
		}
		if c.Table == "" || c.PK == "" {
			return fmt.Errorf("change without table/pk")
		}
		if s.Tables[c.Table] == nil {
			s.Tables[c.Table] = map[string]map[string]any{}
		}
		if r.Type == wal.TypeDelete {
			delete(s.Tables[c.Table], c.PK)
		} else {
			row := tracker.Normalize(c.Table, c.Row)
			s.Tables[c.Table][c.PK] = row
		}
		s.Applied++
	case wal.TypeDDL:
		return tracker.Apply(r.LSN, r.Payload)
	}
	return nil
}

// RowCount 返回某表行数。
func (s *Store) RowCount(table string) int { return len(s.Tables[table]) }

// Snapshot 导出当前库的快照（按表名、主键排序，保证可复现）。
func (s *Store) Snapshot() map[string]any {
	out := map[string]any{}
	for table, rows := range s.Tables {
		keys := make([]string, 0, len(rows))
		for k := range rows {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		ordered := make([]map[string]any, 0, len(keys))
		for _, k := range keys {
			ordered = append(ordered, rows[k])
		}
		out[table] = ordered
	}
	return out
}

// Checksum 对所有行做稳定摘要，用于验证"增量重放 == 全量快照"。
func (s *Store) Checksum() string {
	snap := s.Snapshot()
	raw, _ := json.Marshal(snap)
	var sum uint64
	for _, b := range raw {
		sum = sum*131 + uint64(b)
	}
	return fmt.Sprintf("%016x", sum)
}
