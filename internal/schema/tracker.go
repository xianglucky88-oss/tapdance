// Package schema 跟踪表的列结构随 DDL 的变化。
package schema

import (
	"encoding/json"
	"fmt"
	"sort"
)

// Version 是一个表结构版本。
type Version struct {
	Version int      `json:"version"`
	Columns []string `json:"columns"`
	// Renamed 记录旧列名 → 新列名，用于解释旧记录。
	Renamed map[string]string `json:"renamed,omitempty"`
	LSN     uint64            `json:"lsn"`
}

// Tracker 维护每张表的当前结构。
type Tracker struct {
	tables map[string]*Version
}

// New 构造跟踪器。
func New() *Tracker { return &Tracker{tables: map[string]*Version{}} }

type ddl struct {
	Table  string `json:"table"`
	Op     string `json:"op"` // create | add_column | rename_column | drop_column
	Column string `json:"column"`
	To     string `json:"to"`
}

// Apply 应用一条 DDL 记录。
func (t *Tracker) Apply(lsn uint64, payload []byte) error {
	var d ddl
	if err := json.Unmarshal(payload, &d); err != nil {
		return err
	}
	if d.Table == "" {
		return fmt.Errorf("ddl without table")
	}
	v, ok := t.tables[d.Table]
	if !ok {
		if d.Op != "create" {
			return fmt.Errorf("ddl on unknown table %q", d.Table)
		}
		v = &Version{Version: 1, Columns: []string{}, Renamed: map[string]string{}}
		t.tables[d.Table] = v
	}
	v.LSN = lsn

	switch d.Op {
	case "create":
		if len(v.Columns) == 0 {
			v.Columns = []string{"id"}
		}
	case "add_column":
		v.Columns = append(v.Columns, d.Column)
		v.Version++
	case "rename_column":
		for i, c := range v.Columns {
			if c == d.Column {
				v.Columns[i] = d.To
				v.Renamed[d.Column] = d.To
			}
		}
		v.Version++
	case "drop_column":
		out := v.Columns[:0]
		for _, c := range v.Columns {
			if c != d.Column {
				out = append(out, c)
			}
		}
		v.Columns = out
		v.Version++
	default:
		return fmt.Errorf("unknown ddl op %q", d.Op)
	}
	return nil
}

// Columns 返回表的当前列。
func (t *Tracker) Columns(table string) []string {
	v, ok := t.tables[table]
	if !ok {
		return nil
	}
	return v.Columns
}

// Version 返回表的当前版本号。
func (t *Tracker) VersionOf(table string) int {
	v, ok := t.tables[table]
	if !ok {
		return 0
	}
	return v.Version
}

// Normalize 把一条旧记录的行字段按重命名映射改成新名字。
//
// 当前实现只按"最新一次重命名"处理，不区分记录写入时的版本。
func (t *Tracker) Normalize(table string, row map[string]any) map[string]any {
	v, ok := t.tables[table]
	if !ok || len(v.Renamed) == 0 {
		return row
	}
	out := make(map[string]any, len(row))
	for k, val := range row {
		if to, ok := v.Renamed[k]; ok {
			out[to] = val
			continue
		}
		out[k] = val
	}
	return out
}

// Tables 返回已知表名，按字典序。
func (t *Tracker) Tables() []string {
	out := make([]string, 0, len(t.tables))
	for k := range t.tables {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
