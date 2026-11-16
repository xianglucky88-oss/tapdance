package wal

import "os"

// WriteFile 把记录序列化成 WAL 文件（内部使用支持跨页的 Writer）。
//
// 与旧实现不同，payload 超过单页容量的记录不再报错，而是拆成跨页片段。
func WriteFile(path string, recs []Record) error {
	w, err := Create(path, WriterOptions{})
	if err != nil {
		return err
	}
	if err := w.Append(recs...); err != nil {
		w.Close()
		return err
	}
	return w.Close()
}

// AppendFile 在已有 WAL 文件末尾追加记录。
func AppendFile(path string, recs ...Record) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	w, err := NewWriter(f, WriterOptions{})
	if err != nil {
		f.Close()
		return err
	}
	if err := w.Append(recs...); err != nil {
		w.Close()
		return err
	}
	return w.Close()
}

// ReadFile 严格读取整个 WAL 文件。
//
// 只有文件完全干净（每页 CRC 通过、每条记录完整）时才返回成功；否则返回
// 已经读出的记录以及 *IncompleteError。若需要“坏页之后继续读”，请使用
// ScanFile。
func ReadFile(path string) ([]Record, error) {
	res, err := ScanFile(path, ScanOptions{})
	if err != nil {
		return nil, err
	}
	if err := res.StrictError(); err != nil {
		return res.Records, err
	}
	return res.Records, nil
}

// LastLSN 返回最后一条记录的 LSN。
func LastLSN(recs []Record) uint64 {
	if len(recs) == 0 {
		return 0
	}
	return recs[len(recs)-1].LSN
}
