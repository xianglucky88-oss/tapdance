package wal

import (
	"fmt"
	"os"
)

// WriteFile 把记录序列化成 WAL 文件。记录不跨页。
func WriteFile(path string, recs []Record) error {
	var buf []byte
	pageNo := uint32(0)
	remaining := recs

	for len(remaining) > 0 {
		page, rest := EncodePage(pageNo, remaining)
		if len(rest) == len(remaining) {
			return fmt.Errorf("record %d too large for a page (%d bytes)", pageNo, len(remaining[0].Payload))
		}
		buf = append(buf, page...)
		remaining = rest
		pageNo++
	}
	if pageNo == 0 {
		// 空文件也要写一页，保证读取端有页可读。
		page, _ := EncodePage(0, nil)
		buf = append(buf, page...)
	}
	return os.WriteFile(path, buf, 0o644)
}

// ReadFile 读取整个 WAL 文件。
func ReadFile(path string) ([]Record, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, nil
	}
	if len(raw)%PageSize != 0 {
		return nil, fmt.Errorf("file size %d is not a multiple of page size %d", len(raw), PageSize)
	}
	var out []Record
	for i := 0; i < len(raw); i += PageSize {
		recs, err := DecodePage(raw[i : i+PageSize])
		if err != nil {
			return out, err
		}
		out = append(out, recs...)
	}
	return out, nil
}

// LastLSN 返回最后一条记录的 LSN。
func LastLSN(recs []Record) uint64 {
	if len(recs) == 0 {
		return 0
	}
	return recs[len(recs)-1].LSN
}

// MaxLSN 返回记录中的最高 LSN。
//
// 记录可能乱序（多文件合并、重放），"处理到达点"应取最高值，
// 而不是最后一条记录的 LSN。
func MaxLSN(recs []Record) uint64 {
	var max uint64
	for _, r := range recs {
		if r.LSN > max {
			max = r.LSN
		}
	}
	return max
}
