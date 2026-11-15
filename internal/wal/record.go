// Package wal 定义简化的 WAL 文件格式。
//
// 格式说明（便于替换为真实 PG WAL 时对照）：
//
//	页大小 1024 字节
//	[0:4]   magic "WALP"
//	[4:8]   页号 uint32 大端
//	[8:10]  有效负载长度 uint16
//	[10:14] CRC32(payload)
//	[16:]   记录序列
//
//	记录：
//	[type:1][xid:4 大端][len:2 大端][payload len 字节]
//
// 当前实现不允许一条记录跨页：放不下就补齐到下一页开头。
package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// PageSize 是固定页大小。
const PageSize = 1024

// HeaderSize 是页头长度。
const HeaderSize = 16

// RecordHeaderSize 是单条记录头长度。
const RecordHeaderSize = 7

// Type 是记录类型。
type Type byte

const (
	// TypeBegin 事务开始。
	TypeBegin Type = 'B'
	// TypeCommit 事务提交。
	TypeCommit Type = 'C'
	// TypeAbort 事务回滚。
	TypeAbort Type = 'A'
	// TypeInsert 插入一行。
	TypeInsert Type = 'I'
	// TypeUpdate 更新一行。
	TypeUpdate Type = 'U'
	// TypeDelete 删除一行。
	TypeDelete Type = 'D'
	// TypeDDL 结构变更。
	TypeDDL Type = 'X'
)

func (t Type) String() string {
	switch t {
	case TypeBegin:
		return "BEGIN"
	case TypeCommit:
		return "COMMIT"
	case TypeAbort:
		return "ABORT"
	case TypeInsert:
		return "INSERT"
	case TypeUpdate:
		return "UPDATE"
	case TypeDelete:
		return "DELETE"
	case TypeDDL:
		return "DDL"
	}
	return "UNKNOWN"
}

// Record 是一条 WAL 记录。LSN 由所在页号与页内偏移推导。
type Record struct {
	LSN     uint64
	XID     uint32
	Type    Type
	Payload []byte
}

// EncodePage 把一组记录编码成一页。返回页字节与未放入的记录数。
func EncodePage(pageNo uint32, recs []Record) ([]byte, []Record) {
	page := make([]byte, PageSize)
	copy(page[0:4], "WALP")
	binary.BigEndian.PutUint32(page[4:8], pageNo)

	off := HeaderSize
	used := 0
	consumed := 0
	for i, r := range recs {
		need := RecordHeaderSize + len(r.Payload)
		if off+need > PageSize {
			// 放不下：剩余记录留给下一页。
			break
		}
		page[off] = byte(r.Type)
		binary.BigEndian.PutUint32(page[off+1:off+5], r.XID)
		binary.BigEndian.PutUint16(page[off+5:off+7], uint16(len(r.Payload)))
		copy(page[off+RecordHeaderSize:], r.Payload)
		off += need
		used += need
		consumed = i + 1
	}
	binary.BigEndian.PutUint16(page[8:10], uint16(used))
	binary.BigEndian.PutUint32(page[10:14], crc32.ChecksumIEEE(page[HeaderSize:HeaderSize+used]))
	return page, recs[consumed:]
}

// DecodePage 解析一页。返回记录、剩余未解析的尾部。
func DecodePage(page []byte) ([]Record, error) {
	if len(page) != PageSize {
		return nil, fmt.Errorf("bad page size %d", len(page))
	}
	if string(page[0:4]) != "WALP" {
		return nil, fmt.Errorf("bad magic %q", string(page[0:4]))
	}
	pageNo := binary.BigEndian.Uint32(page[4:8])
	used := int(binary.BigEndian.Uint16(page[8:10]))
	want := binary.BigEndian.Uint32(page[10:14])
	if used > PageSize-HeaderSize {
		return nil, fmt.Errorf("page %d: corrupt length %d", pageNo, used)
	}
	if got := crc32.ChecksumIEEE(page[HeaderSize : HeaderSize+used]); got != want {
		return nil, fmt.Errorf("page %d: checksum mismatch", pageNo)
	}

	var out []Record
	off := HeaderSize
	end := HeaderSize + used
	for off+RecordHeaderSize <= end {
		t := Type(page[off])
		xid := binary.BigEndian.Uint32(page[off+1 : off+5])
		n := int(binary.BigEndian.Uint16(page[off+5 : off+7]))
		if off+RecordHeaderSize+n > end {
			break
		}
		payload := make([]byte, n)
		copy(payload, page[off+RecordHeaderSize:off+RecordHeaderSize+n])
		out = append(out, Record{
			LSN:     uint64(pageNo)*PageSize + uint64(off),
			XID:     xid,
			Type:    t,
			Payload: payload,
		})
		off += RecordHeaderSize + n
	}
	return out, nil
}
