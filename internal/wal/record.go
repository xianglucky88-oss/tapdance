// Package wal 定义简化的 WAL 文件格式。
//
// 格式说明（便于替换为真实 PG WAL 时对照）：
//
//	页大小 1024 字节
//	[0:4]   magic "WALP"
//	[4:8]   页号 uint32 大端
//	[8:10]  有效负载长度 uint16（页头之后真正使用的字节数）
//	[10:14] CRC32(payload，即 [16:16+used])
//	[14:16] 保留，写 0
//	[16:]   页负载：记录或记录片段
//
// 普通记录（不跨页）：
//
//	[type:1][xid:4 大端][len:2 大端][payload len 字节]
//
// 跨页记录（payload 大到一页放不下时）：
//
//	起始片段的记录头不变，但 len 写成哨兵 0xFFFF，随后是 4 字节总长度：
//	[type:1][xid:4][0xFFFF][total_len:4 大端][片段数据...]
//	起始片段一直占用到本页末尾。后续每一页从页负载开头放：
//	[0xFF:1][frag_len:2 大端][片段数据...]
//	最后一个片段之后可以继续放普通记录。0xFF 不是合法的记录类型，
//	因此续页的片段头与普通记录头不会混淆。
//
// 页头布局与 CRC 覆盖范围保持原样。未使用的页尾字节不参与 CRC。
//
// LSN 的语义没有变化：仍等于“记录起始页号 * PageSize + 记录头在页内的偏移”。
// 跨页记录只在收集到 total_len 字节、且所有片段所在页的 CRC 都通过后才会
// 作为一条完整记录返回；任何片段所在页失败都会丢弃整条记录，而不会把片段
// 或半条记录交给上层。
//
// 读取端把三类问题分开：
//   - 页级校验失败（magic / 长度 / CRC / 页号）：整页不可信；
//   - 记录级不完整（页 CRC 通过，但记录或跨页片段没写完）：只影响该记录；
//   - 未确认尾部（文件末尾不足一页，或最后一条记录没写完）：可恢复截断。
//
// 详细的扫描结果、尾部与继续位置见 ScanResult / Tail / Recovery。
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

// FragmentHeaderSize 是跨页记录起始片段的头长度。
const FragmentHeaderSize = 11

// ContinuationHeaderSize 是跨页记录续页片段的头长度。
const ContinuationHeaderSize = 3

// fragmentMarker 出现在记录头的 len 字段，表示这是一条跨页记录。
// 普通记录在单页里最大 1001 字节 payload，因此不会与哨兵冲突。
const fragmentMarker uint16 = 0xFFFF

// continuationMarker 是续页片段头的首字节。0xFF 不是合法的记录类型。
const continuationMarker byte = 0xFF

// MaxSinglePagePayload 是一条普通记录在空页里能容纳的最大 payload。
const MaxSinglePagePayload = PageSize - HeaderSize - RecordHeaderSize

// maxFirstFragment 是跨页记录在起始页最多携带的片段字节数。
const maxFirstFragment = PageSize - HeaderSize - FragmentHeaderSize

// maxContinuationFragment 是续页片段最多携带的字节数。
const maxContinuationFragment = PageSize - HeaderSize - ContinuationHeaderSize

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

// Valid 报告 t 是否为已知的记录类型。
func (t Type) Valid() bool {
	switch t {
	case TypeBegin, TypeCommit, TypeAbort, TypeInsert, TypeUpdate, TypeDelete, TypeDDL:
		return true
	}
	return false
}

// Record 是一条 WAL 记录。LSN 由记录头所在页号与页内偏移推导。
type Record struct {
	LSN     uint64
	XID     uint32
	Type    Type
	Payload []byte
}

// newPage 返回一页长度为 PageSize、页头已填好 pageNo 的空页。
func newPage(pageNo uint32) []byte {
	page := make([]byte, PageSize)
	copy(page[0:4], "WALP")
	binary.BigEndian.PutUint32(page[4:8], pageNo)
	return page
}

// putNormal 把一条普通记录写入 page[off:]，调用方保证空间足够。
func putNormal(page []byte, off int, r Record) {
	page[off] = byte(r.Type)
	binary.BigEndian.PutUint32(page[off+1:off+5], r.XID)
	binary.BigEndian.PutUint16(page[off+5:off+7], uint16(len(r.Payload)))
	copy(page[off+RecordHeaderSize:], r.Payload)
}

// putFragmentStart 把跨页记录的起始片段头写入 page[off:off+FragmentHeaderSize]。
func putFragmentStart(page []byte, off int, r Record, total int) {
	page[off] = byte(r.Type)
	binary.BigEndian.PutUint32(page[off+1:off+5], r.XID)
	binary.BigEndian.PutUint16(page[off+5:off+7], fragmentMarker)
	binary.BigEndian.PutUint32(page[off+7:off+11], uint32(total))
}

// putContinuation 写入续页片段头并返回片段数据区起点。
func putContinuation(page []byte, off int, n int) int {
	page[off] = continuationMarker
	binary.BigEndian.PutUint16(page[off+1:off+3], uint16(n))
	return off + ContinuationHeaderSize
}

// sealPage 填写有效长度与 CRC。off 是第一条空闲字节的页内偏移。
func sealPage(page []byte, off int) {
	used := off - HeaderSize
	binary.BigEndian.PutUint16(page[8:10], uint16(used))
	binary.BigEndian.PutUint32(page[10:14], crc32.ChecksumIEEE(page[HeaderSize:off]))
}

// EncodePage 把尽量多的“不跨页”记录编码成一页，返回页字节与未放入的记录。
//
// 一条 payload 超过 MaxSinglePagePayload 的记录无法放进单页，会原样留在未放入
// 的记录里；真正需要落盘时请使用 Writer.Append，它会自动做跨页拆分。
func EncodePage(pageNo uint32, recs []Record) ([]byte, []Record) {
	page := newPage(pageNo)
	off := HeaderSize
	consumed := 0
	for i, r := range recs {
		need := RecordHeaderSize + len(r.Payload)
		if need > PageSize-HeaderSize || off+need > PageSize {
			break
		}
		putNormal(page, off, r)
		off += need
		consumed = i + 1
	}
	sealPage(page, off)
	return page, recs[consumed:]
}

// DecodePage 解析单独一页里的普通记录。
//
// 它不持有跨页状态，遇到起始片段会返回 ErrNeedsScan；需要处理跨页记录、
// 部分尾页或损坏页时请使用 Scan / ScanFile。
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
		n := binary.BigEndian.Uint16(page[off+5 : off+7])
		if n == fragmentMarker {
			return nil, fmt.Errorf("page %d: cross-page record at offset %d needs multi-page scan", pageNo, off)
		}
		if off+RecordHeaderSize+int(n) > end {
			break
		}
		payload := make([]byte, n)
		copy(payload, page[off+RecordHeaderSize:off+RecordHeaderSize+int(n)])
		out = append(out, Record{
			LSN:     uint64(pageNo)*PageSize + uint64(off),
			XID:     xid,
			Type:    t,
			Payload: payload,
		})
		off += RecordHeaderSize + int(n)
	}
	return out, nil
}
