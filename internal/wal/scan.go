package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// DefaultMaxRecordSize 是重组一条跨页记录时默认允许占用的最大内存。
//
// 页头里的总长度字段是不可信输入（最大可到 4GiB）。扫描器在分配缓冲前
// 先用这个上限拦截；超过上限的记录会被报告为 RecordOversized 并直接跳过
// 其所有片段，不会为了“读一条记录”而把内存撑爆。
const DefaultMaxRecordSize = 16 << 20

// ScanOptions 控制扫描行为。
type ScanOptions struct {
	// MaxRecordSize 单条记录（跨页重组后）允许的最大字节数，0 表示
	// DefaultMaxRecordSize。小于单页容量的值会被提升到单页容量。
	MaxRecordSize int
}

// PageProblemKind 是页级校验失败的类别。
type PageProblemKind uint8

const (
	// PageBadMagic 页头 magic 不是 "WALP"。
	PageBadMagic PageProblemKind = iota + 1
	// PageBadPageNo 页号与文件内位置不一致。
	PageBadPageNo
	// PageBadLength 有效负载长度越界。
	PageBadLength
	// PageChecksum CRC32 与页负载不匹配。
	PageChecksum
)

func (k PageProblemKind) String() string {
	switch k {
	case PageBadMagic:
		return "bad-magic"
	case PageBadPageNo:
		return "bad-page-no"
	case PageBadLength:
		return "bad-length"
	case PageChecksum:
		return "checksum"
	}
	return "unknown"
}

// PageProblem 描述一次页级校验失败。该页上的记录全部不可信，扫描器会跳过
// 整页并继续读取后续页。
type PageProblem struct {
	PageNo uint32          `json:"page_no"`
	Offset int64           `json:"offset"`
	Kind   PageProblemKind `json:"kind"`
	Detail string          `json:"detail"`
}

// RecordProblemKind 是记录级问题的类别。
type RecordProblemKind uint8

const (
	// RecordIncomplete 页校验通过，但记录（或跨页片段）没有写完。
	RecordIncomplete RecordProblemKind = iota + 1
	// RecordOrphanFragment 页首出现没有起始片段的续页片段。
	RecordOrphanFragment
	// RecordUnknownType 记录类型无法识别。
	RecordUnknownType
	// RecordOversized 声明的长度超过 MaxRecordSize，片段被跳过。
	RecordOversized
	// RecordSpansDamage 跨页记录经过了一个损坏页，整条记录被丢弃。
	RecordSpansDamage
	// RecordBadLength 记录头声明的长度与页负载矛盾。
	RecordBadLength
)

func (k RecordProblemKind) String() string {
	switch k {
	case RecordIncomplete:
		return "incomplete"
	case RecordOrphanFragment:
		return "orphan-fragment"
	case RecordUnknownType:
		return "unknown-type"
	case RecordOversized:
		return "oversized"
	case RecordSpansDamage:
		return "spans-damage"
	case RecordBadLength:
		return "bad-length"
	}
	return "unknown"
}

// RecordProblem 描述一条被丢弃的记录。它独立于记录列表，绝不会以“半条
// 记录”的形式混进 ScanResult.Records。
//
// StopsPage 为真表示扫描器在此处放弃了本页剩余字节（记录边界已不可信），
// 但仍会继续解析后续页。OrphanFragment / UnknownType / Oversized 为假：
// 扫描器知道如何跳过它们，后面的记录仍然可信。
type RecordProblem struct {
	LSN    uint64            `json:"lsn"`
	PageNo uint32            `json:"page_no"`
	XID    uint32            `json:"xid,omitempty"`
	Kind   RecordProblemKind `json:"kind"`
	Detail string            `json:"detail"`
	// StopsPage 表示该问题导致本页剩余部分被放弃解析。
	StopsPage bool `json:"stops_page"`
	// HasXID 表示 XID 字段有效；只有从完整记录头解析出的问题才带有 XID。
	HasXID bool `json:"has_xid"`
}

// TailKind 是文件尾部状态的类别。
type TailKind uint8

const (
	// TailNone 文件在页边界结束，且没有未完成的记录。
	TailNone TailKind = iota
	// TailPartialPage 文件末尾不足一整页（正在写入或被截断）。
	TailPartialPage
	// TailIncompleteRecord 最后一条记录/跨页片段没有写完。
	TailIncompleteRecord
	// TailCorruptPage 最后一页整页校验失败，通常来自撕裂写。
	TailCorruptPage
)

func (k TailKind) String() string {
	switch k {
	case TailNone:
		return "none"
	case TailPartialPage:
		return "partial-page"
	case TailIncompleteRecord:
		return "incomplete-record"
	case TailCorruptPage:
		return "corrupt-page"
	}
	return "unknown"
}

// Tail 描述未确认的尾部：从 Offset 开始的数据不能信任，安全的方式是从
// 这里截断后重新追加。Offset 与 LSN 在文件格式里相等（LSN 就是记录头的
// 绝对字节偏移）。
type Tail struct {
	Kind   TailKind `json:"kind"`
	Offset int64    `json:"offset"`
	LSN    uint64   `json:"lsn"`
	Bytes  int      `json:"bytes"`
}

// Recovery 给出重启后继续写入的位置。
type Recovery struct {
	// TruncateOffset 是重新追加前应当把文件截断到的偏移。
	TruncateOffset int64 `json:"truncate_offset"`
	// NextPageNo 是截断后第一个可写的页号。
	NextPageNo uint32 `json:"next_page_no"`
	// AppendLSN 是下一条记录会得到的 LSN（等于 TruncateOffset）。
	AppendLSN uint64 `json:"append_lsn"`
	// SafeLSN 是最后一条已完整读出并交付的记录 LSN，0 表示没有。
	SafeLSN uint64 `json:"safe_lsn"`
}

// ScanResult 是一次扫描的完整结果。
//
// 上层应把 Records 视为“可以可靠读出并已交付”的记录；PageProblems 与
// RecordProblems 说明哪些数据没有交付；Tail/Resume 说明未确认尾部以及
// 重启后从哪里继续。
type ScanResult struct {
	Records        []Record        `json:"-"`
	Pages          int             `json:"pages"`
	PageProblems   []PageProblem   `json:"page_problems,omitempty"`
	RecordProblems []RecordProblem `json:"record_problems,omitempty"`
	Tail           Tail            `json:"tail"`
	Resume         Recovery        `json:"resume"`
	FileSize       int64           `json:"file_size"`
}

// HasDamage 报告是否存在页级或记录级的数据损失。
func (r *ScanResult) HasDamage() bool {
	return len(r.PageProblems) > 0 || len(r.RecordProblems) > 0
}

// Truncated 报告尾部是否有未确认数据。
func (r *ScanResult) Truncated() bool { return r.Tail.Kind != TailNone }

// StrictError 把任何页级失败、记录级失败或未确认尾部折叠成一个错误。
func (r *ScanResult) StrictError() error {
	if !r.HasDamage() && !r.Truncated() {
		return nil
	}
	return &IncompleteError{Result: r}
}

// IncompleteError 表示扫描发现了损坏或未确认的尾部。
type IncompleteError struct{ Result *ScanResult }

func (e *IncompleteError) Error() string {
	r := e.Result
	switch {
	case len(r.PageProblems) > 0:
		p := r.PageProblems[0]
		return fmt.Sprintf("wal: page %d at offset %d: %s (%s); %d record(s) read",
			p.PageNo, p.Offset, p.Kind, p.Detail, len(r.Records))
	case len(r.RecordProblems) > 0:
		p := r.RecordProblems[0]
		return fmt.Sprintf("wal: record at lsn %d dropped: %s (%s); %d record(s) read",
			p.LSN, p.Kind, p.Detail, len(r.Records))
	default:
		return fmt.Sprintf("wal: unconfirmed tail (%s) at offset %d; %d record(s) read",
			r.Tail.Kind, r.Tail.Offset, len(r.Records))
	}
}

// ScanFile 以可恢复的方式扫描一个 WAL 文件。
//
// 页级校验失败不会终止扫描：坏页会被跳过，后续页里的记录仍会返回。
// 只有 I/O 错误才会返回 error。
func ScanFile(path string, opts ScanOptions) (*ScanResult, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return Scan(f, opts)
}

// Scan 从 r 流式扫描 WAL。
//
// 每次只保留一页读取缓冲和当前正在重组的记录，因此可以处理远大于内存的
// 文件；跨页记录的内存占用受 opts.MaxRecordSize 限制。
func Scan(r io.Reader, opts ScanOptions) (*ScanResult, error) {
	if opts.MaxRecordSize <= 0 {
		opts.MaxRecordSize = DefaultMaxRecordSize
	}
	if opts.MaxRecordSize < MaxSinglePagePayload {
		opts.MaxRecordSize = MaxSinglePagePayload
	}
	s := &scanner{opts: opts, res: &ScanResult{Records: []Record{}}}

	var pageNo uint32
	var off int64
	for {
		page := make([]byte, PageSize)
		n, err := io.ReadFull(r, page)
		if err == io.EOF && n == 0 {
			s.finish(off, 0)
			return s.res, nil
		}
		if err != nil && err != io.ErrUnexpectedEOF {
			return nil, err
		}
		if n < PageSize {
			// 不足一页：可能是正在写入，也可能是崩溃截断。
			s.finish(off, n)
			return s.res, nil
		}
		s.res.Pages++
		s.scanPage(page, pageNo, off)
		off += PageSize
		pageNo++
	}
}

type tailCandidate struct {
	offset int64
	kind   TailKind
}

type scanner struct {
	opts ScanOptions
	res  *ScanResult

	pending *partialRecord

	// 只保留“最后一个已完整读到的页”里的问题，供 EOF 时判定尾部。
	// lastIssue 在每进入一个完好页时清空；corruptRun 跨连续坏页保留，
	// 遇到完好页才清空——这样尾部总是从最后一段不可信区域的起点开始。
	lastIssue  *tailCandidate
	corruptRun *tailCandidate
}

// partialRecord 是正在重组的跨页记录。
type partialRecord struct {
	lsn       uint64
	pageNo    uint32
	xid       uint32
	typ       Type
	total     int
	received  int
	buf       []byte
	oversized bool
}

// take 追加一段片段数据。数据长度被钳制在声明的总长度内；oversized 时只
// 计数不分配。
func (p *partialRecord) take(chunk []byte) {
	if rem := p.total - p.received; len(chunk) > rem {
		chunk = chunk[:rem]
	}
	if !p.oversized {
		p.buf = append(p.buf, chunk...)
	}
	p.received += len(chunk)
}

func (s *scanner) scanPage(page []byte, pageNo uint32, pageOffset int64) {
	if kind, detail := validatePage(page, pageNo); detail != "" {
		s.res.PageProblems = append(s.res.PageProblems, PageProblem{
			PageNo: pageNo, Offset: pageOffset, Kind: kind, Detail: detail,
		})
		cand := pageOffset
		if s.pending != nil {
			lsn := int64(s.pending.lsn)
			s.res.RecordProblems = append(s.res.RecordProblems, RecordProblem{
				LSN: s.pending.lsn, PageNo: s.pending.pageNo, XID: s.pending.xid, HasXID: true,
				Kind: RecordSpansDamage, StopsPage: true,
				Detail: fmt.Sprintf("cross-page record interrupted by corrupt page %d", pageNo),
			})
			if lsn < cand {
				cand = lsn
			}
			s.pending = nil
		}
		kind := TailCorruptPage
		if cand != pageOffset {
			kind = TailIncompleteRecord
		}
		// 连续多页损坏时保留这一串的起点，而不是最后一页：恢复需要从
		// 第一个不可信字节开始截断。
		if s.corruptRun == nil {
			s.corruptRun = &tailCandidate{offset: cand, kind: kind}
		}
		return
	}

	// 完好页意味着之前的坏页不在文件尾部，尾部候选重新从这里算起。
	s.corruptRun = nil
	s.lastIssue = nil

	used := int(binary.BigEndian.Uint16(page[8:10]))
	end := HeaderSize + used
	p := HeaderSize

	// 先处理上一页遗留的跨页记录。
	if s.pending != nil {
		if end-p < ContinuationHeaderSize || page[p] != continuationMarker {
			lsn := s.dropPending(RecordIncomplete, "continuation fragment missing")
			s.lastIssue = &tailCandidate{offset: lsn, kind: TailIncompleteRecord}
		} else {
			n := int(binary.BigEndian.Uint16(page[p+1 : p+3]))
			if p+ContinuationHeaderSize+n > end {
				lsn := s.dropPending(RecordBadLength, "continuation fragment exceeds page payload")
				s.lastIssue = &tailCandidate{offset: lsn, kind: TailIncompleteRecord}
				return
			}
			s.pending.take(page[p+ContinuationHeaderSize : p+ContinuationHeaderSize+n])
			p += ContinuationHeaderSize + n
			if s.pending.received >= s.pending.total {
				s.emitPending()
			} else {
				// 记录还要延续到下一页；本页不应再有其他内容。
				return
			}
		}
	}

	for p < end {
		if end-p < RecordHeaderSize {
			s.noteIssue(int64(pageOffset)+int64(p), RecordIncomplete, "torn record header")
			return
		}
		t := Type(page[p])
		xid := binary.BigEndian.Uint32(page[p+1 : p+5])
		nraw := binary.BigEndian.Uint16(page[p+5 : p+7])
		lsn := uint64(pageOffset) + uint64(p)

		if t == Type(continuationMarker) {
			if end-p < ContinuationHeaderSize {
				s.noteIssue(int64(lsn), RecordIncomplete, "torn continuation header")
				return
			}
			n := int(binary.BigEndian.Uint16(page[p+1 : p+3]))
			if p+ContinuationHeaderSize+n > end {
				s.noteIssue(int64(lsn), RecordBadLength, "orphan continuation exceeds page payload")
				return
			}
			// 续页片段头只有 [0xFF][len:2]，没有 XID；不要把紧随其后的
			// 片段数据首字节误读成 XID。
			s.res.RecordProblems = append(s.res.RecordProblems, RecordProblem{
				LSN: lsn, PageNo: pageNo, Kind: RecordOrphanFragment,
				Detail: "continuation fragment without a starting fragment",
			})
			p += ContinuationHeaderSize + n
			continue
		}

		if !t.Valid() {
			s.res.RecordProblems = append(s.res.RecordProblems, RecordProblem{
				LSN: lsn, PageNo: pageNo, XID: xid, Kind: RecordUnknownType,
				Detail: fmt.Sprintf("unknown record type 0x%02x", byte(t)),
			})
			if nraw == fragmentMarker {
				s.noteIssue(int64(lsn), RecordUnknownType, "unknown fragmented record; rest of page skipped")
				return
			}
			if p+RecordHeaderSize+int(nraw) > end {
				s.noteIssue(int64(lsn), RecordIncomplete, "unknown record payload exceeds page")
				return
			}
			p += RecordHeaderSize + int(nraw)
			continue
		}

		if nraw != fragmentMarker {
			if p+RecordHeaderSize+int(nraw) > end {
				s.noteIssue(int64(lsn), RecordIncomplete, "record payload exceeds page payload")
				return
			}
			payload := make([]byte, int(nraw))
			copy(payload, page[p+RecordHeaderSize:p+RecordHeaderSize+int(nraw)])
			s.res.Records = append(s.res.Records, Record{LSN: lsn, XID: xid, Type: t, Payload: payload})
			p += RecordHeaderSize + int(nraw)
			continue
		}

		// 跨页记录的起始片段。
		if end-p < FragmentHeaderSize {
			s.noteIssue(int64(lsn), RecordIncomplete, "torn cross-page record header")
			return
		}
		total := binary.BigEndian.Uint32(page[p+7 : p+11])
		if total == 0 {
			s.noteIssue(int64(lsn), RecordBadLength, "cross-page record declares zero length")
			return
		}
		pr := &partialRecord{lsn: lsn, pageNo: pageNo, xid: xid, typ: t, total: int(total)}
		if int(total) > s.opts.MaxRecordSize {
			pr.oversized = true
			s.res.RecordProblems = append(s.res.RecordProblems, RecordProblem{
				LSN: lsn, PageNo: pageNo, XID: xid, HasXID: true, Kind: RecordOversized,
				Detail: fmt.Sprintf("declared %d bytes exceeds limit %d", total, s.opts.MaxRecordSize),
			})
		}
		pr.take(page[p+FragmentHeaderSize : end])
		if pr.received >= pr.total {
			s.emitPendingRecord(pr)
		} else {
			s.pending = pr
		}
		p = end
	}
}

// noteIssue 记录当前页内的记录级中断，并把它保留为尾部候选。
func (s *scanner) noteIssue(offset int64, kind RecordProblemKind, detail string) {
	s.res.RecordProblems = append(s.res.RecordProblems, RecordProblem{
		LSN:       uint64(offset),
		PageNo:    uint32(offset / PageSize),
		Kind:      kind,
		Detail:    detail,
		StopsPage: true,
	})
	s.lastIssue = &tailCandidate{offset: offset, kind: TailIncompleteRecord}
}

// dropPending 丢弃正在重组的记录并返回它的起始 LSN。
func (s *scanner) dropPending(kind RecordProblemKind, detail string) int64 {
	if s.pending == nil {
		return -1
	}
	p := s.pending
	s.pending = nil
	s.res.RecordProblems = append(s.res.RecordProblems, RecordProblem{
		LSN: p.lsn, PageNo: p.pageNo, XID: p.xid, HasXID: true, Kind: kind, Detail: detail, StopsPage: true,
	})
	return int64(p.lsn)
}

func (s *scanner) emitPending() {
	if s.pending == nil {
		return
	}
	s.emitPendingRecord(s.pending)
	s.pending = nil
}

func (s *scanner) emitPendingRecord(p *partialRecord) {
	if p.oversized {
		return // 已报告为 RecordOversized
	}
	s.res.Records = append(s.res.Records, Record{
		LSN: p.lsn, XID: p.xid, Type: p.typ, Payload: p.buf,
	})
}

// finish 汇总尾部与续写位置。
func (s *scanner) finish(endOffset int64, partialBytes int) {
	res := s.res
	res.FileSize = endOffset + int64(partialBytes)

	var cand *tailCandidate
	consider := func(c *tailCandidate) {
		if c == nil {
			return
		}
		if cand == nil || c.offset < cand.offset {
			cand = c
		}
	}

	if s.pending != nil {
		p := s.pending
		if !p.oversized {
			res.RecordProblems = append(res.RecordProblems, RecordProblem{
				LSN: p.lsn, PageNo: p.pageNo, XID: p.xid, HasXID: true, Kind: RecordIncomplete,
				Detail: fmt.Sprintf("cross-page record incomplete: %d of %d bytes", p.received, p.total),
			})
		}
		consider(&tailCandidate{offset: int64(p.lsn), kind: TailIncompleteRecord})
		s.pending = nil
	}
	consider(s.lastIssue)
	consider(s.corruptRun)
	if partialBytes > 0 {
		consider(&tailCandidate{offset: endOffset, kind: TailPartialPage})
	}

	if cand == nil {
		res.Tail = Tail{Kind: TailNone, Offset: endOffset, LSN: uint64(endOffset)}
	} else {
		res.Tail = Tail{
			Kind:   cand.kind,
			Offset: cand.offset,
			LSN:    uint64(cand.offset),
			Bytes:  int(res.FileSize - cand.offset),
		}
	}
	res.Resume = Recovery{
		TruncateOffset: res.Tail.Offset,
		NextPageNo:     uint32(res.Tail.Offset / PageSize),
		AppendLSN:      uint64(res.Tail.Offset),
		SafeLSN:        safeLSN(res.Records),
	}
}

func safeLSN(recs []Record) uint64 {
	if len(recs) == 0 {
		return 0
	}
	return recs[len(recs)-1].LSN
}

func validatePage(page []byte, pageNo uint32) (PageProblemKind, string) {
	if string(page[0:4]) != "WALP" {
		return PageBadMagic, fmt.Sprintf("bad magic %q", string(page[0:4]))
	}
	if got := binary.BigEndian.Uint32(page[4:8]); got != pageNo {
		return PageBadPageNo, fmt.Sprintf("page number %d, want %d", got, pageNo)
	}
	used := int(binary.BigEndian.Uint16(page[8:10]))
	if used > PageSize-HeaderSize {
		return PageBadLength, fmt.Sprintf("payload length %d exceeds %d", used, PageSize-HeaderSize)
	}
	want := binary.BigEndian.Uint32(page[10:14])
	if got := crc32.ChecksumIEEE(page[HeaderSize : HeaderSize+used]); got != want {
		return PageChecksum, fmt.Sprintf("crc32 %08x, want %08x", got, want)
	}
	return 0, ""
}
