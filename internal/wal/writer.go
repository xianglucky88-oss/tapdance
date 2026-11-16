package wal

import (
	"fmt"
	"io"
	"os"
)

// WriterOptions 控制写入端资源上限。
type WriterOptions struct {
	// MaxRecordSize 单条记录允许的最大字节数，0 表示 DefaultMaxRecordSize。
	// 超过上限的记录会被 Append 拒绝，避免一次写入耗尽内存或磁盘。
	MaxRecordSize int
}

func (o WriterOptions) limit() int {
	if o.MaxRecordSize <= 0 {
		return DefaultMaxRecordSize
	}
	if o.MaxRecordSize < MaxSinglePagePayload {
		return MaxSinglePagePayload
	}
	return o.MaxRecordSize
}

// Writer 以流式方式向 WAL 追加记录。
//
// 当前未写满的页会暂存在内存里，Flush/Sync/Close 时才落盘；因此一次崩溃
// 最多丢掉最后一页，且不会留下能通过 CRC、却只包含半个记录头的页。
// 调用方也可以随时 Flush，把已 Append 的记录落到页边界上，令并发读取端
// 把结尾视为 TailNone。
//
// 写入是确定性的：相同记录序列从相同页号开始，总是得到相同页内偏移，
// 因此 LSN 在“截断后重放”时保持不变。
type Writer struct {
	// w 是底层目标：文件写入器用 f，NewBufferWriter 用 w。
	w io.Writer
	// f 仅在使用 Create/NewWriter/Resume 时非空，用于 Sync/Close。
	f  *os.File
	op WriterOptions
	// closed 防止 Close 后继续写入。
	closed bool

	page   []byte
	off    int
	pageNo uint32
	// written 是已经落盘的字节数，也等于 pageNo*PageSize。
	written int64
}

// Create 新建（或清空）一个 WAL 文件并返回写入器。
func Create(path string, opts WriterOptions) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return &Writer{f: f, w: f, op: opts, page: newPage(0), off: HeaderSize}, nil
}

// NewBufferWriter 返回一个写入 io.Writer 的 Writer，便于测试或写往非文件目标。
func NewBufferWriter(w io.Writer, opts WriterOptions) *Writer {
	return &Writer{w: w, op: opts, page: newPage(0), off: HeaderSize}
}

// NewWriter 在一个已经是页对齐的文件末尾继续追加。
func NewWriter(f *os.File, opts WriterOptions) (*Writer, error) {
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size()%PageSize != 0 {
		return nil, fmt.Errorf("wal: file size %d is not page aligned; use Resume", st.Size())
	}
	if _, err := f.Seek(st.Size(), io.SeekStart); err != nil {
		return nil, err
	}
	pageNo := uint32(st.Size() / PageSize)
	return &Writer{
		f: f, w: f, op: opts, page: newPage(pageNo), off: HeaderSize,
		pageNo: pageNo, written: st.Size(),
	}, nil
}

// Resume 以可恢复方式重新打开 WAL：先扫描，截掉未确认的尾部，再把截断页
// 开头到尾部之间仍然完整的记录重新编码，最后返回位于文件末尾的写入器。
//
// 返回的 ScanResult 描述截断前的状态。只有当页级损坏全部位于可截断尾部
// 之内时 Resume 才会继续：更早位置出现坏页说明日志本身已不可信，此时
// 静默追加会掩盖损坏，因此拒绝并交由调用方处理。
func Resume(path string, opts WriterOptions) (*Writer, *ScanResult, error) {
	res, err := ScanFile(path, ScanOptions{MaxRecordSize: opts.limit()})
	if err != nil {
		return nil, nil, err
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, res, err
	}

	// 尾部通常已经指向未完成记录的起始页。若因为残缺而指向了一张校验
	// 通过、但内容以续页片段开头的页，再往前回退，避免截断后留下没有
	// 起始片段的孤儿续页。损坏页不回退：它本身就是要被截掉的边界。
	tailPage := uint32(res.Tail.Offset / PageSize)
	for tailPage > 0 {
		cont, err := pageStartsWithContinuation(f, tailPage)
		if err != nil {
			f.Close()
			return nil, res, err
		}
		if !cont {
			break
		}
		tailPage--
	}
	cut := int64(tailPage) * PageSize

	for _, p := range res.PageProblems {
		if p.Offset < cut {
			f.Close()
			return nil, res, fmt.Errorf("wal: page %d corrupt at offset %d, before recoverable tail %d; refusing to append",
				p.PageNo, p.Offset, cut)
		}
	}
	for _, p := range res.RecordProblems {
		if int64(p.LSN) < cut {
			f.Close()
			return nil, res, fmt.Errorf("wal: record at lsn %d dropped (%s) before recoverable tail %d; refusing to append without rewriting history",
				p.LSN, p.Kind, cut)
		}
	}

	if err := f.Truncate(cut); err != nil {
		f.Close()
		return nil, res, err
	}
	if _, err := f.Seek(cut, io.SeekStart); err != nil {
		f.Close()
		return nil, res, err
	}

	w := &Writer{
		f: f, w: f, op: opts, page: newPage(tailPage), off: HeaderSize,
		pageNo: tailPage, written: cut,
	}
	// 截断页上 [cut, Tail.Offset) 的完整记录是干净前缀。重新编码后它们必须
	// 落回原来的页内偏移；任何位移都说明中间有记录被丢弃，此时中止。
	for _, r := range res.Records {
		if int64(r.LSN) < cut || int64(r.LSN) >= res.Tail.Offset {
			continue
		}
		if got := w.lsnAfter(len(r.Payload)); got != r.LSN {
			f.Close()
			return nil, res, fmt.Errorf("wal: replay would move record from lsn %d to %d", r.LSN, got)
		}
		if err := w.appendOne(r); err != nil {
			f.Close()
			return nil, res, err
		}
	}
	return w, res, nil
}

// pageStartsWithContinuation 报告第 pageNo 页是否是某个跨页记录的中段。
// 页不存在（部分尾页）或校验失败时返回 false：这类页本来就会被截掉。
func pageStartsWithContinuation(f *os.File, pageNo uint32) (bool, error) {
	buf := make([]byte, PageSize)
	n, err := f.ReadAt(buf, int64(pageNo)*PageSize)
	if n < PageSize {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return false, nil
		}
		return false, err
	}
	if err != nil {
		return false, err
	}
	if kind, _ := validatePage(buf, pageNo); kind != 0 {
		return false, nil
	}
	used := int(beUint16(buf[8:10]))
	return used > 0 && buf[HeaderSize] == continuationMarker, nil
}

// Append 追加若干记录。传入的 Record.LSN 会被忽略，LSN 由写入位置决定。
//
// 一次 Append 先做大小校验再写入，因此超限记录不会留下半批数据。
func (w *Writer) Append(recs ...Record) error {
	if w.closed {
		return fmt.Errorf("wal: writer closed")
	}
	limit := w.op.limit()
	for _, r := range recs {
		if len(r.Payload) > limit {
			return fmt.Errorf("wal: record payload %d bytes exceeds limit %d", len(r.Payload), limit)
		}
	}
	for _, r := range recs {
		if err := w.appendOne(r); err != nil {
			return err
		}
	}
	return nil
}

// lsnAfter 预测下一条给定 payload 长度的记录会被放在哪里，与 appendOne 的
// 换页规则严格一致。
func (w *Writer) lsnAfter(payloadLen int) uint64 {
	need := RecordHeaderSize + payloadLen
	rem := PageSize - w.off
	switch {
	case need <= rem:
		return uint64(w.written) + uint64(w.off)
	case need <= PageSize-HeaderSize:
		return uint64(w.written) + PageSize
	case rem < FragmentHeaderSize:
		return uint64(w.written) + PageSize
	default:
		return uint64(w.written) + uint64(w.off)
	}
}

func (w *Writer) appendOne(r Record) error {
	need := RecordHeaderSize + len(r.Payload)
	rem := PageSize - w.off

	if need <= rem {
		putNormal(w.page, w.off, r)
		w.off += need
		return nil
	}

	// 空页放得下，但当前页剩余空间不够：补齐当前页再换页。
	if need <= PageSize-HeaderSize {
		if err := w.flushPage(); err != nil {
			return err
		}
		putNormal(w.page, w.off, r)
		w.off += need
		return nil
	}

	// 单页放不下：拆成起始片段 + 若干续页片段。
	if rem < FragmentHeaderSize {
		if err := w.flushPage(); err != nil {
			return err
		}
	}
	payload := r.Payload
	putFragmentStart(w.page, w.off, r, len(payload))
	avail := PageSize - (w.off + FragmentHeaderSize)
	n := copy(w.page[w.off+FragmentHeaderSize:], payload[:min(avail, len(payload))])
	payload = payload[n:]
	w.off += FragmentHeaderSize + n
	if err := w.flushPage(); err != nil {
		return err
	}

	for len(payload) > 0 {
		room := PageSize - HeaderSize - ContinuationHeaderSize
		if len(payload) <= room {
			dataOff := putContinuation(w.page, HeaderSize, len(payload))
			copy(w.page[dataOff:], payload)
			w.off = dataOff + len(payload)
			return nil
		}
		dataOff := putContinuation(w.page, HeaderSize, room)
		copy(w.page[dataOff:], payload[:room])
		payload = payload[room:]
		w.off = PageSize
		if err := w.flushPage(); err != nil {
			return err
		}
	}
	return nil
}

// flushPage 把当前页补齐页头与 CRC 后落盘。
//
// 只有确实写了数据才落盘并推进页号：空页不写，避免页号空洞（否则读取端
// 会看到页号跳跃而误判整页损坏）。
func (w *Writer) flushPage() error {
	if w.off == HeaderSize {
		return nil
	}
	sealPage(w.page, w.off)
	if _, err := w.w.Write(w.page); err != nil {
		return err
	}
	w.written += PageSize
	w.resetPage()
	return nil
}

func (w *Writer) resetPage() {
	w.pageNo++
	w.page = newPage(w.pageNo)
	w.off = HeaderSize
}

// Flush 把当前内存中的部分页封口并落盘。之后 Append 会从下一页开始。
func (w *Writer) Flush() error {
	if w.closed {
		return fmt.Errorf("wal: writer closed")
	}
	return w.flushPage()
}

// Sync 落盘并 fsync（非文件目标只做 Flush）。
func (w *Writer) Sync() error {
	if err := w.Flush(); err != nil {
		return err
	}
	if w.f == nil {
		return nil
	}
	return w.f.Sync()
}

// Close 刷出剩余数据、fsync 并关闭文件。对空文件会写一个空页，保持
// “WAL 文件至少有一页”的既有约定。
func (w *Writer) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	if err := w.flushPage(); err != nil {
		w.closeFile()
		return err
	}
	if w.written == 0 {
		sealPage(w.page, HeaderSize)
		if _, err := w.w.Write(w.page); err != nil {
			w.closeFile()
			return err
		}
		w.written += PageSize
	}
	if w.f != nil {
		if err := w.f.Sync(); err != nil {
			w.f.Close()
			return err
		}
		return w.f.Close()
	}
	return nil
}

func (w *Writer) closeFile() {
	if w.f != nil {
		w.f.Close()
	}
}

// Pages 返回已经落盘（含 Close 时写出）的页数。
func (w *Writer) Pages() uint32 { return w.pageNo }

// NextLSN 返回若下一条记录长 payloadLen 将得到的 LSN。
func (w *Writer) NextLSN(payloadLen int) uint64 { return w.lsnAfter(payloadLen) }

func beUint16(b []byte) uint16 { return uint16(b[0])<<8 | uint16(b[1]) }

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
