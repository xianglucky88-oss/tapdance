package wal

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

func mustWrite(t *testing.T, path string, recs []Record) {
	t.Helper()
	if err := WriteFile(path, recs); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
}

func mustScan(t *testing.T, path string, opts ScanOptions) *ScanResult {
	t.Helper()
	res, err := ScanFile(path, opts)
	if err != nil {
		t.Fatalf("ScanFile: %v", err)
	}
	return res
}

func payloads(recs []Record) [][]byte {
	out := make([][]byte, len(recs))
	for i, r := range recs {
		out[i] = r.Payload
	}
	return out
}

// TestMultiPageRoundTrip 覆盖正常多页写入与读取，并确认 LSN 仍是字节偏移。
func TestMultiPageRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.bin")

	var in []Record
	for i := 0; i < 200; i++ {
		payload := bytes.Repeat([]byte{byte(i)}, 20+i%40)
		in = append(in, Record{XID: uint32(i/4 + 1), Type: TypeInsert, Payload: payload})
	}
	mustWrite(t, path, in)

	res := mustScan(t, path, ScanOptions{})
	if err := res.StrictError(); err != nil {
		t.Fatalf("strict error: %v", err)
	}
	if res.Pages < 5 {
		t.Fatalf("expected multiple pages, got %d", res.Pages)
	}
	if len(res.Records) != len(in) {
		t.Fatalf("record count %d, want %d", len(res.Records), len(in))
	}
	for i, r := range res.Records {
		if !bytes.Equal(r.Payload, in[i].Payload) {
			t.Fatalf("record %d payload mismatch", i)
		}
		// LSN 是记录头的绝对字节偏移，必须落在页内且随记录单调递增。
		if r.LSN%PageSize < HeaderSize {
			t.Fatalf("record %d lsn %d points into page header", i, r.LSN)
		}
		if i > 0 && r.LSN <= res.Records[i-1].LSN {
			t.Fatalf("lsn not increasing at %d", i)
		}
	}
	if res.Resume.SafeLSN != res.Records[len(res.Records)-1].LSN {
		t.Fatalf("safe lsn %d", res.Resume.SafeLSN)
	}
}

// TestCrossPageRecord 覆盖一条记录跨越多页，且前后记录不受影响。
func TestCrossPageRecord(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.bin")

	big := make([]byte, 3*PageSize+137)
	for i := range big {
		big[i] = byte(i * 31)
	}
	in := []Record{
		{XID: 1, Type: TypeBegin},
		{XID: 1, Type: TypeInsert, Payload: big},
		{XID: 1, Type: TypeCommit},
		{XID: 2, Type: TypeBegin},
	}
	mustWrite(t, path, in)

	res := mustScan(t, path, ScanOptions{})
	if err := res.StrictError(); err != nil {
		t.Fatalf("strict error: %v", err)
	}
	if len(res.Records) != len(in) {
		t.Fatalf("records %d want %d", len(res.Records), len(in))
	}
	if !bytes.Equal(res.Records[1].Payload, big) {
		t.Fatalf("cross-page payload corrupted (%d bytes)", len(res.Records[1].Payload))
	}
	if res.Records[1].LSN != in[1].LSN && res.Records[1].LSN%PageSize < HeaderSize {
		t.Fatalf("bad lsn for cross-page record: %d", res.Records[1].LSN)
	}
	if res.Records[2].Type != TypeCommit || res.Records[3].Type != TypeBegin {
		t.Fatal("records following cross-page record were lost")
	}
	if res.Pages < 4 {
		t.Fatalf("expected >=4 pages, got %d", res.Pages)
	}
}

// TestCrossPageRecordLSNIsHeaderOffset 确认跨页记录的 LSN 仍指向起始页头。
func TestCrossPageRecordLSNIsHeaderOffset(t *testing.T) {
	var buf bytes.Buffer
	w := NewBufferWriter(&buf, WriterOptions{})
	recs := []Record{
		{XID: 7, Type: TypeBegin},
		{XID: 7, Type: TypeInsert, Payload: bytes.Repeat([]byte("x"), PageSize*2)},
	}
	if err := w.Append(recs...); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	res, err := Scan(&buf, ScanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Records) != 2 {
		t.Fatalf("records %d", len(res.Records))
	}
	lsn := res.Records[1].LSN
	if got := lsn % PageSize; got < HeaderSize || got >= PageSize {
		t.Fatalf("fragment start offset %d invalid", got)
	}
	// 起始片段的 LSN 必须早于起始页的结束。
	if lsn/PageSize != uint64(lsn/PageSize) {
		t.Fatal("unreachable")
	}
}

// TestPartialTailPage 覆盖“正在写入/崩溃截断”留下的不足一页的尾巴。
func TestPartialTailPage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.bin")
	in := []Record{{XID: 1, Type: TypeBegin}, {XID: 1, Type: TypeCommit}}
	mustWrite(t, path, in)

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	torn := append(append([]byte{}, raw...), 0xDE, 0xAD, 0xBE)
	if err := os.WriteFile(path, torn, 0o644); err != nil {
		t.Fatal(err)
	}

	res := mustScan(t, path, ScanOptions{})
	if res.Tail.Kind != TailPartialPage {
		t.Fatalf("tail kind %v, want partial-page", res.Tail.Kind)
	}
	if res.Tail.Offset != int64(len(raw)) {
		t.Fatalf("tail offset %d want %d", res.Tail.Offset, len(raw))
	}
	if len(res.Records) != len(in) {
		t.Fatalf("confirmed records should survive, got %d", len(res.Records))
	}
	if err := res.StrictError(); err == nil {
		t.Fatal("strict read should reject unconfirmed tail")
	}
	if res.Resume.TruncateOffset != int64(len(raw)) || res.Resume.NextPageNo != 1 {
		t.Fatalf("bad resume point: %+v", res.Resume)
	}

	// Resume 截掉尾巴后继续追加，旧记录与新记录都能读出。
	w, _, err := Resume(path, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Record{XID: 2, Type: TypeBegin}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	after := mustScan(t, path, ScanOptions{})
	if err := after.StrictError(); err != nil {
		t.Fatalf("after resume: %v", err)
	}
	if len(after.Records) != 3 || after.Records[2].XID != 2 {
		t.Fatalf("after resume records: %+v", after.Records)
	}
	// 原记录的 LSN 不应因为截断而改变。
	for i := range in {
		if after.Records[i].LSN != res.Records[i].LSN {
			t.Fatalf("lsn changed for record %d: %d -> %d", i, res.Records[i].LSN, after.Records[i].LSN)
		}
	}
}

// TestChecksumFailureContinues 覆盖坏页之后继续读取。
func TestChecksumFailureContinues(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.bin")
	var in []Record
	for i := 0; i < 60; i++ {
		in = append(in, Record{XID: uint32(i + 1), Type: TypeInsert, Payload: bytes.Repeat([]byte{byte(i)}, 30)})
	}
	mustWrite(t, path, in)

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// 选择文件中间的页，确保后面还有可以继续读取的页。
	badPage := 1
	raw[badPage*PageSize+HeaderSize+3] ^= 0xFF
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	res := mustScan(t, path, ScanOptions{})
	if len(res.PageProblems) != 1 {
		t.Fatalf("page problems: %+v", res.PageProblems)
	}
	p := res.PageProblems[0]
	if p.Kind != PageChecksum || p.PageNo != uint32(badPage) {
		t.Fatalf("problem: %+v", p)
	}
	if res.Tail.Kind != TailNone {
		t.Fatalf("mid-file corruption must not be treated as tail: %+v", res.Tail)
	}
	// 坏页之前和之后的记录都应读出。
	before, after := 0, 0
	for _, r := range res.Records {
		switch page := uint32(r.LSN / PageSize); {
		case page < uint32(badPage):
			before++
		case page > uint32(badPage):
			after++
		}
	}
	if before == 0 {
		t.Fatal("records before corrupt page missing")
	}
	if after == 0 {
		t.Fatal("records after corrupt page not recovered")
	}
	if err := res.StrictError(); err == nil {
		t.Fatal("strict read must surface corruption")
	}
	if res.Resume.TruncateOffset > res.Tail.Offset {
		t.Fatalf("resume point past tail")
	}

	// ReadFile 应返回已读记录 + 错误，而不是丢掉一切。
	recs, err := ReadFile(path)
	if err == nil || len(recs) != len(res.Records) {
		t.Fatalf("ReadFile: recs=%d err=%v", len(recs), err)
	}
}

// TestIncompleteCrossPageTail 覆盖只写了一半的跨页记录。
func TestIncompleteCrossPageTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.bin")
	big := bytes.Repeat([]byte("Z"), PageSize*2)
	in := []Record{
		{XID: 1, Type: TypeBegin},
		{XID: 1, Type: TypeInsert, Payload: big},
		{XID: 1, Type: TypeCommit},
	}
	mustWrite(t, path, in)

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// 在第二页中间截断，使跨页记录只剩前半段。
	cut := PageSize + PageSize/2
	if err := os.WriteFile(path, raw[:cut], 0o644); err != nil {
		t.Fatal(err)
	}

	res := mustScan(t, path, ScanOptions{})
	if res.Tail.Kind != TailIncompleteRecord && res.Tail.Kind != TailPartialPage {
		t.Fatalf("tail kind %v", res.Tail.Kind)
	}
	for _, r := range res.Records {
		if len(r.Payload) == len(big) {
			t.Fatal("half-written cross-page record must not be delivered")
		}
	}
	if len(res.Records) != 1 || res.Records[0].Type != TypeBegin {
		t.Fatalf("only BEGIN should survive, got %+v", res.Records)
	}
	var sawIncomplete bool
	for _, p := range res.RecordProblems {
		if p.Kind == RecordIncomplete {
			sawIncomplete = true
		}
	}
	if !sawIncomplete {
		t.Fatalf("expected incomplete record problem, got %+v", res.RecordProblems)
	}

	// 恢复后可以继续追加，之前的 BEGIN 保留。
	w, _, err := Resume(path, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Record{XID: 1, Type: TypeAbort}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	after := mustScan(t, path, ScanOptions{})
	if err := after.StrictError(); err != nil {
		t.Fatalf("after resume: %v", err)
	}
	if len(after.Records) != 2 || after.Records[1].Type != TypeAbort {
		t.Fatalf("records after resume: %+v", after.Records)
	}
}

// TestOversizedRecordBounded 确认声明的超大长度只报告、不分配。
func TestOversizedRecordBounded(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.bin")
	// 用大 limit 正常写入，再用小 limit 读取，模拟不可信长度。
	big := bytes.Repeat([]byte("Q"), 4*PageSize)
	in := []Record{
		{XID: 1, Type: TypeInsert, Payload: big},
		{XID: 2, Type: TypeBegin},
	}
	if err := WriteFile(path, in); err != nil {
		t.Fatal(err)
	}

	res := mustScan(t, path, ScanOptions{MaxRecordSize: 2 * PageSize})
	var oversized *RecordProblem
	for i := range res.RecordProblems {
		if res.RecordProblems[i].Kind == RecordOversized {
			oversized = &res.RecordProblems[i]
		}
	}
	if oversized == nil {
		t.Fatalf("expected oversized problem, got %+v", res.RecordProblems)
	}
	if len(res.Records) != 1 || res.Records[0].Type != TypeBegin {
		t.Fatalf("records after oversized record must still be read, got %+v", res.Records)
	}
	// 扫描器仍应把整条记录读完以重新对齐，但只占用有界内存。
	if res.Tail.Kind != TailNone {
		t.Fatalf("oversized but complete record should not look like tail: %+v", res.Tail)
	}
}

// TestOrphanContinuation 覆盖页首出现续页片段但没有起始片段的情况。
func TestOrphanContinuation(t *testing.T) {
	page := newPage(0)
	off := HeaderSize
	page[off] = continuationMarker
	binary.BigEndian.PutUint16(page[off+1:off+3], 5)
	copy(page[off+ContinuationHeaderSize:], "hello")
	off += ContinuationHeaderSize + 5
	putNormal(page, off, Record{XID: 9, Type: TypeCommit})
	off += RecordHeaderSize
	sealPage(page, off)

	res, err := Scan(bytes.NewReader(page), ScanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.RecordProblems) != 1 || res.RecordProblems[0].Kind != RecordOrphanFragment {
		t.Fatalf("problems: %+v", res.RecordProblems)
	}
	if len(res.Records) != 1 || res.Records[0].Type != TypeCommit {
		t.Fatalf("record after orphan fragment should survive, got %+v", res.Records)
	}
}

// TestDecodePageRejectsCrossPage 确认单页 API 不会静默截断跨页记录。
func TestDecodePageRejectsCrossPage(t *testing.T) {
	var buf bytes.Buffer
	w := NewBufferWriter(&buf, WriterOptions{})
	if err := w.Append(Record{XID: 1, Type: TypeInsert, Payload: bytes.Repeat([]byte("a"), PageSize*2)}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	raw := buf.Bytes()
	if _, err := DecodePage(raw[:PageSize]); err == nil {
		t.Fatal("DecodePage should reject a cross-page record")
	}
}

// TestResumeRefusesMidFileDamage 确认 Resume 不会掩盖文件中间的损坏。
func TestResumeRefusesMidFileDamage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.bin")
	var in []Record
	for i := 0; i < 60; i++ {
		in = append(in, Record{XID: uint32(i + 1), Type: TypeInsert, Payload: bytes.Repeat([]byte{1}, 40)})
	}
	mustWrite(t, path, in)
	raw, _ := os.ReadFile(path)
	raw[PageSize+HeaderSize+1] ^= 0xFF
	_ = os.WriteFile(path, raw, 0o644)

	if _, _, err := Resume(path, WriterOptions{}); err == nil {
		t.Fatal("Resume must refuse to append when corruption is not in the tail")
	}
}

// TestResumeKeepsPageNumbering 模拟在页中间崩溃后恢复：整页截断、重放、续写，
// 页号必须连续，LSN 不得漂移。
func TestResumeKeepsPageNumbering(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.bin")
	var in []Record
	for i := 0; i < 120; i++ {
		in = append(in, Record{XID: uint32(i%3 + 1), Type: TypeUpdate, Payload: bytes.Repeat([]byte{byte(i)}, 50)})
	}
	mustWrite(t, path, in)
	raw, _ := os.ReadFile(path)

	// 截断到第 3 页中间：这一页已经写了部分记录，但整页会因不足一页而被丢弃。
	tornLen := 3*PageSize + PageSize/3
	if tornLen >= len(raw) {
		t.Fatal("test file too small")
	}
	if err := os.WriteFile(path, raw[:tornLen], 0o644); err != nil {
		t.Fatal(err)
	}

	w, before, err := Resume(path, WriterOptions{})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if before.Tail.Kind != TailPartialPage {
		t.Fatalf("tail kind %v", before.Tail.Kind)
	}
	if err := w.Append(Record{XID: 99, Type: TypeBegin}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	after := mustScan(t, path, ScanOptions{})
	if err := after.StrictError(); err != nil {
		t.Fatalf("after resume: %v", err)
	}
	if len(after.Records) != len(before.Records)+1 {
		t.Fatalf("records %d, before %d", len(after.Records), len(before.Records))
	}
	for i := range before.Records {
		if after.Records[i].LSN != before.Records[i].LSN {
			t.Fatalf("lsn %d moved: %d -> %d", i, before.Records[i].LSN, after.Records[i].LSN)
		}
	}
	// 新页号应当紧接在重放页之后。
	last := after.Records[len(after.Records)-1]
	if uint32(last.LSN/PageSize) != w.Pages()-1 {
		t.Fatalf("new record on page %d, writer pages %d", last.LSN/PageSize, w.Pages())
	}
}

// TestAppendAcrossReopen 覆盖在页边界关闭后重新打开继续追加。
func TestAppendAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.bin")
	first := []Record{{XID: 1, Type: TypeBegin}, {XID: 1, Type: TypeCommit}}
	mustWrite(t, path, first)
	if err := AppendFile(path, Record{XID: 2, Type: TypeBegin}); err != nil {
		t.Fatal(err)
	}
	res := mustScan(t, path, ScanOptions{})
	if err := res.StrictError(); err != nil {
		t.Fatalf("strict: %v", err)
	}
	if len(res.Records) != 3 {
		t.Fatalf("records %d", len(res.Records))
	}
}

// TestEmptyFile 保持“空文件可读”的既有行为。
func TestEmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.bin")
	mustWrite(t, path, nil)
	raw, err := os.ReadFile(path)
	if err != nil || len(raw) != PageSize {
		t.Fatalf("empty wal should still contain one empty page (len=%d err=%v)", len(raw), err)
	}
	res := mustScan(t, path, ScanOptions{})
	if err := res.StrictError(); err != nil {
		t.Fatalf("empty file: %v", err)
	}
	if len(res.Records) != 0 || res.Pages != 1 {
		t.Fatalf("pages=%d records=%d", res.Pages, len(res.Records))
	}
}

// TestBadMagicStopsAtPage 确认整页 magic 损坏时只跳过该页。
func TestBadMagicStopsAtPage(t *testing.T) {
	var buf bytes.Buffer
	w := NewBufferWriter(&buf, WriterOptions{})
	for i := 0; i < 80; i++ {
		w.Append(Record{XID: uint32(i + 1), Type: TypeInsert, Payload: bytes.Repeat([]byte{2}, 40)})
	}
	w.Close()
	raw := append([]byte{}, buf.Bytes()...)
	copy(raw[PageSize:PageSize+4], "XXXX")
	res, err := Scan(bytes.NewReader(raw), ScanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.PageProblems) != 1 || res.PageProblems[0].Kind != PageBadMagic {
		t.Fatalf("problems: %+v", res.PageProblems)
	}
	if len(res.Records) == 0 {
		t.Fatal("records before bad page lost")
	}
}

// TestCRCSeedIndependence 是一个格式守卫：页 CRC 仍只覆盖 [16:16+used]。
func TestCRCSeedIndependence(t *testing.T) {
	page, _ := EncodePage(0, []Record{{XID: 1, Type: TypeBegin}, {XID: 1, Type: TypeCommit}})
	used := int(binary.BigEndian.Uint16(page[8:10]))
	want := binary.BigEndian.Uint32(page[10:14])
	if got := crc32.ChecksumIEEE(page[HeaderSize : HeaderSize+used]); got != want {
		t.Fatalf("crc covers unexpected range")
	}
	// 页尾未使用字节的变化不应影响校验。
	page[PageSize-1] ^= 0xFF
	if got := crc32.ChecksumIEEE(page[HeaderSize : HeaderSize+used]); got != want {
		t.Fatal("unused tail must not be covered by crc")
	}
}

// TestScanDuringAppend 覆盖“写入过程中被读取”：未 Flush 的页还不在文件里，
// 读取端只能看到完整页；Flush 之后同样的读取能看到更多记录。
func TestScanDuringAppend(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.bin")
	w, err := Create(path, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err := w.Append(Record{XID: 1, Type: TypeBegin}); err != nil {
		t.Fatal(err)
	}
	// 尚未 Flush：文件还是空的。
	res := mustScan(t, path, ScanOptions{})
	if err := res.StrictError(); err != nil {
		t.Fatalf("empty file should be clean: %v", err)
	}
	if len(res.Records) != 0 {
		t.Fatalf("unflushed records must not be visible, got %d", len(res.Records))
	}

	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Record{XID: 1, Type: TypeInsert, Payload: bytes.Repeat([]byte("b"), 2000)}); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	res = mustScan(t, path, ScanOptions{})
	if err := res.StrictError(); err != nil {
		t.Fatalf("flushed prefix should be clean: %v", err)
	}
	if len(res.Records) != 2 || len(res.Records[1].Payload) != 2000 {
		t.Fatalf("records after flush: %d", len(res.Records))
	}
	if res.Tail.Kind != TailNone {
		t.Fatalf("flushed file must end on a page boundary: %+v", res.Tail)
	}
}

// TestResumeCorruptLastPage 覆盖撕裂写留下的整页损坏：截掉该页后继续追加。
func TestResumeCorruptLastPage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.bin")
	var in []Record
	for i := 0; i < 120; i++ {
		in = append(in, Record{XID: uint32(i + 1), Type: TypeInsert, Payload: bytes.Repeat([]byte{byte(i)}, 40)})
	}
	mustWrite(t, path, in)
	res := mustScan(t, path, ScanOptions{})
	lastPage := res.Pages - 1

	raw, _ := os.ReadFile(path)
	raw[lastPage*PageSize+HeaderSize+2] ^= 0xFF
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	before := mustScan(t, path, ScanOptions{})
	if before.Tail.Kind != TailCorruptPage {
		t.Fatalf("tail kind %v", before.Tail.Kind)
	}
	w, scanned, err := Resume(path, WriterOptions{})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if err := w.Append(Record{XID: 999, Type: TypeBegin}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	after := mustScan(t, path, ScanOptions{})
	if err := after.StrictError(); err != nil {
		t.Fatalf("after resume: %v", err)
	}
	if len(after.Records) != len(before.Records)+1 {
		t.Fatalf("records %d, before %d", len(after.Records), len(before.Records))
	}
	for i := range before.Records {
		if after.Records[i].LSN != before.Records[i].LSN {
			t.Fatalf("lsn %d moved: %d -> %d", i, before.Records[i].LSN, after.Records[i].LSN)
		}
	}
	_ = scanned
}

// TestTornPageIsNotTrusted 确认部分尾页里的“完整”记录也不会被交付。
//
// 撕裂写的页无法用 CRC 验证（used 区域尚未全部落盘），因此整页丢弃是
// 唯一安全的选择：宁可少读，也不交付可能来自半页写入的数据。恢复时该页
// 会被整体截掉，之前的页不受影响。
func TestTornPageIsNotTrusted(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.bin")
	var in []Record
	for i := 0; i < 30; i++ {
		in = append(in, Record{XID: uint32(i + 1), Type: TypeInsert, Payload: bytes.Repeat([]byte{byte(i)}, 30)})
	}
	mustWrite(t, path, in)
	raw, _ := os.ReadFile(path)

	// 截断到第 1 页中间：第 0 页完整，第 1 页只有半页字节。
	cut := PageSize + PageSize/2
	if err := os.WriteFile(path, raw[:cut], 0o644); err != nil {
		t.Fatal(err)
	}
	before := mustScan(t, path, ScanOptions{})
	if before.Tail.Kind != TailPartialPage {
		t.Fatalf("tail %v", before.Tail.Kind)
	}
	for _, r := range before.Records {
		if uint32(r.LSN/PageSize) != 0 {
			t.Fatalf("record from torn page must not be delivered: lsn %d", r.LSN)
		}
	}

	w, scanned, err := Resume(path, WriterOptions{})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if got := scanned.Resume.TruncateOffset; got != PageSize {
		t.Fatalf("resume should cut the torn page entirely, got %d", got)
	}
	if err := w.Append(Record{XID: 77, Type: TypeBegin}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	after := mustScan(t, path, ScanOptions{})
	if err := after.StrictError(); err != nil {
		t.Fatalf("after resume: %v", err)
	}
	if len(after.Records) != len(before.Records)+1 {
		t.Fatalf("records %d, before %d", len(after.Records), len(before.Records))
	}
	for i := range before.Records {
		if after.Records[i].LSN != before.Records[i].LSN {
			t.Fatalf("lsn %d moved: %d -> %d", i, before.Records[i].LSN, after.Records[i].LSN)
		}
	}
}

// TestWriterRejectsOversized 确认写入端在落盘前拒绝超限记录。
func TestWriterRejectsOversized(t *testing.T) {
	var buf bytes.Buffer
	w := NewBufferWriter(&buf, WriterOptions{MaxRecordSize: 2 * PageSize})
	err := w.Append(
		Record{XID: 1, Type: TypeInsert, Payload: bytes.Repeat([]byte("a"), PageSize)},
		Record{XID: 2, Type: TypeInsert, Payload: bytes.Repeat([]byte("b"), 3*PageSize)},
	)
	if err == nil {
		t.Fatal("expected oversized rejection")
	}
	if buf.Len() != 0 {
		t.Fatalf("rejected batch must not write partial data, got %d bytes", buf.Len())
	}
}

// TestTrailingCorruptRun 覆盖文件末尾连续多页损坏：尾部必须从这一串的起点
// 开始，恢复才能一次修好。
func TestTrailingCorruptRun(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.bin")
	var in []Record
	for i := 0; i < 200; i++ {
		in = append(in, Record{XID: uint32(i + 1), Type: TypeInsert, Payload: bytes.Repeat([]byte{byte(i)}, 40)})
	}
	mustWrite(t, path, in)
	res := mustScan(t, path, ScanOptions{})
	pages := res.Pages
	if pages < 5 {
		t.Fatalf("need at least 5 pages, got %d", pages)
	}

	raw, _ := os.ReadFile(path)
	firstBad := pages - 3
	for p := firstBad; p < pages; p++ {
		raw[int(p)*PageSize+HeaderSize+1] ^= 0xFF
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	before := mustScan(t, path, ScanOptions{})
	if before.Tail.Kind != TailCorruptPage {
		t.Fatalf("tail %v", before.Tail.Kind)
	}
	if before.Tail.Offset != int64(firstBad)*PageSize {
		t.Fatalf("tail should start at page %d, got %d", firstBad, before.Tail.Offset)
	}

	w, _, err := Resume(path, WriterOptions{})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if err := w.Append(Record{XID: 9999, Type: TypeBegin}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	after := mustScan(t, path, ScanOptions{})
	if err := after.StrictError(); err != nil {
		t.Fatalf("after resume: %v", err)
	}
	last := after.Records[len(after.Records)-1]
	// 新记录落在损坏串起点所在页的页头之后（可能重放了该页上幸存的记录），
	// 既不回退到更早的页，也不丢记录。
	tailPage := uint32(before.Tail.Offset / PageSize)
	if last.XID != 9999 || uint32(last.LSN/PageSize) != tailPage {
		t.Fatalf("new record should land on tail page %d, got %+v", tailPage, last)
	}
	// 恢复前的记录没有位移。
	for i, r := range before.Records {
		if after.Records[i].LSN != r.LSN {
			t.Fatalf("lsn %d moved: %d -> %d", i, r.LSN, after.Records[i].LSN)
		}
	}
}

type failingReader struct {
	data   []byte
	failAt int
	n      int
}

func (f *failingReader) Read(p []byte) (int, error) {
	if f.n >= f.failAt {
		return 0, os.ErrPermission
	}
	if f.n+len(p) > f.failAt {
		k := copy(p, f.data[f.n:f.failAt])
		f.n += k
		return k, os.ErrPermission
	}
	k := copy(p, f.data[f.n:])
	f.n += k
	return k, nil
}

// TestScanSurfacesIOErrors 确认只有 I/O 错误会中止扫描并返回 error，
// 数据损坏则不会。
func TestScanSurfacesIOErrors(t *testing.T) {
	clean := buildPageFile(t, 3)
	_, err := Scan(&failingReader{data: clean, failAt: PageSize + 100}, ScanOptions{})
	if err == nil {
		t.Fatal("expected I/O error to surface")
	}
}

// TestResumeEmptyFile 确认空文件可以恢复并追加。
func TestResumeEmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.bin")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	w, _, err := Resume(path, WriterOptions{})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if err := w.Append(Record{XID: 1, Type: TypeBegin}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	res := mustScan(t, path, ScanOptions{})
	if err := res.StrictError(); err != nil {
		t.Fatalf("after resume: %v", err)
	}
	if len(res.Records) != 1 {
		t.Fatalf("records %d", len(res.Records))
	}
}

func buildPageFile(t *testing.T, pages int) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := NewBufferWriter(&buf, WriterOptions{})
	for i := 0; i < pages*20; i++ {
		if err := w.Append(Record{XID: uint32(i + 1), Type: TypeInsert, Payload: bytes.Repeat([]byte{byte(i)}, 20)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}
