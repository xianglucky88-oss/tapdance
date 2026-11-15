package wal

import (
	"path/filepath"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.bin")

	in := []Record{
		{XID: 1, Type: TypeBegin},
		{XID: 1, Type: TypeInsert, Payload: []byte(`{"table":"t","pk":"1"}`)},
		{XID: 1, Type: TypeCommit},
	}
	if err := WriteFile(path, in); err != nil {
		t.Fatal(err)
	}

	out, err := ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != len(in) {
		t.Fatalf("record count: got %d want %d", len(out), len(in))
	}
	if out[1].Type != TypeInsert || string(out[1].Payload) != `{"table":"t","pk":"1"}` {
		t.Fatalf("payload mismatch: %+v", out[1])
	}
	if out[2].LSN <= out[1].LSN {
		t.Fatal("LSN should increase within a page")
	}
}

func TestChecksumDetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.bin")
	if err := WriteFile(path, []Record{{XID: 1, Type: TypeBegin}, {XID: 1, Type: TypeCommit}}); err != nil {
		t.Fatal(err)
	}

	raw, err := osReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[HeaderSize+2] ^= 0xFF
	if err := osWriteFile(path, raw); err != nil {
		t.Fatal(err)
	}

	if _, err := ReadFile(path); err == nil {
		t.Fatal("expected checksum error")
	}
}

func TestEncodePageOverflow(t *testing.T) {
	big := Record{XID: 1, Type: TypeInsert, Payload: make([]byte, PageSize)}
	page, rest := EncodePage(0, []Record{big})
	if len(page) != PageSize {
		t.Fatalf("page size %d", len(page))
	}
	if len(rest) != 1 {
		t.Fatalf("oversized record should be returned, got %d", len(rest))
	}
}
