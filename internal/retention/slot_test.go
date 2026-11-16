package retention

import (
	"math"
	"testing"
)

func TestAdvanceMonotonic(t *testing.T) {
	s := NewSlot("a", 100)
	s.Advance(300)
	if s.ConfirmedLSN != 300 || s.RestartLSN != 300 {
		t.Fatalf("advance: confirmed=%d restart=%d", s.ConfirmedLSN, s.RestartLSN)
	}
	s.Advance(300) // 同点推进是空操作
	if s.Regressions != 0 {
		t.Fatalf("same-point advance must not count as regression")
	}
}

func TestConfirmRegressionRejected(t *testing.T) {
	s := NewSlot("a", 100)
	s.Advance(500)
	s.Advance(300) // 确认位点倒退：拒绝并计数
	if s.ConfirmedLSN != 500 {
		t.Fatalf("confirmed moved backwards: %d", s.ConfirmedLSN)
	}
	if s.RestartLSN != 500 {
		t.Fatalf("restart moved backwards: %d", s.RestartLSN)
	}
	if s.Regressions != 1 {
		t.Fatalf("regressions = %d, want 1", s.Regressions)
	}
}

func TestDisconnectedSlotFrozen(t *testing.T) {
	s := NewSlot("a", 100)
	s.Advance(200)
	s.Disconnect()
	s.Advance(900) // 断连期间不能确认新位点
	if s.ConfirmedLSN != 200 {
		t.Fatalf("disconnected slot advanced to %d", s.ConfirmedLSN)
	}
	if s.State() != StateDisconnected {
		t.Fatalf("state = %q", s.State())
	}
}

func TestReconnectResumesAndIsVisible(t *testing.T) {
	s := NewSlot("a", 100)
	s.Disconnect()
	s.Reconnect()
	if !s.Active || s.Reconnects != 1 {
		t.Fatalf("active=%v reconnects=%d", s.Active, s.Reconnects)
	}
	if s.State() != StateReconnected {
		t.Fatalf("state = %q, want reconnected", s.State())
	}
	s.Reconnect() // 已在线，重复调用不计数
	if s.Reconnects != 1 {
		t.Fatalf("reconnects = %d, want 1", s.Reconnects)
	}
	s.Advance(400)
	if s.ConfirmedLSN != 400 {
		t.Fatalf("confirmed = %d after reconnect", s.ConfirmedLSN)
	}
}

func TestStateStrings(t *testing.T) {
	s := NewSlot("a", 0)
	if s.State() != StateActive {
		t.Fatalf("new slot state = %q", s.State())
	}
}

func TestBacklogWithNonMonotonicCurrent(t *testing.T) {
	s := NewSlot("a", 100)
	s.Advance(500)
	if got := s.Backlog(900); got != 400 {
		t.Fatalf("backlog = %d, want 400", got)
	}
	// current 倒挂（比确认位点还小）：积压为 0，不能回绕成巨大正数
	if got := s.Backlog(400); got != 0 {
		t.Fatalf("backlog with regressed current = %d, want 0", got)
	}
	if s.Lagging(900) != true || s.Lagging(400) != false {
		t.Fatal("lagging flag inconsistent with backlog")
	}
}

func TestSegmentsRounding(t *testing.T) {
	cases := []struct {
		bytes uint64
		want  int
	}{
		{0, 0},
		{1, 1},
		{SegmentSize - 1, 1},
		{SegmentSize, 1}, // 正好一段：不多算
		{SegmentSize + 1, 2},
		{2 * SegmentSize, 2},
		{2*SegmentSize + 1, 3},
	}
	for _, c := range cases {
		if got := Segments(c.bytes); got != c.want {
			t.Fatalf("Segments(%d) = %d, want %d", c.bytes, got, c.want)
		}
	}
}

func TestSegmentsOverflowSafe(t *testing.T) {
	// 接近 MaxUint64 的积压：(bytes + SegmentSize - 1) 会溢出回绕成小段数。
	got := Segments(math.MaxUint64)
	want := int(math.MaxUint64/SegmentSize) + 1
	if got != want {
		t.Fatalf("Segments(MaxUint64) = %d, want %d", got, want)
	}
	if Segments(math.MaxUint64-SegmentSize) != int((math.MaxUint64-SegmentSize)/SegmentSize)+1 {
		t.Fatal("boundary below MaxUint64 miscomputed")
	}
}

func TestSummarizeNoSlots(t *testing.T) {
	sum := Summarize(nil, 1000)
	if sum.Slots != 0 || sum.Backlog != 0 || sum.RetainFrom != 0 {
		t.Fatalf("empty summary = %+v", sum)
	}
}

func TestSummarizeMultipleSlotsShareBoundary(t *testing.T) {
	a := NewSlot("a", 0)
	a.Advance(800)
	b := NewSlot("b", 0)
	b.Advance(300)
	b.Disconnect() // 断连的槽位照样钉住保留边界

	sum := Summarize([]*Slot{a, b}, 1000)
	if sum.Slots != 2 {
		t.Fatalf("slots = %d", sum.Slots)
	}
	if sum.RetainFrom != 300 {
		t.Fatalf("retain from = %d, want 300 (laggard pins boundary)", sum.RetainFrom)
	}
	if sum.MinConfirmed != 300 {
		t.Fatalf("min confirmed = %d, want 300", sum.MinConfirmed)
	}
	if sum.Backlog != 700 {
		t.Fatalf("backlog = %d, want 700", sum.Backlog)
	}

	// 落后的槽位追平后，保留边界随之放开
	b.Reconnect()
	b.Advance(800)
	sum = Summarize([]*Slot{a, b}, 1000)
	if sum.RetainFrom != 800 || sum.Backlog != 200 {
		t.Fatalf("after catch-up: retain=%d backlog=%d", sum.RetainFrom, sum.Backlog)
	}
}

func TestSummarizeCurrentBelowConfirmed(t *testing.T) {
	a := NewSlot("a", 0)
	a.Advance(500)
	// current 不单调：比所有确认位点都小
	sum := Summarize([]*Slot{a}, 300)
	if sum.Backlog != 0 {
		t.Fatalf("backlog = %d, want 0", sum.Backlog)
	}
	if sum.RetainFrom != 500 {
		t.Fatalf("retain from = %d, want 500", sum.RetainFrom)
	}
}
