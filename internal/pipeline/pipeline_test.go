package pipeline

import (
	"testing"
	"time"

	"github.com/internal-platform/tapdance/internal/lag"
	"github.com/internal-platform/tapdance/internal/retention"
	"github.com/internal-platform/tapdance/internal/wal"
)

// batch 构造 n 个已提交事务，LSN 从 1000 起每条记录 +100。
// 返回记录与最高 LSN。
func batch(n int) ([]wal.Record, uint64) {
	var recs []wal.Record
	lsn := uint64(1000)
	next := func() uint64 { lsn += 100; return lsn }
	for i := 0; i < n; i++ {
		xid := uint32(i + 1)
		recs = append(recs,
			wal.Record{LSN: next(), XID: xid, Type: wal.TypeBegin},
			wal.Record{LSN: next(), XID: xid, Type: wal.TypeInsert,
				Payload: []byte(`{"table":"t","pk":"` + string(rune('a'+i)) + `","row":{"id":"x"}}`)},
			wal.Record{LSN: next(), XID: xid, Type: wal.TypeCommit},
		)
	}
	return recs, lsn
}

func slotAt(name string, confirmed uint64) *retention.Slot {
	s := retention.NewSlot(name, 0)
	s.Advance(confirmed)
	return s
}

func TestSameBatchDifferentProgress(t *testing.T) {
	recs, last := batch(10) // last = 1000 + 30*100 = 4000
	if last != 4000 {
		t.Fatalf("last = %d, want 4000", last)
	}
	const rate = 100.0 // 兜底速率 100 B/s，便于手算

	// 同一批记录，三种消费进度：没开始 / 消费一半 / 已追平。
	runs := []struct {
		name        string
		confirmed   uint64
		wantBacklog uint64
		wantSegs    int
		wantLag     float64
		wantBasis   string
	}{
		{"nothing consumed", 0, 4000, 1, 40, "fallback"},
		{"half consumed", 2000, 2000, 1, 20, "fallback"},
		{"caught up", 4000, 0, 0, 0, "caught_up"},
		{"beyond current", 9999, 0, 0, 0, "caught_up"}, // current 不单调也不出负积压
	}

	var checksums []string
	for _, tc := range runs {
		t.Run(tc.name, func(t *testing.T) {
			rep := RunWith(recs, Options{
				Slots:       []*retention.Slot{slotAt("c", tc.confirmed)},
				FallbackBPS: rate,
			})
			checksums = append(checksums, rep.Checksum)

			if rep.LastLSN != last {
				t.Fatalf("last_lsn = %d, want %d", rep.LastLSN, last)
			}
			if rep.BacklogByte != tc.wantBacklog {
				t.Fatalf("backlog = %d, want %d", rep.BacklogByte, tc.wantBacklog)
			}
			if rep.BacklogSeg != tc.wantSegs {
				t.Fatalf("segments = %d, want %d", rep.BacklogSeg, tc.wantSegs)
			}
			if rep.LagBasis != tc.wantBasis {
				t.Fatalf("lag basis = %q, want %q", rep.LagBasis, tc.wantBasis)
			}
			if rep.LagSeconds != tc.wantLag {
				t.Fatalf("lag = %v s, want %v", rep.LagSeconds, tc.wantLag)
			}
			if rep.RetainFrom == nil || *rep.RetainFrom != tc.confirmed {
				t.Fatalf("retain_from = %v, want %d", rep.RetainFrom, tc.confirmed)
			}
			// 顶层数字必须与槽位明细一致
			if len(rep.Slots) != 1 {
				t.Fatalf("slots = %d", len(rep.Slots))
			}
			s := rep.Slots[0]
			if s.BacklogBytes != rep.BacklogByte || s.BacklogSegs != rep.BacklogSeg ||
				s.LagSeconds != rep.LagSeconds || s.LagBasis != rep.LagBasis {
				t.Fatalf("slot detail %+v inconsistent with top-level", s)
			}
			if s.ConfirmedLSN != tc.confirmed {
				t.Fatalf("slot confirmed = %d, want %d", s.ConfirmedLSN, tc.confirmed)
			}
			if s.Lagging != (tc.wantBacklog > 0) {
				t.Fatalf("lagging = %v, backlog = %d", s.Lagging, tc.wantBacklog)
			}
		})
	}

	// 消费进度不影响落库结果：同一批记录的 checksum 必须一致。
	for i := 1; i < len(checksums); i++ {
		if checksums[i] != checksums[0] {
			t.Fatalf("checksum changed with consumer progress: %q vs %q", checksums[i], checksums[0])
		}
	}
}

func TestNoSlotsReportsNoRetention(t *testing.T) {
	recs, _ := batch(3)
	rep := Run(recs)
	if rep.LagBasis != "no_slots" {
		t.Fatalf("basis = %q, want no_slots", rep.LagBasis)
	}
	if rep.BacklogByte != 0 || rep.BacklogSeg != 0 || rep.LagSeconds != 0 {
		t.Fatalf("no-slot report must not invent backlog: %+v", rep)
	}
	if rep.RetainFrom != nil {
		t.Fatalf("retain_from = %v, want nil", *rep.RetainFrom)
	}
	if len(rep.Slots) != 0 {
		t.Fatalf("slots = %v", rep.Slots)
	}
}

func TestNonMonotonicRecordsUseMaxLSN(t *testing.T) {
	recs, last := batch(3)
	// 打乱顺序：最后一条记录不是最高 LSN
	recs[0], recs[len(recs)-1] = recs[len(recs)-1], recs[0]
	rep := RunWith(recs, Options{Slots: []*retention.Slot{slotAt("c", 0)}})
	if rep.LastLSN != last {
		t.Fatalf("last_lsn = %d, want max %d", rep.LastLSN, last)
	}
	if rep.BacklogByte != last {
		t.Fatalf("backlog = %d, want %d", rep.BacklogByte, last)
	}
}

func TestMultipleSlotsShareRetentionBoundary(t *testing.T) {
	recs, last := batch(10) // last = 4000

	fast := slotAt("fast", 3000)
	slow := slotAt("slow", 1000)
	slow.Disconnect() // 断连的槽位照样钉住保留边界

	rep := RunWith(recs, Options{Slots: []*retention.Slot{fast, slow}, FallbackBPS: 100})

	if rep.RetainFrom == nil || *rep.RetainFrom != 1000 {
		t.Fatalf("retain_from = %v, want 1000", rep.RetainFrom)
	}
	if rep.BacklogByte != last-1000 {
		t.Fatalf("backlog = %d, want %d", rep.BacklogByte, last-1000)
	}
	if len(rep.Slots) != 2 {
		t.Fatalf("slots = %d", len(rep.Slots))
	}
	// 按名字排序：fast 在前
	if rep.Slots[0].Name != "fast" || rep.Slots[1].Name != "slow" {
		t.Fatalf("slot order: %q, %q", rep.Slots[0].Name, rep.Slots[1].Name)
	}
	if rep.Slots[0].State != "active" || rep.Slots[0].BacklogBytes != 1000 {
		t.Fatalf("fast slot: %+v", rep.Slots[0])
	}
	if rep.Slots[1].State != "disconnected" || rep.Slots[1].BacklogBytes != 3000 {
		t.Fatalf("slow slot: %+v", rep.Slots[1])
	}
	// 顶层延迟由最落后的槽位决定
	if rep.LagSeconds != 30 {
		t.Fatalf("lag = %v, want 30 (driven by slow slot)", rep.LagSeconds)
	}
}

func TestReconnectAndRegressionVisible(t *testing.T) {
	recs, _ := batch(5) // last = 2500

	flapping := slotAt("flapping", 1000)
	flapping.Disconnect()
	flapping.Reconnect()
	flapping.Advance(2000)
	flapping.Advance(1500) // 确认位点倒退：拒绝并计数

	rep := RunWith(recs, Options{Slots: []*retention.Slot{flapping}, FallbackBPS: 100})
	s := rep.Slots[0]
	if s.State != "reconnected" || s.Reconnects != 1 {
		t.Fatalf("state = %q reconnects = %d", s.State, s.Reconnects)
	}
	if s.Regressions != 1 {
		t.Fatalf("regressions = %d, want 1", s.Regressions)
	}
	if s.ConfirmedLSN != 2000 {
		t.Fatalf("confirmed = %d, want 2000 (regression rejected)", s.ConfirmedLSN)
	}
	if s.BacklogBytes != 500 {
		t.Fatalf("backlog = %d, want 500", s.BacklogBytes)
	}
}

func TestCheckpointRateBeatsFallback(t *testing.T) {
	recs, _ := batch(10) // last = 4000
	base := time.Unix(1_700_000_000, 0)
	cps := []lag.Checkpoint{
		{LSN: 0, At: base},
		{LSN: 1000, At: base.Add(10 * time.Second)}, // 100 B/s
		{LSN: 1000, At: base.Add(20 * time.Second)}, // 停顿：不计入
		{LSN: 2000, At: base.Add(30 * time.Second)}, // 100 B/s
	}
	rep := RunWith(recs, Options{
		Slots:       []*retention.Slot{slotAt("c", 2000)},
		Checkpoints: cps,
		FallbackBPS: 1, // 应被采样速率覆盖
	})
	if rep.LagBasis != "checkpoints" {
		t.Fatalf("basis = %q", rep.LagBasis)
	}
	// 有效速率 100 B/s（停顿被排除），积压 2000 → 20s
	if rep.LagSeconds != 20 {
		t.Fatalf("lag = %v, want 20", rep.LagSeconds)
	}
	if rep.Slots[0].LagRateBPS != 100 {
		t.Fatalf("rate = %v, want 100", rep.Slots[0].LagRateBPS)
	}
}

func TestUnknownLagWhenNoRateAtAll(t *testing.T) {
	recs, _ := batch(5)
	rep := RunWith(recs, Options{Slots: []*retention.Slot{slotAt("c", 0)}})
	if rep.LagBasis != "unknown" || rep.LagSeconds != -1 {
		t.Fatalf("basis = %q lag = %v", rep.LagBasis, rep.LagSeconds)
	}
	if rep.BacklogByte == 0 {
		t.Fatal("backlog must still be reported when lag is unknown")
	}
}
