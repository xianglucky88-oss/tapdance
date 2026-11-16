package lag

import (
	"math"
	"testing"
	"time"
)

func at(sec int64) time.Time { return time.Unix(sec, 0) }

func TestRateBasic(t *testing.T) {
	r := Rate(Checkpoint{LSN: 0, At: at(0)}, Checkpoint{LSN: 1000, At: at(10)})
	if r != 100 {
		t.Fatalf("rate = %v, want 100", r)
	}
}

func TestRateDegenerate(t *testing.T) {
	if r := Rate(Checkpoint{LSN: 0, At: at(0)}, Checkpoint{LSN: 1000, At: at(0)}); r != 0 {
		t.Fatalf("zero-duration rate = %v", r)
	}
	if r := Rate(Checkpoint{LSN: 1000, At: at(0)}, Checkpoint{LSN: 500, At: at(10)}); r != 0 {
		t.Fatalf("regressing rate = %v", r)
	}
}

func TestSampleRateInsufficientCheckpoints(t *testing.T) {
	if _, ok := SampleRate(nil); ok {
		t.Fatal("no checkpoints must not produce a rate")
	}
	if _, ok := SampleRate([]Checkpoint{{LSN: 1, At: at(0)}}); ok {
		t.Fatal("single checkpoint must not produce a rate")
	}
}

func TestSampleRateSkipsPause(t *testing.T) {
	// 0→1000 用 10s，然后停顿 10s（LSN 不动），再 1000→2000 用 10s。
	// 停顿是上游没新 WAL，不是消费变慢：有效速率 = 2000B / 20s = 100B/s，
	// 而不是被停顿稀释的 2000B / 30s。
	cps := []Checkpoint{
		{LSN: 0, At: at(0)},
		{LSN: 1000, At: at(10)},
		{LSN: 1000, At: at(20)}, // 停顿区间
		{LSN: 2000, At: at(30)},
	}
	rate, ok := SampleRate(cps)
	if !ok {
		t.Fatal("expected a rate")
	}
	if rate != 100 {
		t.Fatalf("rate = %v, want 100 (pause excluded)", rate)
	}
}

func TestSampleRateSkipsNonMonotonic(t *testing.T) {
	cps := []Checkpoint{
		{LSN: 1000, At: at(0)},
		{LSN: 500, At: at(10)}, // 倒挂区间：跳过
		{LSN: 1500, At: at(20)},
	}
	rate, ok := SampleRate(cps)
	if !ok {
		t.Fatal("expected a rate")
	}
	if rate != 100 { // 只有 500→1500 这一段有效：1000B / 10s
		t.Fatalf("rate = %v, want 100", rate)
	}
}

func TestSampleRateAllPaused(t *testing.T) {
	cps := []Checkpoint{
		{LSN: 100, At: at(0)},
		{LSN: 100, At: at(10)},
	}
	if _, ok := SampleRate(cps); ok {
		t.Fatal("all-paused samples must not produce a rate")
	}
}

func TestEstimateCaughtUp(t *testing.T) {
	est := Estimate(500, 500, nil, 0)
	if est.Basis != BasisCaughtUp || est.Seconds() != 0 {
		t.Fatalf("est = %+v", est)
	}
	// current 倒挂也算追上
	est = Estimate(400, 500, nil, 0)
	if est.Basis != BasisCaughtUp {
		t.Fatalf("est = %+v", est)
	}
}

func TestEstimateFromCheckpoints(t *testing.T) {
	cps := []Checkpoint{
		{LSN: 0, At: at(0)},
		{LSN: 1000, At: at(10)}, // 100 B/s
	}
	est := Estimate(2000, 1000, cps, 0)
	if est.Basis != BasisCheckpoints {
		t.Fatalf("basis = %q", est.Basis)
	}
	if est.Rate != 100 {
		t.Fatalf("rate = %v", est.Rate)
	}
	if est.Seconds() != 10 { // 1000B 积压 / 100B/s
		t.Fatalf("lag = %v s", est.Seconds())
	}
	if est.Backlog != 1000 {
		t.Fatalf("backlog = %d", est.Backlog)
	}
}

func TestEstimateFallback(t *testing.T) {
	est := Estimate(2000, 1000, nil, 250)
	if est.Basis != BasisFallback || est.Rate != 250 {
		t.Fatalf("est = %+v", est)
	}
	if est.Seconds() != 4 {
		t.Fatalf("lag = %v s, want 4", est.Seconds())
	}
}

func TestEstimateUnknown(t *testing.T) {
	est := Estimate(2000, 1000, nil, 0)
	if est.Basis != BasisUnknown {
		t.Fatalf("basis = %q", est.Basis)
	}
	if est.Seconds() != -1 {
		t.Fatalf("unknown lag must be -1, got %v", est.Seconds())
	}
	if est.Backlog != 1000 {
		t.Fatalf("backlog = %d", est.Backlog)
	}
}

func TestEstimateDurationOverflowClamped(t *testing.T) {
	// 积压接近 MaxUint64、速率极低：backlog/rate 超出 time.Duration 范围，
	// 必须钳到最大值而不是回绕成负数。
	est := Estimate(math.MaxUint64, 0, nil, 1e-9)
	if est.Basis != BasisFallback {
		t.Fatalf("basis = %q", est.Basis)
	}
	if est.Lag != time.Duration(math.MaxInt64) {
		t.Fatalf("lag = %v, want clamped to MaxInt64", est.Lag)
	}
	if est.Seconds() <= 0 {
		t.Fatalf("clamped lag must stay positive, got %v", est.Seconds())
	}
}
