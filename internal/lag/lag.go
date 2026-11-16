// Package lag 把 LSN 积压换算成时间延迟。
//
// 速率来源有优先级：checkpoint 采样 > 调用方给的兜底速率 > 无法估算。
// 估算结果带 Basis，让调用方能区分"追上了"和"算不出来"。
package lag

import (
	"math"
	"time"
)

// Checkpoint 是 LSN 与时间的对应点。
type Checkpoint struct {
	LSN uint64    `json:"lsn"`
	At  time.Time `json:"at"`
}

// Basis 说明一次估算的依据。
type Basis string

const (
	// BasisCaughtUp 没有积压，延迟为 0。
	BasisCaughtUp Basis = "caught_up"
	// BasisCheckpoints 用 checkpoint 采样推算的速率。
	BasisCheckpoints Basis = "checkpoints"
	// BasisFallback 采样不足，用调用方配置的固定速率。
	BasisFallback Basis = "fallback"
	// BasisUnknown 采样不足且没有兜底速率，无法估算。
	BasisUnknown Basis = "unknown"
)

// Estimation 是一次延迟估算。
type Estimation struct {
	// Lag 是估算的追赶时间；Basis 为 BasisUnknown 时无意义。
	Lag time.Duration
	// Rate 是估算所用的速率（字节/秒）；BasisUnknown 时为 0。
	Rate float64
	// Basis 说明估算依据。
	Basis Basis
	// Backlog 是参与估算的积压字节数。
	Backlog uint64
}

// Seconds 返回延迟秒数。无法估算（BasisUnknown）时返回 -1，
// 与"追上了"的 0 区分开。
func (e Estimation) Seconds() float64 {
	if e.Basis == BasisUnknown {
		return -1
	}
	return e.Lag.Seconds()
}

// Estimate 估算 confirmed 追上 current 需要的时间。
//
// 速率优先取 checkpoints 采样（SampleRate）；采样不足时退回
// fallbackBPS；两者都没有时返回 BasisUnknown。current <= confirmed
// （含 current 不单调倒挂）视为已追上，延迟为 0。
func Estimate(current, confirmed uint64, checkpoints []Checkpoint, fallbackBPS float64) Estimation {
	var backlog uint64
	if current > confirmed {
		backlog = current - confirmed
	}
	if backlog == 0 {
		return Estimation{Basis: BasisCaughtUp}
	}

	rate, ok := SampleRate(checkpoints)
	basis := BasisCheckpoints
	if !ok {
		if fallbackBPS <= 0 {
			return Estimation{Basis: BasisUnknown, Backlog: backlog}
		}
		rate, basis = fallbackBPS, BasisFallback
	}

	// backlog/rate 可能超出 time.Duration 的表示范围（int64 纳秒），
	// 钳到最大值，避免溢出回绕成负数。
	secs := float64(backlog) / rate
	lag := time.Duration(math.MaxInt64)
	if maxSecs := float64(math.MaxInt64) / float64(time.Second); secs < maxSecs {
		lag = time.Duration(secs * float64(time.Second))
	}
	return Estimation{Lag: lag, Rate: rate, Basis: basis, Backlog: backlog}
}

// Rate 根据两个检查点估算每秒推进的字节数。
// 时间不前进或 LSN 不前进时返回 0。
func Rate(a, b Checkpoint) float64 {
	secs := b.At.Sub(a.At).Seconds()
	if secs <= 0 || b.LSN <= a.LSN {
		return 0
	}
	return float64(b.LSN-a.LSN) / secs
}

// SampleRate 从一组 checkpoint 估算有效消费速率（字节/秒）。
//
// 只统计 LSN 有推进的区间：LSN 原地踏步的区间是上游停顿（没有新 WAL
// 可消费），不是消费变慢，计入会把速率稀释、把延迟估高。LSN 倒挂的
// 区间（采样源切换等）同样跳过。时间不前进的区间无法定义速率，也跳过。
//
// 少于 2 个点、或所有区间都没有有效推进时，ok = false。
func SampleRate(checkpoints []Checkpoint) (rate float64, ok bool) {
	var bytes, secs float64
	for i := 1; i < len(checkpoints); i++ {
		a, b := checkpoints[i-1], checkpoints[i]
		dt := b.At.Sub(a.At).Seconds()
		if dt <= 0 || b.LSN <= a.LSN {
			continue
		}
		bytes += float64(b.LSN - a.LSN)
		secs += dt
	}
	if bytes <= 0 || secs <= 0 {
		return 0, false
	}
	return bytes / secs, true
}
