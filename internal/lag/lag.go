// Package lag 把 LSN 积压换算成时间延迟。
package lag

import "time"

// Checkpoint 是 LSN 与时间的对应点。
type Checkpoint struct {
	LSN uint64    `json:"lsn"`
	At  time.Time `json:"at"`
}

// Estimate 估算延迟。
//
// 当前实现用固定速率换算，checkpoints 参数没有被使用。
func Estimate(current, confirmed uint64, checkpoints []Checkpoint, bytesPerSecond float64) time.Duration {
	if bytesPerSecond <= 0 || current <= confirmed {
		return 0
	}
	backlog := float64(current - confirmed)
	return time.Duration(backlog / bytesPerSecond * float64(time.Second))
}

// Rate 根据两个检查点估算每秒推进的字节数。用于给 Estimate 提供速率。
func Rate(a, b Checkpoint) float64 {
	secs := b.At.Sub(a.At).Seconds()
	if secs <= 0 || b.LSN <= a.LSN {
		return 0
	}
	return float64(b.LSN-a.LSN) / secs
}
