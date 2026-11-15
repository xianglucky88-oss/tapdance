// Package retention 模拟逻辑复制槽的位点与 WAL 保留。
package retention

// SegmentSize 是 WAL 段大小（16MB）。
const SegmentSize = 16 * 1024 * 1024

// Slot 是一个复制槽。
type Slot struct {
	Name         string
	RestartLSN   uint64
	ConfirmedLSN uint64
	Active       bool
}

// NewSlot 创建复制槽。
func NewSlot(name string, restartLSN uint64) *Slot {
	return &Slot{Name: name, RestartLSN: restartLSN, Active: true}
}

// Advance 推进确认位点。只有活跃的槽才能推进。
func (s *Slot) Advance(lsn uint64) {
	if !s.Active || lsn <= s.ConfirmedLSN {
		return
	}
	s.ConfirmedLSN = lsn
}

// Disconnect 模拟消费者断连。
func (s *Slot) Disconnect() { s.Active = false }

// Reconnect 模拟消费者重连。
func (s *Slot) Reconnect() { s.Active = true }

// Backlog 返回积压字节数。
func (s *Slot) Backlog(currentLSN uint64) uint64 {
	if currentLSN <= s.ConfirmedLSN {
		return 0
	}
	return currentLSN - s.ConfirmedLSN
}

// Segments 把字节数换算成 WAL 段数，向上取整。
func Segments(bytes uint64) int {
	if bytes == 0 {
		return 0
	}
	return int((bytes + SegmentSize - 1) / SegmentSize)
}
