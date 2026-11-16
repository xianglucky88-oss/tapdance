// Package retention 模拟逻辑复制槽的位点、状态机与 WAL 保留边界。
//
// 核心约定：
//   - ConfirmedLSN 是消费者确认（flush）的位点，只增不减；
//     任何让它倒退的推进都会被拒绝并计入 Regressions。
//   - RestartLSN 是该槽位要求保留 WAL 的起点，跟随 ConfirmedLSN 前进。
//   - 断连（Disconnect）期间位点冻结：消费者不可能在断连时确认新位点。
//   - 多个槽位共同决定保留边界：边界 = 所有槽位中最小的 RestartLSN，
//     断连的槽位照样钉住保留边界（与 PostgreSQL 一致）。
package retention

import "math"

// SegmentSize 是 WAL 段大小（16MB）。
const SegmentSize = 16 * 1024 * 1024

// State 是槽位的连接状态。
type State string

const (
	// StateActive 消费者在线且从未断开过。
	StateActive State = "active"
	// StateDisconnected 消费者当前断连，位点冻结，但保留边界仍被钉住。
	StateDisconnected State = "disconnected"
	// StateReconnected 消费者断连后已重连（与 active 区分开，便于运维识别抖动）。
	StateReconnected State = "reconnected"
)

// Slot 是一个复制槽。
//
// Name/RestartLSN/ConfirmedLSN/Active 供报告读取；请用方法修改状态，
// 不要直接写字段，否则会绕过单调性与状态机约束。
type Slot struct {
	Name         string
	RestartLSN   uint64
	ConfirmedLSN uint64
	Active       bool
	// Reconnects 记录重连次数（>0 表示经历过断连）。
	Reconnects int
	// Regressions 记录被拒绝的确认位点倒退次数。
	Regressions int
}

// NewSlot 创建复制槽。新槽位在建槽位点上没有积压：confirmed = restart = restartLSN。
func NewSlot(name string, restartLSN uint64) *Slot {
	return &Slot{
		Name:         name,
		RestartLSN:   restartLSN,
		ConfirmedLSN: restartLSN,
		Active:       true,
	}
}

// Advance 推进确认位点。
//
// 断连的槽位不能推进（消费者不在线，谈不上确认）；
// 倒退的推进被拒绝并计入 Regressions，位点保持单调不减。
func (s *Slot) Advance(lsn uint64) {
	if !s.Active {
		return
	}
	if lsn < s.ConfirmedLSN {
		s.Regressions++
		return
	}
	if lsn == s.ConfirmedLSN {
		return
	}
	s.ConfirmedLSN = lsn
	if s.RestartLSN < lsn {
		s.RestartLSN = lsn
	}
}

// Disconnect 模拟消费者断连。位点冻结，但 RestartLSN 继续钉住保留边界。
func (s *Slot) Disconnect() { s.Active = false }

// Reconnect 模拟消费者重连。重复调用（已在线）不计数。
func (s *Slot) Reconnect() {
	if s.Active {
		return
	}
	s.Active = true
	s.Reconnects++
}

// State 返回当前连接状态，区分 active / disconnected / reconnected。
func (s *Slot) State() State {
	switch {
	case !s.Active:
		return StateDisconnected
	case s.Reconnects > 0:
		return StateReconnected
	default:
		return StateActive
	}
}

// Backlog 返回积压字节数。
//
// current 允许不单调（比如故障切换后当前 LSN 反而变小）：
// current <= confirmed 时积压为 0，绝不出现在负数的伪装形式。
func (s *Slot) Backlog(currentLSN uint64) uint64 {
	if currentLSN <= s.ConfirmedLSN {
		return 0
	}
	return currentLSN - s.ConfirmedLSN
}

// Lagging 报告确认位点是否落后于 currentLSN。
func (s *Slot) Lagging(currentLSN uint64) bool { return s.Backlog(currentLSN) > 0 }

// Segments 把字节数换算成 WAL 段数，向上取整。
//
// 不用 (bytes + SegmentSize - 1) / SegmentSize：bytes 接近 MaxUint64 时
// 加法会溢出回绕。这里先除再补余数，结果超出 int 表示范围时钳到 MaxInt。
func Segments(bytes uint64) int {
	if bytes == 0 {
		return 0
	}
	n := bytes / SegmentSize
	if bytes%SegmentSize != 0 {
		n++
	}
	if n > uint64(math.MaxInt) {
		return math.MaxInt
	}
	return int(n)
}

// Summary 汇总一组槽位的保留视图。
type Summary struct {
	// Slots 是参与汇总的槽位数。
	Slots int
	// RetainFrom 是 WAL 保留边界：所有槽位中最小的 RestartLSN。
	RetainFrom uint64
	// MinConfirmed 是所有槽位中最小的确认位点。
	MinConfirmed uint64
	// Backlog 是保留视角下的积压：current 超出 MinConfirmed 的字节数。
	Backlog uint64
}

// Summarize 汇总一组槽位。没有槽位时返回零值（Slots == 0），
// 调用方应据此区分"没有保留需求"与"积压为 0"。
func Summarize(slots []*Slot, currentLSN uint64) Summary {
	sum := Summary{
		RetainFrom:   math.MaxUint64,
		MinConfirmed: math.MaxUint64,
	}
	for _, s := range slots {
		if s == nil {
			continue
		}
		sum.Slots++
		if s.RestartLSN < sum.RetainFrom {
			sum.RetainFrom = s.RestartLSN
		}
		if s.ConfirmedLSN < sum.MinConfirmed {
			sum.MinConfirmed = s.ConfirmedLSN
		}
	}
	if sum.Slots == 0 {
		return Summary{}
	}
	if currentLSN > sum.MinConfirmed {
		sum.Backlog = currentLSN - sum.MinConfirmed
	}
	return sum
}
