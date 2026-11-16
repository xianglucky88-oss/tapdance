// Command tapdance 读取 WAL，还原变更并打印统计。
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/internal-platform/tapdance/internal/lag"
	"github.com/internal-platform/tapdance/internal/pipeline"
	"github.com/internal-platform/tapdance/internal/retention"
	"github.com/internal-platform/tapdance/internal/wal"
)

// multiFlag 收集可重复的字符串 flag。
type multiFlag []string

func (m *multiFlag) String() string { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error {
	*m = append(*m, v)
	return nil
}

func main() {
	var (
		path        = flag.String("wal", "/tmp/tapdance/wal.bin", "WAL file path")
		snapshotAt  = flag.Int64("snapshot-at", -1, "verify snapshot handoff at this LSN (-1 = skip)")
		consumers   multiFlag
		checkpoints multiFlag
		fallbackBPS = flag.Float64("fallback-bps", 0, "fallback consume rate in bytes/sec when checkpoints are insufficient (0 = none)")
	)
	flag.Var(&consumers, "consumer", `consumer progress "name@confirmedLSN[,down]" (repeatable)`)
	flag.Var(&checkpoints, "checkpoint", `WAL progress sample "lsn@unixSeconds" (repeatable)`)
	flag.Parse()

	recs, err := wal.ReadFile(*path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read wal: %v\n", err)
		os.Exit(1)
	}

	slots, err := parseConsumers(consumers)
	if err != nil {
		fmt.Fprintf(os.Stderr, "consumer: %v\n", err)
		os.Exit(1)
	}
	cps, err := parseCheckpoints(checkpoints)
	if err != nil {
		fmt.Fprintf(os.Stderr, "checkpoint: %v\n", err)
		os.Exit(1)
	}

	report := pipeline.RunWith(recs, pipeline.Options{
		Slots:       slots,
		Checkpoints: cps,
		FallbackBPS: *fallbackBPS,
	})
	raw, _ := json.MarshalIndent(report, "", "  ")
	fmt.Println(string(raw))

	if *snapshotAt >= 0 {
		full, handoff, err := pipeline.SnapshotHandoff(recs, uint64(*snapshotAt))
		if err != nil {
			fmt.Fprintf(os.Stderr, "handoff: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("\nsnapshot handoff at LSN %d\n  full    = %s\n  handoff = %s\n  match   = %v\n",
			*snapshotAt, full, handoff, full == handoff)
		if full != handoff {
			os.Exit(2)
		}
	}
}

// parseConsumers 解析 "name@confirmedLSN[,down]"。
// 槽位从 0 建起再推进到确认位点，restart 跟随 confirmed。
func parseConsumers(specs []string) ([]*retention.Slot, error) {
	var slots []*retention.Slot
	for _, spec := range specs {
		name, rest, ok := strings.Cut(spec, "@")
		if !ok || name == "" {
			return nil, fmt.Errorf("bad spec %q, want name@confirmedLSN[,down]", spec)
		}
		lsnStr, modifier, _ := strings.Cut(rest, ",")
		lsn, err := strconv.ParseUint(lsnStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("bad spec %q: %v", spec, err)
		}
		s := retention.NewSlot(name, 0)
		s.Advance(lsn)
		switch modifier {
		case "":
		case "down":
			s.Disconnect()
		default:
			return nil, fmt.Errorf("bad modifier in %q, want ,down or nothing", spec)
		}
		slots = append(slots, s)
	}
	return slots, nil
}

// parseCheckpoints 解析 "lsn@unixSeconds"。
func parseCheckpoints(specs []string) ([]lag.Checkpoint, error) {
	var out []lag.Checkpoint
	for _, spec := range specs {
		lsnStr, tsStr, ok := strings.Cut(spec, "@")
		if !ok {
			return nil, fmt.Errorf("bad spec %q, want lsn@unixSeconds", spec)
		}
		lsn, err := strconv.ParseUint(lsnStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("bad spec %q: %v", spec, err)
		}
		ts, err := strconv.ParseInt(tsStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("bad spec %q: %v", spec, err)
		}
		out = append(out, lag.Checkpoint{LSN: lsn, At: time.Unix(ts, 0)})
	}
	return out, nil
}
