// Command tapdance 读取 WAL，还原变更并打印统计。
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/internal-platform/tapdance/internal/pipeline"
	"github.com/internal-platform/tapdance/internal/wal"
)

func main() {
	var (
		path       = flag.String("wal", "/tmp/tapdance/wal.bin", "WAL file path")
		snapshotAt = flag.Int64("snapshot-at", -1, "verify snapshot handoff at this LSN (-1 = skip)")
	)
	flag.Parse()

	recs, err := wal.ReadFile(*path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read wal: %v\n", err)
		os.Exit(1)
	}

	report := pipeline.Run(recs)
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
