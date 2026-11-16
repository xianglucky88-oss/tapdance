// Command tapdance 读取 WAL，还原变更并打印统计。
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"

	"github.com/internal-platform/tapdance/internal/pipeline"
	"github.com/internal-platform/tapdance/internal/wal"
)

func main() {
	var (
		path       = flag.String("wal", "/tmp/tapdance/wal.bin", "WAL file path")
		snapshotAt = flag.Int64("snapshot-at", -1, "verify snapshot handoff at this LSN (-1 = skip)")
		shuffle    = flag.Bool("shuffle", false, "shuffle record arrival order (simulate out-of-order pages)")
		replay     = flag.Int("replay", 0, "re-deliver each record this many extra times (simulate upstream replay)")
		seed       = flag.Int64("seed", 20261116, "random seed for -shuffle")
	)
	flag.Parse()

	recs, err := wal.ReadFile(*path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read wal: %v\n", err)
		os.Exit(1)
	}

	if *shuffle {
		rnd := rand.New(rand.NewSource(*seed))
		rnd.Shuffle(len(recs), func(i, j int) { recs[i], recs[j] = recs[j], recs[i] })
	}
	if *replay > 0 {
		extra := make([]wal.Record, 0, len(recs)**replay)
		for n := 0; n < *replay; n++ {
			extra = append(extra, recs...)
		}
		recs = append(recs, extra...)
	}

	report, err := pipeline.Run(recs)
	raw, _ := json.MarshalIndent(report, "", "  ")
	fmt.Println(string(raw))
	if err != nil {
		fmt.Fprintf(os.Stderr, "processing error: %v\n", err)
		os.Exit(1)
	}

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
