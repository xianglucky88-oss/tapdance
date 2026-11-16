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
		strict     = flag.Bool("strict", false, "exit non-zero on page damage, dropped records or unconfirmed tail")
		recover_   = flag.Bool("recover", false, "truncate the unconfirmed tail and exit (safe restart point)")
		maxRecord  = flag.Int("max-record", 0, "max bytes allowed for one reassembled record (0 = default)")
	)
	flag.Parse()

	res, err := wal.ScanFile(*path, wal.ScanOptions{MaxRecordSize: *maxRecord})
	if err != nil {
		fmt.Fprintf(os.Stderr, "read wal: %v\n", err)
		os.Exit(1)
	}

	if *recover_ {
		w, scanned, err := wal.Resume(*path, wal.WriterOptions{MaxRecordSize: *maxRecord})
		if err != nil {
			fmt.Fprintf(os.Stderr, "recover: %v\n", err)
			os.Exit(1)
		}
		if err := w.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "recover: close: %v\n", err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "recovered: dropped %s tail at lsn %d (%d bytes)\n",
			scanned.Tail.Kind, scanned.Tail.LSN, scanned.Tail.Bytes)
		res, err = wal.ScanFile(*path, wal.ScanOptions{MaxRecordSize: *maxRecord})
		if err != nil {
			fmt.Fprintf(os.Stderr, "read wal: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("resume point: lsn %d (page %d); last safe record lsn %d\n",
			res.Resume.AppendLSN, res.Resume.NextPageNo, res.Resume.SafeLSN)
	}

	report := pipeline.RunScan(res)
	raw, _ := json.MarshalIndent(report, "", "  ")
	fmt.Println(string(raw))

	if !report.Complete {
		fmt.Fprintf(os.Stderr,
			"wal incomplete: %d corrupt page(s), %d dropped record(s), tail=%s at lsn %d; "+
				"safe lsn %d, resume at lsn %d\n",
			len(res.PageProblems), len(res.RecordProblems), res.Tail.Kind, res.Tail.LSN,
			res.Resume.SafeLSN, res.Resume.AppendLSN)
	}
	if *strict && !report.Complete {
		os.Exit(3)
	}

	if *snapshotAt >= 0 {
		full, handoff, err := pipeline.SnapshotHandoff(res.Records, uint64(*snapshotAt))
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
