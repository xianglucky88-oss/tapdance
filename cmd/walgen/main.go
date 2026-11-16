// Command walgen 生成测试用 WAL 文件：事务、DDL、回滚、未提交事务。
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"

	"github.com/internal-platform/tapdance/internal/wal"
)

func main() {
	var (
		out     = flag.String("out", "/tmp/tapdance/wal.bin", "output WAL path")
		txns    = flag.Int("txns", 40, "number of committed transactions")
		seed    = flag.Int64("seed", 20261114, "random seed")
		corrupt = flag.Bool("corrupt", false, "flip one byte in a middle page to simulate page corruption")
		big     = flag.Int("big", 0, "size in bytes of one cross-page INSERT payload (0 = none)")
		torn    = flag.Int("torn-tail", 0, "truncate the file to this many bytes to simulate a crash mid-write")
	)
	flag.Parse()

	rnd := rand.New(rand.NewSource(*seed))
	var recs []wal.Record
	xid := uint32(100)

	// 建表
	recs = append(recs, wal.Record{XID: 1, Type: wal.TypeBegin, Payload: nil})
	recs = append(recs, wal.Record{XID: 1, Type: wal.TypeDDL, Payload: mustJSON(map[string]any{
		"table": "orders", "op": "create",
	})})
	recs = append(recs, wal.Record{XID: 1, Type: wal.TypeCommit, Payload: nil})

	nextPK := 1
	for t := 0; t < *txns; t++ {
		xid++
		recs = append(recs, wal.Record{XID: xid, Type: wal.TypeBegin, Payload: nil})

		if t == 2 && *big > 0 {
			// 一条跨多页的超大行，用于覆盖跨页记录。
			body := make([]byte, *big)
			for i := range body {
				body[i] = byte('a' + i%26)
			}
			recs = append(recs, wal.Record{XID: xid, Type: wal.TypeInsert, Payload: mustJSON(map[string]any{
				"table": "orders", "pk": "BIG-1", "row": map[string]any{"id": "BIG-1", "blob": string(body)},
			})})
		}

		switch {
		case t == 5:
			// 中途加列
			recs = append(recs, wal.Record{XID: xid, Type: wal.TypeDDL, Payload: mustJSON(map[string]any{
				"table": "orders", "op": "add_column", "column": "coupon",
			})})
		case t == 12:
			// 列改名：旧记录里的 amount 要映射成 total
			recs = append(recs, wal.Record{XID: xid, Type: wal.TypeDDL, Payload: mustJSON(map[string]any{
				"table": "orders", "op": "rename_column", "column": "amount", "to": "total",
			})})
		}

		for i := 0; i < 3; i++ {
			pk := fmt.Sprintf("%d", nextPK)
			nextPK++
			row := map[string]any{"id": pk, "amount": rnd.Intn(1000) + 1}
			if t >= 5 {
				row["coupon"] = "C" + fmt.Sprint(rnd.Intn(9))
			}
			recs = append(recs, wal.Record{XID: xid, Type: wal.TypeInsert, Payload: mustJSON(map[string]any{
				"table": "orders", "pk": pk, "row": row,
			})})
		}

		// 偶尔更新一笔已存在的单
		if t > 3 && rnd.Intn(3) == 0 {
			pk := fmt.Sprintf("%d", rnd.Intn(nextPK-1)+1)
			recs = append(recs, wal.Record{XID: xid, Type: wal.TypeUpdate, Payload: mustJSON(map[string]any{
				"table": "orders", "pk": pk, "row": map[string]any{"id": pk, "amount": 999},
			})})
		}
		recs = append(recs, wal.Record{XID: xid, Type: wal.TypeCommit, Payload: nil})
	}

	// 回滚的事务：不应出现在结果里
	xid++
	recs = append(recs, wal.Record{XID: xid, Type: wal.TypeBegin, Payload: nil})
	recs = append(recs, wal.Record{XID: xid, Type: wal.TypeInsert, Payload: mustJSON(map[string]any{
		"table": "orders", "pk": "ROLLBACK-1", "row": map[string]any{"id": "ROLLBACK-1", "amount": 1},
	})})
	recs = append(recs, wal.Record{XID: xid, Type: wal.TypeAbort, Payload: nil})

	// 未提交的事务：退出时仍在 open 状态
	xid++
	recs = append(recs, wal.Record{XID: xid, Type: wal.TypeBegin, Payload: nil})
	recs = append(recs, wal.Record{XID: xid, Type: wal.TypeInsert, Payload: mustJSON(map[string]any{
		"table": "orders", "pk": "PENDING-1", "row": map[string]any{"id": "PENDING-1", "amount": 2},
	})})

	if err := os.MkdirAll(dirOf(*out), 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir: %v\n", err)
		os.Exit(1)
	}
	if err := wal.WriteFile(*out, recs); err != nil {
		fmt.Fprintf(os.Stderr, "write wal: %v\n", err)
		os.Exit(1)
	}

	if *corrupt {
		raw, _ := os.ReadFile(*out)
		// 翻转文件中间一页的负载字节，模拟页级校验失败；前后页仍应可读。
		if page := len(raw) / wal.PageSize / 2; page > 1 {
			raw[page*wal.PageSize+wal.HeaderSize+3] ^= 0xFF
			_ = os.WriteFile(*out, raw, 0o644)
		}
	}
	if *torn > 0 {
		raw, _ := os.ReadFile(*out)
		if *torn < len(raw) {
			_ = os.WriteFile(*out, raw[:*torn], 0o644)
		}
	}

	fmt.Fprintf(os.Stderr, "wrote %d records to %s\n", len(recs), *out)
}

func mustJSON(v any) []byte {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return raw
}

func dirOf(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' {
			return p[:i]
		}
	}
	return "."
}
