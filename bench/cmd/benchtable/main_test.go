package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/20001172024/stormproof/bench"
)

func TestTable(t *testing.T) {
	dir := t.TempDir()
	write := func(name string, reads float64, clients int, ops, putP50 float64, errs int) {
		res := bench.Result{Config: bench.Config{Reads: reads, Clients: clients}, OpsPerSec: ops,
			ByOp: map[string]bench.Stats{bench.OpPut: {P50Ms: putP50, Errors: errs}, bench.OpGet: {P50Ms: 1}}}
		b, _ := json.Marshal(res)
		if err := os.WriteFile(filepath.Join(dir, name+".json"), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("mixed-c8-r1", 0.5, 8, 300, 3, 0)
	write("mixed-c8-r2", 0.5, 8, 100, 9, 1)
	write("mixed-c8-r3", 0.5, 8, 200, 5, 0)
	write("write-c8-r1", 0.1, 8, 50, 7, 0)

	var out bytes.Buffer
	if err := run(dir, &out); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 4 {
		t.Fatalf("got %d lines:\n%s", len(lines), &out)
	}
	// Sorted by read fraction; medians across the three repetitions.
	if !strings.HasPrefix(lines[2], "| 10% reads | 8 | 1 | 50 |") {
		t.Errorf("row 1: %s", lines[2])
	}
	if !strings.HasPrefix(lines[3], "| 50% reads | 8 | 3 | 200 | 5.00 |") || !strings.HasSuffix(lines[3], "| 1 |") {
		t.Errorf("row 2: %s", lines[3])
	}
	if err := run(t.TempDir(), &out); err == nil {
		t.Error("empty directory accepted")
	}
}

func TestMedian(t *testing.T) {
	if m := median([]float64{4, 1, 3, 2}); m != 2 {
		t.Errorf("median of 4 values = %v, want the lower middle 2", m)
	}
	if m := median([]float64{5}); m != 5 {
		t.Errorf("median of one = %v", m)
	}
}
