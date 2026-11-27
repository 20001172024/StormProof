// Command benchtable summarizes a directory of stormproof-bench results
// (as written by bench/run.sh) as a Markdown table: one row per workload
// and client count, with the median of each figure across repetitions.
//
//	go run ./bench/cmd/benchtable deploy/docker/results/3-servers
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/20001172024/stormproof/bench"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: benchtable DIR")
		os.Exit(2)
	}
	if err := run(os.Args[1], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "benchtable:", err)
		os.Exit(1)
	}
}

// row is one workload and client count; each slice holds one value per
// repetition.
type row struct {
	key                    string
	reps                   int
	ops                    []float64
	putP50, putP99, getP50 []float64
	getP99, errs           []float64
	reads                  float64
	clients                int
}

func run(dir string, out io.Writer) error {
	files, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("no results in %s", dir)
	}
	rows := map[string]*row{}
	var order []string
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		var res bench.Result
		if err := json.Unmarshal(b, &res); err != nil {
			return fmt.Errorf("%s: %w", f, err)
		}
		// Files are <workload>-c<clients>-r<rep>.json.
		name := strings.TrimSuffix(filepath.Base(f), ".json")
		key := name
		if i := strings.LastIndex(name, "-r"); i > 0 {
			key = name[:i]
		}
		r := rows[key]
		if r == nil {
			r = &row{key: key, reads: res.Config.Reads, clients: res.Config.Clients}
			rows[key] = r
			order = append(order, key)
		}
		r.reps++
		r.ops = append(r.ops, res.OpsPerSec)
		put, get := res.ByOp[bench.OpPut], res.ByOp[bench.OpGet]
		r.putP50 = append(r.putP50, put.P50Ms)
		r.putP99 = append(r.putP99, put.P99Ms)
		r.getP50 = append(r.getP50, get.P50Ms)
		r.getP99 = append(r.getP99, get.P99Ms)
		r.errs = append(r.errs, float64(put.Errors+get.Errors))
	}
	slices.SortFunc(order, func(a, b string) int {
		ra, rb := rows[a], rows[b]
		if ra.reads != rb.reads {
			if ra.reads < rb.reads {
				return -1
			}
			return 1
		}
		return ra.clients - rb.clients
	})
	fmt.Fprintln(out, "| Workload | Clients | Runs | ops/sec | Put p50 ms | Put p99 ms | Get p50 ms | Get p99 ms | Errors |")
	fmt.Fprintln(out, "|---|---|---|---|---|---|---|---|---|")
	for _, k := range order {
		r := rows[k]
		fmt.Fprintf(out, "| %.0f%% reads | %d | %d | %.0f | %.2f | %.2f | %.2f | %.2f | %.0f |\n",
			r.reads*100, r.clients, r.reps, median(r.ops), median(r.putP50), median(r.putP99),
			median(r.getP50), median(r.getP99), sum(r.errs))
	}
	return nil
}

// median of the values; for an even count, the lower middle one, so it is
// always a value that was actually measured.
func median(xs []float64) float64 {
	s := slices.Clone(xs)
	slices.Sort(s)
	return s[(len(s)-1)/2]
}

func sum(xs []float64) float64 {
	var t float64
	for _, x := range xs {
		t += x
	}
	return t
}
