package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSweep(t *testing.T) {
	cases := []struct {
		name     string
		args     []string
		wantCode int
		wantOut  string
	}{
		{"raft passes", []string{"-seeds", "20", "-profile", "all"}, exitOK, "20 seeds (1 to 20), 0 failed"},
		{"raft planted bug fails", []string{"-seeds", "20", "-profile", "crashes", "-buggy"}, exitViolation, "term-monotonic"},
		{"toy passes", []string{"-node", "toy", "-seeds", "20", "-profile", "all"}, exitOK, "0 failed"},
		{"toy planted bug fails", []string{"-node", "toy", "-seeds", "20", "-profile", "crashes", "-buggy"}, exitViolation, "counter-monotonic"},
		{"unknown node", []string{"-node", "paxos"}, exitError, ""},
		{"dedup bug fails", []string{"-seeds", "10", "-profile", "flaky-net", "-bug", "dedup"}, exitViolation, "linearizab"},
		{"unknown bug", []string{"-bug", "nope"}, exitError, ""},
		{"raft bug on toy", []string{"-node", "toy", "-bug", "dedup"}, exitError, ""},
		{"quiet longer than run", []string{"-duration", "1s", "-quiet", "2s"}, exitError, ""},
		{"unknown profile", []string{"-profile", "nope"}, exitError, ""},
		{"stray argument", []string{"extra"}, exitError, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var out, errOut bytes.Buffer
			if code := run(c.args, &out, &errOut); code != c.wantCode {
				t.Fatalf("exit code %d, want %d\nstdout: %s\nstderr: %s", code, c.wantCode, &out, &errOut)
			}
			if !strings.Contains(out.String(), c.wantOut) {
				t.Fatalf("stdout %q does not contain %q", &out, c.wantOut)
			}
		})
	}
}

func TestFailingSeedsWriteTraces(t *testing.T) {
	dir := t.TempDir()
	var out, errOut bytes.Buffer
	code := run([]string{"-seeds", "5", "-profile", "crashes", "-buggy", "-trace-dir", dir}, &out, &errOut)
	if code != exitViolation {
		t.Fatalf("exit code %d, want %d\n%s%s", code, exitViolation, &out, &errOut)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "seed-*.jsonl"))
	if len(files) == 0 {
		t.Fatal("no trace files written")
	}
	b, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(b, []byte(`{"type":"header"`)) || !bytes.Contains(b, []byte(`"type":"violation"`)) {
		t.Fatalf("%s is not a trace ending in a violation", files[0])
	}
}

func TestReplayWritesTrace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "t.jsonl")
	var out, errOut bytes.Buffer
	if code := run([]string{"-seed", "3", "-trace", path}, &out, &errOut); code != exitOK {
		t.Fatalf("exit code %d\n%s%s", code, &out, &errOut)
	}
	if fi, err := os.Stat(path); err != nil || fi.Size() == 0 {
		t.Fatalf("trace not written: %v", err)
	}
}
