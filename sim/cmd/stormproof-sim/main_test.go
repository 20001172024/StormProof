package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
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
		{"membership changes", []string{"-seeds", "10", "-profile", "all", "-membership"}, exitOK, "0 failed"},
		{"raft bug on toy", []string{"-node", "toy", "-bug", "dedup"}, exitError, ""},
		{"quiet longer than run", []string{"-duration", "1s", "-quiet", "2s"}, exitError, ""},
		{"unknown profile", []string{"-profile", "nope"}, exitError, ""},
		{"storm profile", []string{"-seeds", "10", "-profile", "storm"}, exitOK, "0 failed"},
		{"batched events", []string{"-seeds", "10", "-profile", "all", "-batch", "-membership"}, exitOK, "0 failed"},
		{"bad parallel", []string{"-parallel", "0"}, exitError, ""},
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

// Running seeds in parallel must not change anything but the speed: the
// same seeds fail, reported in the same order, with the same violations.
func TestParallelSweepMatchesSequential(t *testing.T) {
	args := []string{"-seeds", "24", "-profile", "crashes", "-buggy", "-duration", "3s", "-quiet", "1s"}
	outputs := map[int]string{}
	for _, p := range []int{1, 4} {
		var out, errOut bytes.Buffer
		code := run(append(args, "-parallel", strconv.Itoa(p)), &out, &errOut)
		if code != exitViolation {
			t.Fatalf("-parallel %d: exit code %d\n%s%s", p, code, &out, &errOut)
		}
		// Drop the summary line: it has the wall-clock time.
		lines := strings.Split(strings.TrimSpace(out.String()), "\n")
		outputs[p] = strings.Join(lines[:len(lines)-1], "\n")
	}
	if outputs[1] != outputs[4] {
		t.Fatalf("-parallel 1 and 4 differ:\n--- 1:\n%s\n--- 4:\n%s", outputs[1], outputs[4])
	}
	if strings.Count(outputs[1], "VIOLATION") != 0 || !strings.Contains(outputs[1], "term-monotonic") {
		t.Fatalf("unexpected output:\n%s", outputs[1])
	}
}

// Every failing seed comes with a command that replays exactly that run,
// and running it reproduces the violation.
func TestReplayCommand(t *testing.T) {
	var out, errOut bytes.Buffer
	code := run([]string{"-seeds", "10", "-profile", "all", "-nodes", "5", "-buggy", "-duration", "4s", "-quiet", "2s", "-parallel", "2"}, &out, &errOut)
	if code != exitViolation {
		t.Fatalf("exit code %d\n%s%s", code, &out, &errOut)
	}
	var first, replay string
	for _, l := range strings.Split(out.String(), "\n") {
		if first == "" && strings.HasPrefix(l, "seed ") {
			first = l
		}
		if replay == "" && strings.HasPrefix(l, "  replay: ") {
			replay = strings.TrimPrefix(l, "  replay: ")
		}
	}
	for _, want := range []string{"-nodes 5", "-bug fsync", "-duration 4s", "-quiet 2s", "-trace seed-"} {
		if !strings.Contains(replay, want) {
			t.Fatalf("replay command %q lacks %q", replay, want)
		}
	}
	// Run it (minus the program name and the trace file) and expect the
	// same violation.
	fields := strings.Fields(replay)
	args := fields[3 : len(fields)-2]
	var rout, rerr bytes.Buffer
	if code := run(args, &rout, &rerr); code != exitViolation {
		t.Fatalf("replaying %v: exit code %d\n%s%s", args, code, &rout, &rerr)
	}
	violation := first[strings.Index(first, "): ")+3:]
	if !strings.Contains(rout.String(), violation) {
		t.Fatalf("replay did not reproduce %q:\n%s", violation, &rout)
	}
}
