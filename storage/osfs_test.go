package storage

import (
	"errors"
	"io/fs"
	"slices"
	"testing"
)

func TestOSFS(t *testing.T) {
	o, err := NewOSFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	write := func(f File, s string) {
		t.Helper()
		if _, err := f.Write([]byte(s)); err != nil {
			t.Fatal(err)
		}
		if err := f.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	read := func(name string) string {
		t.Helper()
		b, err := o.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}

	f, err := o.Append("log")
	if err != nil {
		t.Fatal(err)
	}
	write(f, "one,")
	f, _ = o.Append("log")
	write(f, "two")
	if got := read("log"); got != "one,two" {
		t.Fatalf("Append: %q", got)
	}

	f, _ = o.Create("log")
	write(f, "fresh")
	if got := read("log"); got != "fresh" {
		t.Fatalf("Create should truncate: %q", got)
	}

	f, _ = o.Create("tmp")
	write(f, "new")
	if err := o.Rename("tmp", "log"); err != nil {
		t.Fatal(err)
	}
	if err := o.SyncDir(); err != nil {
		t.Fatal(err)
	}
	if got := read("log"); got != "new" {
		t.Fatalf("Rename: %q", got)
	}

	f, _ = o.Create("b")
	write(f, "")
	names, err := o.List()
	if err != nil || !slices.Equal(names, []string{"b", "log"}) {
		t.Fatalf("List = %v, %v", names, err)
	}

	if err := o.Remove("b"); err != nil {
		t.Fatal(err)
	}
	if _, err := o.ReadFile("b"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("ReadFile of removed file: err = %v, want fs.ErrNotExist", err)
	}
}

func TestOSFSRejectsPathsOutsideItsDirectory(t *testing.T) {
	o, err := NewOSFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"", ".", "..", "../x", "a/b", `a\b`} {
		if _, err := o.Create(name); err == nil {
			t.Errorf("Create(%q) succeeded", name)
		}
	}
}
