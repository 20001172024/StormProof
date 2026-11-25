package raft

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// TestPackageIsPure enforces the part of the determinism rule that a linter
// can't: the raft package must never start goroutines or use select.
// A goroutine runs at a time the Go scheduler picks, and select picks
// randomly among ready cases, so either would make a seed replay differently.
// Forbidden imports (time, math/rand, os, net, sync, ...) are checked by
// depguard in .golangci.yml.
func TestPackageIsPure(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	checked := 0
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		checked++
		ast.Inspect(f, func(n ast.Node) bool {
			switch n.(type) {
			case *ast.GoStmt:
				t.Errorf("%s: raft must not start goroutines", fset.Position(n.Pos()))
			case *ast.SelectStmt:
				t.Errorf("%s: raft must not use select", fset.Position(n.Pos()))
			}
			return true
		})
	}
	if checked == 0 {
		t.Fatal("found no non-test .go files to check; is the test running in raft/?")
	}
}
