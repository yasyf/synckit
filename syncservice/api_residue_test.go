package syncservice

import (
	"bytes"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestTransportRunnerSurfaceIsExact(t *testing.T) {
	runner := reflect.TypeFor[TransportRunner]()
	if runner.NumMethod() != 2 {
		t.Fatalf("TransportRunner methods = %d, want 2", runner.NumMethod())
	}
	for i, name := range []string{"SSHStdio", "Stdio"} {
		if got := runner.Method(i).Name; got != name {
			t.Fatalf("TransportRunner method %d = %q, want %q", i, got, name)
		}
	}
}

func TestNoExportedRawPoolTransportConstructors(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	files := token.NewFileSet()
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".go" || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		file, err := parser.ParseFile(files, entry.Name(), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", entry.Name(), err)
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Recv != nil || !function.Name.IsExported() {
				continue
			}
			if function.Name.Name == "Stdio" || function.Name.Name == "SSHStdio" {
				t.Fatalf("removed raw constructor %s returned in %s", function.Name.Name, entry.Name())
			}
			if function.Type.Params != nil && strings.Contains(exprString(files, function.Type.Params), "supervise.Pool") {
				t.Fatalf("exported function %s exposes *supervise.Pool", function.Name.Name)
			}
		}
	}
}

func exprString(files *token.FileSet, node ast.Node) string {
	var output bytes.Buffer
	if err := format.Node(&output, files, node); err != nil {
		return ""
	}
	return output.String()
}
