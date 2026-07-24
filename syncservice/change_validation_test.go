package syncservice

import (
	"strings"
	"testing"
)

func TestChangeRevisionValidation(t *testing.T) {
	schema := strings.Repeat("a", 64)
	tests := []struct {
		name   string
		kind   ChangeKind
		base   uint64
		source uint64
		valid  bool
	}{
		{name: "delta may report no change", kind: ChangeDelta, base: 7, source: 7, valid: true},
		{name: "delta cannot precede base", kind: ChangeDelta, base: 7, source: 6},
		{name: "snapshot requires nonzero source", kind: ChangeSnapshot, base: 0, source: 0},
		{name: "snapshot requires zero base", kind: ChangeSnapshot, base: 1, source: 2},
		{name: "snapshot advances from zero", kind: ChangeSnapshot, base: 0, source: 1, valid: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewExportedChange(
				"reposync", schema, test.kind,
				NewRevision(test.base), NewRevision(test.source), []byte(`{}`),
			)
			if (err == nil) != test.valid {
				t.Fatalf("NewExportedChange() error = %v, valid=%v", err, test.valid)
			}
		})
	}
}
