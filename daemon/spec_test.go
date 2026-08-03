package daemon

import (
	"testing"

	"github.com/yasyf/daemonkit"

	"github.com/yasyf/synckit/rpc"
)

func TestServeSpecIsValidAndCarriesTheExactSuite(t *testing.T) {
	spec := clientSpec()
	if len(spec.Schemas) != 1 || string(spec.Schemas[0]) != rpc.WireBuild {
		t.Fatalf("schemas = %v, want [%s]", spec.Schemas, rpc.WireBuild)
	}
	if spec.Restart != daemonkit.RestartAlways {
		t.Fatalf("restart = %v, want RestartAlways — the zero value never relaunches", spec.Restart)
	}
	if err := spec.ValidateForClient(); err != nil {
		t.Fatalf("validate for client: %v", err)
	}
	if err := spec.ValidateForServe(); err != nil {
		t.Fatalf("validate for serve: %v", err)
	}
}

// TestServeSpecFrameCarriesAWholePayload pins MaxFrame against the payload it
// must move: a session carries (MaxFrame-4096)*3/4, so sizing MaxFrame at the
// payload itself would silently cap bodies at three quarters of it.
func TestServeSpecFrameCarriesAWholePayload(t *testing.T) {
	spec := clientSpec()
	if got := daemonkit.MaxDetail(spec.MaxFrame); got < rpc.MaxPayload {
		t.Fatalf("MaxDetail(spec.MaxFrame) = %d, want >= %d", got, rpc.MaxPayload)
	}
}
