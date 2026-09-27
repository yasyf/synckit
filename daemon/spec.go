package daemon

import (
	"fmt"

	"github.com/yasyf/daemonkit"

	"github.com/yasyf/synckit/internal/servespec"
)

const (
	serveLabel    = servespec.Label
	serveShutdown = servespec.Shutdown
)

func serveSpec(program daemonkit.Program) daemonkit.Daemon { return servespec.Spec(program) }

// stableServeSpec is serveSpec over the executable launchd runs from a copy of
// the invoking one, at a path package upgrades survive.
func stableServeSpec() (daemonkit.Daemon, error) {
	program, err := daemonkit.Stable()
	if err != nil {
		return daemonkit.Daemon{}, fmt.Errorf("resolve stable synckitd program: %w", err)
	}
	return serveSpec(program), nil
}

func clientSpec() daemonkit.Daemon { return servespec.Client() }
