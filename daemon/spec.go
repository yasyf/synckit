package daemon

import (
	"fmt"
	"time"

	"github.com/yasyf/daemonkit"

	"github.com/yasyf/synckit/internal/serviceidentity"
	"github.com/yasyf/synckit/rpc"
)

// serveLabel is the resident daemon's launchd identity. It is unchanged from
// v0.20, so Client.Ensure — which converges exactly its own label — adopts the
// install already on disk instead of stranding it beside a second job.
const serveLabel = serviceidentity.LabelPrefix + ".serve"

// serveShutdown is the whole drain budget and the plist's ExitTimeOut.
const serveShutdown = 30 * time.Second

// serveSpec is the one daemonkit identity the synckitd launcher and the serving
// daemon both read: socket, lock, state dir, record file, and launchd job all
// derive from its Label. Restart is stated because the zero value is
// RestartNever; Args because the program is a copy of synckitd, which prints
// help when launchd runs it bare.
func serveSpec(program daemonkit.Program) daemonkit.Daemon {
	return daemonkit.Daemon{
		Label:    serveLabel,
		Program:  program,
		Args:     []string{"serve"},
		Schemas:  []daemonkit.Schema{rpc.WireBuild},
		Trust:    daemonkit.Trust{Serving: daemonkit.ServingSameUser()},
		Restart:  daemonkit.RestartAlways,
		Shutdown: daemonkit.Grace(serveShutdown),
		MaxFrame: rpc.MaxFrame,
	}
}

// stableServeSpec is serveSpec over the executable launchd runs from a copy of
// the invoking one, at a path package upgrades survive.
func stableServeSpec() (daemonkit.Daemon, error) {
	program, err := daemonkit.Stable()
	if err != nil {
		return daemonkit.Daemon{}, fmt.Errorf("resolve stable synckitd program: %w", err)
	}
	return serveSpec(program), nil
}

// clientSpec is serveSpec's launcher half: Open never reads Program, so a
// caller that only dials the daemon states no placement policy.
func clientSpec() daemonkit.Daemon { return serveSpec(daemonkit.Program{}) }
