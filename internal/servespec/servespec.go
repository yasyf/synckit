// Package servespec is the resident synckitd identity shared by the daemon
// that serves it and the packages that dial it.
package servespec

import (
	"context"
	"time"

	"github.com/yasyf/daemonkit"

	"github.com/yasyf/synckit/internal/serviceidentity"
	"github.com/yasyf/synckit/rpc"
)

// Label is the resident daemon's launchd identity. It is unchanged from
// v0.20, so Client.Ensure — which converges exactly its own label — adopts the
// install already on disk instead of stranding it beside a second job.
const Label = serviceidentity.LabelPrefix + ".serve"

// Shutdown is the whole drain budget and the plist's ExitTimeOut.
const Shutdown = 30 * time.Second

// Spec is the one daemonkit identity the synckitd launcher and the serving
// daemon both read: socket, lock, state dir, record file, and launchd job all
// derive from its Label. Restart is stated because the zero value is
// RestartNever; Args because the program is a copy of synckitd, which prints
// help when launchd runs it bare.
func Spec(program daemonkit.Program) daemonkit.Daemon {
	return daemonkit.Daemon{
		Label:    Label,
		Program:  program,
		Args:     []string{"serve"},
		Schemas:  []daemonkit.Schema{rpc.WireBuild},
		Trust:    daemonkit.Trust{Serving: daemonkit.ServingSameUser()},
		Restart:  daemonkit.RestartAlways,
		Shutdown: daemonkit.Grace(Shutdown),
		MaxFrame: rpc.MaxFrame,
	}
}

// Client is Spec's launcher half: Open never reads Program, so a caller that
// only dials the daemon states no placement policy.
func Client() daemonkit.Daemon { return Spec(daemonkit.Program{}) }

// Dial reaches the resident synckitd over its business lane. Open validates
// the spec here rather than on the first call inside a retry loop.
func Dial() (*rpc.Client, error) {
	client, err := daemonkit.Open(Client())
	if err != nil {
		return nil, err
	}
	return rpc.NewClient(rpc.ClientConfig{
		Open: func(context.Context) (*daemonkit.Business, error) { return client.Business(), nil },
	}), nil
}
