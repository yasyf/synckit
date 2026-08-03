package daemon

import (
	"context"

	"github.com/yasyf/daemonkit"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/internal/synctransport"
	"github.com/yasyf/synckit/manifest"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

// daemonClient reaches the resident synckitd over its business lane. Open
// validates the spec here rather than on the first call inside a retry loop.
func daemonClient() (*rpc.Client, error) {
	client, err := daemonkit.Open(clientSpec())
	if err != nil {
		return nil, err
	}
	return rpc.NewClient(rpc.ClientConfig{
		Open: func(context.Context) (*daemonkit.Business, error) { return client.Business(), nil },
	}), nil
}

// dialTransport is the seam serve and reconcile use to reach a consumer's typed
// sync service. Tests override it to inject an in-process transport.
var dialTransport = resolveTransport

// resolveTransport returns the typed transport to reach manifest m's consumer on
// peer. Local resident services are label-addressed through their helper
// identity and local spawned services ride daemonkit's handoff socketpair.
// Remote traffic uses the exact SSH host fact and fixed rpc-serve-v1 command.
func resolveTransport(scope processScope, m manifest.Manifest, peer, self string) syncservice.Transport {
	if peer == self {
		switch m.Service.Kind {
		case "resident":
			return syncservice.Resident(m.Name)
		case "spawned":
			return synctransport.NewSpawned(scope, m.Binary, m.Name)
		}
		panic("daemon: manifest " + m.Name + " has invalid local service kind " + m.Service.Kind)
	}
	fact, err := hostregistry.Mesh.Host(peer)
	if err != nil {
		return synctransport.Failed(err)
	}
	knownHosts, err := hostregistry.Mesh.KnownHostsPath()
	if err != nil {
		return synctransport.Failed(err)
	}
	return synctransport.NewRemote(scope, fact, knownHosts, m.Name)
}
