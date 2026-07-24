package daemon

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/wire"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/internal/synctransport"
	"github.com/yasyf/synckit/manifest"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

func daemonClient(sock string) *rpc.Client {
	return rpc.NewClient(rpc.ClientConfig{Dial: wire.UnixDialer(sock), WireBuild: rpc.WireBuild})
}

// dialTransport is the seam serve and reconcile use to reach a consumer's typed
// sync service. Tests override it to inject an in-process transport.
var dialTransport = resolveTransport

// resolveTransport returns the typed transport to reach manifest m's consumer on
// peer. Local resident services use their socket and local spawned services use
// daemonkit's sealed spawned-session endpoint. Remote traffic uses the exact SSH
// host fact and fixed rpc-serve-v1 command.
func resolveTransport(manager *proc.Manager, m manifest.Manifest, peer, self string) syncservice.Transport {
	if peer == self {
		switch m.Service.Kind {
		case "resident":
			return syncservice.Socket(expandHome(m.Service.Socket))
		case "spawned":
			return synctransport.NewSpawned(manager, m.Binary, m.Name)
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
	return synctransport.NewRemote(manager, fact, knownHosts, m.Name)
}

// expandHome expands a leading "~/" in p against the current user's home
// directory, returning p unchanged when it has no such prefix.
func expandHome(p string) string {
	if !strings.HasPrefix(p, "~/") {
		return p
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return p
	}
	return filepath.Join(home, p[2:])
}
