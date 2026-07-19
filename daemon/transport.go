package daemon

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/yasyf/daemonkit/wire"

	"github.com/yasyf/synckit/hostregistry"
	"github.com/yasyf/synckit/manifest"
	"github.com/yasyf/synckit/rpc"
	"github.com/yasyf/synckit/syncservice"
)

func daemonClient(sock string) *rpc.Client {
	return rpc.NewClient(rpc.ClientConfig{Dial: wire.UnixDialer(sock), Build: rpc.Build})
}

// dialTransport is the seam serve and reconcile use to reach a consumer's typed
// sync service. Tests override it to inject an in-process transport.
var dialTransport = resolveTransport

// resolveTransport returns the typed transport to reach manifest m's consumer on
// peer. When peer is self, it returns the manifest's local transport — a unix
// socket to the resident helper (transport "socket") or the spawned consumer
// binary's stdio (transport "stdio"). When peer is a remote host, it returns an
// ssh transport that runs the consumer's rpc-serve command on that peer.
func resolveTransport(m manifest.Manifest, peer, self string) syncservice.Transport {
	if peer == self {
		switch m.Service.Transport {
		case "socket":
			return syncservice.Socket(expandHome(m.Service.Sock))
		case "stdio":
			return syncservice.Stdio(m.Binary, m.Service.ServeArgs...)
		}
		panic("daemon: manifest " + m.Name + " has invalid local transport " + m.Service.Transport)
	}
	return syncservice.SSHStdio(peer, remoteServeCmd(m))
}

// remoteServeCmd joins the consumer's rpc-serve invocation into a single shell
// command line for ssh: the binary unquoted followed by each serve arg
// shell-quoted, so argv boundaries survive the remote shell intact.
func remoteServeCmd(m manifest.Manifest) string {
	parts := make([]string, 0, len(m.Service.ServeArgs)+1)
	parts = append(parts, m.Binary)
	for _, a := range m.Service.ServeArgs {
		parts = append(parts, hostregistry.ShellQuote(a))
	}
	return strings.Join(parts, " ")
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
