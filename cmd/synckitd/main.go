// Command synckitd is the generic per-machine sync daemon: it owns the one shared
// host mesh, rpc socket, reconcile tick, and watch supervisor, reads declarative
// JSON manifests, and shells out to each consumer's CLI for domain actions.
package main

import "github.com/yasyf/synckit/daemon"

var version = "dev"

func main() {
	daemon.Execute(version)
}
