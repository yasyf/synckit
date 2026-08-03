// Command servestub runs the real synckitd command tree in its own process, so
// a test can wait on the serve daemon through daemonkit's control lane — which
// pins the serving process and refuses a peer whose PID is the caller's own.
package main

import "github.com/yasyf/synckit/daemon"

func main() { daemon.Execute("v0.0.0-servestub") }
