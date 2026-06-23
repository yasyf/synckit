//go:build !darwin

package rpc

import (
	"net"
	"os"
)

// peerUID returns the current process UID, skipping the LOCAL_PEERCRED check that is
// macOS-only. Off macOS the 0700 socket directory is the access boundary, so the
// returned UID always matches os.Getuid() and Serve admits the connection.
func peerUID(_ *net.UnixConn) (int, error) {
	return os.Getuid(), nil
}
