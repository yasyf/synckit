//go:build !darwin

package rpc

import (
	"errors"
	"net"
	"os"
)

// peerUID returns the current process UID, skipping the LOCAL_PEERCRED check that is
// macOS-only. Off macOS the 0700 socket directory is the access boundary, so the
// returned UID always matches os.Getuid() and Serve admits the connection.
func peerUID(_ *net.UnixConn) (int, error) {
	return os.Getuid(), nil
}

// peerPID reports that no peer PID is available: the LOCAL_PEERPID sockopt is
// macOS-only, so off macOS PeerPID returns false.
func peerPID(_ *net.UnixConn) (int, error) {
	return 0, errors.ErrUnsupported
}

// sidOf reports that no peer session ID is available: it is derived from the
// macOS-only peer PID, so off macOS PeerSID returns false.
func sidOf(_ int) (int, error) {
	return 0, errors.ErrUnsupported
}
