//go:build !darwin

package rpc

import (
	"errors"
	"net"
)

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
