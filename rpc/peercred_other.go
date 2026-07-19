//go:build !darwin

package rpc

import (
	"errors"
)

// sidOf reports that no peer session ID is available: it is derived from the
// macOS-only peer PID, so off macOS PeerSID returns false.
func sidOf(_ int) (int, error) {
	return 0, errors.ErrUnsupported
}
