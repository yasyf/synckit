//go:build darwin

package rpc

import (
	"golang.org/x/sys/unix"
)

// sidOf returns the session ID of pid via getsid(2). The kernel resolves the
// session from the pid captured over LOCAL_PEERPID, so no second sockopt is needed.
func sidOf(pid int) (int, error) {
	return unix.Getsid(pid)
}
