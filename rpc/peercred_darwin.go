//go:build darwin

package rpc

import (
	"fmt"
	"net"

	"golang.org/x/sys/unix"
)

// peerPID returns the PID of the process on the other end of conn, read from the
// kernel via LOCAL_PEERPID on the raw fd. Like peerUID it operates on the socket's
// established credentials, so it is valid before any byte is read.
func peerPID(conn *net.UnixConn) (int, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("raw conn: %w", err)
	}
	var (
		pid     int
		sockErr error
	)
	if err := raw.Control(func(fd uintptr) {
		pid, sockErr = unix.GetsockoptInt(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERPID)
	}); err != nil {
		return 0, fmt.Errorf("control raw conn: %w", err)
	}
	if sockErr != nil {
		return 0, fmt.Errorf("getsockopt LOCAL_PEERPID: %w", sockErr)
	}
	return pid, nil
}

// sidOf returns the session ID of pid via getsid(2). The kernel resolves the
// session from the pid captured over LOCAL_PEERPID, so no second sockopt is needed.
func sidOf(pid int) (int, error) {
	return unix.Getsid(pid)
}
