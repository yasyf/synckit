//go:build darwin

package rpc

import (
	"fmt"
	"net"

	"golang.org/x/sys/unix"
)

// peerUID returns the UID of the process on the other end of conn, read from the
// kernel via LOCAL_PEERCRED on the raw fd. It operates on the socket's established
// credentials, so it is valid before any byte is read.
func peerUID(conn *net.UnixConn) (int, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("raw conn: %w", err)
	}
	var (
		xucred  *unix.Xucred
		sockErr error
	)
	if err := raw.Control(func(fd uintptr) {
		xucred, sockErr = unix.GetsockoptXucred(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
	}); err != nil {
		return 0, fmt.Errorf("control raw conn: %w", err)
	}
	if sockErr != nil {
		return 0, fmt.Errorf("getsockopt LOCAL_PEERCRED: %w", sockErr)
	}
	return int(xucred.Uid), nil
}
