package artifact

import (
	"os"

	"golang.org/x/sys/unix"
)

func syncData(file *os.File) error {
	return unix.Fsync(int(file.Fd())) //nolint:gosec // G115: a file descriptor always fits in an int.
}

func fullSync(file *os.File) error {
	_, err := unix.FcntlInt(file.Fd(), unix.F_FULLFSYNC, 0)
	return err
}
