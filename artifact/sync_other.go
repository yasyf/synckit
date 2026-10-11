//go:build !darwin

package artifact

import "os"

func syncData(file *os.File) error {
	return file.Sync()
}

func fullSync(file *os.File) error {
	return file.Sync()
}
