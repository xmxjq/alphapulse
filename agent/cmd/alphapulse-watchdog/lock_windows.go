package main

import (
	"errors"
	"os"
)

// Production runs on Linux. Windows supports isolated unit tests, not the daemon.
func lockFile(f *os.File) error {
	return errors.New("daemon_requires_unix")
}
