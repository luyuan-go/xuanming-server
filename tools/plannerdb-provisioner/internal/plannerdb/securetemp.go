package plannerdb

import (
	"fmt"
	"os"
)

func newSecureTempDirectory(pattern string) (string, error) {
	directory, err := os.MkdirTemp("", pattern)
	if err != nil {
		return "", err
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		_ = os.RemoveAll(directory)
		return "", err
	}
	if err := hardenPrivatePath(directory, true); err != nil {
		_ = os.RemoveAll(directory)
		return "", err
	}
	return directory, nil
}

func writePrivateFile(path string, content []byte, mode os.FileMode) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	written, writeErr := file.Write(content)
	if writeErr == nil && written != len(content) {
		writeErr = fmt.Errorf("short write: %d/%d", written, len(content))
	}
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if writeErr != nil {
		_ = os.Remove(path)
		return writeErr
	}
	if closeErr != nil {
		_ = os.Remove(path)
		return closeErr
	}
	if err := os.Chmod(path, mode); err != nil {
		_ = os.Remove(path)
		return err
	}
	if err := hardenPrivatePath(path, false); err != nil {
		_ = os.Remove(path)
		return err
	}
	return nil
}
