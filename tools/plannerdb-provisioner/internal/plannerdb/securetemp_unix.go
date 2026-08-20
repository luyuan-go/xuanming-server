//go:build !windows

package plannerdb

import (
	"errors"
	"os"
)

func hardenPrivatePath(path string, directory bool) error {
	if directory {
		if err := os.Chmod(path, 0o700); err != nil {
			return err
		}
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode().Perm()&0o077 != 0 || info.Mode().Perm()&0o600 != 0o600 {
		return errors.New("private path 权限未收紧")
	}
	if directory && info.Mode().Perm() != 0o700 {
		return errors.New("private directory 权限必须是 0700")
	}
	return nil
}
