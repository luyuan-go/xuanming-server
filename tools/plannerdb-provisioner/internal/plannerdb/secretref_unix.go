//go:build !windows

package plannerdb

import (
	"errors"
	"os"
)

func validateSecureSecretFile(_ string, info os.FileInfo) error {
	if info.Mode().Perm()&0o077 != 0 {
		return errors.New("group/other 不能有任何权限（要求 mode 0600 或更严格）")
	}
	return nil
}
