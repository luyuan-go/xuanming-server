//go:build windows

package plannerdb

import (
	"errors"
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

const windowsSecretReadMask windows.ACCESS_MASK = 0x00000001 | 0x00000008 | 0x00000080 | 0x10000000 | 0x80000000

func validateSecureSecretFile(path string, _ os.FileInfo) error {
	descriptor, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		return err
	}
	if descriptor == nil {
		return errors.New("缺少 security descriptor")
	}
	trusted, err := trustedSecretSIDs()
	if err != nil {
		return err
	}
	owner, _, err := descriptor.Owner()
	if err != nil || owner == nil || !trusted[owner.String()] {
		return errors.New("文件 owner 不是当前服务身份、LocalSystem 或 Administrators")
	}
	dacl, _, err := descriptor.DACL()
	if err != nil || dacl == nil {
		return errors.New("文件缺少受限 DACL")
	}
	for index := uint16(0); index < dacl.AceCount; index++ {
		var ace *windows.ACCESS_ALLOWED_ACE
		if err := windows.GetAce(dacl, uint32(index), &ace); err != nil {
			return fmt.Errorf("读取 ACE %d: %w", index, err)
		}
		if ace == nil {
			return fmt.Errorf("ACE %d 为空", index)
		}
		switch ace.Header.AceType {
		case windows.ACCESS_DENIED_ACE_TYPE:
			continue
		case windows.ACCESS_ALLOWED_ACE_TYPE:
			if ace.Mask&windowsSecretReadMask == 0 {
				continue
			}
			sid := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
			if sid == nil {
				return fmt.Errorf("ACE %d 缺少 SID", index)
			}
			if !trusted[sid.String()] {
				return fmt.Errorf("ACE %d 向非受信 SID %s 授予读取权限", index, sid.String())
			}
		default:
			return fmt.Errorf("ACE %d 使用未审核类型 %d", index, ace.Header.AceType)
		}
	}
	return nil
}

func trustedSecretSIDs() (map[string]bool, error) {
	current, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return nil, err
	}
	trusted := map[string]bool{current.User.Sid.String(): true}
	for _, raw := range []string{"S-1-5-18", "S-1-5-32-544"} {
		sid, err := windows.StringToSid(raw)
		if err != nil {
			return nil, err
		}
		trusted[sid.String()] = true
	}
	return trusted, nil
}
