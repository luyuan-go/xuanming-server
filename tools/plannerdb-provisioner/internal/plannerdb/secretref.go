package plannerdb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var secretEnvironmentNamePattern = regexp.MustCompile(`^PANDORA_[A-Z0-9_]{1,127}$`)

// loadSecretRef 只接受 env:NAME 或 file:<absolute path>。调用方永远不能把秘密值
// 本身放进 flag；错误也只描述引用，不回显引用解析出的内容。
func loadSecretRef(reference string, maximumBytes int64) ([]byte, error) {
	if maximumBytes <= 0 {
		return nil, errors.New("secret 最大字节数必须大于 0")
	}
	switch {
	case strings.HasPrefix(reference, "env:"):
		name := strings.TrimPrefix(reference, "env:")
		if !secretEnvironmentNamePattern.MatchString(name) {
			return nil, errors.New("secret env 引用必须是 env:PANDORA_<UPPERCASE_NAME>")
		}
		value, ok := os.LookupEnv(name)
		if !ok || value == "" {
			return nil, fmt.Errorf("secret 环境变量 %s 未设置或为空", name)
		}
		if int64(len(value)) > maximumBytes {
			return nil, fmt.Errorf("secret 环境变量 %s 超过 %d 字节上限", name, maximumBytes)
		}
		return []byte(value), nil
	case strings.HasPrefix(reference, "file:"):
		path := strings.TrimPrefix(reference, "file:")
		if path == "" || !filepath.IsAbs(path) {
			return nil, errors.New("secret file 引用必须使用绝对路径")
		}
		path = filepath.Clean(path)
		info, err := os.Lstat(path)
		if err != nil {
			return nil, fmt.Errorf("读取 secret file 元数据 %q: %w", path, err)
		}
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("secret file %q 必须是非符号链接普通文件", path)
		}
		if info.Size() <= 0 || info.Size() > maximumBytes {
			return nil, fmt.Errorf("secret file %q 大小必须在 1..%d 字节", path, maximumBytes)
		}
		if err := validateSecureSecretFile(path, info); err != nil {
			return nil, fmt.Errorf("secret file %q ACL 不安全: %w", path, err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("读取 secret file %q: %w", path, err)
		}
		if int64(len(data)) != info.Size() {
			return nil, fmt.Errorf("secret file %q 读取期间发生变化", path)
		}
		return data, nil
	default:
		return nil, errors.New("secret 只允许 env:NAME 或 file:<absolute path> 引用，禁止 inline 值")
	}
}
