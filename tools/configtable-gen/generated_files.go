package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"time"
)

// generatedTempFile 是生成代码落盘所需的最小文件接口。生产实现是 *os.File；
// 抽出接口只为能在测试中稳定覆盖 Write/Sync/Close 三种真实失败边界。
type generatedTempFile interface {
	Name() string
	Write([]byte) (int, error)
	Sync() error
	Close() error
	Chmod(os.FileMode) error
}

type generatedFileOps struct {
	readFile   func(string) ([]byte, error)
	createTemp func(string, string) (generatedTempFile, error)
	rename     func(string, string) error
	remove     func(string) error
}

func osGeneratedFileOps() generatedFileOps {
	return generatedFileOps{
		readFile: os.ReadFile,
		createTemp: func(dir, pattern string) (generatedTempFile, error) {
			return os.CreateTemp(dir, pattern)
		},
		rename: replaceGeneratedFile,
		remove: removeGeneratedTemp,
	}
}

// replaceGeneratedFile 在 Windows 上容忍 reader/杀软与 MoveFileEx 的短暂共享冲突。
// Unix 的权限/路径错误不重试；Windows 也只重试 access/sharing/lock violation，超时后
// 原样返回最后错误，旧目标仍未被提前删除。
func replaceGeneratedFile(from, to string) error {
	const retryBudget = time.Second
	deadline := time.Now().Add(retryBudget)
	delay := time.Millisecond
	for {
		err := os.Rename(from, to)
		if err == nil {
			return nil
		}
		if !isTransientWindowsReplaceError(err) || time.Now().Add(delay).After(deadline) {
			return err
		}
		time.Sleep(delay)
		if delay < 25*time.Millisecond {
			delay *= 2
		}
	}
}

func isTransientWindowsReplaceError(err error) bool {
	if runtime.GOOS != "windows" {
		return false
	}
	// Win32: ERROR_ACCESS_DENIED=5、ERROR_SHARING_VIOLATION=32、
	// ERROR_LOCK_VIOLATION=33。syscall.Errno 在各目标都可编译，分支只在 Windows 生效。
	return errors.Is(err, syscall.Errno(5)) ||
		errors.Is(err, syscall.Errno(32)) ||
		errors.Is(err, syscall.Errno(33))
}

func removeGeneratedTemp(path string) error {
	const retryBudget = time.Second
	deadline := time.Now().Add(retryBudget)
	delay := time.Millisecond
	for {
		err := os.Remove(path)
		if err == nil || os.IsNotExist(err) {
			return nil
		}
		if !isTransientWindowsReplaceError(err) || time.Now().Add(delay).After(deadline) {
			return err
		}
		time.Sleep(delay)
		if delay < 25*time.Millisecond {
			delay *= 2
		}
	}
}

type generatedFileChange struct {
	name   string
	target string
	raw    []byte
	temp   string
}

// writeGeneratedFiles 把有变化的 *.gen.go 逐文件完整替换到 outDir。
//
// 所有变化文件都会先在目标同目录写入唯一、非 .go 后缀的临时文件，并且每个临时文件都必须
// Write + Sync + Close 全部成功；只有整批 staging 成功后才开始 os.Rename。Rename 不先删除
// 旧目标，所以单个路径的 reader 只会读到完整旧文件或完整新文件，不会读到 WriteFile 的截断窗口。
//
// 这里只承诺“单文件不半写”，不宣称跨文件事务原子：逐个 Rename 的中途若失败，已经替换的文件
// 保持完整新版本，尚未替换的文件保持完整旧版本，调用方会收到明确错误并停止后续发布。
func writeGeneratedFiles(outDir string, files map[string][]byte) ([]string, error) {
	return writeGeneratedFilesWithOps(outDir, files, osGeneratedFileOps())
}

func writeGeneratedFilesWithOps(outDir string, files map[string][]byte, ops generatedFileOps) ([]string, error) {
	names := make([]string, 0, len(files))
	for name := range files {
		if filepath.Base(name) != name || !strings.HasSuffix(name, ".gen.go") {
			return nil, fmt.Errorf("非法生成代码文件名 %q:只允许当前目录下的 *.gen.go", name)
		}
		names = append(names, name)
	}
	slices.Sort(names)

	changes := make([]generatedFileChange, 0, len(names))
	for _, name := range names {
		target := filepath.Join(outDir, name)
		prev, err := ops.readFile(target)
		switch {
		case err == nil && bytes.Equal(prev, files[name]):
			continue
		case err != nil && !os.IsNotExist(err):
			return nil, fmt.Errorf("读生成代码旧文件 %s 失败: %w", target, err)
		}
		changes = append(changes, generatedFileChange{
			name:   name,
			target: target,
			raw:    files[name],
		})
	}

	for i := range changes {
		temp, err := stageGeneratedFile(outDir, changes[i].name, changes[i].raw, ops)
		if err != nil {
			cleanupErr := cleanupGeneratedTemps(changes[:i], ops)
			return nil, errors.Join(fmt.Errorf("staging %s 失败: %w", changes[i].target, err), cleanupErr)
		}
		changes[i].temp = temp
	}

	replaced := make([]string, 0, len(changes))
	for i := range changes {
		if err := ops.rename(changes[i].temp, changes[i].target); err != nil {
			cleanupErr := cleanupGeneratedTemps(changes[i:], ops)
			return replaced, errors.Join(
				fmt.Errorf("完整替换 %s 失败(此前 %d 个文件可能已完整替换;跨文件不承诺原子性): %w",
					changes[i].target, len(replaced), err),
				cleanupErr,
			)
		}
		changes[i].temp = "" // Rename 成功后临时路径已经不存在,不再参与清理。
		replaced = append(replaced, changes[i].name)
	}
	return replaced, nil
}

func stageGeneratedFile(dir, name string, raw []byte, ops generatedFileOps) (tempPath string, retErr error) {
	// pattern 末尾由 CreateTemp 加随机串，临时文件不会以 .go 结尾，避免被 go list/build 扫入。
	f, err := ops.createTemp(dir, "."+name+".tmp-*")
	if err != nil {
		return "", err
	}
	createdPath := f.Name()
	tempPath = createdPath
	keep := false
	defer func() {
		if keep {
			return
		}
		_ = f.Close()
		if err := ops.remove(createdPath); err != nil && !os.IsNotExist(err) {
			retErr = errors.Join(retErr, fmt.Errorf("清理临时文件 %s 失败: %w", createdPath, err))
		}
	}()

	if strings.HasSuffix(strings.ToLower(tempPath), ".go") {
		return "", fmt.Errorf("临时文件意外以 .go 结尾:%s", tempPath)
	}
	if filepath.Clean(filepath.Dir(tempPath)) != filepath.Clean(dir) {
		return "", fmt.Errorf("临时文件不在目标同目录:%s", tempPath)
	}
	if err := f.Chmod(0o644); err != nil {
		return "", fmt.Errorf("设置临时文件权限 %s: %w", tempPath, err)
	}
	n, err := f.Write(raw)
	if err != nil {
		return "", fmt.Errorf("写临时文件 %s: %w", tempPath, err)
	}
	if n != len(raw) {
		return "", fmt.Errorf("写临时文件 %s:写入 %d/%d 字节: %w", tempPath, n, len(raw), io.ErrShortWrite)
	}
	if err := f.Sync(); err != nil {
		return "", fmt.Errorf("同步临时文件 %s: %w", tempPath, err)
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("关闭临时文件 %s: %w", tempPath, err)
	}
	keep = true
	return tempPath, nil
}

func cleanupGeneratedTemps(changes []generatedFileChange, ops generatedFileOps) error {
	var errs []error
	for _, change := range changes {
		if change.temp == "" {
			continue
		}
		if err := ops.remove(change.temp); err != nil && !os.IsNotExist(err) {
			errs = append(errs, fmt.Errorf("清理临时文件 %s 失败: %w", change.temp, err))
		}
	}
	return errors.Join(errs...)
}
