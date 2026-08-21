package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestWriteGeneratedFilesConcurrentReadersSeeOnlyCompleteVersions(t *testing.T) {
	dir := t.TempDir()
	const name = "level_table.gen.go"
	target := filepath.Join(dir, name)
	old := append([]byte("// old\n"), bytes.Repeat([]byte("o"), 128*1024)...)
	newer := append([]byte("// new\n"), bytes.Repeat([]byte("n"), 128*1024)...)
	if err := os.WriteFile(target, old, 0o644); err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	started := make(chan struct{})
	readerErr := make(chan error, 1)
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		first := true
		for {
			select {
			case <-stop:
				return
			default:
			}
			// Windows 的 os.Open 不带 FILE_SHARE_DELETE；它和 MoveFileEx 的极短窗口
			// 相撞时可能暂时返回 sharing violation。它不代表读到半文件，短暂重试后
			// 仍必须只能得到完整 old/new；持续不可读则照样让测试失败。
			var raw []byte
			var err error
			readDeadline := time.Now().Add(100 * time.Millisecond)
			for {
				raw, err = os.ReadFile(target)
				if err == nil || time.Now().After(readDeadline) {
					break
				}
				time.Sleep(50 * time.Microsecond)
			}
			if err != nil {
				select {
				case readerErr <- fmt.Errorf("并发读取目标持续失败: %w", err):
				default:
				}
				return
			}
			if first {
				close(started)
				first = false
			}
			if !bytes.Equal(raw, old) && !bytes.Equal(raw, newer) {
				select {
				case readerErr <- fmt.Errorf("读到半写文件: len=%d prefix=%q", len(raw), raw[:min(len(raw), 16)]):
				default:
				}
				return
			}
		}
	}()
	<-started

	for i := 0; i < 80; i++ {
		want := newer
		if i%2 == 1 {
			want = old
		}
		if _, err := writeGeneratedFiles(dir, map[string][]byte{name: want}); err != nil {
			close(stop)
			readers.Wait()
			t.Fatalf("第 %d 次替换失败: %v", i, err)
		}
	}
	close(stop)
	readers.Wait()
	select {
	case err := <-readerErr:
		t.Fatal(err)
	default:
	}
}

func TestWriteGeneratedFilesStageFailureKeepsOldTargetsAndRemovesTemps(t *testing.T) {
	tests := []struct {
		name string
		op   string
	}{
		{name: "Write失败", op: "write"},
		{name: "Sync失败", op: "sync"},
		{name: "Close失败", op: "close"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			oldA := []byte("package configtable\n// old a\n")
			oldB := []byte("package configtable\n// old b\n")
			for name, raw := range map[string][]byte{
				"a_table.gen.go": oldA,
				"b_table.gen.go": oldB,
			} {
				if err := os.WriteFile(filepath.Join(dir, name), raw, 0o644); err != nil {
					t.Fatal(err)
				}
			}

			injected := errors.New("injected " + tc.op + " failure")
			ops := osGeneratedFileOps()
			created := 0
			var tempPaths []string
			ops.createTemp = func(dir, pattern string) (generatedTempFile, error) {
				f, err := os.CreateTemp(dir, pattern)
				if err != nil {
					return nil, err
				}
				created++
				tempPaths = append(tempPaths, f.Name())
				if created == 2 {
					return &faultingGeneratedTemp{File: f, op: tc.op, err: injected}, nil
				}
				return f, nil
			}

			_, err := writeGeneratedFilesWithOps(dir, map[string][]byte{
				"a_table.gen.go": []byte("package configtable\n// new a\n"),
				"b_table.gen.go": []byte("package configtable\n// new b\n"),
			}, ops)
			if !errors.Is(err, injected) {
				t.Fatalf("应返回原始注入错误,got %v", err)
			}

			assertFileBytes(t, filepath.Join(dir, "a_table.gen.go"), oldA)
			assertFileBytes(t, filepath.Join(dir, "b_table.gen.go"), oldB)
			if len(tempPaths) != 2 {
				t.Fatalf("应先完整 staging 到第二个文件再失败,temp=%v", tempPaths)
			}
			for _, temp := range tempPaths {
				if strings.HasSuffix(strings.ToLower(temp), ".go") {
					t.Errorf("临时文件不得以 .go 结尾: %s", temp)
				}
				if filepath.Dir(temp) != dir {
					t.Errorf("临时文件必须和目标同目录: %s", temp)
				}
				if _, statErr := os.Stat(temp); !os.IsNotExist(statErr) {
					t.Errorf("失败后临时文件仍残留 %s: %v", temp, statErr)
				}
			}
		})
	}
}

func TestWriteGeneratedFilesReplaceFailureKeepsCurrentOldTargetAndRemovesTemp(t *testing.T) {
	dir := t.TempDir()
	const name = "a_table.gen.go"
	target := filepath.Join(dir, name)
	old := []byte("package configtable\n// old\n")
	if err := os.WriteFile(target, old, 0o644); err != nil {
		t.Fatal(err)
	}

	injected := errors.New("injected replace failure")
	ops := osGeneratedFileOps()
	var tempPath string
	ops.createTemp = func(dir, pattern string) (generatedTempFile, error) {
		f, err := os.CreateTemp(dir, pattern)
		if err == nil {
			tempPath = f.Name()
		}
		return f, err
	}
	ops.rename = func(_, _ string) error { return injected }

	_, err := writeGeneratedFilesWithOps(dir, map[string][]byte{
		name: []byte("package configtable\n// new\n"),
	}, ops)
	if !errors.Is(err, injected) {
		t.Fatalf("应返回原始替换错误,got %v", err)
	}
	assertFileBytes(t, target, old)
	if tempPath == "" {
		t.Fatal("测试未创建 staging 临时文件")
	}
	if _, statErr := os.Stat(tempPath); !os.IsNotExist(statErr) {
		t.Fatalf("替换失败后临时文件仍残留 %s: %v", tempPath, statErr)
	}
}

type faultingGeneratedTemp struct {
	*os.File
	op  string
	err error
}

func (f *faultingGeneratedTemp) Write(p []byte) (int, error) {
	if f.op != "write" {
		return f.File.Write(p)
	}
	if len(p) == 0 {
		return 0, f.err
	}
	n, _ := f.File.Write(p[:len(p)/2])
	return n, f.err
}

func (f *faultingGeneratedTemp) Sync() error {
	if f.op == "sync" {
		return f.err
	}
	return f.File.Sync()
}

func (f *faultingGeneratedTemp) Close() error {
	err := f.File.Close()
	if f.op == "close" {
		return f.err
	}
	return err
}

func assertFileBytes(t *testing.T, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s 内容被改写:\n got=%q\nwant=%q", path, got, want)
	}
}
