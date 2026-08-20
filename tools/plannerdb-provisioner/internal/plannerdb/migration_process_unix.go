//go:build !windows

package plannerdb

import (
	"context"
	"errors"
	"os/exec"
	"path/filepath"
	"syscall"
)

func executeMigrationCommand(ctx context.Context, executable string, arguments, environment []string) ([]byte, error) {
	command := exec.Command(executable, arguments...)
	command.Env = append([]string(nil), environment...)
	command.Dir = filepath.Dir(executable)
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	output := &boundedOutput{limit: maximumMigrationOutputBytes}
	command.Stdout = output
	command.Stderr = output
	if err := command.Start(); err != nil {
		return nil, err
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	var err error
	select {
	case err = <-done:
	case <-ctx.Done():
		_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		<-done
		err = ctx.Err()
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return append([]byte(nil), output.buffer.Bytes()...), err
	}
	return append([]byte(nil), output.buffer.Bytes()...), err
}
