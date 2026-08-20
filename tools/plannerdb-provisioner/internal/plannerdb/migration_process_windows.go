//go:build windows

package plannerdb

import (
	"context"
	"os/exec"
	"path/filepath"
	"unsafe"

	"golang.org/x/sys/windows"
)

func executeMigrationCommand(ctx context.Context, executable string, arguments, environment []string) ([]byte, error) {
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return nil, err
	}
	defer windows.CloseHandle(job)
	limits := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	limits.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if _, err := windows.SetInformationJobObject(job, windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&limits)), uint32(unsafe.Sizeof(limits))); err != nil {
		return nil, err
	}

	command := exec.Command(executable, arguments...)
	command.Env = append([]string(nil), environment...)
	command.Dir = filepath.Dir(executable)
	output := &boundedOutput{limit: maximumMigrationOutputBytes}
	command.Stdout = output
	command.Stderr = output
	if err := command.Start(); err != nil {
		return nil, err
	}
	processHandle, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE, false, uint32(command.Process.Pid))
	if err != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		return append([]byte(nil), output.buffer.Bytes()...), err
	}
	assignErr := windows.AssignProcessToJobObject(job, processHandle)
	windows.CloseHandle(processHandle)
	if assignErr != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		return append([]byte(nil), output.buffer.Bytes()...), assignErr
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	select {
	case err = <-done:
	case <-ctx.Done():
		_ = windows.TerminateJobObject(job, 1)
		<-done
		err = ctx.Err()
	}
	return append([]byte(nil), output.buffer.Bytes()...), err
}
