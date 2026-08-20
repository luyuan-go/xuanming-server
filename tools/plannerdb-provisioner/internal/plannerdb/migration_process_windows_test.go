//go:build windows

package plannerdb

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWindowsMigrationExecutorRunsInsideKillOnCloseJob(t *testing.T) {
	command := filepath.Join(os.Getenv("SystemRoot"), "System32", "cmd.exe")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	output, err := executeMigrationCommand(ctx, command,
		[]string{"/d", "/s", "/c", "ping -n 2 127.0.0.1 >nul && echo job-ok"}, safeMigrationEnvironment())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(output), "job-ok") {
		t.Fatalf("output=%q", output)
	}
}
