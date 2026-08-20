package main

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

const testIssuedRawToken = "ISEhISEhISEhISEhISEhISEhISEhISEhISEhISEhISE"

type secretErrorWriter struct{}

func (secretErrorWriter) Write([]byte) (int, error) {
	return 0, errors.New("writer accidentally included admin-dsn/password")
}

func TestIssuedTokenStdoutFormatsAreStableAndSingleLine(t *testing.T) {
	const workspaceID = "01arz3ndektsv4rrffq69g5fav"
	var ordinary bytes.Buffer
	if err := writeEnrollmentToken(&ordinary, testIssuedRawToken); err != nil {
		t.Fatal(err)
	}
	if got, want := ordinary.String(), testIssuedRawToken+"\n"; got != want {
		t.Fatalf("ordinary stdout=%q want=%q", got, want)
	}

	var recovery bytes.Buffer
	if err := writeRecoveryCode(&recovery, workspaceID, testIssuedRawToken); err != nil {
		t.Fatal(err)
	}
	want := workspaceID + "." + testIssuedRawToken + "\n"
	if got := recovery.String(); got != want || strings.Count(got, "\n") != 1 {
		t.Fatalf("recovery stdout=%q want=%q", got, want)
	}
	parts := strings.Split(strings.TrimSuffix(recovery.String(), "\n"), ".")
	if len(parts) != 2 || parts[0] != workspaceID || parts[1] != testIssuedRawToken {
		t.Fatalf("recovery code cannot be split exactly: %#v", parts)
	}
}

func TestIssuedTokenOutputFailureDoesNotEmbedWriterSecret(t *testing.T) {
	for name, write := range map[string]func(io.Writer) error{
		"enroll": func(writer io.Writer) error { return writeEnrollmentToken(writer, testIssuedRawToken) },
		"recovery": func(writer io.Writer) error {
			return writeRecoveryCode(writer, "01arz3ndektsv4rrffq69g5fav", testIssuedRawToken)
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := write(secretErrorWriter{})
			if !errors.Is(err, errTokenOutputFailed) {
				t.Fatalf("output error=%v", err)
			}
			if strings.Contains(err.Error(), "admin-dsn") || strings.Contains(err.Error(), "password") {
				t.Fatalf("writer secret leaked through output error: %v", err)
			}
		})
	}
}

func TestValidateServeDurationsKeepsWorkerDeadlineOutsideMigrationDeadline(t *testing.T) {
	if err := validateServeDurations(3*time.Hour, 3*time.Hour+5*time.Minute); err != nil {
		t.Fatal(err)
	}
	for _, values := range [][2]time.Duration{
		{30 * time.Second, 2 * time.Minute},
		{time.Hour, time.Hour},
		{time.Hour, time.Hour + 30*time.Second},
		{4*time.Hour + time.Second, 4*time.Hour + 2*time.Minute},
		{4 * time.Hour, 5*time.Hour + time.Second},
	} {
		if err := validateServeDurations(values[0], values[1]); err == nil {
			t.Fatalf("durations unexpectedly accepted: migration=%s provision=%s", values[0], values[1])
		}
	}
}
