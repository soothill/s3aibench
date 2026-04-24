package main

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
)

func TestRunHelp(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := run(context.Background(), []string{"--help"}, &out, &errOut); code != 0 {
		t.Fatalf("exit code=%d, stderr=%s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "s3aibench") {
		t.Fatalf("help output missing name: %s", out.String())
	}
}

func TestRunUnknownCommand(t *testing.T) {
	var out, errOut bytes.Buffer
	code := run(context.Background(), []string{"nope"}, &out, &errOut)
	if code == 0 {
		t.Fatal("expected non-zero exit for unknown command")
	}
	if !strings.Contains(errOut.String(), "error:") {
		t.Fatalf("expected error prefix, got %q", errOut.String())
	}
}

func TestMainImpl(t *testing.T) {
	origExit := osExit
	origArgs := osArgs
	origOut := stdout
	origErr := stderr
	defer func() {
		osExit = origExit
		osArgs = origArgs
		stdout = origOut
		stderr = origErr
	}()
	var code int
	var out bytes.Buffer
	osExit = func(c int) { code = c }
	osArgs = []string{"--help"}
	stdout = &out
	stderr = io.Discard
	mainImpl()
	if code != 0 {
		t.Fatalf("exit=%d", code)
	}
	if !strings.Contains(out.String(), "s3aibench") {
		t.Fatalf("help missing: %s", out.String())
	}
}

func TestMainEntrypoint(t *testing.T) {
	// Hit the main() bridge so it isn't reported as 0% coverage. We override
	// osExit so the test process doesn't terminate.
	origExit := osExit
	origArgs := osArgs
	origOut := stdout
	origErr := stderr
	defer func() {
		osExit = origExit
		osArgs = origArgs
		stdout = origOut
		stderr = origErr
	}()
	osExit = func(int) {}
	osArgs = []string{"--help"}
	stdout = io.Discard
	stderr = io.Discard
	main()
}
