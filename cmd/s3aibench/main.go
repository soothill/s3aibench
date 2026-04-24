package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/darrensoothill/s3aibench/internal/cli"
)

// run is the testable entry point. It returns the process exit code so main
// can forward it to os.Exit without swallowing test failures.
func run(ctx context.Context, args []string, out, errOut io.Writer) int {
	root := cli.NewRoot(ctx, out, errOut)
	root.SetArgs(args)
	if err := root.ExecuteContext(ctx); err != nil {
		fmt.Fprintf(errOut, "error: %v\n", err)
		return 1
	}
	return 0
}

// Vars below allow tests to override process-level glue without invoking a
// subprocess.
var (
	osExit           = os.Exit
	osArgs           = os.Args[1:]
	stdout io.Writer = os.Stdout
	stderr io.Writer = os.Stderr
)

func mainImpl() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	osExit(run(ctx, osArgs, stdout, stderr))
}

func main() { mainImpl() }
