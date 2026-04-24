package cli

import (
	"context"
	"io"
	"os"

	"github.com/darrensoothill/s3aibench/internal/version"
	"github.com/spf13/cobra"
)

// Persistent root-level flags shared across subcommands.
type rootFlags struct {
	pprofAddr string
}

// rootFlagValues is set by NewRoot so doRun can read the persistent flags
// after parsing. Tests can swap it.
var rootFlagValues rootFlags

// NewRoot returns the top-level cobra command. Out/err/ctx are injected for
// tests; a nil ctx defaults to the process's signal-aware context.
func NewRoot(ctx context.Context, out, errOut io.Writer) *cobra.Command {
	if out == nil {
		out = os.Stdout
	}
	if errOut == nil {
		errOut = os.Stderr
	}
	root := &cobra.Command{
		Use:           "s3aibench",
		Short:         "S3 AI workload benchmark",
		SilenceUsage:  true,
		SilenceErrors: true,
		Version:       version.String(),
	}
	root.SetOut(out)
	root.SetErr(errOut)
	root.PersistentFlags().StringVar(&rootFlagValues.pprofAddr, "pprof-addr", "", "bind address for net/http/pprof (empty=off)")
	root.AddCommand(newValidateCmd(ctx))
	root.AddCommand(newRunCmd(ctx))
	root.AddCommand(newPrepopulateCmd(ctx))
	root.AddCommand(newCleanupCmd(ctx))
	return root
}
