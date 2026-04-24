package cli

import (
	"context"
	"fmt"

	"github.com/darrensoothill/s3aibench/internal/safety"
	"github.com/spf13/cobra"
)

func newCleanupCmd(_ context.Context) *cobra.Command {
	var planPath string
	cmd := &cobra.Command{
		Use:   "cleanup",
		Short: "Delete every object under the plan's run prefix",
	}
	getOverrides := flagsIntoOverrides(cmd.Flags())
	cmd.Flags().StringVar(&planPath, "plan", "", "path to plan YAML")
	_ = cmd.MarkFlagRequired("plan")
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		logger := newLogger("info", stderrFile(cmd))
		_, cfg, err := loadAndResolve(planPath, getOverrides(), osEnviron, logger)
		if err != nil {
			return err
		}
		client, err := defaultClientFactory(cfg)
		if err != nil {
			return err
		}
		n, err := safety.Cleanup(cmd.Context(), client, cfg.Prefix)
		if err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "cleaned %d objects under %s\n", n, cfg.Prefix)
		return nil
	}
	return cmd
}
