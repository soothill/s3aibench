package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
)

func newValidateCmd(_ context.Context) *cobra.Command {
	var planPath string
	cmd := &cobra.Command{
		Use:   "validate",
		Short: "Parse a plan + env + flags and print the resolved config",
	}
	getOverrides := flagsIntoOverrides(cmd.Flags())
	cmd.Flags().StringVar(&planPath, "plan", "", "path to plan YAML")
	_ = cmd.MarkFlagRequired("plan")
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		logger := newLogger("info", stderrFile(cmd))
		p, cfg, err := loadAndResolve(planPath, getOverrides(), osEnviron, logger)
		if err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "plan: %s (%d workloads)\n", p.Name, len(p.Workloads))
		for _, line := range cfg.DumpSources() {
			fmt.Fprintln(cmd.OutOrStdout(), line)
		}
		fmt.Fprintf(cmd.OutOrStdout(), "config_hash=%s\n", cfg.ConfigHash)
		return nil
	}
	return cmd
}
