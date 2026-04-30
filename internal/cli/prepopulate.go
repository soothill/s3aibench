package cli

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/darrensoothill/s3aibench/internal/workload"

	"github.com/oklog/ulid/v2"
	"github.com/spf13/cobra"
)

func newPrepopulateCmd(_ context.Context) *cobra.Command {
	var planPath string
	cmd := &cobra.Command{
		Use:   "prepopulate",
		Short: "Create the dataset described by a plan without running workloads",
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
		logResolvedConfig(logger, cfg)
		cfg.Prefix = cfg.Prefix + ulid.Make().String() + "/"

		registerWorkloads()
		client, err := defaultClientFactory(cfg)
		if err != nil {
			return err
		}
		for _, entry := range cfg.Workloads {
			w, err := workload.Build(entry)
			if err != nil {
				return fmt.Errorf("build workload %q: %w", entry.Name, err)
			}
			if err := w.Prepopulate(cmd.Context(), &workload.Env{
				S3:          client,
				Threads:     cfg.Threads,
				Logger:      logger,
				Rand:        rand.New(rand.NewSource(cfg.RandomSeed + 1)),
				Seed:        cfg.RandomSeed,
				RunPrefix:   cfg.Prefix,
				PartSize:    cfg.MultipartPartSize,
				Concurrency: cfg.MultipartConcurrency,
			}); err != nil {
				return err
			}
		}
		fmt.Fprintf(cmd.OutOrStdout(), "prepopulated under prefix %s\n", cfg.Prefix)
		return nil
	}
	return cmd
}
