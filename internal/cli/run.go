package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/credentials"

	appcfg "github.com/darrensoothill/s3aibench/internal/config"
	"github.com/darrensoothill/s3aibench/internal/metrics"
	"github.com/darrensoothill/s3aibench/internal/plan"
	"github.com/darrensoothill/s3aibench/internal/report"
	"github.com/darrensoothill/s3aibench/internal/runner"
	"github.com/darrensoothill/s3aibench/internal/s3client"
	"github.com/darrensoothill/s3aibench/internal/safety"
	"github.com/darrensoothill/s3aibench/internal/workload"
	"github.com/darrensoothill/s3aibench/internal/workload/checkpoint"
	"github.com/darrensoothill/s3aibench/internal/workload/dataloader"
	"github.com/darrensoothill/s3aibench/internal/workload/lancedb"
	"github.com/darrensoothill/s3aibench/internal/workload/largeobj"
	"github.com/darrensoothill/s3aibench/internal/workload/metadata"
	"github.com/darrensoothill/s3aibench/internal/workload/mix"
	"github.com/darrensoothill/s3aibench/internal/workload/smallobj"
	"github.com/darrensoothill/s3aibench/pkg/reportschema"

	"github.com/oklog/ulid/v2"
	"github.com/spf13/cobra"
)

// ClientFactory allows tests to inject a fake s3client.Client.
type ClientFactory func(cfg *appcfg.Config) (s3client.Client, error)

// defaultClientFactory wires an AWS-backed client. Package-level so tests can
// swap it.
var defaultClientFactory ClientFactory = buildAWSClient

func newRunCmd(_ context.Context) *cobra.Command {
	var planPath string
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Execute a benchmark plan",
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
		// Re-create the logger at the resolved log level for the actual run.
		logger = loggerFor(cfg.LogLevel, stderrFile(cmd))
		return doRun(cmd.Context(), p, cfg, cmd.OutOrStdout(), logger, defaultClientFactory)
	}
	return cmd
}

// runRunner is the indirection point so tests can inject a deterministic
// runner without spinning up goroutines.
var runRunner = runner.Run

func doRun(ctx context.Context, p *plan.Plan, cfg *appcfg.Config, out io.Writer, logger *slog.Logger, mkClient ClientFactory) error {
	_ = p
	runID := ulid.Make().String()
	cfg.Prefix = cfg.Prefix + runID + "/"

	startPprof(ctx, rootFlagValues.pprofAddr, func(err error) {
		logger.Error("pprof server error", "err", err)
	})

	registerWorkloads()

	client, err := mkClient(cfg)
	if err != nil {
		return err
	}
	if err := safety.CheckBucket(ctx, client, cfg.Prefix, cfg.AllowSharedBucket); err != nil {
		return err
	}

	var wls []workload.Workload
	for _, entry := range cfg.Workloads {
		w, err := workload.Build(entry)
		if err != nil {
			return fmt.Errorf("build workload %q: %w", entry.Name, err)
		}
		wls = append(wls, w)
	}

	rec := metrics.NewCollector()

	// Optional live progress ticker.
	if cfg.Progress {
		tickCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		tk := &report.Ticker{Recorder: rec, Interval: time.Second, Out: out}
		go tk.Run(tickCtx)
	}

	if _, err = runRunner(ctx, runner.Options{
		S3:          client,
		Recorder:    rec,
		Logger:      logger,
		Workloads:   wls,
		Duration:    cfg.Duration,
		Warmup:      cfg.Warmup,
		Threads:     cfg.Threads,
		PartSize:    cfg.MultipartPartSize,
		Concurrency: cfg.MultipartConcurrency,
		RunPrefix:   cfg.Prefix,
		Seed:        cfg.RandomSeed,
		Prepopulate: cfg.Prepopulate,
	}); err != nil {
		return err
	}

	rep := report.Build(rec.Snapshot(), cfg)
	if err := writeReports(rep, cfg, out); err != nil {
		return err
	}

	if cfg.Cleanup {
		for _, w := range wls {
			if cerr := w.Cleanup(ctx, &workload.Env{S3: client, Recorder: rec}); cerr != nil {
				return cerr
			}
		}
		if _, cerr := safety.Cleanup(ctx, client, cfg.Prefix); cerr != nil {
			return cerr
		}
	}
	return nil
}

// writeReports emits text to cfg.OutputText (or stdout if empty) and JSON to
// cfg.OutputJSON when set.
func writeReports(rep *reportschema.Report, cfg *appcfg.Config, stdout io.Writer) error {
	textOut := stdout
	if cfg.OutputText != "" {
		f, err := os.Create(cfg.OutputText)
		if err != nil {
			return err
		}
		defer f.Close()
		textOut = f
	}
	if err := report.WriteText(textOut, rep); err != nil {
		return err
	}
	if cfg.OutputJSON != "" {
		f, err := os.Create(cfg.OutputJSON)
		if err != nil {
			return err
		}
		defer f.Close()
		if err := report.WriteJSON(f, rep); err != nil {
			return err
		}
	}
	return nil
}

// buildAWSClient constructs a real AWS-backed s3client.Client from cfg.
func buildAWSClient(cfg *appcfg.Config) (s3client.Client, error) {
	if cfg.AccessKey == "" || cfg.SecretKey == "" {
		return nil, errors.New("no credentials resolved from plan; set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY or inline access_key/secret_key")
	}
	provider := credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, "")
	httpClient := &http.Client{
		Transport: s3client.NewTransport(s3client.TransportOptions{
			MaxIdleConnsPerHost: cfg.ConnectionPoolSize,
			TLSSkipVerify:       cfg.TLSSkipVerify,
			ForceHTTP2:          cfg.HTTP2,
		}),
	}
	return s3client.NewAWS(s3client.AWSConfig{
		Endpoint:             cfg.Endpoint,
		Region:               cfg.Region,
		Bucket:               cfg.Bucket,
		Prefix:               cfg.Prefix,
		PathStyle:            cfg.PathStyle,
		Credentials:          provider,
		HTTPClient:           httpClient,
		MultipartPartSize:    cfg.MultipartPartSize,
		MultipartConcurrency: cfg.MultipartConcurrency,
		MaxRetries:           cfg.MaxRetries,
	}), nil
}

// stderrFile returns the error writer for a cobra command, defaulted to stderr.
func stderrFile(cmd *cobra.Command) *os.File {
	_ = cmd
	return os.Stderr
}

// registerWorkloads is the single place where M0+M1 workload factories are
// wired into the registry. Each subcommand calls this before Build().
func registerWorkloads() {
	workload.Reset()
	smallobj.Register()
	largeobj.Register()
	checkpoint.Register()
	lancedb.Register()
	dataloader.Register()
	metadata.Register()
	// mix must be registered last so nested workloads can resolve at Build-time.
	mix.Register()
}
