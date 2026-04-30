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

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
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
	logResolvedConfig(logger, cfg)
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
	specs, err := buildWorkloadSpecs(cfg, wls)
	if err != nil {
		return err
	}

	rec := metrics.NewCollector()

	// Optional live progress ticker.
	if cfg.Progress {
		tickCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		tk := &report.Ticker{Recorder: rec, Interval: cfg.ProgressInterval, Out: out}
		go tk.Run(tickCtx)
	}

	var runErr error
	if _, err = runRunner(ctx, runner.Options{
		S3:          client,
		Recorder:    rec,
		Logger:      logger,
		Specs:       specs,
		Duration:    cfg.Duration,
		Warmup:      cfg.Warmup,
		Threads:     cfg.Threads,
		PartSize:    cfg.MultipartPartSize,
		Concurrency: cfg.MultipartConcurrency,
		RunPrefix:   cfg.Prefix,
		Seed:        cfg.RandomSeed,
		Prepopulate: cfg.Prepopulate,
	}); err != nil {
		runErr = err
	} else {
		rep := report.Build(rec.Snapshot(), cfg)
		runErr = writeReports(rep, cfg, out)
	}

	if cfg.Cleanup {
		if cerr := cleanupAfterRun(context.WithoutCancel(ctx), client, cfg.Prefix); cerr != nil {
			return errors.Join(runErr, cerr)
		}
	}
	return runErr
}

func logResolvedConfig(logger *slog.Logger, cfg *appcfg.Config) {
	if logger == nil || cfg == nil {
		return
	}
	for _, line := range cfg.DumpSources() {
		logger.Info("resolved config", "field", line)
	}
	logger.Info("resolved config hash", "config_hash", cfg.ConfigHash)
}

func buildWorkloadSpecs(cfg *appcfg.Config, wls []workload.Workload) ([]runner.WorkloadSpec, error) {
	if cfg.Threads <= 0 {
		return nil, fmt.Errorf("defaults.threads must be >0")
	}
	if len(cfg.Workloads) != len(wls) {
		return nil, fmt.Errorf("workload plan/build mismatch")
	}
	specs := make([]runner.WorkloadSpec, len(wls))
	explicitTotal := 0
	var weighted []int
	totalWeight := 0
	for i, w := range cfg.Workloads {
		d := w.Duration.AsDuration()
		if d <= 0 {
			d = cfg.Duration
		}
		if d <= 0 {
			return nil, fmt.Errorf("workload %q duration must be >0", w.Name)
		}
		specs[i] = runner.WorkloadSpec{Workload: wls[i], Duration: d}
		if w.Threads > 0 {
			specs[i].Threads = w.Threads
			explicitTotal += w.Threads
			continue
		}
		weighted = append(weighted, i)
		weight := w.Weight
		if weight <= 0 {
			weight = 1
		}
		totalWeight += weight
	}
	if explicitTotal > cfg.Threads {
		return nil, fmt.Errorf("explicit workload threads (%d) exceed defaults.threads (%d)", explicitTotal, cfg.Threads)
	}
	remaining := cfg.Threads - explicitTotal
	if len(weighted) > 0 {
		if remaining < len(weighted) {
			return nil, fmt.Errorf("remaining workload threads (%d) cannot give each weighted workload at least one thread", remaining)
		}
		assigned := 0
		type rem struct {
			idx int
			val int
		}
		remainders := make([]rem, 0, len(weighted))
		for _, idx := range weighted {
			weight := cfg.Workloads[idx].Weight
			if weight <= 0 {
				weight = 1
			}
			product := remaining * weight
			n := product / totalWeight
			if n == 0 {
				n = 1
			}
			specs[idx].Threads = n
			assigned += n
			remainders = append(remainders, rem{idx: idx, val: product % totalWeight})
		}
		for assigned > remaining {
			for i := len(weighted) - 1; i >= 0 && assigned > remaining; i-- {
				idx := weighted[i]
				if specs[idx].Threads > 1 {
					specs[idx].Threads--
					assigned--
				}
			}
		}
		for assigned < remaining {
			best := 0
			for i := 1; i < len(remainders); i++ {
				if remainders[i].val > remainders[best].val {
					best = i
				}
			}
			specs[remainders[best].idx].Threads++
			assigned++
			remainders[best].val = -1
		}
	}
	return specs, nil
}

func cleanupAfterRun(ctx context.Context, client s3client.Client, prefix string) error {
	if _, err := safety.Cleanup(ctx, client, prefix); err != nil {
		return err
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
	provider, err := credentialProvider(cfg)
	if err != nil {
		return nil, err
	}
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

func credentialProvider(cfg *appcfg.Config) (aws.CredentialsProvider, error) {
	if cfg.AccessKey != "" || cfg.SecretKey != "" {
		if cfg.AccessKey == "" || cfg.SecretKey == "" {
			return nil, errors.New("inline access_key and secret_key must be set together")
		}
		return credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""), nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	opts := []func(*awscfg.LoadOptions) error{}
	if cfg.Region != "" {
		opts = append(opts, awscfg.WithRegion(cfg.Region))
	}
	loaded, err := awscfg.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS credential chain: %w", err)
	}
	if _, err := loaded.Credentials.Retrieve(ctx); err != nil {
		return nil, fmt.Errorf("resolve AWS credentials from environment/profile/metadata: %w", err)
	}
	return loaded.Credentials, nil
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
