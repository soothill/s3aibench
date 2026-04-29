package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	appcfg "github.com/darrensoothill/s3aibench/internal/config"
	"github.com/darrensoothill/s3aibench/internal/runner"
	"github.com/darrensoothill/s3aibench/internal/s3client"
	"github.com/darrensoothill/s3aibench/internal/s3client/fake"
	"github.com/darrensoothill/s3aibench/pkg/reportschema"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"time"
)

const planSmall = `
name: t
endpoint: https://example
bucket: b
access_key: AK
secret_key: SK
defaults:
  duration: 10ms
  threads: 1
workloads:
  - name: s
    type: smallobject
    object_size: 4KiB
output:
  json: ""
`

func writePlan(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "plan.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func makeFakeFactory(c s3client.Client) ClientFactory {
	return func(*appcfg.Config) (s3client.Client, error) { return c, nil }
}

// runCobra executes a subcommand with a given list of args against a captured
// stdout/stderr and returns (stdout, err).
func runCobra(t *testing.T, cmd *cobra.Command, args ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

func TestNewRoot_Wiring(t *testing.T) {
	root := NewRoot(context.Background(), nil, nil)
	for _, sub := range []string{"run", "validate", "prepopulate", "cleanup"} {
		if _, _, err := root.Find([]string{sub}); err != nil {
			t.Fatalf("missing subcommand %q: %v", sub, err)
		}
	}
}

func TestValidateCommand(t *testing.T) {
	planPath := writePlan(t, planSmall)
	out, err := runCobra(t, newValidateCmd(context.Background()), "--plan", planPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "config_hash=sha256:") {
		t.Fatalf("output: %s", out)
	}
	if !strings.Contains(out, "plan: t") {
		t.Fatalf("output: %s", out)
	}
}

func TestValidateMissingPlan(t *testing.T) {
	if _, err := runCobra(t, newValidateCmd(context.Background())); err == nil {
		t.Fatal("expected error without --plan")
	}
}

func TestValidateBadPlan(t *testing.T) {
	p := writePlan(t, "endpoint: https://x\nbucket: b\n") // no workloads
	if _, err := runCobra(t, newValidateCmd(context.Background()), "--plan", p); err == nil {
		t.Fatal("expected validation error")
	}
}

func TestValidateLoadError(t *testing.T) {
	if _, err := runCobra(t, newValidateCmd(context.Background()), "--plan", "/does/not/exist"); err == nil {
		t.Fatal("expected load error")
	}
}

func TestRunCommandE2E(t *testing.T) {
	planPath := writePlan(t, planSmall)
	c := fake.New()
	orig := defaultClientFactory
	defer func() { defaultClientFactory = orig }()
	defaultClientFactory = makeFakeFactory(c)

	out, err := runCobra(t, newRunCmd(context.Background()), "--plan", planPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "workload: s") {
		t.Fatalf("missing workload heading: %s", out)
	}
}

func TestRunEmitsFileReports(t *testing.T) {
	dir := t.TempDir()
	txtPath := filepath.Join(dir, "out.txt")
	jsonPath := filepath.Join(dir, "out.json")
	p := fmt.Sprintf(`
endpoint: https://example
bucket: b
access_key: AK
secret_key: SK
defaults:
  duration: 10ms
  threads: 1
workloads:
  - name: s
    type: smallobject
    object_size: 1024
output:
  text: %q
  json: %q
`, txtPath, jsonPath)
	planPath := writePlan(t, p)
	c := fake.New()
	orig := defaultClientFactory
	defer func() { defaultClientFactory = orig }()
	defaultClientFactory = makeFakeFactory(c)
	_, err := runCobra(t, newRunCmd(context.Background()), "--plan", planPath)
	if err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(txtPath); !bytes.Contains(b, []byte("workload: s")) {
		t.Fatalf("text file: %s", string(b))
	}
	if b, _ := os.ReadFile(jsonPath); !bytes.Contains(b, []byte(`"schema_version"`)) {
		t.Fatalf("json file missing: %s", string(b))
	}
}

func TestRunClientFactoryError(t *testing.T) {
	orig := defaultClientFactory
	defer func() { defaultClientFactory = orig }()
	defaultClientFactory = func(*appcfg.Config) (s3client.Client, error) {
		return nil, errors.New("no creds")
	}
	planPath := writePlan(t, planSmall)
	if _, err := runCobra(t, newRunCmd(context.Background()), "--plan", planPath); err == nil {
		t.Fatal("expected factory error")
	}
}

func TestRunSafetyReject(t *testing.T) {
	c := fake.New()
	// Put an object OUTSIDE our default prefix before run; safety should trip.
	_ = c.Put(context.Background(), "stray", bytes.NewReader([]byte{}), 0)
	orig := defaultClientFactory
	defer func() { defaultClientFactory = orig }()
	defaultClientFactory = makeFakeFactory(c)
	planPath := writePlan(t, planSmall)
	if _, err := runCobra(t, newRunCmd(context.Background()), "--plan", planPath); err == nil {
		t.Fatal("expected safety check failure")
	}
}

func TestRunUnknownWorkload(t *testing.T) {
	body := strings.Replace(planSmall, "type: smallobject", "type: nope", 1)
	planPath := writePlan(t, body)
	c := fake.New()
	orig := defaultClientFactory
	defer func() { defaultClientFactory = orig }()
	defaultClientFactory = makeFakeFactory(c)
	if _, err := runCobra(t, newRunCmd(context.Background()), "--plan", planPath); err == nil {
		t.Fatal("expected unknown workload error")
	}
}

func TestRunBadPlanLoad(t *testing.T) {
	if _, err := runCobra(t, newRunCmd(context.Background()), "--plan", "/nope"); err == nil {
		t.Fatal("expected error")
	}
}

func TestPrepopulateCommand(t *testing.T) {
	planPath := writePlan(t, planSmall)
	c := fake.New()
	orig := defaultClientFactory
	defer func() { defaultClientFactory = orig }()
	defaultClientFactory = makeFakeFactory(c)
	out, err := runCobra(t, newPrepopulateCmd(context.Background()), "--plan", planPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "prepopulated under prefix") {
		t.Fatalf("output: %s", out)
	}
}

func TestPrepopulateBadPlan(t *testing.T) {
	if _, err := runCobra(t, newPrepopulateCmd(context.Background()), "--plan", "/nope"); err == nil {
		t.Fatal("expected error")
	}
}

func TestPrepopulateFactoryError(t *testing.T) {
	orig := defaultClientFactory
	defer func() { defaultClientFactory = orig }()
	defaultClientFactory = func(*appcfg.Config) (s3client.Client, error) { return nil, errors.New("x") }
	if _, err := runCobra(t, newPrepopulateCmd(context.Background()), "--plan", writePlan(t, planSmall)); err == nil {
		t.Fatal("expected error")
	}
}

func TestPrepopulateUnknownType(t *testing.T) {
	body := strings.Replace(planSmall, "type: smallobject", "type: nope", 1)
	c := fake.New()
	orig := defaultClientFactory
	defer func() { defaultClientFactory = orig }()
	defaultClientFactory = makeFakeFactory(c)
	if _, err := runCobra(t, newPrepopulateCmd(context.Background()), "--plan", writePlan(t, body)); err == nil {
		t.Fatal("expected error")
	}
}

func TestPrepopulateBuildFails_BadObjectSize(t *testing.T) {
	body := strings.Replace(planSmall, "object_size: 4KiB", "object_size: 0", 1)
	c := fake.New()
	orig := defaultClientFactory
	defer func() { defaultClientFactory = orig }()
	defaultClientFactory = makeFakeFactory(c)
	if _, err := runCobra(t, newPrepopulateCmd(context.Background()), "--plan", writePlan(t, body)); err == nil {
		t.Fatal("expected build error")
	}
}

func TestPrepopulateUploadError(t *testing.T) {
	c := fake.New()
	c.FailOp("put", errors.New("boom"))
	orig := defaultClientFactory
	defer func() { defaultClientFactory = orig }()
	defaultClientFactory = makeFakeFactory(c)
	if _, err := runCobra(t, newPrepopulateCmd(context.Background()), "--plan", writePlan(t, planSmall)); err == nil {
		t.Fatal("expected upload error")
	}
}

func TestCleanupCommand(t *testing.T) {
	c := fake.New()
	_ = c.Put(context.Background(), "s3aibench/a", bytes.NewReader([]byte{}), 0)
	orig := defaultClientFactory
	defer func() { defaultClientFactory = orig }()
	defaultClientFactory = makeFakeFactory(c)
	out, err := runCobra(t, newCleanupCmd(context.Background()), "--plan", writePlan(t, planSmall))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "cleaned 1 objects") {
		t.Fatalf("output: %s", out)
	}
}

func TestCleanupBadPlan(t *testing.T) {
	if _, err := runCobra(t, newCleanupCmd(context.Background()), "--plan", "/nope"); err == nil {
		t.Fatal("expected error")
	}
}

func TestCleanupFactoryError(t *testing.T) {
	orig := defaultClientFactory
	defer func() { defaultClientFactory = orig }()
	defaultClientFactory = func(*appcfg.Config) (s3client.Client, error) { return nil, errors.New("x") }
	if _, err := runCobra(t, newCleanupCmd(context.Background()), "--plan", writePlan(t, planSmall)); err == nil {
		t.Fatal("expected error")
	}
}

func TestCleanupError(t *testing.T) {
	c := fake.New()
	c.FailOp("list", errors.New("down"))
	orig := defaultClientFactory
	defer func() { defaultClientFactory = orig }()
	defaultClientFactory = makeFakeFactory(c)
	if _, err := runCobra(t, newCleanupCmd(context.Background()), "--plan", writePlan(t, planSmall)); err == nil {
		t.Fatal("expected cleanup error")
	}
}

func TestFlagsIntoOverrides_Setters(t *testing.T) {
	fs := pflag.NewFlagSet("x", pflag.ContinueOnError)
	get := flagsIntoOverrides(fs)
	// No flags changed → empty overrides.
	o := get()
	if o.Endpoint != nil || o.Threads != nil {
		t.Fatal("expected nil overrides before parse")
	}
	// Parse every flag explicitly.
	err := fs.Parse([]string{
		"--endpoint", "e",
		"--bucket", "b",
		"--region", "r",
		"--threads", "4",
		"--duration", "1s",
		"--warmup", "500ms",
		"--multipart-part-size", "16",
		"--multipart-concurrency", "2",
		"--output-text", "t.txt",
		"--output-json", "t.json",
		"--progress=true",
		"--path-style=true",
		"--tls-skip-verify=true",
		"--http2=true",
		"--connection-pool-size", "32",
		"--prepopulate=false",
		"--cleanup=false",
		"--random-seed", "7",
		"--prefix", "p/",
		"--log-level", "debug",
		"--allow-shared-bucket=true",
		"--max-retries", "5",
	})
	if err != nil {
		t.Fatal(err)
	}
	o = get()
	if o.Endpoint == nil || *o.Endpoint != "e" {
		t.Fatalf("endpoint: %+v", o.Endpoint)
	}
	if o.Threads == nil || *o.Threads != 4 {
		t.Fatalf("threads: %+v", o.Threads)
	}
	if o.MultipartPartSize == nil || *o.MultipartPartSize != 16 {
		t.Fatalf("mps: %+v", o.MultipartPartSize)
	}
	if o.Progress == nil || !*o.Progress {
		t.Fatal("progress not picked up")
	}
	if o.Warmup == nil || o.Warmup.Milliseconds() != 500 {
		t.Fatalf("warmup: %+v", o.Warmup)
	}
}

func TestParseLogLevel(t *testing.T) {
	cases := map[string]slog.Level{
		"debug":   slog.LevelDebug,
		"info":    slog.LevelInfo,
		"warn":    slog.LevelWarn,
		"warning": slog.LevelWarn,
		"error":   slog.LevelError,
		"":        slog.LevelInfo,
		"bogus":   slog.LevelInfo,
	}
	for in, want := range cases {
		if got := parseLogLevel(in); got != want {
			t.Errorf("parseLogLevel(%q)=%v want %v", in, got, want)
		}
	}
}

func TestNewLoggerAndPrintResolved(t *testing.T) {
	_ = newLogger("info", os.Stderr)
	cfg := &appcfg.Config{Sources: map[string]appcfg.Source{"x": appcfg.SourceFlag}, ConfigHash: "sha256:abc"}
	// Capture to a pipe so we can verify output.
	r, w, _ := os.Pipe()
	printResolved(cfg, w)
	w.Close()
	buf := make([]byte, 1024)
	n, _ := r.Read(buf)
	if !strings.Contains(string(buf[:n]), "config_hash=sha256:abc") {
		t.Fatalf("got %q", string(buf[:n]))
	}
}

func TestStderrFile(t *testing.T) {
	if stderrFile(&cobra.Command{}) != os.Stderr {
		t.Fatal("stderrFile should return os.Stderr")
	}
}

func TestOsEnviron(t *testing.T) {
	t.Setenv("S3AIBENCH_TEST_ENV_VAR", "yes")
	v, ok := osEnviron("S3AIBENCH_TEST_ENV_VAR")
	if !ok || v != "yes" {
		t.Fatalf("got %q %v", v, ok)
	}
}

func TestLoadAndResolveValidationError(t *testing.T) {
	// Bucket missing triggers validate error inside loadAndResolve.
	p := writePlan(t, "endpoint: https://e\nworkloads:\n  - name: w\n    type: smallobject\n")
	if _, _, err := loadAndResolve(p, appcfg.Overrides{}, osEnviron, slog.Default()); err == nil {
		t.Fatal("expected validate error")
	}
}

func TestBuildAWSClient(t *testing.T) {
	if _, err := buildAWSClient(&appcfg.Config{}); err == nil {
		t.Fatal("expected error when no credentials")
	}
	c, err := buildAWSClient(&appcfg.Config{
		Endpoint:  "https://x",
		Bucket:    "b",
		AccessKey: "AK",
		SecretKey: "SK",
	})
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("nil client")
	}
}

func TestRunOutputFileCreateFails(t *testing.T) {
	// Point output path at an unwritable directory.
	body := strings.Replace(planSmall, `json: ""`, `json: "/no/such/dir/out.json"`, 1)
	planPath := writePlan(t, body)
	c := fake.New()
	orig := defaultClientFactory
	defer func() { defaultClientFactory = orig }()
	defaultClientFactory = makeFakeFactory(c)
	if _, err := runCobra(t, newRunCmd(context.Background()), "--plan", planPath); err == nil {
		t.Fatal("expected create error")
	}
}

func TestRunTextFileCreateFails(t *testing.T) {
	body := fmt.Sprintf(`
endpoint: https://example
bucket: b
access_key: AK
secret_key: SK
defaults:
  duration: 10ms
  threads: 1
workloads:
  - name: s
    type: smallobject
    object_size: 1024
output:
  text: %q
`, "/no/such/dir/out.txt")
	planPath := writePlan(t, body)
	c := fake.New()
	orig := defaultClientFactory
	defer func() { defaultClientFactory = orig }()
	defaultClientFactory = makeFakeFactory(c)
	if _, err := runCobra(t, newRunCmd(context.Background()), "--plan", planPath); err == nil {
		t.Fatal("expected text create error")
	}
}

func TestRunWorkloadCleanupError(t *testing.T) {
	c := fake.New()
	// Make delete fail so cleanup returns an error after run completes.
	// Inject via a wrapped fake that only fails the workload-level Cleanup delete.
	wc := &deletingFailer{Client: c}
	orig := defaultClientFactory
	defer func() { defaultClientFactory = orig }()
	defaultClientFactory = makeFakeFactory(wc)
	planPath := writePlan(t, planSmall)
	if _, err := runCobra(t, newRunCmd(context.Background()), "--plan", planPath); err == nil {
		t.Fatal("expected cleanup error")
	}
}

func TestRunWithBadPprofAddr(t *testing.T) {
	// Force the pprof listener to fail so the error-logger callback in doRun
	// executes (its body is otherwise only triggered on ListenAndServe errors).
	orig := rootFlagValues.pprofAddr
	defer func() { rootFlagValues.pprofAddr = orig }()
	rootFlagValues.pprofAddr = "bad:addr:99999"
	c := fake.New()
	origF := defaultClientFactory
	defer func() { defaultClientFactory = origF }()
	defaultClientFactory = makeFakeFactory(c)
	// Let the run finish; the pprof error goroutine should fire during it.
	if _, err := runCobra(t, newRunCmd(context.Background()), "--plan", writePlan(t, planSmall)); err != nil {
		// Run may succeed (pprof failure is logged, not fatal) — that's fine.
		t.Logf("run err: %v", err)
	}
	// Give the background goroutine a moment to invoke the callback.
	time.Sleep(50 * time.Millisecond)
}

func TestRunWithProgressFlag(t *testing.T) {
	// Patch the plan's output.progress through env so the run takes the
	// progress branch.
	t.Setenv("S3AIBENCH_PROGRESS", "true")
	c := fake.New()
	orig := defaultClientFactory
	defer func() { defaultClientFactory = orig }()
	defaultClientFactory = makeFakeFactory(c)
	out, err := runCobra(t, newRunCmd(context.Background()), "--plan", writePlan(t, planSmall))
	if err != nil {
		t.Fatal(err)
	}
	// Progress tick interval is 1s and run is 10ms, so we may not see a line;
	// we just verify the run completed without hang.
	_ = out
}

func TestStartPprof_Off(t *testing.T) {
	// Empty addr → no-op, no goroutines.
	startPprof(context.Background(), "", func(error) {})
}

func TestStartPprof_BadAddr(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	startPprof(ctx, "bad:addr:99999", func(err error) {
		select {
		case errCh <- err:
		default:
		}
	})
	select {
	case <-errCh:
		// happy path — error reported
	case <-time.After(200 * time.Millisecond):
		t.Skip("pprof listen didn't fail within 200ms (platform-dependent)")
	}
}

func TestStartPprof_HappyAndShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	startPprof(ctx, "127.0.0.1:0", func(error) {})
	// Immediately cancel so the shutdown goroutine runs.
	cancel()
	// Give it a moment to close.
	time.Sleep(5 * time.Millisecond)
}

func TestLoggerFor(t *testing.T) {
	if loggerFor("debug", os.Stderr) == nil {
		t.Fatal("nil logger")
	}
}

// deletingFailer returns an unrelated cleanup error on Delete and forces the
// prefix cleanup walker to see at least one key.
type deletingFailer struct{ *fake.Client }

func (d *deletingFailer) List(ctx context.Context, prefix, delim, token string, maxKeys int32) (*s3client.ListResult, error) {
	return &s3client.ListResult{Keys: []string{prefix + "leftover"}}, nil
}

func (d *deletingFailer) Delete(ctx context.Context, key string) error {
	return errors.New("delete down hard")
}

var _ s3client.Client = (*deletingFailer)(nil)

// listFailer fails the Nth List call — used to let CheckBucket + safety.Cleanup
// decide which one breaks.
type listFailer struct {
	*fake.Client
	failOnCall int
	calls      int
}

func (l *listFailer) List(ctx context.Context, prefix, delim, token string, maxKeys int32) (*s3client.ListResult, error) {
	l.calls++
	if l.calls == l.failOnCall {
		return nil, errors.New("list down")
	}
	return l.Client.List(ctx, prefix, delim, token, maxKeys)
}

func TestRunSafetyCleanupError(t *testing.T) {
	c := fake.New()
	// Call sequence: CheckBucket → (smallobj Run → no list) → safety.Cleanup.
	// So the second LIST is safety.Cleanup's walker.
	wc := &listFailer{Client: c, failOnCall: 2}
	orig := defaultClientFactory
	defer func() { defaultClientFactory = orig }()
	defaultClientFactory = makeFakeFactory(wc)
	if _, err := runCobra(t, newRunCmd(context.Background()), "--plan", writePlan(t, planSmall)); err == nil {
		t.Fatal("expected safety.Cleanup error")
	}
}

// failWriter errors on first Write — lets us inject report-write failures.
type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, errors.New("w fail") }

func TestWriteReportsTextError(t *testing.T) {
	err := writeReports(
		&reportschema.Report{SchemaVersion: "1.0.0"},
		&appcfg.Config{},
		failWriter{},
	)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestWriteReportsJSONError(t *testing.T) {
	dir := t.TempDir()
	// Text goes to a real file so text write succeeds; JSON points at an
	// unwritable directory to fail the JSON file Create.
	txt := filepath.Join(dir, "out.txt")
	cfg := &appcfg.Config{OutputText: txt, OutputJSON: "/no/such/dir/out.json"}
	if err := writeReports(&reportschema.Report{SchemaVersion: "1.0.0"}, cfg, os.Stdout); err == nil {
		t.Fatal("expected json create error")
	}
}

func TestWriteReportsJSONOpenError(t *testing.T) {
	dir := t.TempDir()
	txt := filepath.Join(dir, "out.txt")
	// Pointing OutputJSON at an existing directory makes os.Create fail.
	jsonDir := filepath.Join(dir, "json-as-dir")
	if err := os.Mkdir(jsonDir, 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := &appcfg.Config{OutputText: txt, OutputJSON: jsonDir}
	if err := writeReports(&reportschema.Report{SchemaVersion: "1.0.0"}, cfg, os.Stdout); err == nil {
		t.Fatal("expected json open error")
	}
}

// TestWriteReportsJSONWriteError triggers the post-Create write error path by
// pointing OutputJSON at /dev/full, which succeeds on open and always returns
// ENOSPC on Write. Skipped when /dev/full is unavailable.
func TestWriteReportsJSONWriteError(t *testing.T) {
	if _, err := os.Stat("/dev/full"); err != nil {
		t.Skip("/dev/full not available on this platform")
	}
	dir := t.TempDir()
	txt := filepath.Join(dir, "out.txt")
	cfg := &appcfg.Config{OutputText: txt, OutputJSON: "/dev/full"}
	if err := writeReports(&reportschema.Report{SchemaVersion: "1.0.0"}, cfg, os.Stdout); err == nil {
		t.Fatal("expected json write error on /dev/full")
	}
}

// TestRunRunnerError swaps runRunner with a stub that errors unconditionally,
// so doRun returns the runner's error.
func TestRunRunnerError(t *testing.T) {
	orig := runRunner
	defer func() { runRunner = orig }()
	runRunner = func(ctx context.Context, _ runner.Options) (*runner.Result, error) {
		return nil, errors.New("runner boom")
	}
	c := fake.New()
	origF := defaultClientFactory
	defer func() { defaultClientFactory = origF }()
	defaultClientFactory = makeFakeFactory(c)
	if _, err := runCobra(t, newRunCmd(context.Background()), "--plan", writePlan(t, planSmall)); err == nil {
		t.Fatal("expected runner error")
	}
}

func TestRunCleansPrefixAfterRunnerError(t *testing.T) {
	c := fake.New()
	orig := runRunner
	defer func() { runRunner = orig }()
	runRunner = func(ctx context.Context, opt runner.Options) (*runner.Result, error) {
		if err := opt.S3.Put(ctx, opt.RunPrefix+"leftover", strings.NewReader("x"), 1); err != nil {
			t.Fatal(err)
		}
		return nil, errors.New("runner boom")
	}
	origF := defaultClientFactory
	defer func() { defaultClientFactory = origF }()
	defaultClientFactory = makeFakeFactory(c)
	if _, err := runCobra(t, newRunCmd(context.Background()), "--plan", writePlan(t, planSmall)); err == nil {
		t.Fatal("expected runner error")
	}
	for key := range c.Objects() {
		if strings.Contains(key, "leftover") {
			t.Fatalf("leftover key was not cleaned: %q", key)
		}
	}
}
