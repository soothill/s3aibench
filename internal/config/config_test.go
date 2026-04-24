package config

import (
	"strings"
	"testing"
	"time"

	"github.com/darrensoothill/s3aibench/internal/plan"
)

func mkPlan() *plan.Plan {
	return &plan.Plan{
		Name:     "p",
		Endpoint: "https://plan-endpoint",
		Bucket:   "plan-bucket",
		Region:   "plan-region",
		Defaults: plan.Defaults{
			Duration:             plan.Duration(10 * time.Minute),
			Warmup:               plan.Duration(30 * time.Second),
			Threads:              256,
			MultipartPartSize:    16 * 1024 * 1024,
			MultipartConcurrency: 4,
		},
		Workloads: []plan.Workload{{Name: "lance-query", Type: "smallobject", Weight: 1}},
	}
}

func emptyEnv(string) (string, bool) { return "", false }

func mapEnv(m map[string]string) Environ {
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}

func TestResolveDefaults(t *testing.T) {
	p := mkPlan()
	p.Endpoint = ""
	p.Bucket = ""
	p.Defaults.Warmup = 0 // force pickDuration default branch
	c, err := Resolve(p, Overrides{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.Warmup != 0 || c.Sources["defaults.warmup"] != SourceDefault {
		t.Fatalf("warmup=%v source=%s", c.Warmup, c.Sources["defaults.warmup"])
	}
	if c.Prefix != "s3aibench/" || c.Sources["prefix"] != SourceDefault {
		t.Fatalf("prefix=%q source=%s", c.Prefix, c.Sources["prefix"])
	}
	if c.Endpoint != "" || c.Sources["endpoint"] != SourceDefault {
		t.Fatalf("expected default endpoint")
	}
	if c.LogLevel != "info" {
		t.Fatalf("log_level=%q", c.LogLevel)
	}
	if c.ConfigHash == "" || !strings.HasPrefix(c.ConfigHash, "sha256:") {
		t.Fatalf("bad hash %q", c.ConfigHash)
	}
}

func TestResolvePlanPrecedence(t *testing.T) {
	c, err := Resolve(mkPlan(), Overrides{}, emptyEnv)
	if err != nil {
		t.Fatal(err)
	}
	if c.Endpoint != "https://plan-endpoint" || c.Sources["endpoint"] != SourcePlan {
		t.Fatalf("endpoint=%q source=%s", c.Endpoint, c.Sources["endpoint"])
	}
	if c.Duration != 10*time.Minute || c.Sources["defaults.duration"] != SourcePlan {
		t.Fatalf("duration=%v source=%s", c.Duration, c.Sources["defaults.duration"])
	}
	if c.Threads != 256 || c.Sources["defaults.threads"] != SourcePlan {
		t.Fatalf("threads=%d", c.Threads)
	}
}

func TestResolveEnvOverrides(t *testing.T) {
	env := mapEnv(map[string]string{
		"S3AIBENCH_ENDPOINT":             "https://env-endpoint",
		"S3AIBENCH_THREADS":              "512",
		"S3AIBENCH_DURATION":             "5m",
		"S3AIBENCH_MULTIPART_PART_SIZE":  "8MiB",
		"S3AIBENCH_PATH_STYLE":           "true",
		"S3AIBENCH_PROGRESS":             "true",
		"S3AIBENCH_RANDOM_SEED":          "42",
		"S3AIBENCH_MAX_RETRIES":          "5",
		"S3AIBENCH_WORKLOAD_LANCE_QUERY_THREADS":  "16",
		"S3AIBENCH_WORKLOAD_LANCE_QUERY_DURATION": "2m",
		"S3AIBENCH_WORKLOAD_LANCE_QUERY_WEIGHT":   "7",
	})
	c, err := Resolve(mkPlan(), Overrides{}, env)
	if err != nil {
		t.Fatal(err)
	}
	if c.Endpoint != "https://env-endpoint" || c.Sources["endpoint"] != SourceEnv {
		t.Fatalf("endpoint=%q source=%s", c.Endpoint, c.Sources["endpoint"])
	}
	if c.Threads != 512 || c.Sources["defaults.threads"] != SourceEnv {
		t.Fatalf("threads=%d", c.Threads)
	}
	if c.Duration != 5*time.Minute {
		t.Fatalf("duration=%v", c.Duration)
	}
	if c.MultipartPartSize != 8*1024*1024 {
		t.Fatalf("part=%d", c.MultipartPartSize)
	}
	if !c.PathStyle || !c.Progress {
		t.Fatalf("bools not propagated")
	}
	if c.RandomSeed != 42 {
		t.Fatalf("seed=%d", c.RandomSeed)
	}
	if c.Workloads[0].Threads != 16 || c.Workloads[0].Duration.AsDuration() != 2*time.Minute || c.Workloads[0].Weight != 7 {
		t.Fatalf("workload env override failed: %+v", c.Workloads[0])
	}
	if c.MaxRetries != 5 {
		t.Fatalf("max_retries=%d", c.MaxRetries)
	}
}

func TestResolveFlagPrecedence(t *testing.T) {
	endp := "https://flag-endpoint"
	dur := 1 * time.Minute
	warm := 5 * time.Second
	threads := 128
	partSize := int64(4 * 1024 * 1024)
	mpc := 2
	pool := 1024
	seed := int64(99)
	bucket := "flag-bucket"
	region := "flag-region"
	output := "out.txt"
	outj := "out.json"
	progress := true
	pathStyle := true
	tlsSkip := true
	http2 := true
	prepop := false
	cleanup := false
	prefix := "myprefix/"
	level := "debug"
	asb := true
	retries := 9
	env := mapEnv(map[string]string{"S3AIBENCH_ENDPOINT": "should-be-ignored"})
	c, err := Resolve(mkPlan(), Overrides{
		Endpoint: &endp, Duration: &dur, Warmup: &warm, Threads: &threads,
		MultipartPartSize: &partSize, MultipartConcurrency: &mpc,
		ConnectionPoolSize: &pool, RandomSeed: &seed, Bucket: &bucket,
		Region: &region, OutputText: &output, OutputJSON: &outj,
		Progress: &progress, PathStyle: &pathStyle, TLSSkipVerify: &tlsSkip,
		HTTP2: &http2, Prepopulate: &prepop, Cleanup: &cleanup,
		Prefix: &prefix, LogLevel: &level, AllowSharedBucket: &asb,
		MaxRetries: &retries,
	}, env)
	if err != nil {
		t.Fatal(err)
	}
	if c.Endpoint != endp || c.Sources["endpoint"] != SourceFlag {
		t.Fatalf("endpoint=%q source=%s", c.Endpoint, c.Sources["endpoint"])
	}
	if c.Duration != dur || c.Threads != threads || c.MultipartPartSize != partSize ||
		c.MultipartConcurrency != mpc || c.ConnectionPoolSize != pool ||
		c.RandomSeed != seed || c.Bucket != bucket || c.Region != region ||
		c.OutputText != output || c.OutputJSON != outj || !c.Progress ||
		!c.PathStyle || !c.TLSSkipVerify || !c.HTTP2 || c.Prepopulate ||
		c.Cleanup || c.Prefix != prefix || c.LogLevel != level ||
		!c.AllowSharedBucket || c.Warmup != warm || c.MaxRetries != retries {
		t.Fatalf("flag precedence failed: %+v", c)
	}
}

func TestResolveEnvParseErrorsFallThrough(t *testing.T) {
	env := mapEnv(map[string]string{
		"S3AIBENCH_THREADS":             "not-a-number",
		"S3AIBENCH_DURATION":            "not-a-duration",
		"S3AIBENCH_PATH_STYLE":          "not-a-bool",
		"S3AIBENCH_MULTIPART_PART_SIZE": "not-a-size",
		"S3AIBENCH_RANDOM_SEED":         "bad",
		"S3AIBENCH_WORKLOAD_LANCE_QUERY_THREADS":  "bad",
		"S3AIBENCH_WORKLOAD_LANCE_QUERY_DURATION": "bad",
		"S3AIBENCH_WORKLOAD_LANCE_QUERY_WEIGHT":   "bad",
	})
	c, err := Resolve(mkPlan(), Overrides{}, env)
	if err != nil {
		t.Fatal(err)
	}
	// Bad env values fall through to plan values.
	if c.Threads != 256 || c.Sources["defaults.threads"] != SourcePlan {
		t.Fatalf("threads=%d source=%s", c.Threads, c.Sources["defaults.threads"])
	}
	if c.Duration != 10*time.Minute {
		t.Fatalf("duration=%v", c.Duration)
	}
	if c.PathStyle {
		t.Fatal("expected default path_style=false")
	}
	// Per-workload bad values leave workload fields untouched from plan.
	if c.Workloads[0].Threads != 0 || c.Workloads[0].Weight != 1 {
		t.Fatalf("workload unexpectedly mutated: %+v", c.Workloads[0])
	}
}

func TestDumpSources(t *testing.T) {
	c, err := Resolve(mkPlan(), Overrides{}, emptyEnv)
	if err != nil {
		t.Fatal(err)
	}
	lines := c.DumpSources()
	if len(lines) == 0 {
		t.Fatal("expected sources")
	}
	prev := ""
	for _, l := range lines {
		if prev > l {
			t.Fatalf("not sorted: %s > %s", prev, l)
		}
		prev = l
	}
}

func TestConfigHashDeterministic(t *testing.T) {
	a, err := Resolve(mkPlan(), Overrides{}, emptyEnv)
	if err != nil {
		t.Fatal(err)
	}
	// Same plan via env with identical values must yield identical hash.
	p := mkPlan()
	p.Endpoint = ""
	env := mapEnv(map[string]string{"S3AIBENCH_ENDPOINT": "https://plan-endpoint"})
	b, err := Resolve(p, Overrides{}, env)
	if err != nil {
		t.Fatal(err)
	}
	if a.ConfigHash != b.ConfigHash {
		t.Fatalf("hashes differ: %s vs %s", a.ConfigHash, b.ConfigHash)
	}
}

func TestNormalizeWorkloadName(t *testing.T) {
	if got := NormalizeWorkloadName("lance-query"); got != "LANCE_QUERY" {
		t.Fatalf("got %q", got)
	}
}

func TestBoolPtrHelper(t *testing.T) {
	if !*boolPtr(true) || *boolPtr(false) {
		t.Fatal("boolPtr broken")
	}
}
