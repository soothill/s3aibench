// Package config resolves a fully-qualified runtime configuration from three
// layers — CLI flags, environment variables, and the YAML plan — recording
// the origin of every field so reports can explain exactly what ran.
package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/darrensoothill/s3aibench/internal/plan"
)

// Source names the layer that supplied a given field.
type Source string

const (
	SourceFlag    Source = "flag"
	SourceEnv     Source = "env"
	SourcePlan    Source = "plan"
	SourceDefault Source = "default"
)

// Config is the fully-resolved, runtime-ready configuration.
type Config struct {
	PlanName string
	Endpoint string
	Region   string
	Bucket   string
	Prefix   string

	PathStyle          bool
	TLSSkipVerify      bool
	ConnectionPoolSize int
	HTTP2              bool

	Duration             time.Duration
	Warmup               time.Duration
	Threads              int
	MultipartPartSize    int64
	MultipartConcurrency int

	Prepopulate bool
	Cleanup     bool
	Progress    bool
	RandomSeed  int64
	LogLevel    string

	OutputText string
	OutputJSON string

	AllowSharedBucket bool

	// MaxRetries caps the AWS SDK retry count. 1 disables retries beyond the
	// first attempt so transient throttling doesn't skew latency percentiles.
	MaxRetries int

	// AccessKey / SecretKey are resolved from the plan if set, but credentials
	// are primarily resolved by the AWS SDK (env, shared config, IMDS).
	AccessKey string
	SecretKey string

	// Workloads preserves the ordered plan entries so each workload's
	// per-workload parameters can flow to its factory.
	Workloads []plan.Workload

	// Sources records which layer supplied each top-level field.
	Sources map[string]Source

	// ConfigHash is sha256(canonicalJSON(resolved)) excluding Sources and creds.
	ConfigHash string
}

// Overrides is the flag layer passed to Resolve.
type Overrides struct {
	Endpoint             *string
	Bucket               *string
	Region               *string
	Duration             *time.Duration
	Warmup               *time.Duration
	Threads              *int
	MultipartPartSize    *int64
	MultipartConcurrency *int
	OutputText           *string
	OutputJSON           *string
	Progress             *bool
	PathStyle            *bool
	TLSSkipVerify        *bool
	ConnectionPoolSize   *int
	HTTP2                *bool
	Prepopulate          *bool
	Cleanup              *bool
	RandomSeed           *int64
	Prefix               *string
	LogLevel             *string
	AllowSharedBucket    *bool
	MaxRetries           *int
}

// Environ is the subset of os.Getenv the resolver reads; injected for tests.
type Environ func(key string) (string, bool)

// Resolve merges flags, env, and plan layers into a final Config, recording
// the source of each field and computing a config hash over the result.
func Resolve(p *plan.Plan, flags Overrides, env Environ) (*Config, error) {
	if env == nil {
		env = func(string) (string, bool) { return "", false }
	}
	cfg := &Config{Sources: map[string]Source{}}

	pickString(cfg, "endpoint", &cfg.Endpoint, flags.Endpoint, env, "S3AIBENCH_ENDPOINT", p.Endpoint, "")
	pickString(cfg, "region", &cfg.Region, flags.Region, env, "S3AIBENCH_REGION", p.Region, "")
	pickString(cfg, "bucket", &cfg.Bucket, flags.Bucket, env, "S3AIBENCH_BUCKET", p.Bucket, "")
	pickString(cfg, "prefix", &cfg.Prefix, flags.Prefix, env, "S3AIBENCH_PREFIX", "", "s3aibench/")
	pickString(cfg, "output.text", &cfg.OutputText, flags.OutputText, env, "S3AIBENCH_OUTPUT_TEXT", p.Output.Text, "")
	pickString(cfg, "output.json", &cfg.OutputJSON, flags.OutputJSON, env, "S3AIBENCH_OUTPUT_JSON", p.Output.JSON, "")
	pickString(cfg, "log_level", &cfg.LogLevel, flags.LogLevel, env, "S3AIBENCH_LOG_LEVEL", "", "info")

	pickDuration(cfg, "defaults.duration", &cfg.Duration, flags.Duration, env, "S3AIBENCH_DURATION", p.Defaults.Duration.AsDuration(), 60*time.Second)
	pickDuration(cfg, "defaults.warmup", &cfg.Warmup, flags.Warmup, env, "S3AIBENCH_WARMUP", p.Defaults.Warmup.AsDuration(), 0)

	pickInt(cfg, "defaults.threads", &cfg.Threads, flags.Threads, env, "S3AIBENCH_THREADS", p.Defaults.Threads, 64)
	pickInt64(cfg, "defaults.multipart_part_size", &cfg.MultipartPartSize, flags.MultipartPartSize, env, "S3AIBENCH_MULTIPART_PART_SIZE", int64(p.Defaults.MultipartPartSize), 16*1024*1024)
	pickInt(cfg, "defaults.multipart_concurrency", &cfg.MultipartConcurrency, flags.MultipartConcurrency, env, "S3AIBENCH_MULTIPART_CONCURRENCY", p.Defaults.MultipartConcurrency, 8)
	pickInt(cfg, "connection_pool_size", &cfg.ConnectionPoolSize, flags.ConnectionPoolSize, env, "S3AIBENCH_CONNECTION_POOL_SIZE", p.ConnectionPool, 4096)
	pickInt64(cfg, "random_seed", &cfg.RandomSeed, flags.RandomSeed, env, "S3AIBENCH_RANDOM_SEED", p.RandomSeed, 0)

	pickBool(cfg, "path_style", &cfg.PathStyle, flags.PathStyle, env, "S3AIBENCH_PATH_STYLE", p.PathStyle, false)
	pickBool(cfg, "tls_skip_verify", &cfg.TLSSkipVerify, flags.TLSSkipVerify, env, "S3AIBENCH_TLS_SKIP_VERIFY", p.TLSSkipVerify, false)
	pickBool(cfg, "http2", &cfg.HTTP2, flags.HTTP2, env, "S3AIBENCH_HTTP2", p.HTTP2, false)
	pickBool(cfg, "prepopulate", &cfg.Prepopulate, flags.Prepopulate, env, "S3AIBENCH_PREPOPULATE", p.Prepopulate, true)
	pickBool(cfg, "cleanup", &cfg.Cleanup, flags.Cleanup, env, "S3AIBENCH_CLEANUP", p.Cleanup, true)
	pickBool(cfg, "output.progress", &cfg.Progress, flags.Progress, env, "S3AIBENCH_PROGRESS", boolPtr(p.Output.Progress), false)
	pickBool(cfg, "allow_shared_bucket", &cfg.AllowSharedBucket, flags.AllowSharedBucket, env, "S3AIBENCH_ALLOW_SHARED_BUCKET", nil, false)
	pickInt(cfg, "max_retries", &cfg.MaxRetries, flags.MaxRetries, env, "S3AIBENCH_MAX_RETRIES", 0, 1)

	cfg.PlanName = p.Name
	cfg.AccessKey = p.AccessKey
	cfg.SecretKey = p.SecretKey

	// Apply per-workload env overrides (THREADS, DURATION, WEIGHT).
	cfg.Workloads = append(cfg.Workloads, p.Workloads...)
	applyWorkloadEnv(cfg.Workloads, env)

	cfg.ConfigHash = computeHash(cfg)
	return cfg, nil
}

// DumpSources returns "key=value (source=...)" lines in stable order, for the
// startup info log.
func (c *Config) DumpSources() []string {
	keys := make([]string, 0, len(c.Sources))
	for k := range c.Sources {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, fmt.Sprintf("%s (source=%s)", k, c.Sources[k]))
	}
	return out
}

func boolPtr(b bool) *bool { return &b }

// computeHash computes sha256 over a canonical JSON encoding of the resolved
// config with credentials and source map omitted. The canonical struct only
// contains JSON-safe values (primitives and YAML-derived maps), so
// json.Marshal cannot fail and we don't propagate an error.
func computeHash(c *Config) string {
	type workloadHash struct {
		Name       string
		Type       string
		Weight     int
		ObjectSize int64
		Threads    int
		Duration   int64
		Params     map[string]interface{}
	}
	type canonical struct {
		Endpoint             string
		Region               string
		Bucket               string
		Prefix               string
		PathStyle            bool
		TLSSkipVerify        bool
		ConnectionPoolSize   int
		HTTP2                bool
		Duration             int64
		Warmup               int64
		Threads              int
		MultipartPartSize    int64
		MultipartConcurrency int
		Prepopulate          bool
		Cleanup              bool
		Progress             bool
		RandomSeed           int64
		LogLevel             string
		OutputText           string
		OutputJSON           string
		AllowSharedBucket    bool
		MaxRetries           int
		Workloads            []workloadHash
	}
	wls := make([]workloadHash, len(c.Workloads))
	for i, w := range c.Workloads {
		wls[i] = workloadHash{
			Name: w.Name, Type: w.Type, Weight: w.Weight,
			ObjectSize: int64(w.ObjectSize), Threads: w.Threads,
			Duration: int64(w.Duration.AsDuration()),
			Params:   w.Params,
		}
	}
	cn := canonical{
		Endpoint: c.Endpoint, Region: c.Region, Bucket: c.Bucket, Prefix: c.Prefix,
		PathStyle: c.PathStyle, TLSSkipVerify: c.TLSSkipVerify,
		ConnectionPoolSize: c.ConnectionPoolSize, HTTP2: c.HTTP2,
		Duration: int64(c.Duration), Warmup: int64(c.Warmup),
		Threads: c.Threads, MultipartPartSize: c.MultipartPartSize,
		MultipartConcurrency: c.MultipartConcurrency,
		Prepopulate:          c.Prepopulate, Cleanup: c.Cleanup, Progress: c.Progress,
		RandomSeed: c.RandomSeed, LogLevel: c.LogLevel,
		OutputText: c.OutputText, OutputJSON: c.OutputJSON,
		AllowSharedBucket: c.AllowSharedBucket,
		MaxRetries:        c.MaxRetries,
		Workloads:         wls,
	}
	b, _ := json.Marshal(&cn) // canonical struct is JSON-safe by construction
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}
