// Package cli wires cobra commands into the internal pieces.
package cli

import (
	"time"

	"github.com/darrensoothill/s3aibench/internal/config"
	"github.com/spf13/pflag"
)

// flagsIntoOverrides adds common flags to a flag set and returns a function
// that, post-parse, returns a populated Overrides containing only the fields
// that the user actually set.
func flagsIntoOverrides(fs *pflag.FlagSet) func() config.Overrides {
	endpoint := fs.String("endpoint", "", "S3 endpoint")
	bucket := fs.String("bucket", "", "S3 bucket")
	region := fs.String("region", "", "S3 region")
	threads := fs.Int("threads", 0, "worker threads per workload")
	duration := fs.Duration("duration", 0, "measurement duration (overrides plan)")
	warmup := fs.Duration("warmup", 0, "warmup window before measurement")
	partSize := fs.Int64("multipart-part-size", 0, "multipart part size in bytes")
	partConcurrency := fs.Int("multipart-concurrency", 0, "multipart upload concurrency")
	outText := fs.String("output-text", "", "text report path (empty=stdout)")
	outJSON := fs.String("output-json", "", "JSON report path (empty=skip)")
	progress := fs.Bool("progress", false, "print live progress")
	timeline := fs.Bool("timeline", false, "include per-second operation timelines in reports")
	progressInterval := fs.Duration("progress-interval", 0, "live progress interval")
	pathStyle := fs.Bool("path-style", false, "use path-style addressing")
	tlsSkip := fs.Bool("tls-skip-verify", false, "skip TLS verification")
	http2 := fs.Bool("http2", false, "force HTTP/2")
	pool := fs.Int("connection-pool-size", 0, "max idle connections per host")
	prepop := fs.Bool("prepopulate", true, "prepopulate dataset before run")
	cleanup := fs.Bool("cleanup", true, "delete created objects on exit")
	seed := fs.Int64("random-seed", 0, "deterministic RNG seed")
	prefix := fs.String("prefix", "", "key prefix for all writes")
	logLevel := fs.String("log-level", "", "log level (debug|info|warn|error)")
	shared := fs.Bool("allow-shared-bucket", false, "allow running against a bucket with unrelated keys")
	retries := fs.Int("max-retries", 0, "max SDK retry attempts per request (>=1; 1 disables retries)")

	return func() config.Overrides {
		o := config.Overrides{}
		setIfChanged(fs, "endpoint", endpoint, &o.Endpoint)
		setIfChanged(fs, "bucket", bucket, &o.Bucket)
		setIfChanged(fs, "region", region, &o.Region)
		setIfChangedInt(fs, "threads", threads, &o.Threads)
		setIfChangedDuration(fs, "duration", duration, &o.Duration)
		setIfChangedDuration(fs, "warmup", warmup, &o.Warmup)
		setIfChangedInt64(fs, "multipart-part-size", partSize, &o.MultipartPartSize)
		setIfChangedInt(fs, "multipart-concurrency", partConcurrency, &o.MultipartConcurrency)
		setIfChanged(fs, "output-text", outText, &o.OutputText)
		setIfChanged(fs, "output-json", outJSON, &o.OutputJSON)
		setIfChangedBool(fs, "progress", progress, &o.Progress)
		setIfChangedBool(fs, "timeline", timeline, &o.Timeline)
		setIfChangedDuration(fs, "progress-interval", progressInterval, &o.ProgressInterval)
		setIfChangedBool(fs, "path-style", pathStyle, &o.PathStyle)
		setIfChangedBool(fs, "tls-skip-verify", tlsSkip, &o.TLSSkipVerify)
		setIfChangedBool(fs, "http2", http2, &o.HTTP2)
		setIfChangedInt(fs, "connection-pool-size", pool, &o.ConnectionPoolSize)
		setIfChangedBool(fs, "prepopulate", prepop, &o.Prepopulate)
		setIfChangedBool(fs, "cleanup", cleanup, &o.Cleanup)
		setIfChangedInt64(fs, "random-seed", seed, &o.RandomSeed)
		setIfChanged(fs, "prefix", prefix, &o.Prefix)
		setIfChanged(fs, "log-level", logLevel, &o.LogLevel)
		setIfChangedBool(fs, "allow-shared-bucket", shared, &o.AllowSharedBucket)
		setIfChangedInt(fs, "max-retries", retries, &o.MaxRetries)
		return o
	}
}

func setIfChanged(fs *pflag.FlagSet, name string, src *string, dst **string) {
	if fs.Changed(name) {
		v := *src
		*dst = &v
	}
}

func setIfChangedInt(fs *pflag.FlagSet, name string, src *int, dst **int) {
	if fs.Changed(name) {
		v := *src
		*dst = &v
	}
}

func setIfChangedInt64(fs *pflag.FlagSet, name string, src *int64, dst **int64) {
	if fs.Changed(name) {
		v := *src
		*dst = &v
	}
}

func setIfChangedBool(fs *pflag.FlagSet, name string, src *bool, dst **bool) {
	if fs.Changed(name) {
		v := *src
		*dst = &v
	}
}

func setIfChangedDuration(fs *pflag.FlagSet, name string, src *time.Duration, dst **time.Duration) {
	if fs.Changed(name) {
		v := *src
		*dst = &v
	}
}
