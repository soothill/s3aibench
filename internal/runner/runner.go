// Package runner orchestrates warmup → measurement execution across a set of
// workloads. M0 launches them sequentially; M2 will add concurrent weighted
// composites.
package runner

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/darrensoothill/s3aibench/internal/metrics"
	"github.com/darrensoothill/s3aibench/internal/s3client"
	"github.com/darrensoothill/s3aibench/internal/workload"
)

// Options describes a single Run invocation.
type Options struct {
	S3          s3client.Client
	Recorder    metrics.Recorder
	Logger      *slog.Logger
	Workloads   []workload.Workload
	Duration    time.Duration
	Warmup      time.Duration
	Threads     int
	PartSize    int64
	Concurrency int
	RunPrefix   string
	Seed        int64
	Prepopulate bool
}

// Result reports which workloads failed.
type Result struct {
	Errors []error
}

// Run performs warmup (if any) then drives each workload for `Duration`.
// Workloads are executed sequentially in M0. Each workload uses a fresh rand
// seeded deterministically from Opts.Seed so runs are reproducible.
func Run(ctx context.Context, opt Options) (*Result, error) {
	if opt.Recorder == nil {
		return nil, errors.New("runner: recorder required")
	}
	if opt.Logger == nil {
		opt.Logger = slog.Default()
	}
	if opt.Duration <= 0 {
		return nil, errors.New("runner: duration must be >0")
	}
	if len(opt.Workloads) == 0 {
		return nil, errors.New("runner: no workloads")
	}

	res := &Result{}
	var nextShardID atomic.Int64
	mkEnv := func(i int) *workload.Env {
		return &workload.Env{
			S3:           opt.S3,
			Recorder:     opt.Recorder,
			Logger:       opt.Logger,
			Rand:         rand.New(rand.NewSource(workload.WorkerSeed(opt.Seed, i))),
			Seed:         opt.Seed,
			Threads:      opt.Threads,
			RunPrefix:    opt.RunPrefix,
			PartSize:     opt.PartSize,
			Concurrency:  opt.Concurrency,
			ShardCounter: &nextShardID,
		}
	}

	if opt.Prepopulate {
		for i, w := range opt.Workloads {
			if err := w.Prepopulate(ctx, mkEnv(i)); err != nil {
				res.Errors = append(res.Errors, err)
				opt.Logger.Error("prepopulate failed", "workload", w.Name(), "err", err)
			}
		}
	}

	if opt.Warmup > 0 {
		warmCtx, cancel := context.WithTimeout(ctx, opt.Warmup)
		runConcurrent(warmCtx, opt.Workloads, mkEnv, metricsDiscard{}, opt.Logger)
		cancel()
	}

	runCtx, cancel := context.WithTimeout(ctx, opt.Duration)
	defer cancel()
	runConcurrent(runCtx, opt.Workloads, mkEnv, opt.Recorder, opt.Logger)
	return res, nil
}

func runConcurrent(ctx context.Context, wls []workload.Workload,
	mkEnv func(int) *workload.Env, rec metrics.Recorder, logger *slog.Logger) {
	var wg sync.WaitGroup
	wg.Add(len(wls))
	for i, w := range wls {
		go func(i int, w workload.Workload) {
			defer wg.Done()
			env := mkEnv(i)
			env.Recorder = rec
			if err := w.Run(ctx, env); err != nil && ctx.Err() == nil {
				logger.Error("workload run failed", "workload", w.Name(), "err", err)
			}
		}(i, w)
	}
	wg.Wait()
}

// metricsDiscard is a Recorder that drops every call — used during warmup.
type metricsDiscard struct{}

func (d metricsDiscard) Record(string, metrics.Op, time.Duration, int64, error) { _ = d }
func (d metricsDiscard) Snapshot() metrics.Snapshot                             { return metrics.Snapshot{} }
func (d metricsDiscard) Totals() metrics.Totals                                 { return metrics.Totals{} }
func (d metricsDiscard) ShardFor(int) metrics.Recorder                          { return d }
