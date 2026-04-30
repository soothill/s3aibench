// Package runner orchestrates warmup -> measurement execution across a set of
// workloads.
package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/darrensoothill/s3aibench/internal/metrics"
	"github.com/darrensoothill/s3aibench/internal/s3client"
	"github.com/darrensoothill/s3aibench/internal/workload"
	"golang.org/x/sync/errgroup"
)

// Options describes a single Run invocation.
type Options struct {
	S3          s3client.Client
	Recorder    metrics.Recorder
	Logger      *slog.Logger
	Workloads   []workload.Workload
	Specs       []WorkloadSpec
	Duration    time.Duration
	Warmup      time.Duration
	Threads     int
	PartSize    int64
	Concurrency int
	RunPrefix   string
	Seed        int64
	Prepopulate bool
}

// WorkloadSpec is the fully-resolved execution plan for one top-level workload.
type WorkloadSpec struct {
	Workload workload.Workload
	Threads  int
	Duration time.Duration
}

// Result reports which workloads failed.
type Result struct {
	Errors []error
}

// Run performs warmup (if any) then drives each workload for its configured
// duration. Each workload gets a fresh rand seeded deterministically from
// Opts.Seed so runs are reproducible.
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
	specs := opt.Specs
	if len(specs) == 0 {
		for _, w := range opt.Workloads {
			threads := opt.Threads
			if threads <= 0 {
				threads = 1
			}
			specs = append(specs, WorkloadSpec{Workload: w, Threads: threads, Duration: opt.Duration})
		}
	}
	if len(specs) == 0 {
		return nil, errors.New("runner: no workloads")
	}
	for _, spec := range specs {
		if spec.Workload == nil {
			return nil, errors.New("runner: nil workload")
		}
		if spec.Threads <= 0 {
			return nil, errors.New("runner: workload threads must be >0")
		}
		if spec.Duration <= 0 {
			return nil, errors.New("runner: workload duration must be >0")
		}
	}

	res := &Result{}
	var nextShardID atomic.Int64
	mkEnv := func(i int, threads int) *workload.Env {
		return &workload.Env{
			S3:           opt.S3,
			Recorder:     opt.Recorder,
			Logger:       opt.Logger,
			Rand:         rand.New(rand.NewSource(workload.WorkerSeed(opt.Seed, i))),
			Seed:         opt.Seed,
			Threads:      threads,
			RunPrefix:    opt.RunPrefix,
			PartSize:     opt.PartSize,
			Concurrency:  opt.Concurrency,
			ShardCounter: &nextShardID,
		}
	}

	if opt.Prepopulate {
		for i, spec := range specs {
			if err := spec.Workload.Prepopulate(ctx, mkEnv(i, spec.Threads)); err != nil {
				res.Errors = append(res.Errors, err)
				opt.Logger.Error("prepopulate failed", "workload", spec.Workload.Name(), "err", err)
			}
		}
		if len(res.Errors) > 0 {
			return res, errors.Join(res.Errors...)
		}
	}

	if opt.Warmup > 0 {
		warmCtx, cancel := context.WithTimeout(ctx, opt.Warmup)
		_ = runConcurrent(warmCtx, specs, mkEnv, metricsDiscard{}, opt.Logger, false)
		cancel()
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := runConcurrent(runCtx, specs, mkEnv, opt.Recorder, opt.Logger, true); err != nil {
		res.Errors = append(res.Errors, err)
		return res, err
	}
	return res, nil
}

func runConcurrent(ctx context.Context, specs []WorkloadSpec,
	mkEnv func(int, int) *workload.Env, rec metrics.Recorder, logger *slog.Logger, useSpecDuration bool) error {
	group, groupCtx := errgroup.WithContext(ctx)
	for i, spec := range specs {
		i, spec := i, spec
		group.Go(func() error {
			runCtx := groupCtx
			var cancel context.CancelFunc
			if useSpecDuration {
				runCtx, cancel = context.WithTimeout(groupCtx, spec.Duration)
				defer cancel()
			}
			env := mkEnv(i, spec.Threads)
			env.Recorder = rec
			if err := spec.Workload.Run(runCtx, env); err != nil && runCtx.Err() == nil {
				logger.Error("workload run failed", "workload", spec.Workload.Name(), "err", err)
				return fmt.Errorf("workload %q: %w", spec.Workload.Name(), err)
			}
			return nil
		})
	}
	return group.Wait()
}

// metricsDiscard is a Recorder that drops every call — used during warmup.
type metricsDiscard struct{}

func (d metricsDiscard) Record(string, metrics.Op, time.Duration, int64, error) { _ = d }
func (d metricsDiscard) Snapshot() metrics.Snapshot                             { return metrics.Snapshot{} }
func (d metricsDiscard) Totals() metrics.Totals                                 { return metrics.Totals{} }
func (d metricsDiscard) ShardFor(int) metrics.Recorder                          { return d }
