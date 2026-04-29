package runner

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/darrensoothill/s3aibench/internal/metrics"
	"github.com/darrensoothill/s3aibench/internal/s3client/fake"
	"github.com/darrensoothill/s3aibench/internal/workload"
)

type spyWL struct {
	name                      string
	runCalls                  atomic.Int32
	capturedThreads           atomic.Int32
	prepopCalls, cleanupCalls atomic.Int32
	prepopErr, runErr         error
	blockUntilCancel          bool
}

func (s *spyWL) Name() string { return s.name }
func (s *spyWL) Type() string { return "spy" }
func (s *spyWL) Prepopulate(_ context.Context, _ *workload.Env) error {
	s.prepopCalls.Add(1)
	return s.prepopErr
}
func (s *spyWL) Run(ctx context.Context, env *workload.Env) error {
	s.runCalls.Add(1)
	s.capturedThreads.Store(int32(env.Threads))
	if s.blockUntilCancel {
		<-ctx.Done()
	}
	return s.runErr
}
func (s *spyWL) Cleanup(_ context.Context, _ *workload.Env) error {
	s.cleanupCalls.Add(1)
	return nil
}

func TestRunValidates(t *testing.T) {
	if _, err := Run(context.Background(), Options{}); err == nil {
		t.Fatal("expected err for missing recorder")
	}
	if _, err := Run(context.Background(), Options{Recorder: metrics.NewCollector()}); err == nil {
		t.Fatal("expected err for zero duration")
	}
	if _, err := Run(context.Background(), Options{
		Recorder: metrics.NewCollector(), Duration: time.Second,
	}); err == nil {
		t.Fatal("expected err for no workloads")
	}
}

func TestRunHappyWithWarmupAndPrepopulate(t *testing.T) {
	w := &spyWL{name: "w", blockUntilCancel: true}
	rec := metrics.NewCollector()
	res, err := Run(context.Background(), Options{
		S3:          fake.New(),
		Recorder:    rec,
		Workloads:   []workload.Workload{w},
		Duration:    15 * time.Millisecond,
		Warmup:      5 * time.Millisecond,
		Threads:     1,
		Prepopulate: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Errors) != 0 {
		t.Fatalf("unexpected errors: %v", res.Errors)
	}
	// Warmup + measurement = 2 Run calls.
	if w.runCalls.Load() != 2 {
		t.Fatalf("runs=%d", w.runCalls.Load())
	}
	if w.prepopCalls.Load() != 1 {
		t.Fatalf("prepops=%d", w.prepopCalls.Load())
	}
}

func TestRunRecordsPrepopulateError(t *testing.T) {
	w := &spyWL{name: "w", prepopErr: errors.New("boom")}
	rec := metrics.NewCollector()
	res, err := Run(context.Background(), Options{
		Recorder:    rec,
		Workloads:   []workload.Workload{w},
		Duration:    5 * time.Millisecond,
		Prepopulate: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Errors) != 1 {
		t.Fatalf("want 1 error, got %v", res.Errors)
	}
}

func TestRunLogsRunError(t *testing.T) {
	w := &spyWL{name: "w", runErr: errors.New("fail")}
	rec := metrics.NewCollector()
	_, err := Run(context.Background(), Options{
		Recorder: rec, Workloads: []workload.Workload{w},
		Duration: 5 * time.Millisecond, Logger: slog.Default(),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRunUsesResolvedSpecs(t *testing.T) {
	w := &spyWL{name: "w", blockUntilCancel: true}
	rec := metrics.NewCollector()
	_, err := Run(context.Background(), Options{
		Recorder: rec,
		Specs: []WorkloadSpec{{
			Workload: w,
			Threads:  3,
			Duration: 5 * time.Millisecond,
		}},
		Duration: 30 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if w.capturedThreads.Load() != 3 {
		t.Fatalf("threads=%d", w.capturedThreads.Load())
	}
}

func TestRunRejectsBadSpecs(t *testing.T) {
	w := &spyWL{name: "w"}
	for _, specs := range [][]WorkloadSpec{
		{{Threads: 1, Duration: time.Millisecond}},
		{{Workload: w, Duration: time.Millisecond}},
		{{Workload: w, Threads: 1}},
	} {
		if _, err := Run(context.Background(), Options{Recorder: metrics.NewCollector(), Specs: specs, Duration: time.Millisecond}); err == nil {
			t.Fatalf("expected error for specs=%+v", specs)
		}
	}
}

func TestMetricsDiscard(t *testing.T) {
	var d metricsDiscard
	d.Record("w", metrics.OpPut, time.Millisecond, 0, nil)
	if s := d.Snapshot(); s.Workloads != nil {
		t.Fatal("expected empty snapshot")
	}
	if totals := d.Totals(); totals != (metrics.Totals{}) {
		t.Fatalf("expected zero totals, got %+v", totals)
	}
	// ShardFor must return a working Recorder (itself, in the discard case).
	shard := d.ShardFor(0)
	if shard == nil {
		t.Fatal("ShardFor returned nil")
	}
	shard.Record("w", metrics.OpPut, time.Millisecond, 0, nil) // no-op, but exercises interface
}
