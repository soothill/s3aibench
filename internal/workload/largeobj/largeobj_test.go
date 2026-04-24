package largeobj

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"testing"
	"time"

	"github.com/darrensoothill/s3aibench/internal/metrics"
	"github.com/darrensoothill/s3aibench/internal/plan"
	"github.com/darrensoothill/s3aibench/internal/s3client/fake"
	"github.com/darrensoothill/s3aibench/internal/workload"
)

func makeEnv(t *testing.T, threads int) (*workload.Env, *fake.Client, *metrics.Collector) {
	t.Helper()
	c := metrics.NewCollector()
	fc := fake.New()
	env := &workload.Env{
		S3:       fc,
		Recorder: c,
		Logger:   slog.Default(),
		Threads:  threads,
		Rand:     rand.New(rand.NewSource(1)),
		Seed:     1,
	}
	return env, fc, c
}

func build(t *testing.T, p plan.Workload) workload.Workload {
	t.Helper()
	workload.Reset()
	Register()
	w, err := workload.Build(p)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func TestFactoryRequiresSize(t *testing.T) {
	workload.Reset()
	Register()
	if _, err := workload.Build(plan.Workload{Name: "x", Type: TypeName}); err == nil {
		t.Fatal("expected error")
	}
}

func TestReadRatioParam(t *testing.T) {
	workload.Reset()
	Register()
	w, err := workload.Build(plan.Workload{
		Name: "w", Type: TypeName, ObjectSize: 1024,
		Params: map[string]interface{}{"read_ratio": 0.5},
	})
	if err != nil {
		t.Fatal(err)
	}
	if w.(*Workload).readRatio != 0.5 {
		t.Fatalf("readRatio=%v", w.(*Workload).readRatio)
	}
}

func TestPrepopulateAndRun(t *testing.T) {
	env, fc, coll := makeEnv(t, 2)
	w := build(t, plan.Workload{Name: "w", Type: TypeName, ObjectSize: 256})
	if err := w.Prepopulate(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	if fc.Objects()[w.(*Workload).keyFor(int64(0))] != 256 {
		t.Fatalf("prepopulate size wrong")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	if err := w.Run(ctx, env); err != nil {
		t.Fatal(err)
	}
	snap := coll.Snapshot()
	if snap.Workloads["w"][metrics.OpMultipartComplete] == nil {
		t.Fatal("no multipart recorded")
	}
	if err := w.Cleanup(context.Background(), env); err != nil {
		t.Fatal(err)
	}
}

func TestReadPath(t *testing.T) {
	env, _, coll := makeEnv(t, 2)
	w := build(t, plan.Workload{
		Name: "w", Type: TypeName, ObjectSize: 128,
		Params: map[string]interface{}{"read_ratio": 1.0},
	})
	if err := w.Prepopulate(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_ = w.Run(ctx, env)
	if coll.Snapshot().Workloads["w"][metrics.OpRangeGet] == nil {
		t.Fatal("no range gets")
	}
}

func TestReadWithNoKeys(t *testing.T) {
	env, _, coll := makeEnv(t, 1)
	w := build(t, plan.Workload{
		Name: "w", Type: TypeName, ObjectSize: 128,
		Params: map[string]interface{}{"read_ratio": 1.0},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_ = w.Run(ctx, env)
	rg := coll.Snapshot().Workloads["w"]
	if rg != nil && rg[metrics.OpRangeGet] != nil && rg[metrics.OpRangeGet].Count > 0 {
		t.Fatal("should not have recorded range GETs with no keys")
	}
}

func TestReadPathError(t *testing.T) {
	env, fc, coll := makeEnv(t, 1)
	w := build(t, plan.Workload{
		Name: "w", Type: TypeName, ObjectSize: 128,
		Params: map[string]interface{}{"read_ratio": 1.0},
	})
	if err := w.Prepopulate(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	fc.FailOp("range_get", errors.New("boom"))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_ = w.Run(ctx, env)
	rg := coll.Snapshot().Workloads["w"][metrics.OpRangeGet]
	if rg == nil || rg.Errors == 0 {
		t.Fatal("expected error recorded")
	}
}

func TestPrepopulateError(t *testing.T) {
	env, fc, _ := makeEnv(t, 1)
	w := build(t, plan.Workload{Name: "w", Type: TypeName, ObjectSize: 128})
	fc.FailOp("multipart_complete", errors.New("x"))
	if err := w.Prepopulate(context.Background(), env); err == nil {
		t.Fatal("expected error")
	}
}

func TestRunZeroThreads(t *testing.T) {
	env, _, _ := makeEnv(t, 0)
	w := build(t, plan.Workload{Name: "w", Type: TypeName, ObjectSize: 128})
	if err := w.Run(context.Background(), env); err == nil {
		t.Fatal("expected error")
	}
}

func TestCleanupError(t *testing.T) {
	env, fc, _ := makeEnv(t, 1)
	w := build(t, plan.Workload{Name: "w", Type: TypeName, ObjectSize: 128})
	if err := w.Prepopulate(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	fc.FailOp("delete", errors.New("unrelated"))
	if err := w.Cleanup(context.Background(), env); err == nil {
		t.Fatal("expected error")
	}
}

func TestCleanupIgnoresNotFound(t *testing.T) {
	env, fc, _ := makeEnv(t, 1)
	w := build(t, plan.Workload{Name: "w", Type: TypeName, ObjectSize: 128})
	if err := w.Prepopulate(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	fc.FailOp("delete", fake.ErrNotFound)
	if err := w.Cleanup(context.Background(), env); err != nil {
		t.Fatal(err)
	}
}

func TestNameAndType(t *testing.T) {
	w := build(t, plan.Workload{Name: "abc", Type: TypeName, ObjectSize: 128})
	if w.Name() != "abc" || w.Type() != TypeName {
		t.Fatalf("got name=%q type=%q", w.Name(), w.Type())
	}
}
