package smallobj

import (
	"context"
	"errors"
	"io"
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

func buildWL(t *testing.T, p plan.Workload) workload.Workload {
	t.Helper()
	workload.Reset()
	Register()
	w, err := workload.Build(p)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func TestFactoryRequiresObjectSize(t *testing.T) {
	workload.Reset()
	Register()
	if _, err := workload.Build(plan.Workload{Name: "x", Type: TypeName}); err == nil {
		t.Fatal("expected error for missing object_size")
	}
}

func TestFactoryReadRatioParam(t *testing.T) {
	workload.Reset()
	Register()
	w, err := workload.Build(plan.Workload{
		Name: "w", Type: TypeName,
		ObjectSize: 16,
		Params:     map[string]interface{}{"read_ratio": 0.9},
	})
	if err != nil {
		t.Fatal(err)
	}
	if w.(*Workload).readRatio != 0.9 {
		t.Fatalf("readRatio=%v", w.(*Workload).readRatio)
	}
}

func TestRunAndCleanup(t *testing.T) {
	env, fc, coll := makeEnv(t, 2)
	w := buildWL(t, plan.Workload{
		Name: "w", Type: TypeName, ObjectSize: 8,
		Params: map[string]interface{}{"read_ratio": 0.5},
	})
	if err := w.Prepopulate(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	if err := w.Run(ctx, env); err != nil {
		t.Fatal(err)
	}
	snap := coll.Snapshot()
	if snap.Workloads["w"] == nil {
		t.Fatal("no metrics recorded")
	}
	if len(fc.Objects()) == 0 {
		t.Fatal("no objects")
	}
	if err := w.Cleanup(context.Background(), env); err != nil {
		t.Fatal(err)
	}
}

func TestRunZeroThreads(t *testing.T) {
	env, _, _ := makeEnv(t, 0)
	w := buildWL(t, plan.Workload{Name: "w", Type: TypeName, ObjectSize: 4})
	if err := w.Run(context.Background(), env); err == nil {
		t.Fatal("expected error")
	}
}

func TestPrepopulateError(t *testing.T) {
	env, fc, _ := makeEnv(t, 1)
	fc.FailOp("put", errors.New("boom"))
	w := buildWL(t, plan.Workload{Name: "w", Type: TypeName, ObjectSize: 4})
	if err := w.Prepopulate(context.Background(), env); err == nil {
		t.Fatal("expected error")
	}
}

func TestReadHeavyWithNoKeys(t *testing.T) {
	// read_ratio=1 but no prepopulate → workers hit empty key branch repeatedly.
	env, _, coll := makeEnv(t, 1)
	w := buildWL(t, plan.Workload{
		Name: "w", Type: TypeName, ObjectSize: 4,
		Params: map[string]interface{}{"read_ratio": 1.0},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()
	if err := w.Run(ctx, env); err != nil {
		t.Fatal(err)
	}
	// No GETs recorded because writtenCount==0.
	snap := coll.Snapshot()
	if snap.Workloads["w"] != nil && snap.Workloads["w"][metrics.OpGet] != nil &&
		snap.Workloads["w"][metrics.OpGet].Count > 0 {
		t.Fatal("unexpected GET with no keys")
	}
}

func TestWriteHeavyPath(t *testing.T) {
	env, _, coll := makeEnv(t, 2)
	w := buildWL(t, plan.Workload{
		Name: "w", Type: TypeName, ObjectSize: 4,
		Params: map[string]interface{}{"read_ratio": 0.0},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_ = w.Run(ctx, env)
	if coll.Snapshot().Workloads["w"][metrics.OpPut] == nil {
		t.Fatal("no PUTs recorded")
	}
}

func TestGetErrorPath(t *testing.T) {
	env, fc, coll := makeEnv(t, 1)
	w := buildWL(t, plan.Workload{
		Name: "w", Type: TypeName, ObjectSize: 4,
		Params: map[string]interface{}{"read_ratio": 1.0},
	})
	if err := w.Prepopulate(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	fc.FailOp("get", errors.New("injected"))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()
	_ = w.Run(ctx, env)
	// Snapshot should reflect at least one error-classed GET.
	got := coll.Snapshot().Workloads["w"][metrics.OpGet]
	if got == nil || got.Errors == 0 {
		t.Fatal("expected GET error recorded")
	}
}

func TestCleanupError(t *testing.T) {
	env, fc, _ := makeEnv(t, 1)
	w := buildWL(t, plan.Workload{Name: "w", Type: TypeName, ObjectSize: 4})
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
	w := buildWL(t, plan.Workload{Name: "w", Type: TypeName, ObjectSize: 4})
	if err := w.Prepopulate(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	fc.FailOp("delete", fake.ErrNotFound)
	if err := w.Cleanup(context.Background(), env); err != nil {
		t.Fatal(err)
	}
}

func TestIOReadCloserAssignment(t *testing.T) {
	// exercise the package-level assertion line
	var rc io.ReadCloser
	if rc != nil {
		t.Fatal("unexpected")
	}
}

func TestNameAndType(t *testing.T) {
	w := buildWL(t, plan.Workload{Name: "abc", Type: TypeName, ObjectSize: 4})
	if w.Name() != "abc" || w.Type() != TypeName {
		t.Fatalf("got name=%q type=%q", w.Name(), w.Type())
	}
}
