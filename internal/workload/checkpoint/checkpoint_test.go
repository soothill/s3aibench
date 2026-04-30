package checkpoint

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
	return &workload.Env{
		S3:       fc,
		Recorder: c,
		Logger:   slog.Default(),
		Threads:  threads,
		Rand:     rand.New(rand.NewSource(1)),
		Seed:     1,
	}, fc, c
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

func TestFactoryParams(t *testing.T) {
	workload.Reset()
	Register()
	w, err := workload.Build(plan.Workload{
		Name: "w", Type: TypeName, ObjectSize: 1024,
		Params: map[string]interface{}{
			"writers":         float64(2),
			"retain_versions": 5,
			"burst_interval":  "10ms",
			"resume":          true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ww := w.(*Workload)
	if ww.writers != 2 || ww.retain != 5 || ww.burstInterval != 10*time.Millisecond || !ww.resume {
		t.Fatalf("params not applied: %+v", ww)
	}
}

func TestFactoryRejectsNegativeRetain(t *testing.T) {
	workload.Reset()
	Register()
	if _, err := workload.Build(plan.Workload{
		Name: "w", Type: TypeName, ObjectSize: 1024,
		Params: map[string]interface{}{"retain_versions": -1},
	}); err == nil {
		t.Fatal("expected error")
	}
}

func TestResumeIsPaced(t *testing.T) {
	env, _, coll := makeEnv(t, 1)
	w := build(t, plan.Workload{
		Name: "w", Type: TypeName, ObjectSize: 128,
		Params: map[string]interface{}{
			"writers":        1,
			"burst_interval": "50ms",
			"resume":         true,
		},
	})
	if err := w.Prepopulate(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := w.Run(ctx, env); err != nil {
		t.Fatal(err)
	}
	if got := coll.Snapshot().Workloads["w"][metrics.OpGet].Count; got > 1 {
		t.Fatalf("resume GETs not paced: %d", got)
	}
}

func TestBurstCycle(t *testing.T) {
	env, fc, coll := makeEnv(t, 1)
	w := build(t, plan.Workload{
		Name: "w", Type: TypeName, ObjectSize: 512,
		Params: map[string]interface{}{
			"writers":         1,
			"retain_versions": 2,
			"burst_interval":  "5ms",
			"resume":          true,
		},
	})
	if err := w.Prepopulate(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	if err := w.Run(ctx, env); err != nil {
		t.Fatal(err)
	}
	stats := coll.Snapshot().Workloads["w"]
	if stats[metrics.OpCheckpointPut] == nil || stats[metrics.OpCheckpointPut].Count < 2 {
		t.Fatalf("expected multiple checkpoint puts, got %+v", stats[metrics.OpCheckpointPut])
	}
	if stats[metrics.OpGet] == nil {
		t.Fatal("expected resume GETs")
	}
	// retain_versions=2 → fake bucket should have at most 2 live checkpoint keys
	// at end-of-run. (There may be transient additional keys during retention delete.)
	cnt := 0
	for k := range fc.Objects() {
		if len(k) > 0 {
			cnt++
		}
	}
	if cnt > 3 { // small slack for race window
		t.Fatalf("retention exceeded: %d", cnt)
	}
	if err := w.Cleanup(context.Background(), env); err != nil {
		t.Fatal(err)
	}
}

func TestZeroWriters(t *testing.T) {
	env, _, _ := makeEnv(t, 1)
	w := build(t, plan.Workload{
		Name: "w", Type: TypeName, ObjectSize: 8,
		Params: map[string]interface{}{"writers": 0},
	})
	if err := w.Run(context.Background(), env); err == nil {
		t.Fatal("expected error")
	}
}

func TestWriteError(t *testing.T) {
	env, fc, coll := makeEnv(t, 1)
	fc.FailOp("multipart_complete", errors.New("boom"))
	w := build(t, plan.Workload{
		Name: "w", Type: TypeName, ObjectSize: 8,
		Params: map[string]interface{}{"writers": 1, "burst_interval": "5ms"},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()
	_ = w.Run(ctx, env)
	st := coll.Snapshot().Workloads["w"][metrics.OpCheckpointPut]
	if st == nil || st.Errors == 0 {
		t.Fatal("expected recorded errors")
	}
}

func TestPrepopulateError(t *testing.T) {
	env, fc, _ := makeEnv(t, 1)
	fc.FailOp("multipart_complete", errors.New("x"))
	w := build(t, plan.Workload{Name: "w", Type: TypeName, ObjectSize: 8})
	if err := w.Prepopulate(context.Background(), env); err == nil {
		t.Fatal("expected error")
	}
}

func TestResumeEmpty(t *testing.T) {
	env, _, _ := makeEnv(t, 1)
	w := &Workload{name: "w", objectSize: 8}
	w.readLatest(context.Background(), env, env.Recorder) // no versions → no-op, no panic
}

func TestResumeGetError(t *testing.T) {
	env, fc, coll := makeEnv(t, 1)
	w := &Workload{name: "w", objectSize: 8, versions: []string{"w/ckpt-missing"}}
	fc.FailOp("get", errors.New("x"))
	w.readLatest(context.Background(), env, env.Recorder)
	st := coll.Snapshot().Workloads["w"][metrics.OpGet]
	if st == nil || st.Errors == 0 {
		t.Fatal("expected recorded GET error")
	}
}

func TestRetentionDeleteUnrelatedError(t *testing.T) {
	// Writer succeeds but retention delete fails with an unrelated error — should
	// log a warning but not abort the run.
	env, fc, _ := makeEnv(t, 1)
	// Put with retain=1 so the second burst tries to delete the first.
	w := &Workload{name: "w", objectSize: 8, writers: 1, retain: 1, burstInterval: time.Millisecond}
	if err := w.writeOne(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	fc.FailOp("delete", errors.New("server down"))
	if err := w.writeOne(context.Background(), env); err != nil {
		t.Fatal(err)
	}
}

func TestCleanupPropagatesUnrelatedDeleteError(t *testing.T) {
	env, fc, _ := makeEnv(t, 1)
	w := build(t, plan.Workload{Name: "w", Type: TypeName, ObjectSize: 8})
	if err := w.Prepopulate(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	fc.FailOp("delete", errors.New("down"))
	if err := w.Cleanup(context.Background(), env); err == nil {
		t.Fatal("expected error")
	}
}

func TestCleanupSwallowsNotFound(t *testing.T) {
	env, fc, _ := makeEnv(t, 1)
	w := build(t, plan.Workload{Name: "w", Type: TypeName, ObjectSize: 8})
	if err := w.Prepopulate(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	fc.FailOp("delete", fake.ErrNotFound)
	if err := w.Cleanup(context.Background(), env); err != nil {
		t.Fatal(err)
	}
}

func TestNameAndType(t *testing.T) {
	w := build(t, plan.Workload{Name: "abc", Type: TypeName, ObjectSize: 8})
	if w.Name() != "abc" || w.Type() != TypeName {
		t.Fatal("name/type wrong")
	}
}

func TestHelpers(t *testing.T) {
	// Helper coverage lives in internal/workload.
}
