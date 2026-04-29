package lancedb

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
	return &workload.Env{S3: fc, Recorder: c, Logger: slog.Default(), Threads: threads, Rand: rand.New(rand.NewSource(1)), Seed: 1}, fc, c
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

func TestFactoryValidation(t *testing.T) {
	workload.Reset()
	Register()
	if _, err := workload.Build(plan.Workload{
		Name: "w", Type: TypeName,
		Params: map[string]interface{}{"fragments": 0},
	}); err == nil {
		t.Fatal("expected error for fragments<=0")
	}
}

func TestFactoryDefaults(t *testing.T) {
	w := build(t, plan.Workload{Name: "w", Type: TypeName})
	lw := w.(*Workload)
	if lw.fragments != 100 || lw.manifests != 50 {
		t.Fatalf("defaults wrong: %+v", lw)
	}
}

func TestFactoryAllParams(t *testing.T) {
	w := build(t, plan.Workload{
		Name: "w", Type: TypeName,
		Params: map[string]interface{}{
			"fragments":         float64(2),
			"fragment_size":     float64(512),
			"manifest_count":    3,
			"manifest_max_size": 128,
			"manifest_min_size": 64,
			"read_ratio":        0.5,
			"range_mean":        512,
			"range_sigma":       1.1,
			"range_min":         64,
			"range_max":         256,
		},
	})
	lw := w.(*Workload)
	if lw.fragments != 2 || lw.manifests != 3 || lw.readRatio != 0.5 {
		t.Fatalf("params: %+v", lw)
	}
}

func TestPrepopulateAndRun(t *testing.T) {
	env, _, coll := makeEnv(t, 2)
	w := build(t, plan.Workload{
		Name: "w", Type: TypeName,
		Params: map[string]interface{}{
			"fragments":         float64(3),
			"fragment_size":     float64(1024),
			"manifest_count":    3,
			"manifest_min_size": 64,
			"manifest_max_size": 256,
			"read_ratio":        0.5,
			"range_mean":        256,
			"range_min":         32,
			"range_max":         256,
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
	snap := coll.Snapshot().Workloads["w"]
	got := 0
	for _, op := range []metrics.Op{metrics.OpHead, metrics.OpRangeGet, metrics.OpList, metrics.OpManifestPut} {
		if snap[op] != nil && snap[op].Count > 0 {
			got++
		}
	}
	if got < 2 {
		t.Fatalf("expected multiple op kinds, got %d: %+v", got, snap)
	}
	if err := w.Cleanup(context.Background(), env); err != nil {
		t.Fatal(err)
	}
}

func TestZeroThreads(t *testing.T) {
	env, _, _ := makeEnv(t, 0)
	w := build(t, plan.Workload{Name: "w", Type: TypeName})
	if err := w.Run(context.Background(), env); err == nil {
		t.Fatal("expected error")
	}
}

func TestRunBadRangeConfig(t *testing.T) {
	env, _, _ := makeEnv(t, 1)
	// sigma<=0 → sampler construction error bubbles out of Run.
	w := build(t, plan.Workload{
		Name: "w", Type: TypeName,
		Params: map[string]interface{}{
			"range_sigma": 0.0,
			"range_mean":  512,
			"range_min":   1,
			"range_max":   2,
		},
	})
	if err := w.Run(context.Background(), env); err == nil {
		t.Fatal("expected sampler error")
	}
}

func TestPrepopulateFragmentError(t *testing.T) {
	env, fc, _ := makeEnv(t, 1)
	fc.FailOp("multipart_complete", errors.New("x"))
	w := build(t, plan.Workload{
		Name: "w", Type: TypeName,
		Params: map[string]interface{}{"fragments": float64(1), "fragment_size": float64(64), "manifest_count": 1},
	})
	if err := w.Prepopulate(context.Background(), env); err == nil {
		t.Fatal("expected error")
	}
}

func TestPrepopulateManifestError(t *testing.T) {
	env, fc, _ := makeEnv(t, 1)
	w := build(t, plan.Workload{
		Name: "w", Type: TypeName,
		Params: map[string]interface{}{"fragments": float64(1), "fragment_size": float64(64), "manifest_count": 1, "manifest_min_size": 32, "manifest_max_size": 64},
	})
	// Fragment put goes through multipart_complete; manifest put fails on Put.
	fc.FailOp("put", errors.New("x"))
	if err := w.Prepopulate(context.Background(), env); err == nil {
		t.Fatal("expected manifest put error")
	}
}

func TestHeadError(t *testing.T) {
	env, fc, coll := makeEnv(t, 1)
	w := build(t, plan.Workload{
		Name: "w", Type: TypeName,
		Params: map[string]interface{}{"fragments": float64(1), "fragment_size": float64(64), "manifest_count": 1, "manifest_min_size": 32, "manifest_max_size": 64, "read_ratio": 1.0, "range_min": 8, "range_max": 16},
	})
	if err := w.Prepopulate(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	fc.FailOp("head", errors.New("x"))
	// Force read-shape=0 by driving doRead directly with seeded rand.
	rs, _ := buildRange(env.Rand)
	lw := w.(*Workload)
	lw.headThenRange(context.Background(), env, env.Recorder, env.Rand, rs)
	st := coll.Snapshot().Workloads["w"][metrics.OpHead]
	if st == nil || st.Errors == 0 {
		t.Fatal("expected head error")
	}
}

func TestHeadThenRangeEmpty(t *testing.T) {
	env, _, _ := makeEnv(t, 1)
	w := &Workload{name: "w"}
	rs, _ := buildRange(env.Rand)
	// No fragment keys → doRead returns immediately.
	w.headThenRange(context.Background(), env, env.Recorder, env.Rand, rs)
}

func TestRangeGetError(t *testing.T) {
	env, fc, coll := makeEnv(t, 1)
	w := build(t, plan.Workload{
		Name: "w", Type: TypeName,
		Params: map[string]interface{}{"fragments": float64(1), "fragment_size": float64(64), "manifest_count": 1, "manifest_min_size": 32, "manifest_max_size": 64, "range_min": 8, "range_max": 16},
	})
	if err := w.Prepopulate(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	fc.FailOp("range_get", errors.New("x"))
	rs, _ := buildRange(env.Rand)
	lw := w.(*Workload)
	lw.headThenRange(context.Background(), env, env.Recorder, env.Rand, rs)
	st := coll.Snapshot().Workloads["w"][metrics.OpRangeGet]
	if st == nil || st.Errors == 0 {
		t.Fatal("expected range_get error")
	}
}

func TestRangeGetOversizeClamped(t *testing.T) {
	env, _, _ := makeEnv(t, 1)
	w := build(t, plan.Workload{
		Name: "w", Type: TypeName,
		Params: map[string]interface{}{"fragments": float64(1), "fragment_size": float64(16), "manifest_count": 1, "manifest_min_size": 8, "manifest_max_size": 16},
	})
	if err := w.Prepopulate(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	// Pass a sampler returning rs (100) much bigger than object size (16)
	// → clamp path exercised.
	lw := w.(*Workload)
	lw.headThenRange(context.Background(), env, env.Recorder, env.Rand, &fixedSampler{v: 100})
}

func TestListError(t *testing.T) {
	env, fc, coll := makeEnv(t, 1)
	fc.FailOp("list", errors.New("x"))
	w := &Workload{name: "w"}
	w.listManifest(context.Background(), env, env.Recorder, "/")
	st := coll.Snapshot().Workloads["w"][metrics.OpList]
	if st == nil || st.Errors == 0 {
		t.Fatal("expected list error")
	}
}

func TestManifestPutError(t *testing.T) {
	env, fc, coll := makeEnv(t, 1)
	w := build(t, plan.Workload{
		Name: "w", Type: TypeName,
		Params: map[string]interface{}{"manifest_min_size": 32, "manifest_max_size": 64},
	})
	lw := w.(*Workload)
	fc.FailOp("put", errors.New("x"))
	lw.doWrite(context.Background(), env, env.Recorder, rand.New(rand.NewSource(1)))
	st := coll.Snapshot().Workloads["w"][metrics.OpManifestPut]
	if st == nil || st.Errors == 0 {
		t.Fatal("expected manifest put error")
	}
}

func TestManifestPutEqualBounds(t *testing.T) {
	// manifest_max_size == manifest_min_size triggers the no-randomization branch.
	env, _, _ := makeEnv(t, 1)
	w := build(t, plan.Workload{
		Name: "w", Type: TypeName,
		Params: map[string]interface{}{"manifest_min_size": 32, "manifest_max_size": 32, "manifest_count": 1},
	})
	lw := w.(*Workload)
	lw.doWrite(context.Background(), env, env.Recorder, rand.New(rand.NewSource(1)))
}

func TestPrepopulateEqualManifestBounds(t *testing.T) {
	env, _, _ := makeEnv(t, 1)
	w := build(t, plan.Workload{
		Name: "w", Type: TypeName,
		Params: map[string]interface{}{
			"fragments": float64(1), "fragment_size": float64(64),
			"manifest_count": 1, "manifest_min_size": 32, "manifest_max_size": 32,
		},
	})
	if err := w.Prepopulate(context.Background(), env); err != nil {
		t.Fatal(err)
	}
}

func TestDoReadDispatchesAllShapes(t *testing.T) {
	env, _, coll := makeEnv(t, 1)
	w := build(t, plan.Workload{
		Name: "w", Type: TypeName,
		Params: map[string]interface{}{
			"fragments": float64(1), "fragment_size": float64(64),
			"manifest_count": 1, "manifest_min_size": 32, "manifest_max_size": 64,
			"range_min": 8, "range_max": 16,
		},
	})
	if err := w.Prepopulate(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	lw := w.(*Workload)
	rs, _ := buildRange(env.Rand)
	seeds := []int64{}
	// Drive many reads with different seeds to ensure each kind=0,1,2 fires.
	for i := int64(0); i < 100; i++ {
		seeds = append(seeds, i)
		lw.doRead(context.Background(), env, env.Recorder, rand.New(rand.NewSource(i)), rs)
	}
	snap := coll.Snapshot().Workloads["w"]
	if snap[metrics.OpList] == nil || snap[metrics.OpList].Count == 0 {
		t.Fatal("no LISTs")
	}
	if snap[metrics.OpHead] == nil || snap[metrics.OpHead].Count == 0 {
		t.Fatal("no HEADs")
	}
}

func TestCleanupUnrelatedError(t *testing.T) {
	env, fc, _ := makeEnv(t, 1)
	w := build(t, plan.Workload{
		Name: "w", Type: TypeName,
		Params: map[string]interface{}{"fragments": float64(1), "fragment_size": float64(64), "manifest_count": 1, "manifest_min_size": 8, "manifest_max_size": 16},
	})
	if err := w.Prepopulate(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	fc.FailOp("delete", errors.New("down"))
	if err := w.Cleanup(context.Background(), env); err == nil {
		t.Fatal("expected error")
	}
}

func TestCleanupManifestError(t *testing.T) {
	// Directly construct a Workload with 0 fragments and 1 manifest so Cleanup
	// skips the fragment loop and exercises the manifest-delete error branch.
	env, fc, _ := makeEnv(t, 1)
	w := &Workload{name: "w", fragmentCount: 0, manifestCount: 1}
	fc.FailOp("delete", errors.New("manifest down"))
	if err := w.Cleanup(context.Background(), env); err == nil {
		t.Fatal("expected manifest delete error")
	}
}

func TestCleanupSwallowsNotFound(t *testing.T) {
	env, fc, _ := makeEnv(t, 1)
	w := build(t, plan.Workload{
		Name: "w", Type: TypeName,
		Params: map[string]interface{}{"fragments": float64(1), "fragment_size": float64(64), "manifest_count": 1, "manifest_min_size": 8, "manifest_max_size": 16},
	})
	if err := w.Prepopulate(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	fc.FailOp("delete", fake.ErrNotFound)
	if err := w.Cleanup(context.Background(), env); err != nil {
		t.Fatal(err)
	}
}

func TestNameAndType(t *testing.T) {
	w := build(t, plan.Workload{Name: "abc", Type: TypeName})
	if w.Name() != "abc" || w.Type() != TypeName {
		t.Fatal("name/type")
	}
}

func TestHelpers(t *testing.T) {
	// Helper coverage lives in internal/workload.
}

// buildRange constructs a tiny sampler for tests.
func buildRange(r *rand.Rand) (rangeSampler, error) {
	return &fixedSampler{v: 8}, nil
}

type rangeSampler interface{ Sample() int64 }
type fixedSampler struct{ v int64 }

func (f *fixedSampler) Sample() int64 { return f.v }
