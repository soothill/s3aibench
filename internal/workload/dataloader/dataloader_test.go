package dataloader

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

func TestFactoryErrors(t *testing.T) {
	workload.Reset()
	Register()
	// object_count<=0
	if _, err := workload.Build(plan.Workload{Name: "w", Type: TypeName, Params: map[string]interface{}{"object_count": 0}}); err == nil {
		t.Fatal("expected count error")
	}
	// bad pattern
	if _, err := workload.Build(plan.Workload{Name: "w", Type: TypeName, Params: map[string]interface{}{"access_pattern": "bogus"}}); err == nil {
		t.Fatal("expected pattern error")
	}
}

func TestPrepopulateAndRunAllPatterns(t *testing.T) {
	for _, pattern := range []string{"sequential", "shuffled", "zipfian"} {
		t.Run(pattern, func(t *testing.T) {
			env, _, coll := makeEnv(t, 2)
			w := build(t, plan.Workload{
				Name: "w", Type: TypeName,
				Params: map[string]interface{}{
					"object_count":   float64(10),
					"size_mean":      float64(256),
					"size_sigma":     0.5,
					"size_min":       32,
					"size_max":       512,
					"access_pattern": pattern,
					"zipfian_s":      1.2,
				},
			})
			if err := w.Prepopulate(context.Background(), env); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			if err := w.Run(ctx, env); err != nil {
				t.Fatal(err)
			}
			st := coll.Snapshot().Workloads["w"][metrics.OpGet]
			if st == nil || st.Count == 0 {
				t.Fatal("no GETs recorded")
			}
			if err := w.Cleanup(context.Background(), env); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRangeGetProbability(t *testing.T) {
	env, _, coll := makeEnv(t, 1)
	w := build(t, plan.Workload{
		Name: "w", Type: TypeName,
		Params: map[string]interface{}{
			"object_count":      float64(5),
			"size_mean":         float64(256),
			"size_min":          32,
			"size_max":          512,
			"range_probability": 1.0,
		},
	})
	if err := w.Prepopulate(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_ = w.Run(ctx, env)
	st := coll.Snapshot().Workloads["w"][metrics.OpRangeGet]
	if st == nil || st.Count == 0 {
		t.Fatal("no range GETs recorded")
	}
}

func TestZeroThreads(t *testing.T) {
	env, _, _ := makeEnv(t, 0)
	w := build(t, plan.Workload{Name: "w", Type: TypeName})
	// Prepopulate writes a lot; shortcut: pretend dataset already exists.
	lw := w.(*Workload)
	lw.keys = []string{"w/k"}
	if err := w.Run(context.Background(), env); err == nil {
		t.Fatal("expected zero-threads error")
	}
}

func TestRunEmptyDataset(t *testing.T) {
	env, _, _ := makeEnv(t, 1)
	w := build(t, plan.Workload{Name: "w", Type: TypeName})
	if err := w.Run(context.Background(), env); err == nil {
		t.Fatal("expected empty-dataset error")
	}
}

func TestZipfianParamError(t *testing.T) {
	env, _, _ := makeEnv(t, 1)
	w := build(t, plan.Workload{
		Name: "w", Type: TypeName,
		Params: map[string]interface{}{
			"object_count":   float64(5),
			"access_pattern": "zipfian",
			"zipfian_s":      1.0, // invalid
		},
	})
	lw := w.(*Workload)
	lw.keys = []string{"a", "b"}
	if err := w.Run(context.Background(), env); err == nil {
		t.Fatal("expected sampler error")
	}
}

func TestPrepopulateSamplerError(t *testing.T) {
	env, _, _ := makeEnv(t, 1)
	w := build(t, plan.Workload{
		Name: "w", Type: TypeName,
		Params: map[string]interface{}{"size_sigma": 0.0},
	})
	if err := w.Prepopulate(context.Background(), env); err == nil {
		t.Fatal("expected sampler error")
	}
}

func TestPrepopulatePutError(t *testing.T) {
	env, fc, _ := makeEnv(t, 1)
	fc.FailOp("put", errors.New("x"))
	w := build(t, plan.Workload{
		Name: "w", Type: TypeName,
		Params: map[string]interface{}{"object_count": float64(1), "size_mean": float64(64), "size_min": 16, "size_max": 128},
	})
	if err := w.Prepopulate(context.Background(), env); err == nil {
		t.Fatal("expected put error")
	}
}

func TestCleanupUnrelatedError(t *testing.T) {
	env, fc, _ := makeEnv(t, 1)
	w := build(t, plan.Workload{
		Name: "w", Type: TypeName,
		Params: map[string]interface{}{"object_count": float64(1), "size_mean": float64(64), "size_min": 16, "size_max": 128},
	})
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
	w := build(t, plan.Workload{
		Name: "w", Type: TypeName,
		Params: map[string]interface{}{"object_count": float64(1), "size_mean": float64(64), "size_min": 16, "size_max": 128},
	})
	if err := w.Prepopulate(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	fc.FailOp("delete", fake.ErrNotFound)
	if err := w.Cleanup(context.Background(), env); err != nil {
		t.Fatal(err)
	}
}

func TestGetAndRangeErrors(t *testing.T) {
	env, fc, coll := makeEnv(t, 1)
	w := &Workload{name: "w"}
	fc.FailOp("get", errors.New("g"))
	w.doGet(context.Background(), env, env.Recorder, "k")
	fc.FailOp("range_get", errors.New("r"))
	w.doRangeGet(context.Background(), env, env.Recorder, "k")
	snap := coll.Snapshot().Workloads["w"]
	if snap[metrics.OpGet] == nil || snap[metrics.OpGet].Errors == 0 {
		t.Fatal("expected get error recorded")
	}
	if snap[metrics.OpRangeGet] == nil || snap[metrics.OpRangeGet].Errors == 0 {
		t.Fatal("expected range_get error recorded")
	}
}

func TestNameAndType(t *testing.T) {
	w := build(t, plan.Workload{Name: "abc", Type: TypeName})
	if w.Name() != "abc" || w.Type() != TypeName {
		t.Fatal("name/type")
	}
}

func TestHelpers(t *testing.T) {
	if intParam(map[string]interface{}{"k": 7}, "k", 0) != 7 {
		t.Fatal("intParam int")
	}
	if intParam(map[string]interface{}{"k": "x"}, "k", 3) != 3 {
		t.Fatal("intParam default")
	}
	if floatParam(map[string]interface{}{"k": 2.5}, "k", 0) != 2.5 {
		t.Fatal("floatParam float")
	}
	if floatParam(map[string]interface{}{"k": 3}, "k", 0) != 3 {
		t.Fatal("floatParam int")
	}
	if floatParam(map[string]interface{}{"k": "2.5"}, "k", 0) != 2.5 {
		t.Fatal("floatParam string")
	}
	if floatParam(map[string]interface{}{"k": "bad"}, "k", 9) != 9 {
		t.Fatal("floatParam bad-string fallback")
	}
	if floatParam(map[string]interface{}{"k": false}, "k", 9) != 9 {
		t.Fatal("floatParam bool fallback")
	}
	if floatParam(nil, "k", 9) != 9 {
		t.Fatal("floatParam missing")
	}
	if sizeParam(map[string]interface{}{"k": float64(10)}, "k", 0) != 10 {
		t.Fatal("sizeParam float")
	}
	if sizeParam(map[string]interface{}{"k": 7}, "k", 0) != 7 {
		t.Fatal("sizeParam int")
	}
	if sizeParam(map[string]interface{}{"k": "n"}, "k", 3) != 3 {
		t.Fatal("sizeParam bad-type default")
	}
	if sizeParam(nil, "k", 3) != 3 {
		t.Fatal("sizeParam missing")
	}
	if stringParam(map[string]interface{}{"k": "hi"}, "k", "d") != "hi" {
		t.Fatal("stringParam value")
	}
	if stringParam(map[string]interface{}{"k": 7}, "k", "d") != "d" {
		t.Fatal("stringParam bad-type fallback")
	}
	if stringParam(nil, "k", "d") != "d" {
		t.Fatal("stringParam missing")
	}
}
