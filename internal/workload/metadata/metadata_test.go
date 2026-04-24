package metadata

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
	cases := []map[string]interface{}{
		{"prefix_fanout": 0},
		{"prefix_depth": 0},
		{"objects_per_prefix": 0},
	}
	for _, c := range cases {
		if _, err := workload.Build(plan.Workload{Name: "w", Type: TypeName, Params: c}); err == nil {
			t.Fatalf("expected error for %v", c)
		}
	}
}

func TestFactoryDefaults(t *testing.T) {
	w := build(t, plan.Workload{Name: "w", Type: TypeName})
	mw := w.(*Workload)
	if mw.fanout != 4 || mw.depth != 2 || mw.perPrefix != 25 || mw.objectSize != 1024 {
		t.Fatalf("defaults wrong: %+v", mw)
	}
}

func TestFactoryCustom(t *testing.T) {
	w := build(t, plan.Workload{
		Name: "w", Type: TypeName, ObjectSize: 2048,
		Params: map[string]interface{}{
			"prefix_fanout":      float64(2),
			"prefix_depth":       1,
			"objects_per_prefix": 3,
		},
	})
	mw := w.(*Workload)
	if mw.fanout != 2 || mw.depth != 1 || mw.perPrefix != 3 || mw.objectSize != 2048 {
		t.Fatalf("params wrong: %+v", mw)
	}
}

func TestRunAllVerbs(t *testing.T) {
	env, fc, coll := makeEnv(t, 4)
	w := build(t, plan.Workload{
		Name: "w", Type: TypeName, ObjectSize: 64,
		Params: map[string]interface{}{
			"prefix_fanout": float64(2), "prefix_depth": 1, "objects_per_prefix": 4,
		},
	})
	if err := w.Prepopulate(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	if len(fc.Objects()) == 0 {
		t.Fatal("prepopulate produced nothing")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	if err := w.Run(ctx, env); err != nil {
		t.Fatal(err)
	}
	snap := coll.Snapshot().Workloads["w"]
	for _, op := range []metrics.Op{metrics.OpHead, metrics.OpList, metrics.OpCopy, metrics.OpPutTagging, metrics.OpGetTagging} {
		if snap[op] == nil || snap[op].Count == 0 {
			t.Fatalf("verb %s not exercised", op)
		}
	}
	if err := w.Cleanup(context.Background(), env); err != nil {
		t.Fatal(err)
	}
}

func TestRunEmptyDataset(t *testing.T) {
	env, _, _ := makeEnv(t, 1)
	w := build(t, plan.Workload{Name: "w", Type: TypeName})
	if err := w.Run(context.Background(), env); err == nil {
		t.Fatal("expected empty-dataset error")
	}
}

func TestRunZeroThreads(t *testing.T) {
	env, _, _ := makeEnv(t, 0)
	w := build(t, plan.Workload{Name: "w", Type: TypeName})
	lw := w.(*Workload)
	lw.baseKeys = []string{"w/k"}
	if err := w.Run(context.Background(), env); err == nil {
		t.Fatal("expected zero-threads error")
	}
}

func TestPrepopulatePutError(t *testing.T) {
	env, fc, _ := makeEnv(t, 1)
	fc.FailOp("put", errors.New("x"))
	w := build(t, plan.Workload{Name: "w", Type: TypeName,
		Params: map[string]interface{}{"prefix_fanout": float64(1), "prefix_depth": 1, "objects_per_prefix": 1},
	})
	if err := w.Prepopulate(context.Background(), env); err == nil {
		t.Fatal("expected error")
	}
}

func TestListPagination(t *testing.T) {
	// Put >100 objects so LIST paginates and exercises the truncation branch.
	env, _, coll := makeEnv(t, 1)
	w := &Workload{name: "w", objectSize: 1}
	body := []byte("x")
	ctx := context.Background()
	for i := 0; i < 110; i++ {
		k := "w/obj-" + string(rune('A'+i%26)) + "/" + string(rune('A'+i))
		_ = env.S3.Put(ctx, k, bytesNewReader(body), int64(len(body)))
		w.baseKeys = append(w.baseKeys, k)
	}
	w.listPrefix(ctx, env, env.Recorder, "")
	st := coll.Snapshot().Workloads["w"][metrics.OpList]
	if st == nil || st.Count < 2 {
		t.Fatalf("expected multiple LIST requests, got %v", st)
	}
}

func TestListError(t *testing.T) {
	env, fc, coll := makeEnv(t, 1)
	fc.FailOp("list", errors.New("x"))
	w := &Workload{name: "w"}
	w.listPrefix(context.Background(), env, env.Recorder, "")
	st := coll.Snapshot().Workloads["w"][metrics.OpList]
	if st == nil || st.Errors == 0 {
		t.Fatal("expected error recorded")
	}
}

func TestHeadError(t *testing.T) {
	env, fc, coll := makeEnv(t, 1)
	fc.FailOp("head", errors.New("x"))
	w := &Workload{name: "w"}
	w.head(context.Background(), env, env.Recorder, "w/k")
	st := coll.Snapshot().Workloads["w"][metrics.OpHead]
	if st == nil || st.Errors == 0 {
		t.Fatal("expected error recorded")
	}
}

func TestCopyError(t *testing.T) {
	env, fc, coll := makeEnv(t, 1)
	fc.FailOp("copy", errors.New("x"))
	w := &Workload{name: "w"}
	w.copy(context.Background(), env, env.Recorder, "src")
	st := coll.Snapshot().Workloads["w"][metrics.OpCopy]
	if st == nil || st.Errors == 0 {
		t.Fatal("expected error")
	}
}

func TestPutTagsError(t *testing.T) {
	env, fc, coll := makeEnv(t, 1)
	fc.FailOp("put_tagging", errors.New("x"))
	w := &Workload{name: "w"}
	w.putTags(context.Background(), env, env.Recorder, "w/k")
	st := coll.Snapshot().Workloads["w"][metrics.OpPutTagging]
	if st == nil || st.Errors == 0 {
		t.Fatal("expected error")
	}
}

func TestGetTagsError(t *testing.T) {
	env, fc, coll := makeEnv(t, 1)
	fc.FailOp("get_tagging", errors.New("x"))
	w := &Workload{name: "w"}
	w.getTags(context.Background(), env, env.Recorder, "w/k")
	st := coll.Snapshot().Workloads["w"][metrics.OpGetTagging]
	if st == nil || st.Errors == 0 {
		t.Fatal("expected error")
	}
}

func TestCleanupUnrelatedError(t *testing.T) {
	env, fc, _ := makeEnv(t, 1)
	w := build(t, plan.Workload{Name: "w", Type: TypeName,
		Params: map[string]interface{}{"prefix_fanout": float64(1), "prefix_depth": 1, "objects_per_prefix": 1},
	})
	if err := w.Prepopulate(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	fc.FailOp("delete", errors.New("down"))
	if err := w.Cleanup(context.Background(), env); err == nil {
		t.Fatal("expected error")
	}
}

func TestCleanupCopyKeyError(t *testing.T) {
	// Build a workload with no base keys but a registered copy destination
	// so Cleanup skips the base loop and fails on the copy-delete path.
	env, fc, _ := makeEnv(t, 1)
	w := &Workload{name: "w"}
	w.copyCount.Store(1)
	fc.FailOp("delete", errors.New("copy delete down"))
	if err := w.Cleanup(context.Background(), env); err == nil {
		t.Fatal("expected copy delete error")
	}
}

func TestCleanupSwallowsNotFound(t *testing.T) {
	env, fc, _ := makeEnv(t, 1)
	w := build(t, plan.Workload{Name: "w", Type: TypeName,
		Params: map[string]interface{}{"prefix_fanout": float64(1), "prefix_depth": 1, "objects_per_prefix": 1},
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

func TestBuildPrefixesDepthZero(t *testing.T) {
	// Directly exercise the depth<=0 short-circuit.
	p := buildPrefixes("x", 2, 0)
	if len(p) != 1 || p[0] != "x" {
		t.Fatalf("got %v", p)
	}
}

func TestIntParamHelper(t *testing.T) {
	if intParam(map[string]interface{}{"k": 7}, "k", 0) != 7 {
		t.Fatal("int")
	}
	if intParam(map[string]interface{}{"k": "x"}, "k", 3) != 3 {
		t.Fatal("bad-type default")
	}
	if intParam(nil, "k", 3) != 3 {
		t.Fatal("missing default")
	}
}
