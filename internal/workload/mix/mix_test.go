package mix

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"sync/atomic"
	"testing"

	"github.com/darrensoothill/s3aibench/internal/metrics"
	"github.com/darrensoothill/s3aibench/internal/plan"
	"github.com/darrensoothill/s3aibench/internal/s3client/fake"
	"github.com/darrensoothill/s3aibench/internal/workload"
)

type stub struct {
	name                                string
	runCalls, prepopCalls, cleanupCalls atomic.Int32
	prepopErr, runErr, cleanupErr       error
	capturedThreads                     atomic.Int32
}

func (s *stub) Name() string { return s.name }
func (s *stub) Type() string { return "stub" }
func (s *stub) Prepopulate(_ context.Context, _ *workload.Env) error {
	s.prepopCalls.Add(1)
	return s.prepopErr
}
func (s *stub) Run(_ context.Context, env *workload.Env) error {
	s.runCalls.Add(1)
	s.capturedThreads.Store(int32(env.Threads))
	return s.runErr
}
func (s *stub) Cleanup(_ context.Context, _ *workload.Env) error {
	s.cleanupCalls.Add(1)
	return s.cleanupErr
}

func registerStubs(t *testing.T, stubs map[string]*stub) {
	t.Helper()
	workload.Reset()
	Register()
	workload.Register("stub", func(p plan.Workload) (workload.Workload, error) {
		s, ok := stubs[p.Name]
		if !ok {
			return nil, errors.New("no stub for " + p.Name)
		}
		return s, nil
	})
}

func makeEnv() *workload.Env {
	return &workload.Env{
		S3:       fake.New(),
		Recorder: metrics.NewCollector(),
		Logger:   slog.Default(),
		Threads:  6,
		Rand:     rand.New(rand.NewSource(1)),
	}
}

func mixPlan(nested []interface{}) plan.Workload {
	return plan.Workload{Name: "m", Type: TypeName, Params: map[string]interface{}{"workloads": nested}}
}

func TestFactoryErrors(t *testing.T) {
	workload.Reset()
	Register()
	// Missing workloads
	if _, err := workload.Build(plan.Workload{Name: "m", Type: TypeName}); err == nil {
		t.Fatal("expected missing-workloads error")
	}
	// Wrong type for workloads
	if _, err := workload.Build(plan.Workload{Name: "m", Type: TypeName, Params: map[string]interface{}{"workloads": "nope"}}); err == nil {
		t.Fatal("expected wrong-type error")
	}
	// Entry not a map
	if _, err := workload.Build(plan.Workload{Name: "m", Type: TypeName, Params: map[string]interface{}{"workloads": []interface{}{"nope"}}}); err == nil {
		t.Fatal("expected non-map entry error")
	}
	// Missing name/type inside nested
	if _, err := workload.Build(plan.Workload{Name: "m", Type: TypeName, Params: map[string]interface{}{"workloads": []interface{}{map[string]interface{}{}}}}); err == nil {
		t.Fatal("expected missing name/type error")
	}
	// Zero nested after parse (empty list)
	if _, err := workload.Build(plan.Workload{Name: "m", Type: TypeName, Params: map[string]interface{}{"workloads": []interface{}{}}}); err == nil {
		t.Fatal("expected no-nested error")
	}
}

func TestFactoryChildBuildError(t *testing.T) {
	// No "stub" factory registered → child Build fails.
	workload.Reset()
	Register()
	_, err := workload.Build(mixPlan([]interface{}{
		map[string]interface{}{"name": "a", "type": "stub"},
	}))
	if err == nil {
		t.Fatal("expected child build error")
	}
}

func TestMixHappyPath(t *testing.T) {
	a := &stub{name: "a"}
	b := &stub{name: "b"}
	registerStubs(t, map[string]*stub{"a": a, "b": b})
	w, err := workload.Build(mixPlan([]interface{}{
		map[string]interface{}{"name": "a", "type": "stub", "weight": float64(1)},
		map[string]interface{}{"name": "b", "type": "stub", "weight": 2},
	}))
	if err != nil {
		t.Fatal(err)
	}
	env := makeEnv()
	if err := w.Prepopulate(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	if err := w.Run(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	if err := w.Cleanup(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	// Total weight 1+2=3, env.Threads=6 → a gets 2, b gets 4.
	if a.capturedThreads.Load() != 2 || b.capturedThreads.Load() != 4 {
		t.Fatalf("thread allocation wrong: a=%d b=%d", a.capturedThreads.Load(), b.capturedThreads.Load())
	}
	if a.runCalls.Load() == 0 || b.runCalls.Load() == 0 {
		t.Fatal("children not run")
	}
}

func TestMixThreadMinimumOne(t *testing.T) {
	// total weight = 101, env.Threads=1 → allocations must stay within the
	// requested budget instead of oversubscribing to 2 threads.
	a := &stub{name: "a"}
	b := &stub{name: "b"}
	registerStubs(t, map[string]*stub{"a": a, "b": b})
	w, _ := workload.Build(mixPlan([]interface{}{
		map[string]interface{}{"name": "a", "type": "stub", "weight": 1},
		map[string]interface{}{"name": "b", "type": "stub", "weight": 100},
	}))
	env := makeEnv()
	env.Threads = 1
	if err := w.Run(context.Background(), env); err != nil {
		t.Fatal(err)
	}
	if a.runCalls.Load() != 0 || b.capturedThreads.Load() != 1 {
		t.Fatalf("unexpected allocation: a calls=%d b threads=%d", a.runCalls.Load(), b.capturedThreads.Load())
	}
}

func TestMixChildRunError(t *testing.T) {
	a := &stub{name: "a", runErr: errors.New("boom")}
	registerStubs(t, map[string]*stub{"a": a})
	w, _ := workload.Build(mixPlan([]interface{}{
		map[string]interface{}{"name": "a", "type": "stub"},
	}))
	if err := w.Run(context.Background(), makeEnv()); err == nil {
		t.Fatal("expected child error")
	}
}

func TestMixPrepopulateError(t *testing.T) {
	a := &stub{name: "a", prepopErr: errors.New("x")}
	registerStubs(t, map[string]*stub{"a": a})
	w, _ := workload.Build(mixPlan([]interface{}{
		map[string]interface{}{"name": "a", "type": "stub"},
	}))
	if err := w.Prepopulate(context.Background(), makeEnv()); err == nil {
		t.Fatal("expected error")
	}
}

func TestMixCleanupError(t *testing.T) {
	a := &stub{name: "a", cleanupErr: errors.New("x")}
	registerStubs(t, map[string]*stub{"a": a})
	w, _ := workload.Build(mixPlan([]interface{}{
		map[string]interface{}{"name": "a", "type": "stub"},
	}))
	if err := w.Cleanup(context.Background(), makeEnv()); err == nil {
		t.Fatal("expected error")
	}
}

func TestNameAndType(t *testing.T) {
	a := &stub{name: "a"}
	registerStubs(t, map[string]*stub{"a": a})
	w, _ := workload.Build(mixPlan([]interface{}{
		map[string]interface{}{"name": "a", "type": "stub"},
	}))
	if w.Name() != "m" || w.Type() != TypeName {
		t.Fatalf("name/type: %q %q", w.Name(), w.Type())
	}
}

func TestParseNestedParamPassthrough(t *testing.T) {
	a := &stub{name: "a"}
	registerStubs(t, map[string]*stub{"a": a})
	// object_size + params pass through to the nested factory so we exercise
	// both branches in parseNested.
	n, err := parseNested(map[string]interface{}{
		"name": "a", "type": "stub",
		"weight":      -5, // forces weight<=0 → default to 1
		"object_size": float64(1024),
		"params":      map[string]interface{}{"x": "y"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if n.weight != 1 || n.plan.ObjectSize != 1024 || n.plan.Params["x"] != "y" {
		t.Fatalf("nested: %+v", n)
	}
	// Int weight and int object_size branches.
	n2, err := parseNested(map[string]interface{}{"name": "a", "type": "stub", "weight": 3, "object_size": 7})
	if err != nil {
		t.Fatal(err)
	}
	if n2.weight != 3 || n2.plan.ObjectSize != 7 {
		t.Fatalf("nested int: %+v", n2)
	}
}

func TestRunZeroTotalWeight(t *testing.T) {
	// Construct a Workload with empty children/weights to force total<=0.
	w := &Workload{name: "m"}
	if err := w.Run(context.Background(), makeEnv()); err == nil {
		t.Fatal("expected total-weight error")
	}
}

func TestRunZeroThreads(t *testing.T) {
	a := &stub{name: "a"}
	registerStubs(t, map[string]*stub{"a": a})
	w, _ := workload.Build(mixPlan([]interface{}{
		map[string]interface{}{"name": "a", "type": "stub"},
	}))
	env := makeEnv()
	env.Threads = 0
	if err := w.Run(context.Background(), env); err == nil {
		t.Fatal("expected zero-threads error")
	}
}

func TestAllocateThreadsKeepsBudget(t *testing.T) {
	alloc := allocateThreads(5, []int{1, 1, 1})
	total := 0
	for _, n := range alloc {
		total += n
	}
	if total != 5 {
		t.Fatalf("allocated %d threads", total)
	}
}

func TestAllocateThreadsEdgeCases(t *testing.T) {
	if got := allocateThreads(0, []int{1, 2}); len(got) != 2 || got[0] != 0 || got[1] != 0 {
		t.Fatalf("zero-thread alloc=%v", got)
	}
	if got := allocateThreads(3, nil); len(got) != 0 {
		t.Fatalf("nil weights alloc=%v", got)
	}
	if got := allocateThreads(3, []int{0, 0}); len(got) != 2 || got[0] != 0 || got[1] != 0 {
		t.Fatalf("zero-weight alloc=%v", got)
	}
}
