package workload

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/darrensoothill/s3aibench/internal/metrics"
	"github.com/darrensoothill/s3aibench/internal/plan"
)

type fakeWL struct{ name string }

func (f *fakeWL) Name() string                                { return f.name }
func (f *fakeWL) Type() string                                { return "fakeWL" }
func (f *fakeWL) Prepopulate(_ context.Context, _ *Env) error { return nil }
func (f *fakeWL) Run(_ context.Context, _ *Env) error         { return nil }
func (f *fakeWL) Cleanup(_ context.Context, _ *Env) error     { return nil }

func TestRegisterAndBuild(t *testing.T) {
	Reset()
	Register("fake", func(w plan.Workload) (Workload, error) {
		return &fakeWL{name: w.Name}, nil
	})
	got, err := Build(plan.Workload{Name: "n", Type: "fake"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Name() != "n" {
		t.Fatalf("got %q", got.Name())
	}
	if len(Registered()) != 1 {
		t.Fatalf("registered=%v", Registered())
	}
}

func TestBuildUnknown(t *testing.T) {
	Reset()
	if _, err := Build(plan.Workload{Type: "nope"}); err == nil {
		t.Fatal("expected error")
	}
}

func TestRegisterDuplicatePanics(t *testing.T) {
	Reset()
	Register("dup", func(plan.Workload) (Workload, error) { return &fakeWL{}, nil })
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic")
		}
	}()
	Register("dup", func(plan.Workload) (Workload, error) { return &fakeWL{}, nil })
}

func TestFactoryError(t *testing.T) {
	Reset()
	Register("err", func(plan.Workload) (Workload, error) { return nil, errors.New("x") })
	if _, err := Build(plan.Workload{Type: "err"}); err == nil {
		t.Fatal("expected error")
	}
}

func TestWorkerHelpers(t *testing.T) {
	if got := WorkerSeed(41, 0); got != 42 {
		t.Fatalf("WorkerSeed() = %d, want 42", got)
	}

	nilRand := WorkerRand(nil, 1)
	if got := nilRand.Int63(); got != WorkerRand(nil, 1).Int63() {
		t.Fatalf("nil WorkerRand not deterministic: %d", got)
	}

	env := &Env{Seed: 99}
	r1 := WorkerRand(env, 2)
	r2 := WorkerRand(env, 2)
	if got1, got2 := r1.Int63(), r2.Int63(); got1 != got2 {
		t.Fatalf("seeded WorkerRand mismatch: %d != %d", got1, got2)
	}

	if got := AppendKeyDecimal(make([]byte, 0, 16), "p/", 123); got != "p/123" {
		t.Fatalf("AppendKeyDecimal() = %q", got)
	}
	if got := AppendKeyPadded(make([]byte, 0, 16), "p/", 7, 3); got != "p/007" {
		t.Fatalf("AppendKeyPadded() = %q", got)
	}
}

type shardSpyRecorder struct {
	mu  sync.Mutex
	ids []int
}

func (s *shardSpyRecorder) Record(string, metrics.Op, time.Duration, int64, error) {}
func (s *shardSpyRecorder) Snapshot() metrics.Snapshot                             { return metrics.Snapshot{} }
func (s *shardSpyRecorder) Totals() metrics.Totals                                 { return metrics.Totals{} }
func (s *shardSpyRecorder) ShardFor(id int) metrics.Recorder {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ids = append(s.ids, id)
	return s
}

func TestShardRecorderUsesSharedCounter(t *testing.T) {
	var counter atomic.Int64
	rec := &shardSpyRecorder{}
	envA := &Env{Recorder: rec, ShardCounter: &counter}
	envB := &Env{Recorder: rec, ShardCounter: &counter}

	envA.ShardRecorder(0)
	envB.ShardRecorder(0)

	if len(rec.ids) != 2 || rec.ids[0] != 0 || rec.ids[1] != 1 {
		t.Fatalf("shard ids = %v", rec.ids)
	}
}

func TestShardRecorderFallsBackToWorkerID(t *testing.T) {
	rec := &shardSpyRecorder{}
	env := &Env{Recorder: rec}

	env.ShardRecorder(7)

	if len(rec.ids) != 1 || rec.ids[0] != 7 {
		t.Fatalf("shard ids = %v", rec.ids)
	}
}

func TestShardRecorderNilCases(t *testing.T) {
	var env *Env
	if got := env.ShardRecorder(1); got != nil {
		t.Fatalf("nil env returned %v", got)
	}
	if got := (&Env{}).ShardRecorder(1); got != nil {
		t.Fatalf("nil recorder returned %v", got)
	}
}

func TestParamHelpers(t *testing.T) {
	if IntParam(map[string]interface{}{"k": float64(7)}, "k", 0) != 7 {
		t.Fatal("IntParam float")
	}
	if IntParam(map[string]interface{}{"k": 7}, "k", 0) != 7 {
		t.Fatal("IntParam int")
	}
	if IntParam(map[string]interface{}{"k": "x"}, "k", 3) != 3 {
		t.Fatal("IntParam default")
	}
	if SizeParam(map[string]interface{}{"k": float64(10)}, "k", 0) != 10 {
		t.Fatal("SizeParam float")
	}
	if SizeParam(map[string]interface{}{"k": 7}, "k", 0) != 7 {
		t.Fatal("SizeParam int")
	}
	if SizeParam(map[string]interface{}{"k": "n"}, "k", 3) != 3 {
		t.Fatal("SizeParam default")
	}
	if FloatParam(map[string]interface{}{"k": 2.5}, "k", 0) != 2.5 {
		t.Fatal("FloatParam float")
	}
	if FloatParam(map[string]interface{}{"k": 3}, "k", 0) != 3 {
		t.Fatal("FloatParam int")
	}
	if FloatParam(map[string]interface{}{"k": "2.5"}, "k", 0) != 2.5 {
		t.Fatal("FloatParam string")
	}
	if FloatParam(map[string]interface{}{"k": "bad"}, "k", 9) != 9 {
		t.Fatal("FloatParam fallback")
	}
	if StringParam(map[string]interface{}{"k": "hi"}, "k", "d") != "hi" {
		t.Fatal("StringParam value")
	}
	if StringParam(map[string]interface{}{"k": 7}, "k", "d") != "d" {
		t.Fatal("StringParam fallback")
	}
	if !BoolParam(map[string]interface{}{"k": true}, "k", false) {
		t.Fatal("BoolParam true")
	}
	if BoolParam(map[string]interface{}{"k": 1}, "k", false) {
		t.Fatal("BoolParam fallback")
	}
	if DurationParam(map[string]interface{}{"k": "1s"}, "k", 0) != time.Second {
		t.Fatal("DurationParam parse")
	}
	if DurationParam(map[string]interface{}{"k": "bad"}, "k", time.Minute) != time.Minute {
		t.Fatal("DurationParam fallback")
	}
}
