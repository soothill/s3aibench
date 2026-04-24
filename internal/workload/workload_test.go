package workload

import (
	"context"
	"errors"
	"testing"

	"github.com/darrensoothill/s3aibench/internal/plan"
)

type fakeWL struct{ name string }

func (f *fakeWL) Name() string                                   { return f.name }
func (f *fakeWL) Type() string                                   { return "fakeWL" }
func (f *fakeWL) Prepopulate(_ context.Context, _ *Env) error    { return nil }
func (f *fakeWL) Run(_ context.Context, _ *Env) error            { return nil }
func (f *fakeWL) Cleanup(_ context.Context, _ *Env) error        { return nil }

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
