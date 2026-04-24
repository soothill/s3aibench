// Package workload defines the Workload interface every benchmark pattern
// implements, plus a registry that the runner consults to instantiate them.
package workload

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"strconv"
	"sync"

	"github.com/darrensoothill/s3aibench/internal/metrics"
	"github.com/darrensoothill/s3aibench/internal/plan"
	"github.com/darrensoothill/s3aibench/internal/s3client"
)

// Env is shared runtime context handed to each Workload.Run call.
type Env struct {
	S3          s3client.Client
	Recorder    metrics.Recorder
	Logger      *slog.Logger
	Rand        *rand.Rand
	Seed        int64
	Threads     int
	RunPrefix   string
	PartSize    int64
	Concurrency int
}

// Workload is the pluggable interface every workload implements.
type Workload interface {
	Name() string
	Type() string
	Prepopulate(ctx context.Context, env *Env) error
	Run(ctx context.Context, env *Env) error
	Cleanup(ctx context.Context, env *Env) error
}

// Factory constructs a Workload from a plan.Workload entry. Factories validate
// and normalize their inputs; returning an error aborts the run.
type Factory func(w plan.Workload) (Workload, error)

var (
	registryMu sync.RWMutex
	registry   = map[string]Factory{}
)

// Register adds a Factory under a workload type name. Safe for init() use.
func Register(typeName string, f Factory) {
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, dup := registry[typeName]; dup {
		panic("workload type already registered: " + typeName)
	}
	registry[typeName] = f
}

// Build looks up a registered Factory and constructs the Workload.
func Build(w plan.Workload) (Workload, error) {
	registryMu.RLock()
	f, ok := registry[w.Type]
	registryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown workload type %q", w.Type)
	}
	return f(w)
}

// Reset clears the registry; for tests only.
func Reset() {
	registryMu.Lock()
	defer registryMu.Unlock()
	registry = map[string]Factory{}
}

// Registered returns the sorted list of currently registered type names.
func Registered() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	out := make([]string, 0, len(registry))
	for k := range registry {
		out = append(out, k)
	}
	return out
}

// WorkerSeed returns the deterministic seed for a given worker ID.
func WorkerSeed(baseSeed int64, workerID int) int64 {
	return baseSeed + int64(workerID) + 1
}

// WorkerRand returns a per-worker RNG derived from the run seed.
func WorkerRand(env *Env, workerID int) *rand.Rand {
	if env == nil {
		return rand.New(rand.NewSource(WorkerSeed(0, workerID)))
	}
	return rand.New(rand.NewSource(WorkerSeed(env.Seed, workerID)))
}

// AppendKeyDecimal appends `prefix` + decimal(n) into dst and returns the resulting string.
func AppendKeyDecimal(dst []byte, prefix string, n int64) string {
	dst = append(dst[:0], prefix...)
	dst = strconv.AppendInt(dst, n, 10)
	return string(dst)
}

// AppendKeyPadded appends `prefix` + zero-padded decimal(n) into dst and returns the resulting string.
func AppendKeyPadded(dst []byte, prefix string, n int64, width int) string {
	dst = append(dst[:0], prefix...)
	var buf [20]byte
	scratch := strconv.AppendInt(buf[:0], n, 10)
	for i := len(scratch); i < width; i++ {
		dst = append(dst, '0')
	}
	dst = append(dst, scratch...)
	return string(dst)
}
