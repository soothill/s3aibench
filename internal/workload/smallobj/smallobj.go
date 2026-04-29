// Package smallobj is the uniform small-object PUT/GET workload — the IOPS
// stress pattern from PRD §5.5.
package smallobj

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/darrensoothill/s3aibench/internal/bodygen"
	"github.com/darrensoothill/s3aibench/internal/drain"
	"github.com/darrensoothill/s3aibench/internal/metrics"
	"github.com/darrensoothill/s3aibench/internal/plan"
	"github.com/darrensoothill/s3aibench/internal/s3client"
	"github.com/darrensoothill/s3aibench/internal/workload"
)

// TypeName is the YAML `type:` value that selects this workload.
const TypeName = "smallobject"

// Register wires the smallobj factory into the global registry. Call from
// main() (or a dedicated init aggregator) rather than init() so tests can
// register on demand.
func Register() {
	workload.Register(TypeName, func(w plan.Workload) (workload.Workload, error) {
		if w.ObjectSize <= 0 {
			return nil, errors.New("smallobj " + w.Name + ": object_size must be >0")
		}
		readRatio := 0.5
		if r, ok := w.Params["read_ratio"].(float64); ok {
			readRatio = r
		}
		return &Workload{
			name:       w.Name,
			keyPrefix:  w.Name + "/k-",
			objectSize: int64(w.ObjectSize),
			readRatio:  readRatio,
		}, nil
	})
}

// Workload is the smallobj implementation. Lock-free: the only shared state
// is an atomic counter; keys are derived deterministically from the counter.
type Workload struct {
	name       string
	keyPrefix  string // pre-built so keyFor avoids fmt.Sprintf
	objectSize int64
	readRatio  float64
	nextIndex  atomic.Int64
	keysMu     sync.RWMutex
	keys       []string // successfully written keys
}

func (w *Workload) Name() string { return w.name }
func (w *Workload) Type() string { return TypeName }

// keyFor concatenates the pre-built prefix with the decimal index — one
// allocation per call (the result string) versus four or more for fmt.Sprintf.
func (w *Workload) keyFor(n int64) string {
	return w.keyPrefix + strconv.FormatInt(n, 10)
}

// Prepopulate writes a single base object at index 0 so GETs have a target
// from the first iteration.
func (w *Workload) Prepopulate(ctx context.Context, env *workload.Env) error {
	body := bodygen.NewReader(w.objectSize)
	key := w.keyFor(0)
	err := env.S3.Put(ctx, key, body, w.objectSize)
	bodygen.Release(body)
	if err != nil {
		return err
	}
	w.nextIndex.Store(1)
	w.keysMu.Lock()
	w.keys = []string{key}
	w.keysMu.Unlock()
	return nil
}

// Run launches env.Threads workers that loop PUT/GET until ctx is done.
// Writes claim unique key indices up front, but readers only see keys that
// were actually committed successfully.
func (w *Workload) Run(ctx context.Context, env *workload.Env) error {
	if env.Threads <= 0 {
		return errors.New("smallobj " + w.name + ": threads must be >0")
	}
	var wg sync.WaitGroup
	wg.Add(env.Threads)
	for i := 0; i < env.Threads; i++ {
		go func(id int) {
			defer wg.Done()
			local := workload.WorkerRand(env, id)
			rec := env.ShardRecorder(id)
			for ctx.Err() == nil {
				if local.Float64() < w.readRatio {
					k, ok := w.sampleKey(local)
					if !ok {
						continue
					}
					start := time.Now()
					rc, err := env.S3.Get(ctx, k)
					var n int64
					if err == nil {
						n, _ = drain.Drain(rc)
						rc.Close()
					}
					rec.Record(w.name, metrics.OpGet, time.Since(start), n, err)
				} else {
					idx := w.nextIndex.Add(1) - 1
					k := w.keyFor(idx)
					body := bodygen.NewReader(w.objectSize)
					start := time.Now()
					err := env.S3.Put(ctx, k, body, w.objectSize)
					bodygen.Release(body)
					if err == nil {
						w.publishKey(k)
					}
					rec.Record(w.name, metrics.OpPut, time.Since(start), w.objectSize, err)
				}
			}
		}(i)
	}
	wg.Wait()
	return nil
}

func (w *Workload) loadKeys() []string {
	w.keysMu.RLock()
	defer w.keysMu.RUnlock()
	if len(w.keys) == 0 {
		return nil
	}
	keys := make([]string, len(w.keys))
	copy(keys, w.keys)
	return keys
}

func (w *Workload) sampleKey(r *rand.Rand) (string, bool) {
	w.keysMu.RLock()
	defer w.keysMu.RUnlock()
	if len(w.keys) == 0 {
		return "", false
	}
	return w.keys[r.Intn(len(w.keys))], true
}

func (w *Workload) publishKey(key string) {
	w.keysMu.Lock()
	w.keys = append(w.keys, key)
	w.keysMu.Unlock()
}

// Cleanup deletes every key known to have been written successfully.
func (w *Workload) Cleanup(ctx context.Context, env *workload.Env) error {
	for _, key := range w.loadKeys() {
		if err := env.S3.Delete(ctx, key); err != nil && !s3client.IsNotFound(err) {
			return err
		}
	}
	return nil
}

// Keep io import stable for future helpers.
var _ io.Reader = (io.Reader)(nil)
