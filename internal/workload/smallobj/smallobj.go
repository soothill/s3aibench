// Package smallobj is the uniform small-object PUT/GET workload — the IOPS
// stress pattern from PRD §5.5.
package smallobj

import (
	"context"
	"errors"
	"io"
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
	name         string
	keyPrefix    string // pre-built so keyFor avoids fmt.Sprintf
	objectSize   int64
	readRatio    float64
	writtenCount atomic.Int64 // number of keys written (indices 0..writtenCount-1 valid)
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
	err := env.S3.Put(ctx, w.keyFor(0), body, w.objectSize)
	bodygen.Release(body)
	if err != nil {
		return err
	}
	w.writtenCount.Store(1)
	return nil
}

// Run launches env.Threads workers that loop PUT/GET until ctx is done.
// Writes use `atomic.Int64.Add` to claim a unique key index; reads pick a
// random valid index. No mutexes in the hot path.
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
			rec := env.Recorder.ShardFor(id)
			for ctx.Err() == nil {
				if local.Float64() < w.readRatio {
					cnt := w.writtenCount.Load()
					if cnt == 0 {
						continue
					}
					k := w.keyFor(local.Int63n(cnt))
					start := time.Now()
					rc, err := env.S3.Get(ctx, k)
					var n int64
					if err == nil {
						n, _ = drain.Drain(rc)
						rc.Close()
					}
					rec.Record(w.name, metrics.OpGet, time.Since(start), n, err)
				} else {
					idx := w.writtenCount.Add(1) - 1
					k := w.keyFor(idx)
					body := bodygen.NewReader(w.objectSize)
					start := time.Now()
					err := env.S3.Put(ctx, k, body, w.objectSize)
					bodygen.Release(body)
					rec.Record(w.name, metrics.OpPut, time.Since(start), w.objectSize, err)
				}
			}
		}(i)
	}
	wg.Wait()
	return nil
}

// Cleanup deletes every key claimed by the workload. Indices 0..writtenCount-1
// are always valid, which eliminates the shared list.
func (w *Workload) Cleanup(ctx context.Context, env *workload.Env) error {
	cnt := w.writtenCount.Load()
	for i := int64(0); i < cnt; i++ {
		if err := env.S3.Delete(ctx, w.keyFor(i)); err != nil && !s3client.IsNotFound(err) {
			return err
		}
	}
	return nil
}

// Keep io import stable for future helpers.
var _ io.Reader = (io.Reader)(nil)
