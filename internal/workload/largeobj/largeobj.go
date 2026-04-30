// Package largeobj is the uniform large-object multipart PUT / range GET
// workload from PRD §5.6 — the throughput ceiling test.
package largeobj

import (
	"context"
	"errors"
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
const TypeName = "largeobject"

// Register wires the largeobj factory.
func Register() {
	workload.Register(TypeName, func(w plan.Workload) (workload.Workload, error) {
		if w.ObjectSize <= 0 {
			return nil, errors.New("largeobj " + w.Name + ": object_size must be >0")
		}
		readRatio := 0.0
		if r, ok := w.Params["read_ratio"].(float64); ok {
			readRatio = r
		}
		return &Workload{
			name:       w.Name,
			keyPrefix:  w.Name + "/obj-",
			objectSize: int64(w.ObjectSize),
			readRatio:  readRatio,
		}, nil
	})
}

// Workload is the largeobj implementation. Lock-free: keys are derived from
// an atomic counter so readers and writers never share a slice.
type Workload struct {
	name         string
	keyPrefix    string
	objectSize   int64
	readRatio    float64
	writtenCount atomic.Int64
}

func (w *Workload) Name() string { return w.name }
func (w *Workload) Type() string { return TypeName }

func (w *Workload) keyFor(n int64) string {
	return w.keyPrefix + strconv.FormatInt(n, 10)
}

// Prepopulate uploads one large object at index 0 so GET paths have a target.
func (w *Workload) Prepopulate(ctx context.Context, env *workload.Env) error {
	body := bodygen.NewReader(w.objectSize)
	err := env.S3.MultipartUpload(ctx, w.keyFor(0), body, w.objectSize)
	bodygen.Release(body)
	if err != nil {
		return err
	}
	w.writtenCount.Store(1)
	return nil
}

// Run performs multipart PUTs (and optional range GETs) on each worker.
func (w *Workload) Run(ctx context.Context, env *workload.Env) error {
	if env.Threads <= 0 {
		return errors.New("largeobj " + w.name + ": threads must be >0")
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
					cnt := w.writtenCount.Load()
					if cnt == 0 {
						continue
					}
					k := w.keyFor(local.Int63n(cnt))
					start := time.Now()
					rc, err := env.S3.RangeGet(ctx, k, 0, w.objectSize)
					var n int64
					if err == nil {
						n, _ = drain.Drain(rc)
						rc.Close()
					}
					rec.Record(w.name, metrics.OpRangeGet, time.Since(start), n, err)
				} else {
					idx := w.writtenCount.Add(1) - 1
					key := w.keyFor(idx)
					body := bodygen.NewReader(w.objectSize)
					start := time.Now()
					err := env.S3.MultipartUpload(ctx, key, body, w.objectSize)
					bodygen.Release(body)
					rec.Record(w.name, metrics.OpMultipartComplete, time.Since(start), w.objectSize, err)
				}
			}
		}(i)
	}
	wg.Wait()
	return nil
}

// Cleanup deletes every object uploaded by this workload.
func (w *Workload) Cleanup(ctx context.Context, env *workload.Env) error {
	cnt := w.writtenCount.Load()
	for i := int64(0); i < cnt; i++ {
		if err := env.S3.Delete(ctx, w.keyFor(i)); err != nil && !s3client.IsNotFound(err) {
			return err
		}
	}
	return nil
}
