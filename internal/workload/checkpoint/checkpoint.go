// Package checkpoint simulates distributed-training checkpoint writes
// (PRD §5.1). Writer threads emit one large object per cycle on a configurable
// burst interval, then idle. Optional resume mode periodically re-reads the
// most recent checkpoint.
package checkpoint

import (
	"context"
	"fmt"
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

// TypeName is the YAML `type:` value.
const TypeName = "checkpointing"

// Register wires the factory.
func Register() {
	workload.Register(TypeName, func(w plan.Workload) (workload.Workload, error) {
		if w.ObjectSize <= 0 {
			return nil, fmt.Errorf("checkpoint %q: object_size must be >0", w.Name)
		}
		writers := intParam(w.Params, "writers", 4)
		retain := intParam(w.Params, "retain_versions", 3)
		burst := durationParam(w.Params, "burst_interval", 60*time.Second)
		resume := boolParam(w.Params, "resume", false)
		return &Workload{
			name: w.Name, keyPrefix: w.Name + "/ckpt-", objectSize: int64(w.ObjectSize),
			writers: writers, retain: retain, burstInterval: burst, resume: resume,
		}, nil
	})
}

// Workload is the checkpoint implementation.
type Workload struct {
	name          string
	keyPrefix     string
	objectSize    int64
	writers       int
	retain        int
	burstInterval time.Duration
	resume        bool

	mu       sync.Mutex
	versions []string
	counter  atomic.Int64
}

func (w *Workload) Name() string { return w.name }
func (w *Workload) Type() string { return TypeName }

// Prepopulate writes one initial checkpoint so resume reads have a target.
func (w *Workload) Prepopulate(ctx context.Context, env *workload.Env) error {
	return w.writeOne(ctx, env)
}

// Run launches `writers` goroutines that each burst-write a single checkpoint
// object, idle for `burst_interval`, then repeat. If `resume` is true an
// additional reader goroutine loops re-reading the latest version.
func (w *Workload) Run(ctx context.Context, env *workload.Env) error {
	if w.writers <= 0 {
		return fmt.Errorf("checkpoint %q: writers must be >0", w.name)
	}
	var wg sync.WaitGroup
	wg.Add(w.writers)
	for i := 0; i < w.writers; i++ {
		rec := env.Recorder.ShardFor(i)
		go func(rec metrics.Recorder) {
			defer wg.Done()
			timer := time.NewTimer(0)
			defer timer.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-timer.C:
				}
				if err := w.writeOneAt(ctx, env, rec); err != nil && ctx.Err() == nil {
					env.Logger.Warn("checkpoint burst failed", "err", err)
				}
				timer.Reset(w.burstInterval)
			}
		}(rec)
	}
	if w.resume {
		wg.Add(1)
		// Resume reader gets its own shard — avoids contending with writers.
		resumeRec := env.Recorder.ShardFor(w.writers)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				w.readLatest(ctx, env, resumeRec)
			}
		}()
	}
	wg.Wait()
	return nil
}

// writeOne preserves the single-writer signature used by Prepopulate; it
// routes through the fallback recorder (env.Recorder) because Prepopulate has
// no worker ID to shard on.
func (w *Workload) writeOne(ctx context.Context, env *workload.Env) error {
	return w.writeOneAt(ctx, env, env.Recorder)
}

func (w *Workload) writeOneAt(ctx context.Context, env *workload.Env, rec metrics.Recorder) error {
	n := w.counter.Add(1)
	key := workload.AppendKeyPadded(nil, w.keyPrefix, n, 8)
	body := bodygen.NewReader(w.objectSize)
	start := time.Now()
	err := env.S3.MultipartUpload(ctx, key, body, w.objectSize)
	bodygen.Release(body)
	rec.Record(w.name, metrics.OpCheckpointPut, time.Since(start), w.objectSize, err)
	if err != nil {
		return err
	}
	w.mu.Lock()
	w.versions = append(w.versions, key)
	// Eagerly delete older versions beyond the retain window.
	var toDel []string
	if len(w.versions) > w.retain {
		toDel = append(toDel, w.versions[:len(w.versions)-w.retain]...)
		w.versions = w.versions[len(w.versions)-w.retain:]
	}
	w.mu.Unlock()
	for _, k := range toDel {
		if derr := env.S3.Delete(ctx, k); derr != nil && !s3client.IsNotFound(derr) {
			env.Logger.Warn("retain-delete failed", "key", k, "err", derr)
		}
	}
	return nil
}

func (w *Workload) readLatest(ctx context.Context, env *workload.Env, rec metrics.Recorder) {
	w.mu.Lock()
	var latest string
	if len(w.versions) > 0 {
		latest = w.versions[len(w.versions)-1]
	}
	w.mu.Unlock()
	if latest == "" {
		return
	}
	start := time.Now()
	rc, err := env.S3.Get(ctx, latest)
	var n int64
	if err == nil {
		n, _ = drain.Drain(rc)
		rc.Close()
	}
	rec.Record(w.name, metrics.OpGet, time.Since(start), n, err)
}

// Cleanup deletes every checkpoint still tracked.
func (w *Workload) Cleanup(ctx context.Context, env *workload.Env) error {
	w.mu.Lock()
	keys := append([]string(nil), w.versions...)
	w.mu.Unlock()
	for _, k := range keys {
		if err := env.S3.Delete(ctx, k); err != nil && !s3client.IsNotFound(err) {
			return err
		}
	}
	return nil
}

// -- helpers -----------------------------------------------------------------

func intParam(p map[string]interface{}, key string, def int) int {
	if v, ok := p[key]; ok {
		if f, fok := v.(float64); fok {
			return int(f)
		}
		if i, iok := v.(int); iok {
			return i
		}
	}
	return def
}

func boolParam(p map[string]interface{}, key string, def bool) bool {
	if v, ok := p[key]; ok {
		if b, bok := v.(bool); bok {
			return b
		}
	}
	return def
}

func durationParam(p map[string]interface{}, key string, def time.Duration) time.Duration {
	if v, ok := p[key]; ok {
		if s, sok := v.(string); sok {
			if d, err := time.ParseDuration(s); err == nil {
				return d
			}
		}
	}
	return def
}
