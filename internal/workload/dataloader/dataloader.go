// Package dataloader simulates training data-loader GET traffic (PRD §5.3):
// a pre-populated dataset of many small-to-medium objects, accessed with a
// configurable pattern (sequential | shuffled | zipfian).
package dataloader

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/darrensoothill/s3aibench/internal/bodygen"
	"github.com/darrensoothill/s3aibench/internal/drain"
	"github.com/darrensoothill/s3aibench/internal/metrics"
	"github.com/darrensoothill/s3aibench/internal/plan"
	"github.com/darrensoothill/s3aibench/internal/s3client"
	"github.com/darrensoothill/s3aibench/internal/sizedist"
	"github.com/darrensoothill/s3aibench/internal/workload"
)

// TypeName is the YAML `type:` value.
const TypeName = "training_data"

// Register wires the factory.
func Register() {
	workload.Register(TypeName, func(w plan.Workload) (workload.Workload, error) {
		count := workload.IntParam(w.Params, "object_count", 1000)
		if count <= 0 {
			return nil, fmt.Errorf("dataloader %q: object_count must be >0", w.Name)
		}
		sizeMean := workload.SizeParam(w.Params, "size_mean", 512*1024)
		sigma := workload.FloatParam(w.Params, "size_sigma", 0.8)
		sizeMin := workload.SizeParam(w.Params, "size_min", 64*1024)
		sizeMax := workload.SizeParam(w.Params, "size_max", 4*1024*1024)
		pattern := workload.StringParam(w.Params, "access_pattern", "shuffled")
		if pattern != "sequential" && pattern != "shuffled" && pattern != "zipfian" {
			return nil, fmt.Errorf("dataloader %q: access_pattern %q not supported", w.Name, pattern)
		}
		zs := workload.FloatParam(w.Params, "zipfian_s", 1.07)
		rangeProb := workload.FloatParam(w.Params, "range_probability", 0.0)
		return &Workload{
			name: w.Name, keyPrefix: w.Name + "/obj-",
			objectCount: count,
			sizeMean:    float64(sizeMean), sizeSigma: sigma,
			sizeMin: sizeMin, sizeMax: sizeMax,
			pattern: pattern, zipfianS: zs,
			rangeProb: rangeProb,
		}, nil
	})
}

// Workload is the dataloader implementation.
type Workload struct {
	name        string
	keyPrefix   string
	objectCount int
	sizeMean    float64
	sizeSigma   float64
	sizeMin     int64
	sizeMax     int64
	pattern     string
	zipfianS    float64
	rangeProb   float64

	keys []string
}

func (w *Workload) Name() string { return w.name }
func (w *Workload) Type() string { return TypeName }

func (w *Workload) keyFor(i int) string {
	return workload.AppendKeyPadded(nil, w.keyPrefix, int64(i), 8)
}

// Prepopulate uploads the entire dataset before measurement begins.
func (w *Workload) Prepopulate(ctx context.Context, env *workload.Env) error {
	sampler, err := sizedist.NewLognormal(w.sizeMean, w.sizeSigma, w.sizeMin, w.sizeMax, env.Rand)
	if err != nil {
		return err
	}
	keys := make([]string, 0, w.objectCount)
	for i := 0; i < w.objectCount; i++ {
		k := w.keyFor(i)
		size := sampler.Sample()
		body := bodygen.NewReader(size)
		err := env.S3.Put(ctx, k, body, size)
		bodygen.Release(body)
		if err != nil {
			return err
		}
		keys = append(keys, k)
	}
	w.keys = keys
	return nil
}

// Run drives high-concurrency GETs in the configured access pattern.
func (w *Workload) Run(ctx context.Context, env *workload.Env) error {
	if env.Threads <= 0 {
		return fmt.Errorf("dataloader %q: threads must be >0", w.name)
	}
	keys := w.keys
	if len(keys) == 0 {
		return fmt.Errorf("dataloader %q: dataset empty; prepopulate first", w.name)
	}
	var zipfTemplate *sizedist.Zipfian
	if w.pattern == "zipfian" {
		z, err := sizedist.NewZipfian(int64(len(keys)), w.zipfianS, env.Rand)
		if err != nil {
			return err
		}
		zipfTemplate = z
	}
	var wg sync.WaitGroup
	wg.Add(env.Threads)
	for i := 0; i < env.Threads; i++ {
		go func(id int) {
			defer wg.Done()
			local := workload.WorkerRand(env, id)
			rec := env.ShardRecorder(id)
			// Per-worker Zipfian — lock-free on the hot path.
			var zipf *sizedist.Zipfian
			if zipfTemplate != nil {
				zipf = zipfTemplate.ForWorker(id)
			}
			// Shuffled access is driven by splitmix64 on (workerID, counter),
			// which gives O(1)-space pseudo-random key selection. For 1M keys
			// and 512 workers this saves ~4 GiB versus local.Perm(len(keys)).
			var counter uint64
			seed := uint64(id)*0x9E3779B97F4A7C15 + 1
			for ctx.Err() == nil {
				counter++
				k := w.nextKey(id, env.Threads, seed, counter, zipf, keys)
				if local.Float64() < w.rangeProb {
					w.doRangeGet(ctx, env, rec, k)
				} else {
					w.doGet(ctx, env, rec, k)
				}
			}
		}(i)
	}
	wg.Wait()
	return nil
}

func (w *Workload) nextKey(workerID, threads int, seed, counter uint64, z *sizedist.Zipfian, keys []string) string {
	switch w.pattern {
	case "sequential":
		i := sequentialIndex(workerID, threads, counter, len(keys))
		return keys[i]
	case "zipfian":
		return keys[z.Sample()]
	default: // "shuffled"
		return keys[int(mix64(seed+counter)%uint64(len(keys)))]
	}
}

func sequentialIndex(workerID, threads int, counter uint64, total int) int {
	if total == 0 {
		return 0
	}
	if threads <= 0 {
		threads = 1
	}
	return (workerID + int(counter-1)*threads) % total
}

// mix64 is splitmix64 — a well-known 64-bit integer finaliser with excellent
// avalanche properties. Cheap enough to run in the GET hot path.
func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// doGet / doRangeGet take a recorder explicitly so per-worker shards bypass
// the collector-level mutex on the hot path.
func (w *Workload) doGet(ctx context.Context, env *workload.Env, rec metrics.Recorder, key string) {
	start := time.Now()
	rc, err := env.S3.Get(ctx, key)
	var n int64
	if err == nil {
		n, _ = drain.Drain(rc)
		rc.Close()
	}
	rec.Record(w.name, metrics.OpGet, time.Since(start), n, err)
}

func (w *Workload) doRangeGet(ctx context.Context, env *workload.Env, rec metrics.Recorder, key string) {
	start := time.Now()
	rc, err := env.S3.RangeGet(ctx, key, 0, 64*1024)
	var n int64
	if err == nil {
		n, _ = drain.Drain(rc)
		rc.Close()
	}
	rec.Record(w.name, metrics.OpRangeGet, time.Since(start), n, err)
}

// Cleanup deletes every dataset object.
func (w *Workload) Cleanup(ctx context.Context, env *workload.Env) error {
	for _, k := range w.keys {
		if err := env.S3.Delete(ctx, k); err != nil && !s3client.IsNotFound(err) {
			return err
		}
	}
	return nil
}
