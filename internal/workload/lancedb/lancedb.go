// Package lancedb simulates the access pattern of LanceDB-style columnar
// vector stores on S3 (PRD §5.2): small manifest writes, large data-fragment
// multipart writes, range reads against fragments with HEAD-before-GET, and
// LIST of the manifest prefix.
package lancedb

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	_ "unsafe" // keep imports stable

	"github.com/darrensoothill/s3aibench/internal/bodygen"
	"github.com/darrensoothill/s3aibench/internal/drain"
	"github.com/darrensoothill/s3aibench/internal/metrics"
	"github.com/darrensoothill/s3aibench/internal/plan"
	"github.com/darrensoothill/s3aibench/internal/s3client"
	"github.com/darrensoothill/s3aibench/internal/sizedist"
	"github.com/darrensoothill/s3aibench/internal/workload"
)

// TypeName is the YAML type: value.
const TypeName = "lancedb"

// Register wires the factory.
func Register() {
	workload.Register(TypeName, func(w plan.Workload) (workload.Workload, error) {
		fragments := workload.IntParam(w.Params, "fragments", 100)
		fragSize := workload.SizeParam(w.Params, "fragment_size", 128*1024*1024)
		manifests := workload.IntParam(w.Params, "manifest_count", 50)
		manifestMax := workload.SizeParam(w.Params, "manifest_max_size", 64*1024)
		manifestMin := workload.SizeParam(w.Params, "manifest_min_size", 1024)
		ratio := workload.FloatParam(w.Params, "read_ratio", 0.8)
		rangeMean := workload.SizeParam(w.Params, "range_mean", 64*1024)
		rangeSigma := workload.FloatParam(w.Params, "range_sigma", 1.5)
		rangeMin := workload.SizeParam(w.Params, "range_min", 4*1024)
		rangeMax := workload.SizeParam(w.Params, "range_max", 4*1024*1024)
		if fragments <= 0 || manifests <= 0 {
			return nil, fmt.Errorf("lancedb %q: fragments and manifest_count must be >0", w.Name)
		}
		if fragSize <= 0 {
			return nil, fmt.Errorf("lancedb %q: fragment_size must be >0", w.Name)
		}
		if manifestMin <= 0 || manifestMax < manifestMin {
			return nil, fmt.Errorf("lancedb %q: manifest_min_size must be >0 and manifest_max_size must be >= manifest_min_size", w.Name)
		}
		if rangeMin <= 0 || rangeMax < rangeMin {
			return nil, fmt.Errorf("lancedb %q: range_min must be >0 and range_max must be >= range_min", w.Name)
		}
		return &Workload{
			name:           w.Name,
			dataKeyPrefix:  w.Name + "/data/frag-",
			manifestPrefix: w.Name + "/_versions/ver-",
			fragments:      fragments,
			fragmentSize:   fragSize,
			manifests:      manifests,
			manifestMin:    manifestMin,
			manifestMax:    manifestMax,
			readRatio:      ratio,
			rangeMean:      float64(rangeMean),
			rangeSigma:     rangeSigma,
			rangeMin:       rangeMin,
			rangeMax:       rangeMax,
		}, nil
	})
}

// Workload is the lancedb implementation.
type Workload struct {
	name           string
	dataKeyPrefix  string
	manifestPrefix string
	fragments      int
	fragmentSize   int64
	manifests      int
	manifestMin    int64
	manifestMax    int64
	readRatio      float64
	rangeMean      float64
	rangeSigma     float64
	rangeMin       int64
	rangeMax       int64

	// fragmentCount and manifestCount are finalised at end of Prepopulate and
	// then only read. manifestsWritten atomically counts Run-time manifest
	// PUTs so Cleanup can delete them deterministically — no shared slice.
	fragmentCount    int
	manifestCount    int
	manifestsWritten atomic.Int64
}

func (w *Workload) Name() string { return w.name }
func (w *Workload) Type() string { return TypeName }

func (w *Workload) dataKey(i int) string {
	return workload.AppendKeyPadded(nil, w.dataKeyPrefix, int64(i), 6)
}

func (w *Workload) manifestKey(i int) string {
	return workload.AppendKeyPadded(nil, w.manifestPrefix, int64(i), 8) + ".manifest"
}

// Prepopulate uploads `fragments` data-fragment objects and `manifests`
// manifest objects so reads have a non-empty dataset.
func (w *Workload) Prepopulate(ctx context.Context, env *workload.Env) error {
	for i := 0; i < w.fragments; i++ {
		body := bodygen.NewReader(w.fragmentSize)
		err := env.S3.MultipartUpload(ctx, w.dataKey(i), body, w.fragmentSize)
		bodygen.Release(body)
		if err != nil {
			return err
		}
		w.fragmentCount = i + 1
	}
	for i := 0; i < w.manifests; i++ {
		size := w.manifestMin
		if w.manifestMax > w.manifestMin {
			size = w.manifestMin + env.Rand.Int63n(w.manifestMax-w.manifestMin+1)
		}
		body := bodygen.NewReader(size)
		err := env.S3.Put(ctx, w.manifestKey(i), body, size)
		bodygen.Release(body)
		if err != nil {
			return err
		}
		w.manifestCount = i + 1
	}
	return nil
}

// Run drives threads that randomly interleave reads (range GET preceded by
// HEAD, LIST of manifests) and writes (new manifest PUT) according to the
// configured read:write ratio.
func (w *Workload) Run(ctx context.Context, env *workload.Env) error {
	if env.Threads <= 0 {
		return fmt.Errorf("lancedb %q: threads must be >0", w.name)
	}
	template, err := sizedist.NewLognormal(w.rangeMean, w.rangeSigma, w.rangeMin, w.rangeMax, env.Rand)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	wg.Add(env.Threads)
	for i := 0; i < env.Threads; i++ {
		go func(id int) {
			defer wg.Done()
			local := workload.WorkerRand(env, id)
			rec := env.ShardRecorder(id)
			// Per-worker sampler — lock-free Sample() on the hot path.
			rangeSampler := template.ForWorker(id)
			for ctx.Err() == nil {
				switch {
				case local.Float64() < w.readRatio:
					w.doRead(ctx, env, rec, local, rangeSampler)
				default:
					w.doWrite(ctx, env, rec, local)
				}
			}
		}(i)
	}
	wg.Wait()
	return nil
}

func (w *Workload) doRead(ctx context.Context, env *workload.Env, rec metrics.Recorder, r *rand.Rand, sampler sizedist.Sampler) {
	// Randomly choose one of three read shapes: HEAD→range, LIST manifest prefix
	// with delimiter, LIST without delimiter.
	kind := r.Intn(3)
	switch kind {
	case 0:
		w.headThenRange(ctx, env, rec, r, sampler)
	case 1:
		w.listManifest(ctx, env, rec, "/")
	default:
		w.listManifest(ctx, env, rec, "")
	}
}

func (w *Workload) headThenRange(ctx context.Context, env *workload.Env, rec metrics.Recorder, r *rand.Rand, sampler sizedist.Sampler) {
	if w.fragmentCount == 0 {
		return
	}
	k := w.dataKey(r.Intn(w.fragmentCount))
	start := time.Now()
	size, err := env.S3.Head(ctx, k)
	rec.Record(w.name, metrics.OpHead, time.Since(start), 0, err)
	if err != nil {
		return
	}
	rs := sampler.Sample()
	if rs > size {
		rs = size
	}
	var offset int64
	if size-rs > 0 {
		offset = r.Int63n(size - rs + 1)
	}
	start = time.Now()
	rc, err := env.S3.RangeGet(ctx, k, offset, rs)
	var n int64
	if err == nil {
		n, _ = drain.Drain(rc)
		rc.Close()
	}
	rec.Record(w.name, metrics.OpRangeGet, time.Since(start), n, err)
}

func (w *Workload) listManifest(ctx context.Context, env *workload.Env, rec metrics.Recorder, delim string) {
	start := time.Now()
	res, err := env.S3.List(ctx, w.name+"/_versions/", delim, "", 100)
	_ = res
	rec.Record(w.name, metrics.OpList, time.Since(start), 0, err)
}

func (w *Workload) doWrite(ctx context.Context, env *workload.Env, rec metrics.Recorder, r *rand.Rand) {
	n := w.manifestsWritten.Add(1) - 1
	k := w.manifestKey(int(int64(w.manifestCount) + n))
	size := w.manifestMin
	if w.manifestMax > w.manifestMin {
		size = w.manifestMin + r.Int63n(w.manifestMax-w.manifestMin+1)
	}
	body := bodygen.NewReader(size)
	start := time.Now()
	err := env.S3.Put(ctx, k, body, size)
	bodygen.Release(body)
	rec.Record(w.name, metrics.OpManifestPut, time.Since(start), size, err)
}

// Cleanup deletes every fragment and manifest produced by this workload —
// derivable from the counters without any shared slice.
func (w *Workload) Cleanup(ctx context.Context, env *workload.Env) error {
	for i := 0; i < w.fragmentCount; i++ {
		if err := env.S3.Delete(ctx, w.dataKey(i)); err != nil && !s3client.IsNotFound(err) {
			return err
		}
	}
	total := w.manifestCount + int(w.manifestsWritten.Load())
	for i := 0; i < total; i++ {
		if err := env.S3.Delete(ctx, w.manifestKey(i)); err != nil && !s3client.IsNotFound(err) {
			return err
		}
	}
	return nil
}
