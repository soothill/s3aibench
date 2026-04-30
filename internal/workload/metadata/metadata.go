// Package metadata stresses S3 metadata and listing paths (PRD §5.4):
// HEAD, LIST (with and without prefix/delimiter, paginated), COPY,
// GetObjectTagging, PutObjectTagging. Prefix fanout and depth are
// configurable to model bucket layouts.
package metadata

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/darrensoothill/s3aibench/internal/bodygen"
	"github.com/darrensoothill/s3aibench/internal/metrics"
	"github.com/darrensoothill/s3aibench/internal/plan"
	"github.com/darrensoothill/s3aibench/internal/s3client"
	"github.com/darrensoothill/s3aibench/internal/workload"
)

// TypeName is the YAML type: value.
const TypeName = "metadata"

// Register wires the factory.
func Register() {
	workload.Register(TypeName, func(w plan.Workload) (workload.Workload, error) {
		fanout := workload.IntParam(w.Params, "prefix_fanout", 4)
		depth := workload.IntParam(w.Params, "prefix_depth", 2)
		perPrefix := workload.IntParam(w.Params, "objects_per_prefix", 25)
		paginationDepth := workload.IntParam(w.Params, "pagination_depth", 0)
		listMaxKeys := workload.IntParam(w.Params, "list_max_keys", 100)
		objectSize := int64(w.ObjectSize)
		if objectSize <= 0 {
			objectSize = 1024 // stub objects
		}
		if fanout <= 0 || depth <= 0 || perPrefix <= 0 {
			return nil, fmt.Errorf("metadata %q: fanout/depth/objects_per_prefix must be >0", w.Name)
		}
		if paginationDepth < 0 || listMaxKeys <= 0 {
			return nil, fmt.Errorf("metadata %q: pagination_depth must be >=0 and list_max_keys must be >0", w.Name)
		}
		return &Workload{
			name: w.Name, copyPrefix: w.Name + "/copy/dst-",
			fanout: fanout, depth: depth,
			perPrefix: perPrefix, objectSize: objectSize,
			paginationDepth: paginationDepth, listMaxKeys: int32(listMaxKeys),
		}, nil
	})
}

// Workload is the metadata implementation. Lock-free at steady state: the
// base key set is fixed at Prepopulate time, and COPY destinations are
// derived from an atomic counter.
type Workload struct {
	name            string
	copyPrefix      string
	fanout          int
	depth           int
	perPrefix       int
	objectSize      int64
	paginationDepth int
	listMaxKeys     int32

	baseKeys  []string     // finalised in Prepopulate, read-only after
	copyCount atomic.Int64 // number of COPY destinations claimed
}

func (w *Workload) Name() string { return w.name }
func (w *Workload) Type() string { return TypeName }

// Prepopulate builds a nested prefix layout so LIST pagination has something
// to walk. Total objects = fanout^depth * perPrefix (kept small on purpose for
// a metadata-focused workload).
func (w *Workload) Prepopulate(ctx context.Context, env *workload.Env) error {
	prefixes := buildPrefixes(w.name, w.fanout, w.depth)
	for _, p := range prefixes {
		for i := 0; i < w.perPrefix; i++ {
			k := fmt.Sprintf("%s/obj-%04d", p, i)
			body := bodygen.NewReader(w.objectSize)
			err := env.S3.Put(ctx, k, body, w.objectSize)
			bodygen.Release(body)
			if err != nil {
				return err
			}
			w.baseKeys = append(w.baseKeys, k)
		}
	}
	return nil
}

func buildPrefixes(base string, fanout, depth int) []string {
	if depth <= 0 {
		return []string{base}
	}
	var out []string
	for i := 0; i < fanout; i++ {
		child := fmt.Sprintf("%s/p%02d", base, i)
		out = append(out, buildPrefixes(child, fanout, depth-1)...)
	}
	return out
}

// Run picks a random verb each iteration and executes it against a random key
// (or prefix). Each verb contributes independent per-op latencies. The base
// key slice is immutable after Prepopulate, so worker reads are lock-free.
func (w *Workload) Run(ctx context.Context, env *workload.Env) error {
	if env.Threads <= 0 {
		return fmt.Errorf("metadata %q: threads must be >0", w.name)
	}
	if len(w.baseKeys) == 0 {
		return fmt.Errorf("metadata %q: dataset empty", w.name)
	}
	var wg sync.WaitGroup
	wg.Add(env.Threads)
	for i := 0; i < env.Threads; i++ {
		go func(id int) {
			defer wg.Done()
			local := workload.WorkerRand(env, id)
			rec := env.ShardRecorder(id)
			for ctx.Err() == nil {
				w.oneOp(ctx, env, rec, local, w.baseKeys)
			}
		}(i)
	}
	wg.Wait()
	return nil
}

func (w *Workload) oneOp(ctx context.Context, env *workload.Env, rec metrics.Recorder, r *rand.Rand, keys []string) {
	k := keys[r.Intn(len(keys))]
	switch r.Intn(6) {
	case 0:
		w.head(ctx, env, rec, k)
	case 1:
		w.listPrefix(ctx, env, rec, "")
	case 2:
		w.listPrefix(ctx, env, rec, "/")
	case 3:
		w.copy(ctx, env, rec, k)
	case 4:
		w.putTags(ctx, env, rec, k)
	default:
		w.getTags(ctx, env, rec, k)
	}
}

func (w *Workload) head(ctx context.Context, env *workload.Env, rec metrics.Recorder, k string) {
	start := time.Now()
	_, err := env.S3.Head(ctx, k)
	rec.Record(w.name, metrics.OpHead, time.Since(start), 0, err)
}

// listPrefix iterates LIST pagination, summing total records seen across
// continuation tokens so LIST rate counts objects, not just requests.
func (w *Workload) listPrefix(ctx context.Context, env *workload.Env, rec metrics.Recorder, delim string) {
	token := ""
	pages := 0
	maxKeys := w.listMaxKeys
	if maxKeys <= 0 {
		maxKeys = 100
	}
	for {
		start := time.Now()
		res, err := env.S3.List(ctx, w.name+"/", delim, token, maxKeys)
		var n int64
		if err == nil {
			n = int64(len(res.Keys) + len(res.CommonPrefixes))
		}
		rec.Record(w.name, metrics.OpList, time.Since(start), n, err)
		if err != nil || res == nil || !res.IsTruncated {
			return
		}
		pages++
		if w.paginationDepth > 0 && pages >= w.paginationDepth {
			return
		}
		token = res.NextContinuation
	}
}

func (w *Workload) copy(ctx context.Context, env *workload.Env, rec metrics.Recorder, src string) {
	n := w.copyCount.Add(1) - 1
	dst := w.copyKeyFor(n)
	start := time.Now()
	err := env.S3.Copy(ctx, src, dst)
	rec.Record(w.name, metrics.OpCopy, time.Since(start), 0, err)
}

func (w *Workload) copyKeyFor(n int64) string {
	return workload.AppendKeyPadded(nil, w.copyPrefix, n, 12)
}

func (w *Workload) putTags(ctx context.Context, env *workload.Env, rec metrics.Recorder, k string) {
	start := time.Now()
	err := env.S3.PutObjectTagging(ctx, k, map[string]string{"bench": "1"})
	rec.Record(w.name, metrics.OpPutTagging, time.Since(start), 0, err)
}

func (w *Workload) getTags(ctx context.Context, env *workload.Env, rec metrics.Recorder, k string) {
	start := time.Now()
	_, err := env.S3.GetObjectTagging(ctx, k)
	rec.Record(w.name, metrics.OpGetTagging, time.Since(start), 0, err)
}

// Cleanup removes every base key and every COPY destination produced during
// Run. COPY destinations are derivable from copyCount, so no shared slice.
func (w *Workload) Cleanup(ctx context.Context, env *workload.Env) error {
	for _, k := range w.baseKeys {
		if err := env.S3.Delete(ctx, k); err != nil && !s3client.IsNotFound(err) {
			return err
		}
	}
	n := w.copyCount.Load()
	for i := int64(0); i < n; i++ {
		if err := env.S3.Delete(ctx, w.copyKeyFor(i)); err != nil && !s3client.IsNotFound(err) {
			return err
		}
	}
	return nil
}
