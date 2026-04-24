package smallobj

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/darrensoothill/s3aibench/internal/workload"
)

var (
	benchFloat float64
	benchInt   int64
)

// BenchmarkKeyFor measures the current strconv-based key builder. One alloc
// per call (the returned string); no fmt state-machine overhead.
func BenchmarkKeyFor(b *testing.B) {
	b.ReportAllocs()
	w := &Workload{name: "w", keyPrefix: "w/k-"}
	for i := 0; i < b.N; i++ {
		_ = w.keyFor(int64(i))
	}
}

// BenchmarkKeyForFmt is the pre-optimisation baseline, kept for comparison.
func BenchmarkKeyForFmt(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = fmt.Sprintf("%s/k-%010d", "w", int64(i))
	}
}

// BenchmarkStrconvPrefixConcat isolates the prefix + strconv technique with
// no method dispatch, to bound the intrinsic cost.
func BenchmarkStrconvPrefixConcat(b *testing.B) {
	b.ReportAllocs()
	pre := "w/k-"
	for i := 0; i < b.N; i++ {
		_ = pre + strconv.FormatInt(int64(i), 10)
	}
}

// BenchmarkWorkerRandParallel measures the per-worker RNG path used in the
// hot loop after removing the shared mutex-protected RNG.
func BenchmarkWorkerRandParallel(b *testing.B) {
	b.ReportAllocs()
	var workerID atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		id := int(workerID.Add(1) - 1)
		r := workload.WorkerRand(&workload.Env{Seed: 7}, id)
		for pb.Next() {
			benchFloat = r.Float64()
			benchInt = r.Int63n(1024)
		}
	})
}
