package metrics

import (
	"errors"
	"testing"
	"time"

	"github.com/aws/smithy-go"
)

type apiErr struct{ code, msg string }

func (a *apiErr) Error() string                 { return a.msg }
func (a *apiErr) ErrorCode() string             { return a.code }
func (a *apiErr) ErrorMessage() string          { return a.msg }
func (a *apiErr) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

func TestRecordAndSnapshot(t *testing.T) {
	c := NewCollector()
	c.Record("w", OpPut, 10*time.Millisecond, 1024, nil)
	c.Record("w", OpPut, 20*time.Millisecond, 2048, nil)
	c.Record("w", OpPut, 30*time.Millisecond, 4096, &apiErr{"SlowDown", "slow"})
	c.Record("w", OpGet, 5*time.Millisecond, 512, errors.New("boom"))

	s := c.Snapshot()
	put := s.Workloads["w"][OpPut]
	if put.Count != 3 || put.Errors != 1 || put.Bytes != 1024+2048+4096 {
		t.Fatalf("bad put stats: %+v", put)
	}
	if put.LatMin != 10*time.Millisecond || put.LatMax != 30*time.Millisecond {
		t.Fatalf("bad min/max: %v/%v", put.LatMin, put.LatMax)
	}
	if put.Mean() != 20*time.Millisecond {
		t.Fatalf("mean=%v", put.Mean())
	}
	// HDR quantizes values to 3 significant figures, so allow up to 1% drift.
	approx := func(got, want time.Duration, frac float64) bool {
		d := got - want
		if d < 0 {
			d = -d
		}
		return float64(d)/float64(want) <= frac
	}
	if !approx(put.Percentile(0.5), 20*time.Millisecond, 0.01) {
		t.Fatalf("p50=%v", put.Percentile(0.5))
	}
	// Clamp-branch coverage: p<0 and p>1 should not panic and should return a
	// sensible value (HDR quantile(0) can be 0, quantile(1) must be max).
	_ = put.Percentile(-1)
	if !approx(put.Percentile(2), 30*time.Millisecond, 0.01) {
		t.Fatalf("p>1 clamp wrong: %v", put.Percentile(2))
	}
	if !approx(put.Percentile(0.99), 30*time.Millisecond, 0.01) {
		t.Fatalf("p99=%v", put.Percentile(0.99))
	}
	if put.ErrByCode["SlowDown"].Count != 1 {
		t.Fatalf("missing SlowDown")
	}
	if s.Workloads["w"][OpGet].ErrByCode["unknown"].Count != 1 {
		t.Fatalf("missing unknown")
	}
	if put.StdDev() == 0 {
		t.Fatal("expected nonzero stddev")
	}
	// Two records with same latency → stddev 0 only in <2 sample case.
	c2 := NewCollector()
	c2.Record("w", OpPut, 10*time.Millisecond, 0, nil)
	s2 := c2.Snapshot()
	if s2.Workloads["w"][OpPut].StdDev() != 0 {
		t.Fatal("expected 0 stddev with 1 sample")
	}
}

func TestEmptyStats(t *testing.T) {
	var s OpStats
	// Without a histogram set, Percentile/StdDev must return 0.
	if s.Mean() != 0 || s.StdDev() != 0 || s.Percentile(0.5) != 0 {
		t.Fatal("empty stats should return 0")
	}
	// Histogram on a nil hist returns empty slices.
	b, c := s.Histogram()
	if len(b) != 0 || len(c) != 0 {
		t.Fatal("expected empty histogram")
	}
}

func TestSnapshotEmptyMinResets(t *testing.T) {
	c := NewCollector()
	// Insert an empty OpStats into the fallback shard with its initial sentinel
	// LatMin. Snapshot should reset LatMin to 0 for reporting.
	c.fallback.workloads["w"] = map[Op]*OpStats{
		OpPut: {LatMin: time.Duration(hdrMax), ErrByCode: map[string]*ErrAgg{}, hist: newHDR()},
	}
	s := c.Snapshot()
	if s.Workloads["w"][OpPut].LatMin != 0 {
		t.Fatalf("expected LatMin reset, got %v", s.Workloads["w"][OpPut].LatMin)
	}
}

func TestRecordClamp(t *testing.T) {
	c := NewCollector()
	// Negative duration → clamped to hdrMin.
	c.Record("w", OpPut, -1, 0, nil)
	// Duration beyond hdrMax (> 1 hour) → clamped to hdrMax.
	c.Record("w", OpPut, time.Duration(hdrMax)+time.Hour, 0, nil)
	s := c.Snapshot().Workloads["w"][OpPut]
	if s.Count != 2 {
		t.Fatal("clamp path missed")
	}
}

func TestHistBucketsEmpty(t *testing.T) {
	b, c := histBuckets(newHDR())
	if len(b) != 0 || len(c) != 0 {
		t.Fatal("empty histogram should yield empty slices")
	}
}

func TestShardForRoutesAndMerges(t *testing.T) {
	c := NewCollector()
	// Record through a shard recorder — fast path.
	r1 := c.ShardFor(1)
	r1.Record("w", OpPut, 3*time.Millisecond, 64, nil)
	r1.Record("w", OpPut, 5*time.Millisecond, 128, nil)
	// Record through a separate shard — must merge in Snapshot.
	r2 := c.ShardFor(2)
	r2.Record("w", OpPut, 7*time.Millisecond, 32, nil)
	// Also via the fallback path.
	c.Record("w", OpPut, 9*time.Millisecond, 256, nil)

	snap := r1.Snapshot()
	st := snap.Workloads["w"][OpPut]
	if st.Count != 4 {
		t.Fatalf("count=%d want 4", st.Count)
	}
	if st.Bytes != 64+128+32+256 {
		t.Fatalf("bytes=%d", st.Bytes)
	}
	// Fetching the same shard twice must return a recorder for the same state.
	r1b := c.ShardFor(1)
	r1b.Record("w", OpGet, time.Millisecond, 0, nil)
	snap2 := r1b.Snapshot()
	if snap2.Workloads["w"][OpGet].Count != 1 {
		t.Fatal("shard cache lookup failed")
	}
	// shardRecorder.ShardFor delegates back to the parent.
	r3 := r1.ShardFor(99)
	r3.Record("w", OpPut, time.Millisecond, 1, nil)
	if snap3 := c.Snapshot(); snap3.Workloads["w"][OpPut].Count != 5 {
		t.Fatal("nested ShardFor didn't merge")
	}
}

func TestMergeErrorAggregation(t *testing.T) {
	c := NewCollector()
	r1 := c.ShardFor(1)
	r1.Record("w", OpPut, time.Millisecond, 0, errors.New("down"))
	r2 := c.ShardFor(2)
	r2.Record("w", OpPut, time.Millisecond, 0, errors.New("down"))
	snap := c.Snapshot()
	st := snap.Workloads["w"][OpPut]
	if st.ErrByCode["unknown"].Count != 2 {
		t.Fatalf("error merge failed: %+v", st.ErrByCode)
	}
}

func TestTotals(t *testing.T) {
	c := NewCollector()
	c.Record("w", OpPut, time.Millisecond, 64, nil)
	r := c.ShardFor(1)
	r.Record("w", OpGet, 2*time.Millisecond, 32, errors.New("boom"))

	totals := c.Totals()
	if totals.Count != 2 || totals.Errors != 1 || totals.Bytes != 96 {
		t.Fatalf("bad totals: %+v", totals)
	}
	if totals.End.Before(totals.Start) {
		t.Fatalf("end before start: %+v", totals)
	}
	// shardRecorder.Totals delegates back to the parent collector.
	if shardTotals := r.Totals(); shardTotals.Count != totals.Count || shardTotals.Errors != totals.Errors || shardTotals.Bytes != totals.Bytes {
		t.Fatalf("delegated totals mismatch: got %+v want %+v", shardTotals, totals)
	}
}

func TestHistogramBucketsPopulated(t *testing.T) {
	c := NewCollector()
	// Record a spread of values so CDF has multiple rows.
	for i := 0; i < 100; i++ {
		c.Record("w", OpPut, time.Duration(i+1)*time.Microsecond, 0, nil)
	}
	snap := c.Snapshot()
	st := snap.Workloads["w"][OpPut]
	b, cnts := st.Histogram()
	if len(b) != 16 || len(cnts) != 16 {
		t.Fatalf("got %d buckets %d counts", len(b), len(cnts))
	}
	var total int64
	for _, x := range cnts {
		total += x
	}
	if total != 100 {
		t.Fatalf("bucket counts sum=%d, want 100", total)
	}
}

func TestClassify(t *testing.T) {
	if classify(nil) != "" {
		t.Fatal("nil err should be empty")
	}
	if classify(errors.New("x")) != "unknown" {
		t.Fatal("plain err should classify as unknown")
	}
	if classify(&apiErr{"NoSuchKey", "m"}) != "NoSuchKey" {
		t.Fatal("apiErr code lost")
	}
}
