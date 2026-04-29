// Package metrics records per-operation latency, byte, and error counts.
// The underlying latency store is an hdrhistogram-go histogram per (workload,
// op) — sized for 1 ns – 1 hour with 3-sig-figure precision. The Collector
// shards state per worker so the hot Record path is uncontended at high
// concurrency; all shards are merged at Snapshot time.
package metrics

import (
	"sync"
	"time"

	hdr "github.com/HdrHistogram/hdrhistogram-go"
)

// Op identifies a single S3 operation class.
type Op string

const (
	OpPut      Op = "put"
	OpGet      Op = "get"
	OpHead     Op = "head"
	OpList     Op = "list"
	OpDelete   Op = "delete"
	OpCopy     Op = "copy"
	OpRangeGet Op = "range_get"

	OpMultipartInit     Op = "multipart_init"
	OpMultipartPart     Op = "multipart_part"
	OpMultipartComplete Op = "multipart_complete"
	OpManifestPut       Op = "manifest_put"
	OpCheckpointPut     Op = "checkpoint_put"
	OpGetTagging        Op = "get_tagging"
	OpPutTagging        Op = "put_tagging"
)

// Recorder is the minimal interface workloads use to report operations.
// Call ShardFor(workerID) from each worker goroutine to get an uncontended
// recorder backed by a private shard.
type Recorder interface {
	Record(workload string, op Op, latency time.Duration, bytes int64, err error)
	Snapshot() Snapshot
	Totals() Totals
	ShardFor(workerID int) Recorder
}

// Snapshot is the frozen set of per-workload, per-op aggregates for reporting.
type Snapshot struct {
	Start     time.Time
	End       time.Time
	Workloads map[string]map[Op]*OpStats
}

// Totals is a lightweight aggregate for live progress reporting.
type Totals struct {
	Start  time.Time
	End    time.Time
	Count  int64
	Errors int64
	Bytes  int64
}

// OpStats aggregates per-op statistics. Fields remain public so the report
// package can format them without extra accessors.
type OpStats struct {
	Count     int64
	Errors    int64
	Bytes     int64
	LatSum    time.Duration
	LatMax    time.Duration
	LatMin    time.Duration
	ErrByCode map[string]*ErrAgg
	Timeline  map[int64]TimelineBucket

	hist *hdr.Histogram // unexported: consumed via Percentile/StdDev/Histogram
}

// TimelineBucket aggregates one second of activity for optional reports.
type TimelineBucket struct {
	Second int64
	Ops    int64
	Bytes  int64
	Errors int64
}

// ErrAgg aggregates errors by code.
type ErrAgg struct {
	Count         int64
	SampleMessage string
}

// shard holds the state for a single worker (or the fallback bucket when
// ShardFor is never called). Its mutex is uncontended when one worker owns it.
type shard struct {
	mu        sync.Mutex
	workloads map[string]map[Op]*OpStats
	start     time.Time
}

func newShard(start time.Time) *shard {
	return &shard{workloads: map[string]map[Op]*OpStats{}, start: start}
}

func (s *shard) record(workload string, op Op, latency time.Duration, bytes int64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	wl := s.workloads[workload]
	if wl == nil {
		wl = map[Op]*OpStats{}
		s.workloads[workload] = wl
	}
	st := wl[op]
	if st == nil {
		st = &OpStats{
			LatMin:    time.Duration(hdrMax),
			ErrByCode: map[string]*ErrAgg{},
			Timeline:  map[int64]TimelineBucket{},
			hist:      newHDR(),
		}
		wl[op] = st
	}
	st.Count++
	st.Bytes += bytes
	st.LatSum += latency
	if latency > st.LatMax {
		st.LatMax = latency
	}
	if latency < st.LatMin {
		st.LatMin = latency
	}
	v := latency.Nanoseconds()
	if v < hdrMin {
		v = hdrMin
	}
	if v > hdrMax {
		v = hdrMax
	}
	_ = st.hist.RecordValue(v)
	if err != nil {
		st.Errors++
		code := classify(err)
		agg := st.ErrByCode[code]
		if agg == nil {
			agg = &ErrAgg{SampleMessage: err.Error()}
			st.ErrByCode[code] = agg
		}
		agg.Count++
	}
	sec := int64(0)
	if !s.start.IsZero() {
		sec = int64(time.Since(s.start) / time.Second)
	}
	b := st.Timeline[sec]
	b.Second = sec
	b.Ops++
	b.Bytes += bytes
	if err != nil {
		b.Errors++
	}
	st.Timeline[sec] = b
}

// Collector is the default Recorder implementation. Record() goes through a
// shared fallback shard; high-throughput workloads should call ShardFor(id)
// to get a lock-unshared recorder.
type Collector struct {
	mu       sync.Mutex // guards shards map
	shards   map[int]*shard
	fallback *shard
	start    time.Time
}

// NewCollector returns a thread-safe Recorder backed by HDR histograms.
func NewCollector() *Collector {
	start := time.Now()
	return &Collector{
		shards:   map[int]*shard{},
		fallback: newShard(start),
		start:    start,
	}
}

// Record routes to the fallback shard — slow path for callers that don't
// shard themselves.
func (c *Collector) Record(workload string, op Op, latency time.Duration, bytes int64, err error) {
	c.fallback.record(workload, op, latency, bytes, err)
}

// ShardFor returns a Recorder bound to a per-worker shard. The returned
// recorder is safe to call only from the goroutine that requested it; the
// shard mutex is uncontended as long as exactly one goroutine uses each ID.
func (c *Collector) ShardFor(workerID int) Recorder {
	c.mu.Lock()
	s, ok := c.shards[workerID]
	if !ok {
		s = newShard(c.start)
		c.shards[workerID] = s
	}
	c.mu.Unlock()
	return &shardRecorder{s: s, parent: c}
}

// shardRecorder is a Recorder bound to a single shard. Its Record path skips
// the collector-level mutex entirely; only the shard's own mutex is taken.
type shardRecorder struct {
	s      *shard
	parent *Collector
}

func (r *shardRecorder) Record(wl string, op Op, lat time.Duration, bytes int64, err error) {
	r.s.record(wl, op, lat, bytes, err)
}

func (r *shardRecorder) Snapshot() Snapshot { return r.parent.Snapshot() }
func (r *shardRecorder) Totals() Totals     { return r.parent.Totals() }

// ShardFor on a shardRecorder delegates to the parent so nested shard
// assignments still land on the collector's canonical shards.
func (r *shardRecorder) ShardFor(workerID int) Recorder { return r.parent.ShardFor(workerID) }

// Snapshot merges every shard into a single aggregated Snapshot.
func (c *Collector) Snapshot() Snapshot {
	out := Snapshot{Start: c.start, End: time.Now(), Workloads: map[string]map[Op]*OpStats{}}
	c.mu.Lock()
	all := make([]*shard, 0, len(c.shards)+1)
	all = append(all, c.fallback)
	for _, s := range c.shards {
		all = append(all, s)
	}
	c.mu.Unlock()
	for _, s := range all {
		s.mu.Lock()
		for wl, ops := range s.workloads {
			dst := out.Workloads[wl]
			if dst == nil {
				dst = map[Op]*OpStats{}
				out.Workloads[wl] = dst
			}
			for op, st := range ops {
				merged := dst[op]
				if merged == nil {
					merged = &OpStats{
						LatMin:    time.Duration(hdrMax),
						ErrByCode: map[string]*ErrAgg{},
						Timeline:  map[int64]TimelineBucket{},
						hist:      newHDR(),
					}
					dst[op] = merged
				}
				mergeStats(merged, st)
			}
		}
		s.mu.Unlock()
	}
	// Normalise LatMin sentinel so empty histograms report 0, not hdrMax.
	for _, ops := range out.Workloads {
		for _, st := range ops {
			if st.LatMin == time.Duration(hdrMax) {
				st.LatMin = 0
			}
		}
	}
	return out
}

// Totals merges only the count/error/byte counters across shards.
func (c *Collector) Totals() Totals {
	out := Totals{Start: c.start, End: time.Now()}
	c.mu.Lock()
	all := make([]*shard, 0, len(c.shards)+1)
	all = append(all, c.fallback)
	for _, s := range c.shards {
		all = append(all, s)
	}
	c.mu.Unlock()
	for _, s := range all {
		s.mu.Lock()
		for _, ops := range s.workloads {
			for _, st := range ops {
				out.Count += st.Count
				out.Errors += st.Errors
				out.Bytes += st.Bytes
			}
		}
		s.mu.Unlock()
	}
	return out
}

// mergeStats merges src into dst (dst is a fresh OpStats owned by Snapshot).
func mergeStats(dst, src *OpStats) {
	dst.Count += src.Count
	dst.Errors += src.Errors
	dst.Bytes += src.Bytes
	dst.LatSum += src.LatSum
	if src.LatMax > dst.LatMax {
		dst.LatMax = src.LatMax
	}
	if src.LatMin < dst.LatMin {
		dst.LatMin = src.LatMin
	}
	if src.hist != nil {
		dst.hist.Merge(src.hist)
	}
	if dst.Timeline == nil {
		dst.Timeline = map[int64]TimelineBucket{}
	}
	for sec, bucket := range src.Timeline {
		merged := dst.Timeline[sec]
		merged.Second = sec
		merged.Ops += bucket.Ops
		merged.Bytes += bucket.Bytes
		merged.Errors += bucket.Errors
		dst.Timeline[sec] = merged
	}
	for code, agg := range src.ErrByCode {
		m := dst.ErrByCode[code]
		if m == nil {
			m = &ErrAgg{SampleMessage: agg.SampleMessage}
			dst.ErrByCode[code] = m
		}
		m.Count += agg.Count
	}
}

// Percentile returns the requested quantile from the recorded latencies.
// Input is clamped to [0, 1].
func (s *OpStats) Percentile(p float64) time.Duration {
	if s.hist == nil || s.hist.TotalCount() == 0 {
		return 0
	}
	if p < 0 {
		p = 0
	}
	if p > 1 {
		p = 1
	}
	return time.Duration(s.hist.ValueAtQuantile(p * 100))
}

// Mean returns the arithmetic mean of recorded latencies.
func (s *OpStats) Mean() time.Duration {
	if s.Count == 0 {
		return 0
	}
	return s.LatSum / time.Duration(s.Count)
}

// StdDev returns the standard deviation of recorded latencies.
func (s *OpStats) StdDev() time.Duration {
	if s.hist == nil || s.hist.TotalCount() < 2 {
		return 0
	}
	return time.Duration(s.hist.StdDev())
}

// Histogram returns (bucket upper-bound latencies, counts) for the JSON report.
func (s *OpStats) Histogram() (bounds, counts []int64) {
	if s.hist == nil {
		return []int64{}, []int64{}
	}
	return histBuckets(s.hist)
}
