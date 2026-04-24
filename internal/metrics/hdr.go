package metrics

import (
	hdr "github.com/HdrHistogram/hdrhistogram-go"
)

// Bounds for the hdrhistogram — 1 ns minimum, 1 hour maximum, 3-sig-figure
// precision. This covers every latency the tool will measure while keeping
// memory per histogram modest (~160 KiB).
const (
	hdrMin = 1
	hdrMax = int64(60 * 60 * 1_000_000_000) // 1 hour in ns
	hdrSig = 3
)

func newHDR() *hdr.Histogram { return hdr.New(hdrMin, hdrMax, hdrSig) }

// histBuckets produces a 16-bucket percentile-spaced histogram for the JSON
// report. Each bucket[i]'s upper bound is the value at the ((i+1)/16)-th
// quantile; counts[i] is the number of draws whose value falls in
// (bucket[i-1], bucket[i]]. The result is suitable for dashboards but is
// lossy compared to the full hdr histogram — which is available to
// downstream consumers that want to re-snapshot from the raw collector.
func histBuckets(h *hdr.Histogram) (bounds []int64, counts []int64) {
	const buckets = 16
	total := h.TotalCount()
	if total == 0 {
		return []int64{}, []int64{}
	}
	bounds = make([]int64, buckets)
	counts = make([]int64, buckets)
	// Percentile-evenly-spaced upper bounds — each bucket captures ~total/buckets
	// draws by construction.
	for i := 0; i < buckets; i++ {
		p := float64(i+1) * 100.0 / float64(buckets)
		bounds[i] = h.ValueAtQuantile(p)
	}
	// Count via CumulativeDistribution: walk cdf rows, assigning each row's
	// count delta to the appropriate bucket.
	cdf := h.CumulativeDistribution()
	prevCount := int64(0)
	bi := 0
	for _, row := range cdf {
		delta := row.Count - prevCount
		prevCount = row.Count
		// Advance bi until the row's ValueAt fits in bucket bi.
		for bi < buckets-1 && row.ValueAt > bounds[bi] {
			bi++
		}
		counts[bi] += delta
	}
	return bounds, counts
}
