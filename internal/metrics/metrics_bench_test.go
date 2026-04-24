package metrics

import (
	"testing"
	"time"
)

// BenchmarkShardedRecord exercises the uncontended per-shard Record path —
// representative of high-concurrency workloads that call ShardFor(id) at
// worker spin-up. Target: single-digit ns/op, few allocs after steady state.
func BenchmarkShardedRecord(b *testing.B) {
	b.ReportAllocs()
	c := NewCollector()
	r := c.ShardFor(0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Record("w", OpPut, 5*time.Microsecond, 1024, nil)
	}
}

// BenchmarkFallbackRecord measures the shared-mutex path — for comparison
// with the sharded fast path. Expect notably higher ns/op under -cpu > 1.
func BenchmarkFallbackRecord(b *testing.B) {
	b.ReportAllocs()
	c := NewCollector()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Record("w", OpPut, 5*time.Microsecond, 1024, nil)
	}
}

// BenchmarkShardedRecordParallel simulates N worker goroutines each hammering
// their own shard. Demonstrates the contention win: with sharding, ns/op
// scales with CPU count; with the single-mutex fallback, it does not.
func BenchmarkShardedRecordParallel(b *testing.B) {
	b.ReportAllocs()
	c := NewCollector()
	var workerID int64
	b.RunParallel(func(pb *testing.PB) {
		id := int(workerID)
		workerID++
		r := c.ShardFor(id)
		for pb.Next() {
			r.Record("w", OpPut, 5*time.Microsecond, 1024, nil)
		}
	})
}

// BenchmarkFallbackRecordParallel is the same pattern through the shared
// fallback mutex — the contention baseline.
func BenchmarkFallbackRecordParallel(b *testing.B) {
	b.ReportAllocs()
	c := NewCollector()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Record("w", OpPut, 5*time.Microsecond, 1024, nil)
		}
	})
}

// BenchmarkTotals measures the lightweight progress snapshot path.
func BenchmarkTotals(b *testing.B) {
	c := NewCollector()
	r := c.ShardFor(0)
	for i := 0; i < 1024; i++ {
		r.Record("w", OpPut, 5*time.Microsecond, 1024, nil)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.Totals()
	}
}
