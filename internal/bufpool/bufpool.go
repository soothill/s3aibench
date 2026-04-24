// Package bufpool provides byte-buffer pools keyed by size bucket, so PUT and
// GET hot paths avoid heap allocation on every part. M1 workloads allocated
// bodies directly; M3 swaps them onto this pool as part of the zero-alloc
// audit.
package bufpool

import "sync"

// Buckets are powers of two across the PRD's common part sizes.
var sizes = []int{
	64 * 1024,
	1 * 1024 * 1024,
	16 * 1024 * 1024,
	256 * 1024 * 1024,
}

// Pool returns a pooled buffer of at least `n` bytes. Buffers are returned
// via Put.
type Pool struct {
	pools []*sync.Pool
}

// New returns a Pool sized for the default bucket ladder.
func New() *Pool {
	p := &Pool{pools: make([]*sync.Pool, len(sizes))}
	for i, sz := range sizes {
		sz := sz
		p.pools[i] = &sync.Pool{New: func() interface{} {
			b := make([]byte, sz)
			return &b
		}}
	}
	return p
}

// Get returns a buffer whose length is at least `n` bytes. The returned slice
// is trimmed to exactly `n` bytes; the backing array is preserved for Put.
func (p *Pool) Get(n int) []byte {
	idx := p.bucketOf(n)
	if idx == -1 {
		// Larger than the biggest bucket → fall back to a fresh allocation.
		return make([]byte, n)
	}
	b := p.pools[idx].Get().(*[]byte)
	return (*b)[:n]
}

// Put returns a buffer to the pool. Buffers that don't match any bucket are
// dropped so the caller can Get a fresh one next time.
func (p *Pool) Put(b []byte) {
	for i, sz := range sizes {
		if cap(b) == sz {
			full := b[:cap(b)]
			p.pools[i].Put(&full)
			return
		}
	}
}

// bucketOf returns the smallest pool index whose buffer is ≥ n, or -1 if n is
// larger than the biggest bucket.
func (p *Pool) bucketOf(n int) int {
	for i, sz := range sizes {
		if n <= sz {
			return i
		}
	}
	return -1
}
