// Package sizedist implements the size and access-pattern samplers used by
// the AI workloads — lognormal (object sizes / range sizes), uniform (fixed
// ranges), and zipfian (hot-key access skew).
package sizedist

import (
	"errors"
	"math"
	"math/rand"
)

// Sampler returns a positive int64 draw per call.
type Sampler interface {
	Sample() int64
}

// Uniform returns exactly `size` every call. Useful as the trivial sampler.
type Uniform struct{ Size int64 }

// Sample returns the configured fixed value.
func (u Uniform) Sample() int64 { return u.Size }

// UniformRange returns a uniformly-distributed int64 in [Min, Max].
type UniformRange struct {
	Min, Max int64
	R        *rand.Rand
}

// Sample draws from the configured range (inclusive).
func (u UniformRange) Sample() int64 {
	if u.Max <= u.Min {
		return u.Min
	}
	return u.Min + u.R.Int63n(u.Max-u.Min+1)
}

// Lognormal draws int64 values from a log-normal distribution parameterised
// by the target mean (in bytes) and sigma. Values below `min` are clamped up;
// above `max` clamped down, so it can safely stand in for a bounded size
// distribution without a long tail that bloats test fixtures.
//
// A single Lognormal owns a *rand.Rand and is NOT thread-safe. High-concurrency
// callers should invoke ForWorker(id) to get a per-worker clone backed by its
// own RNG, which eliminates the mutex from the hot path entirely.
type Lognormal struct {
	Mu, Sigma float64
	Min, Max  int64
	r         *rand.Rand
}

// NewLognormal builds a Lognormal sampler whose draws have the requested
// arithmetic mean. Given mean m and sigma s, mu = ln(m) - s^2/2.
func NewLognormal(mean float64, sigma float64, min, max int64, r *rand.Rand) (*Lognormal, error) {
	if mean <= 0 {
		return nil, errors.New("sizedist: lognormal mean must be >0")
	}
	if sigma <= 0 {
		return nil, errors.New("sizedist: lognormal sigma must be >0")
	}
	if min < 0 || max < min {
		return nil, errors.New("sizedist: invalid lognormal bounds")
	}
	mu := math.Log(mean) - (sigma*sigma)/2
	return &Lognormal{Mu: mu, Sigma: sigma, Min: min, Max: max, r: r}, nil
}

// Sample returns a single draw, clamped to [Min, Max]. Not safe for
// concurrent use; see ForWorker.
func (l *Lognormal) Sample() int64 {
	norm := l.r.NormFloat64()
	x := math.Exp(l.Mu + l.Sigma*norm)
	n := int64(math.Round(x))
	if n < l.Min {
		n = l.Min
	}
	if l.Max > 0 && n > l.Max {
		n = l.Max
	}
	return n
}

// ForWorker returns a per-worker clone backed by an independent RNG seeded
// from `workerID`. The returned Lognormal has no mutex — each worker calls
// Sample lock-free.
func (l *Lognormal) ForWorker(workerID int) *Lognormal {
	c := *l
	c.r = rand.New(rand.NewSource(workerSeed(workerID)))
	return &c
}

// Zipfian draws integer keys in [0, N) with a Zipf(s, v) distribution: the
// "hot key" access pattern where a few keys see most of the traffic.
//
// A single Zipfian owns a *rand.Rand and is NOT thread-safe. High-concurrency
// callers should invoke ForWorker(id) to get a per-worker clone backed by its
// own RNG; the expensive zeta normalisation is re-run per clone (one-shot,
// O(N)) but only once per worker.
type Zipfian struct {
	z *rand.Zipf
	N int64
	s float64
}

// NewZipfian constructs a Zipfian over [0, n) with exponent s (s>1) and
// offset v=1.
func NewZipfian(n int64, s float64, r *rand.Rand) (*Zipfian, error) {
	if n <= 0 {
		return nil, errors.New("sizedist: zipfian N must be >0")
	}
	if s <= 1 {
		return nil, errors.New("sizedist: zipfian s must be >1")
	}
	return &Zipfian{z: rand.NewZipf(r, s, 1, uint64(n-1)), N: n, s: s}, nil
}

// Sample returns a key index in [0, N). Not safe for concurrent use; see
// ForWorker.
func (z *Zipfian) Sample() int64 {
	return int64(z.z.Uint64())
}

// workerSeed derives a deterministic int64 seed from a worker ID by mixing
// through the golden-ratio constant. Using uint64 avoids the constant-overflow
// compile error int64 produces.
func workerSeed(workerID int) int64 {
	return int64(uint64(workerID)*0x9E3779B97F4A7C15 + 1)
}

// ForWorker returns a per-worker clone with an independent RNG and a freshly
// built *rand.Zipf. Zeta is recomputed (O(N)) per clone, but only once at
// worker spin-up.
func (z *Zipfian) ForWorker(workerID int) *Zipfian {
	r := rand.New(rand.NewSource(workerSeed(workerID)))
	return &Zipfian{
		z: rand.NewZipf(r, z.s, 1, uint64(z.N-1)),
		N: z.N,
		s: z.s,
	}
}
