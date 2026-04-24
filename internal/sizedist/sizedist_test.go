package sizedist

import (
	"math"
	"math/rand"
	"testing"
)

func TestUniform(t *testing.T) {
	u := Uniform{Size: 1024}
	for i := 0; i < 5; i++ {
		if u.Sample() != 1024 {
			t.Fatal("not constant")
		}
	}
}

func TestUniformRange(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	u := UniformRange{Min: 5, Max: 10, R: r}
	for i := 0; i < 1000; i++ {
		n := u.Sample()
		if n < 5 || n > 10 {
			t.Fatalf("out of range: %d", n)
		}
	}
	// Degenerate range → returns Min.
	u2 := UniformRange{Min: 7, Max: 7, R: r}
	if u2.Sample() != 7 {
		t.Fatal("degenerate range should return Min")
	}
	u3 := UniformRange{Min: 10, Max: 5, R: r} // Max<Min
	if u3.Sample() != 10 {
		t.Fatal("Max<Min should return Min")
	}
}

func TestLognormalErrors(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	cases := []struct {
		mean, sigma float64
		min, max    int64
	}{
		{0, 1, 0, 10},   // mean=0
		{-1, 1, 0, 10},  // mean<0
		{1, 0, 0, 10},   // sigma=0
		{1, -1, 0, 10},  // sigma<0
		{1, 1, -1, 10},  // min<0
		{1, 1, 10, 5},   // max<min
	}
	for _, c := range cases {
		if _, err := NewLognormal(c.mean, c.sigma, c.min, c.max, r); err == nil {
			t.Fatalf("expected error for %+v", c)
		}
	}
}

func TestLognormalMeanAndBounds(t *testing.T) {
	r := rand.New(rand.NewSource(42))
	targetMean := 512.0 * 1024 // 512 KiB
	sigma := 0.8
	ln, err := NewLognormal(targetMean, sigma, 1024, 10*1024*1024, r)
	if err != nil {
		t.Fatal(err)
	}
	const N = 100_000
	var sum float64
	for i := 0; i < N; i++ {
		s := ln.Sample()
		if s < 1024 || s > 10*1024*1024 {
			t.Fatalf("out of bounds: %d", s)
		}
		sum += float64(s)
	}
	got := sum / float64(N)
	// Allow ±15% due to clamping and sample variance.
	if math.Abs(got-targetMean)/targetMean > 0.15 {
		t.Fatalf("mean drift too large: got %.0f want ~%.0f", got, targetMean)
	}
}

func TestLognormalMinClamp(t *testing.T) {
	// Very small mean drives clamping to Min on most draws.
	r := rand.New(rand.NewSource(7))
	ln, _ := NewLognormal(2.0, 0.5, 100, 10000, r)
	saw := false
	for i := 0; i < 200; i++ {
		if ln.Sample() == 100 {
			saw = true
		}
	}
	if !saw {
		t.Fatal("expected min clamp to fire")
	}
}

func TestLognormalMaxClamp(t *testing.T) {
	// Very large mean + low max → clamping to Max on most draws.
	r := rand.New(rand.NewSource(11))
	ln, _ := NewLognormal(1e9, 0.5, 1, 1000, r)
	saw := false
	for i := 0; i < 200; i++ {
		if ln.Sample() == 1000 {
			saw = true
		}
	}
	if !saw {
		t.Fatal("expected max clamp to fire")
	}
}

func TestLognormalMaxZeroSkipsUpperClamp(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	ln, _ := NewLognormal(1, 0.1, 0, 0, r)
	// Just verify no panic and ≥0 return.
	for i := 0; i < 20; i++ {
		if ln.Sample() < 0 {
			t.Fatal("negative sample")
		}
	}
}

func TestZipfianErrors(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	if _, err := NewZipfian(0, 1.1, r); err == nil {
		t.Fatal("expected n<=0 error")
	}
	if _, err := NewZipfian(10, 1.0, r); err == nil {
		t.Fatal("expected s<=1 error")
	}
}

func TestLognormalForWorkerIndependent(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	base, _ := NewLognormal(1024, 0.5, 1, 10000, r)
	c1 := base.ForWorker(1)
	c2 := base.ForWorker(2)
	// Different workers, different RNG streams → at least one sample diverges.
	diverged := false
	for i := 0; i < 20; i++ {
		if c1.Sample() != c2.Sample() {
			diverged = true
			break
		}
	}
	if !diverged {
		t.Fatal("per-worker streams collided unexpectedly")
	}
}

func TestZipfianForWorkerIndependent(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	base, err := NewZipfian(1000, 1.1, r)
	if err != nil {
		t.Fatal(err)
	}
	c1 := base.ForWorker(1)
	c2 := base.ForWorker(2)
	diverged := false
	for i := 0; i < 20; i++ {
		if c1.Sample() != c2.Sample() {
			diverged = true
			break
		}
	}
	if !diverged {
		t.Fatal("per-worker streams collided unexpectedly")
	}
}

func TestZipfianHotKeyDominance(t *testing.T) {
	r := rand.New(rand.NewSource(99))
	const N = int64(10_000)
	z, err := NewZipfian(N, 1.07, r)
	if err != nil {
		t.Fatal(err)
	}
	counts := make([]int, N)
	const draws = 200_000
	for i := 0; i < draws; i++ {
		k := z.Sample()
		if k < 0 || k >= N {
			t.Fatalf("out of range: %d", k)
		}
		counts[k]++
	}
	// Top 10% of keys should own >50% of traffic.
	top := int64(float64(N) * 0.1)
	var topSum int
	for i := int64(0); i < top; i++ {
		topSum += counts[i]
	}
	if float64(topSum)/float64(draws) < 0.5 {
		t.Fatalf("top 10%% got %.1f%% of draws, want >50%%", 100*float64(topSum)/float64(draws))
	}
}
