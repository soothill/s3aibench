package bufpool

import "testing"

func TestGetAndPut(t *testing.T) {
	p := New()
	b := p.Get(128)
	if len(b) != 128 {
		t.Fatalf("len=%d", len(b))
	}
	if cap(b) != 64*1024 {
		t.Fatalf("cap=%d", cap(b))
	}
	p.Put(b)
	// Get again should hit the pool and reuse the buffer.
	b2 := p.Get(200)
	if cap(b2) != 64*1024 {
		t.Fatalf("expected reuse, got cap=%d", cap(b2))
	}
}

func TestAllBuckets(t *testing.T) {
	p := New()
	for _, n := range []int{64 * 1024, 1 << 20, 16 << 20, 256 << 20} {
		b := p.Get(n)
		if len(b) != n {
			t.Fatalf("n=%d len=%d", n, len(b))
		}
		p.Put(b)
	}
}

func TestOversize(t *testing.T) {
	p := New()
	n := 512 << 20
	b := p.Get(n)
	if len(b) != n {
		t.Fatal("oversize not honored")
	}
	// Put of a buffer that matches no bucket should be a no-op.
	p.Put(b)
}

func TestPutNonMatchingDropped(t *testing.T) {
	p := New()
	// Hand-rolled buffer whose capacity doesn't match any bucket.
	b := make([]byte, 123)
	p.Put(b) // no panic, no re-pool
}

func TestBucketOfOversize(t *testing.T) {
	p := New()
	if p.bucketOf(1 << 40) != -1 {
		t.Fatal("expected -1 for oversize")
	}
}
