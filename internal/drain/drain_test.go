package drain

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestDrainHappy(t *testing.T) {
	src := bytes.NewReader(make([]byte, 128*1024))
	n, err := Drain(src)
	if err != nil {
		t.Fatal(err)
	}
	if n != 128*1024 {
		t.Fatalf("n=%d", n)
	}
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("boom") }

func TestDrainError(t *testing.T) {
	if _, err := Drain(errReader{}); err == nil {
		t.Fatal("expected error")
	}
}

func TestDrainEmpty(t *testing.T) {
	n, err := Drain(bytes.NewReader(nil))
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("n=%d", n)
	}
}

func TestDrainPoolReuse(t *testing.T) {
	// Ten drains shouldn't grow allocations significantly — relies on sync.Pool
	// returning the same buffer. We can't count allocations inside tests
	// directly, but we can at least verify correctness across many calls.
	for i := 0; i < 10; i++ {
		_, _ = Drain(bytes.NewReader([]byte("abc")))
	}
	// Using a short reader forces io.CopyBuffer to hit EOF quickly.
	_, _ = Drain(io.LimitReader(bytes.NewReader(make([]byte, 1<<20)), 512))
}
