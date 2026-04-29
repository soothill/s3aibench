package bodygen

import (
	"bytes"
	"io"
	"testing"
	"testing/iotest"
)

func TestReaderSizes(t *testing.T) {
	for _, size := range []int64{0, 1, 512, 1 << 20, (1 << 20) * 3, (1<<20)*3 + 7} {
		r := NewReader(size)
		if r.Size() != size {
			t.Fatalf("Size=%d want %d", r.Size(), size)
		}
		out, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		if int64(len(out)) != size {
			t.Fatalf("read %d want %d", len(out), size)
		}
		Release(r)
	}
}

func TestSeekStartCurrentEnd(t *testing.T) {
	r := NewReader(100)
	defer Release(r)
	// Read 10 bytes
	buf := make([]byte, 10)
	if _, err := io.ReadFull(r, buf); err != nil {
		t.Fatal(err)
	}
	if got, err := r.Seek(0, io.SeekStart); err != nil || got != 0 {
		t.Fatalf("seek start: %d %v", got, err)
	}
	if got, err := r.Seek(5, io.SeekCurrent); err != nil || got != 5 {
		t.Fatalf("seek current: %d %v", got, err)
	}
	if got, err := r.Seek(-20, io.SeekEnd); err != nil || got != 80 {
		t.Fatalf("seek end: %d %v", got, err)
	}
}

func TestSeekErrors(t *testing.T) {
	r := NewReader(10)
	defer Release(r)
	if _, err := r.Seek(-1, io.SeekStart); err == nil {
		t.Fatal("expected error")
	}
	if _, err := r.Seek(11, io.SeekStart); err == nil {
		t.Fatal("expected error")
	}
	if _, err := r.Seek(0, 99); err == nil {
		t.Fatal("expected invalid whence")
	}
}

func TestLen(t *testing.T) {
	r := NewReader(50)
	defer Release(r)
	if r.Len() != 50 {
		t.Fatalf("len=%d", r.Len())
	}
	_, _ = io.CopyN(io.Discard, r, 20)
	if r.Len() != 30 {
		t.Fatalf("len=%d after read", r.Len())
	}
}

func TestReleaseNilNoOp(t *testing.T) {
	Release(nil)
}

func TestContentMatchesEntropy(t *testing.T) {
	// The first N bytes of a fresh reader must equal the first N bytes of the
	// shared entropy block — guarantees that multiple readers of the same size
	// produce identical bodies.
	r := NewReader(1024)
	defer Release(r)
	out, _ := io.ReadAll(r)
	if !bytes.Equal(out, entropy[:1024]) {
		t.Fatal("content diverged from entropy block")
	}
}

func TestReadBehavesLikeReader(t *testing.T) {
	// iotest.TestReader validates the full io.Reader contract.
	r := NewReader(4096)
	defer Release(r)
	want, _ := io.ReadAll(NewReader(4096))
	if err := iotest.TestReader(r, want); err != nil {
		t.Fatal(err)
	}
}

func TestReadWrapsEntropyBlock(t *testing.T) {
	r := NewReader(blockSize + 8)
	defer Release(r)
	if _, err := r.Seek(blockSize-4, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 8)
	if _, err := io.ReadFull(r, buf); err != nil {
		t.Fatal(err)
	}
	want := append(append([]byte(nil), entropy[blockSize-4:]...), entropy[:4]...)
	if !bytes.Equal(buf, want) {
		t.Fatalf("wrapped read mismatch: got %x want %x", buf, want)
	}
}

func TestReadHandlesLargeSingleRead(t *testing.T) {
	size := int64(3*blockSize + 7)
	r := NewReader(size)
	defer Release(r)
	buf := make([]byte, size)
	n, err := io.ReadFull(r, buf)
	if err != nil {
		t.Fatal(err)
	}
	if int64(n) != size {
		t.Fatalf("read %d want %d", n, size)
	}
	for offset := 0; offset < 3*blockSize; offset += blockSize {
		if !bytes.Equal(buf[offset:offset+blockSize], entropy) {
			t.Fatalf("block at %d did not repeat entropy", offset)
		}
	}
	if !bytes.Equal(buf[3*blockSize:], entropy[:7]) {
		t.Fatal("tail did not wrap correctly")
	}
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestFillEntropyErrorFallback(t *testing.T) {
	b := fillEntropy(zeroReader{})
	if len(b) != blockSize {
		t.Fatalf("expected %d bytes, got %d", blockSize, len(b))
	}
	// On error, we get an all-zero buffer. That's deliberate; we just assert
	// the function doesn't panic.
	if b[0] != 0 {
		t.Fatal("fallback should return zeroed buffer on read error")
	}
}

func TestReaderIsReadSeeker(t *testing.T) {
	// The AWS SDK's manager.Uploader streams multipart parts only when the
	// body is an io.ReadSeeker; otherwise it buffers each part fully. Assert
	// at compile+runtime that *Reader satisfies that interface so we never
	// accidentally regress to the buffered path.
	var _ io.ReadSeeker = NewReader(0)
	r := NewReader(16)
	defer Release(r)
	if _, ok := interface{}(r).(io.ReadSeeker); !ok {
		t.Fatal("bodygen.Reader must be an io.ReadSeeker for multipart streaming")
	}
}

func TestRereadAfterRelease(t *testing.T) {
	// Pool reuse: a Release followed by NewReader should hand us a zeroed
	// Reader, not the previous one's mid-read state.
	r := NewReader(20)
	_, _ = io.CopyN(io.Discard, r, 10)
	Release(r)
	r2 := NewReader(20)
	defer Release(r2)
	if r2.Len() != 20 {
		t.Fatalf("pooled reader not reset, len=%d", r2.Len())
	}
}
