package bodygen

import (
	"io"
	"testing"
)

// BenchmarkReaderRoundTrip acquires a pooled Reader, drains it, and releases
// it. Target: 0 allocs/op. Establishes that PUT hot paths incur no heap
// allocation for body construction once steady-state is reached.
func BenchmarkReaderRoundTrip(b *testing.B) {
	b.ReportAllocs()
	buf := make([]byte, 32*1024) // caller-owned scratch; not measured as alloc
	for i := 0; i < b.N; i++ {
		r := NewReader(int64(len(buf)))
		_, _ = io.ReadFull(r, buf)
		Release(r)
	}
}

// BenchmarkReaderLargeBody sizes the body at 16 MiB — a typical multipart
// part — to verify no allocation scales with body size.
func BenchmarkReaderLargeBody(b *testing.B) {
	b.ReportAllocs()
	const size = 16 * 1024 * 1024
	buf := make([]byte, 64*1024)
	for i := 0; i < b.N; i++ {
		r := NewReader(size)
		for {
			_, err := r.Read(buf)
			if err == io.EOF {
				break
			}
		}
		Release(r)
	}
}

// BenchmarkReaderWrapRead exercises a single read that crosses the entropy
// block boundary, which used to devolve into a short read.
func BenchmarkReaderWrapRead(b *testing.B) {
	b.ReportAllocs()
	buf := make([]byte, 64*1024)
	for i := 0; i < b.N; i++ {
		r := NewReader(blockSize + int64(len(buf)))
		if _, err := r.Seek(blockSize-int64(len(buf))/2, io.SeekStart); err != nil {
			b.Fatal(err)
		}
		if _, err := io.ReadFull(r, buf); err != nil {
			b.Fatal(err)
		}
		Release(r)
	}
}
