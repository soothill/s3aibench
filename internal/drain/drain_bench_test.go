package drain

import (
	"bytes"
	"io"
	"testing"
)

// BenchmarkDrain reuses a single bytes.Reader (reset via Seek each iteration)
// so the measurement reflects only the Drain call, not the per-iteration
// bytes.NewReader allocation.
func BenchmarkDrain(b *testing.B) {
	b.ReportAllocs()
	src := make([]byte, 1<<20)
	r := bytes.NewReader(src)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = r.Seek(0, io.SeekStart)
		_, _ = Drain(r)
	}
}
