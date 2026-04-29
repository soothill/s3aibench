// Package bodygen emits deterministic pseudo-random PUT bodies without
// per-request heap allocation.
//
// A single 1 MiB process-wide entropy block is generated once at init; every
// *Reader streams bytes from that block on demand, cycling through it for
// objects larger than 1 MiB. Reader structs are pooled via sync.Pool, so the
// only per-request cost is an atomic pool Get/Put and a few field writes.
//
// Reader implements io.ReaderAt and io.ReadSeeker, which lets manager.Uploader
// stream multipart parts from independent section readers instead of sharing
// the stateful Read offset across multipart worker goroutines.
package bodygen

import (
	"crypto/rand"
	"errors"
	"io"
	"sync"
)

// blockSize is the size of the shared entropy block. Reads can cycle across it
// multiple times when callers request buffers larger than 1 MiB.
const blockSize = 1 << 20 // 1 MiB

// entropy is the shared random block. Generated exactly once with crypto/rand
// so multi-run reproducibility is not a concern (the block's content is
// incidental; only its non-compressibility matters for realistic benchmarks).
var entropy = fillEntropy(rand.Reader)

// fillEntropy is extracted for test coverage of the error path. Production
// always passes crypto/rand.Reader which never returns a short read in
// practice; returning a best-effort buffer on error keeps the tool running.
func fillEntropy(r io.Reader) []byte {
	b := make([]byte, blockSize)
	_, _ = io.ReadFull(r, b) // on failure, leave zeros — unit-testable
	return b
}

var readerPool = sync.Pool{New: func() interface{} { return &Reader{} }}

// Reader emits exactly `size` bytes drawn from the shared entropy block.
// Safe for a single goroutine. Callers must return the Reader to the pool
// via Release once done.
type Reader struct {
	size   int64
	offset int64
}

// NewReader returns a pooled Reader sized to emit `size` bytes. Callers must
// Release once the reader is no longer needed.
func NewReader(size int64) *Reader {
	r := readerPool.Get().(*Reader)
	r.size = size
	r.offset = 0
	return r
}

// Release returns r to the pool. Passing nil is a no-op.
func Release(r *Reader) {
	if r == nil {
		return
	}
	r.size = 0
	r.offset = 0
	readerPool.Put(r)
}

// Size returns the total byte count this reader was configured to emit.
func (r *Reader) Size() int64 { return r.size }

// Len returns the remaining bytes — required by some AWS SDK code paths that
// check *bytes.Reader-like interfaces.
func (r *Reader) Len() int { return int(r.size - r.offset) }

// Read implements io.Reader.
func (r *Reader) Read(p []byte) (int, error) {
	if r.offset >= r.size {
		return 0, io.EOF
	}
	remain := r.size - r.offset
	n := int64(len(p))
	if n > remain {
		n = remain
	}
	fill(p[:int(n)], r.offset)
	r.offset += n
	return int(n), nil
}

// ReadAt implements io.ReaderAt without mutating the Reader's current offset.
func (r *Reader) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("bodygen: negative ReadAt offset")
	}
	if off >= r.size {
		return 0, io.EOF
	}
	n := int64(len(p))
	if remain := r.size - off; n > remain {
		n = remain
	}
	fill(p[:int(n)], off)
	if n < int64(len(p)) {
		return int(n), io.EOF
	}
	return int(n), nil
}

// fill copies deterministic data from entropy into dst, cycling as needed.
func fill(dst []byte, offset int64) {
	written := 0
	for written < len(dst) {
		start := int((offset + int64(written)) % int64(len(entropy)))
		chunk := len(dst) - written
		if avail := len(entropy) - start; chunk > avail {
			chunk = avail
		}
		copy(dst[written:written+chunk], entropy[start:start+chunk])
		written += chunk
	}
}

// Seek implements io.Seeker, enabling manager.Uploader's streaming path.
func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = r.offset + offset
	case io.SeekEnd:
		abs = r.size + offset
	default:
		return 0, errors.New("bodygen: invalid whence")
	}
	if abs < 0 || abs > r.size {
		return 0, errors.New("bodygen: seek out of range")
	}
	r.offset = abs
	return abs, nil
}
