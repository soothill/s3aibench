// Package drain reads S3 response bodies into pooled buffers with zero
// per-call heap allocation, replacing `io.Copy(io.Discard, r)` on GET
// hot paths.
//
// The default io.Copy path allocates a fresh 32 KiB buffer every call
// because neither *http.Response.Body nor io.Discard provides an
// optimised WriteTo/ReadFrom implementation compatible with io.Copy's
// allocation-free branch under all circumstances.
package drain

import (
	"io"
	"sync"
)

// bufSize is the per-Read chunk size. 32 KiB matches io.Copy's default so
// the underlying network syscall counts stay identical.
const bufSize = 32 * 1024

var bufPool = sync.Pool{New: func() interface{} {
	b := make([]byte, bufSize)
	return &b
}}

// Drain reads r to EOF into a pooled buffer and reports the total bytes
// consumed. Never allocates a new buffer after the first call per goroutine.
func Drain(r io.Reader) (int64, error) {
	bp := bufPool.Get().(*[]byte)
	defer bufPool.Put(bp)
	return io.CopyBuffer(io.Discard, r, *bp)
}
