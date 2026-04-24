package metadata

import (
	"bytes"
	"io"
)

// bytesNewReader is a tiny adapter so tests don't import bytes twice.
func bytesNewReader(b []byte) io.Reader { return bytes.NewReader(b) }
