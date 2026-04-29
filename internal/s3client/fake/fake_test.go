package fake

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"strings"
	"testing"
	"time"
)

func TestPutGetDelete(t *testing.T) {
	c := New()
	ctx := context.Background()
	if err := c.Put(ctx, "k", bytes.NewReader([]byte("hello")), 5); err != nil {
		t.Fatal(err)
	}
	if c.Objects()["k"] != 5 {
		t.Fatalf("objects=%v", c.Objects())
	}
	rc, err := c.Get(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(rc)
	if string(b) != "hello" {
		t.Fatalf("got %q", b)
	}
	if err := c.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(ctx, "k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestPutNegativeSize(t *testing.T) {
	c := New()
	if err := c.Put(context.Background(), "k", bytes.NewReader([]byte("abc")), -1); err != nil {
		t.Fatal(err)
	}
}

func TestPutSizeMismatch(t *testing.T) {
	c := New()
	if err := c.Put(context.Background(), "k", bytes.NewReader([]byte("abc")), 10); err == nil {
		t.Fatal("expected error")
	}
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("boom") }

func TestPutBodyError(t *testing.T) {
	c := New()
	if err := c.Put(context.Background(), "k", errReader{}, 1); err == nil {
		t.Fatal("expected error")
	}
}

func TestMultipartUploadHappy(t *testing.T) {
	c := New()
	if err := c.MultipartUpload(context.Background(), "k", bytes.NewReader([]byte("abc")), 3); err != nil {
		t.Fatal(err)
	}
}

func TestMultipartBodyError(t *testing.T) {
	c := New()
	if err := c.MultipartUpload(context.Background(), "k", errReader{}, 0); err == nil {
		t.Fatal("expected error")
	}
}

func TestFailOp(t *testing.T) {
	c := New()
	c.FailOp("put", errors.New("injected"))
	if err := c.Put(context.Background(), "k", bytes.NewReader([]byte{}), 0); err == nil {
		t.Fatal("expected error")
	}
	// Second call should succeed (fail was one-shot).
	if err := c.Put(context.Background(), "k", bytes.NewReader([]byte{}), 0); err != nil {
		t.Fatal(err)
	}
}

func TestFailAllOps(t *testing.T) {
	ops := []string{"get", "range_get", "head", "delete", "list", "copy", "get_tagging", "put_tagging", "multipart_complete"}
	for _, op := range ops {
		c := New()
		c.FailOp(op, errors.New("x"))
		var err error
		switch op {
		case "get":
			_, err = c.Get(context.Background(), "k")
		case "range_get":
			_, err = c.RangeGet(context.Background(), "k", 0, 1)
		case "head":
			_, err = c.Head(context.Background(), "k")
		case "delete":
			err = c.Delete(context.Background(), "k")
		case "list":
			_, err = c.List(context.Background(), "", "", "", 0)
		case "copy":
			err = c.Copy(context.Background(), "a", "b")
		case "get_tagging":
			_, err = c.GetObjectTagging(context.Background(), "k")
		case "put_tagging":
			err = c.PutObjectTagging(context.Background(), "k", map[string]string{})
		case "multipart_complete":
			err = c.MultipartUpload(context.Background(), "k", bytes.NewReader([]byte{}), 0)
		}
		if err == nil {
			t.Fatalf("op %s: expected error", op)
		}
	}
}

func TestDelay(t *testing.T) {
	c := New()
	c.SetDelay(1 * time.Millisecond)
	start := time.Now()
	_ = c.Put(context.Background(), "k", bytes.NewReader(nil), 0)
	if time.Since(start) < time.Millisecond {
		t.Fatal("delay not applied")
	}
	// Also cover Get/Head/Range/Multipart delay paths.
	_, _ = c.Get(context.Background(), "k")
	_, _ = c.Head(context.Background(), "k")
	_, _ = c.RangeGet(context.Background(), "k", 0, 1)
	_ = c.MultipartUpload(context.Background(), "kk", bytes.NewReader(nil), 0)
}

func TestRangeGet(t *testing.T) {
	c := New()
	ctx := context.Background()
	if err := c.Put(ctx, "k", bytes.NewReader([]byte("abcdefg")), 7); err != nil {
		t.Fatal(err)
	}
	// normal range
	rc, err := c.RangeGet(ctx, "k", 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(rc)
	if string(b) != "bcd" {
		t.Fatalf("got %q", b)
	}
	// range beyond end
	rc, _ = c.RangeGet(ctx, "k", 5, 10)
	b, _ = io.ReadAll(rc)
	if string(b) != "fg" {
		t.Fatalf("got %q", b)
	}
	// offset past end
	rc, _ = c.RangeGet(ctx, "k", 99, 10)
	b, _ = io.ReadAll(rc)
	if len(b) != 0 {
		t.Fatalf("got %q", b)
	}
	// overflow clamps to object end rather than wrapping the slice indexes
	rc, err = c.RangeGet(ctx, "k", 1, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	b, _ = io.ReadAll(rc)
	if string(b) != "bcdefg" {
		t.Fatalf("got %q", b)
	}
	// missing key
	if _, err := c.RangeGet(ctx, "missing", 0, 1); err == nil {
		t.Fatal("expected error")
	}
}

func TestRangeGetInvalidRange(t *testing.T) {
	c := New()
	ctx := context.Background()
	if err := c.Put(ctx, "k", bytes.NewReader([]byte("abcdefg")), 7); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		offset int64
		length int64
	}{
		{offset: -1, length: 1},
		{offset: 0, length: 0},
		{offset: 0, length: -1},
	} {
		if _, err := c.RangeGet(ctx, "k", tc.offset, tc.length); err == nil {
			t.Fatalf("expected error for offset=%d length=%d", tc.offset, tc.length)
		}
	}
}

func TestHeadMissing(t *testing.T) {
	c := New()
	if _, err := c.Head(context.Background(), "missing"); err == nil {
		t.Fatal("expected error")
	}
}

func TestList(t *testing.T) {
	c := New()
	ctx := context.Background()
	for _, k := range []string{"a/1", "a/2", "b/1", "a/sub/x"} {
		_ = c.Put(ctx, k, bytes.NewReader([]byte("x")), 1)
	}
	// prefix+delimiter → common prefixes
	r, err := c.List(ctx, "a/", "/", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.CommonPrefixes) != 1 || r.CommonPrefixes[0] != "a/sub/" {
		t.Fatalf("prefixes=%v", r.CommonPrefixes)
	}
	// pagination
	r, _ = c.List(ctx, "", "", "", 2)
	if !r.IsTruncated || r.NextContinuation == "" {
		t.Fatal("expected truncation")
	}
	r2, _ := c.List(ctx, "", "", r.NextContinuation, 100)
	if !strings.Contains(strings.Join(r2.Keys, ","), "b/1") {
		t.Fatalf("page 2=%v", r2.Keys)
	}
}

func TestListDefaultMaxKeys(t *testing.T) {
	c := New()
	_ = c.Put(context.Background(), "k", bytes.NewReader([]byte("x")), 1)
	r, err := c.List(context.Background(), "", "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Keys) != 1 {
		t.Fatalf("expected default max keys to allow 1 result, got %d", len(r.Keys))
	}
}

func TestCopyMissing(t *testing.T) {
	c := New()
	if err := c.Copy(context.Background(), "src", "dst"); err == nil {
		t.Fatal("expected error")
	}
}

func TestCopyHappy(t *testing.T) {
	c := New()
	_ = c.Put(context.Background(), "src", bytes.NewReader([]byte("xy")), 2)
	if err := c.Copy(context.Background(), "src", "dst"); err != nil {
		t.Fatal(err)
	}
	if c.Objects()["dst"] != 2 {
		t.Fatal("copy did not land")
	}
}

func TestTagging(t *testing.T) {
	c := New()
	ctx := context.Background()
	if err := c.PutObjectTagging(ctx, "k", map[string]string{"a": "b"}); err != nil {
		t.Fatal(err)
	}
	tags, err := c.GetObjectTagging(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if tags["a"] != "b" {
		t.Fatalf("tags=%v", tags)
	}
	// missing key → empty map, no error
	tags, err = c.GetObjectTagging(ctx, "missing")
	if err != nil || len(tags) != 0 {
		t.Fatalf("missing tags: %v %v", tags, err)
	}
}
