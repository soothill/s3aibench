package safety

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/darrensoothill/s3aibench/internal/s3client/fake"
)

func TestCheckBucketAllowShared(t *testing.T) {
	c := fake.New()
	_ = c.Put(context.Background(), "stray", bytes.NewReader([]byte{}), 0)
	if err := CheckBucket(context.Background(), c, "s3aibench/", true); err != nil {
		t.Fatalf("allowShared=true must bypass: %v", err)
	}
}

func TestCheckBucketEmpty(t *testing.T) {
	c := fake.New()
	if err := CheckBucket(context.Background(), c, "s3aibench", false); err != nil {
		t.Fatal(err)
	}
}

func TestCheckBucketAllInPrefix(t *testing.T) {
	c := fake.New()
	_ = c.Put(context.Background(), "s3aibench/01/x", bytes.NewReader([]byte{}), 0)
	if err := CheckBucket(context.Background(), c, "s3aibench/", false); err != nil {
		t.Fatal(err)
	}
}

func TestCheckBucketForeign(t *testing.T) {
	c := fake.New()
	_ = c.Put(context.Background(), "unrelated/x", bytes.NewReader([]byte{}), 0)
	err := CheckBucket(context.Background(), c, "s3aibench/", false)
	if !errors.Is(err, ErrSharedBucket) {
		t.Fatalf("got %v", err)
	}
}

func TestCheckBucketListError(t *testing.T) {
	c := fake.New()
	c.FailOp("list", errors.New("down"))
	if err := CheckBucket(context.Background(), c, "s3aibench/", false); err == nil {
		t.Fatal("expected error")
	}
}

func TestCleanupIdempotent(t *testing.T) {
	c := fake.New()
	for _, k := range []string{"s3aibench/a", "s3aibench/b", "s3aibench/c"} {
		_ = c.Put(context.Background(), k, bytes.NewReader([]byte{}), 0)
	}
	n, err := Cleanup(context.Background(), c, "s3aibench")
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("deleted %d", n)
	}
	// Second run should be a no-op.
	n2, err := Cleanup(context.Background(), c, "s3aibench/")
	if err != nil {
		t.Fatal(err)
	}
	if n2 != 0 {
		t.Fatalf("expected zero deletes, got %d", n2)
	}
}

func TestCleanupListError(t *testing.T) {
	c := fake.New()
	c.FailOp("list", errors.New("down"))
	if _, err := Cleanup(context.Background(), c, "s3aibench/"); err == nil {
		t.Fatal("expected error")
	}
}

func TestCleanupDeleteError(t *testing.T) {
	c := fake.New()
	_ = c.Put(context.Background(), "s3aibench/k", bytes.NewReader([]byte{}), 0)
	c.FailOp("delete", errors.New("server down"))
	if _, err := Cleanup(context.Background(), c, "s3aibench/"); err == nil {
		t.Fatal("expected error")
	}
}

func TestCleanupSkipsNotFound(t *testing.T) {
	wc := &notFoundDeleter{Client: fake.New()}
	n, err := Cleanup(context.Background(), wc, "s3aibench/")
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("expected 0 deleted, got %d", n)
	}
}

func TestCleanupDeletesVersionsFirst(t *testing.T) {
	c := fake.New()
	_ = c.Put(context.Background(), "s3aibench/current", bytes.NewReader([]byte{}), 0)
	wc := &versionCleaner{Client: c, deleted: 2}
	n, err := Cleanup(context.Background(), wc, "s3aibench/")
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("deleted %d", n)
	}
	if !wc.called {
		t.Fatal("version cleanup was not called")
	}
}

func TestCleanupVersionError(t *testing.T) {
	wc := &versionCleaner{Client: fake.New(), deleted: 2, err: errors.New("version delete down")}
	n, err := Cleanup(context.Background(), wc, "s3aibench/")
	if err == nil {
		t.Fatal("expected error")
	}
	if n != 2 {
		t.Fatalf("deleted %d", n)
	}
}

func TestCleanupTruncatedPagination(t *testing.T) {
	c := fake.New()
	// Use our fake's default page size via many keys; fake truncates at maxKeys=1000 but
	// Cleanup asks for 1000 per page, so we need >1000 to exercise truncation. Cheaper:
	// drive truncation directly via a custom wrapper.
	wc := &truncClient{Client: c}
	n, err := Cleanup(context.Background(), wc, "s3aibench/")
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("deleted %d", n)
	}
}
