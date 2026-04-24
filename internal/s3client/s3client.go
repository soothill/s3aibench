// Package s3client wraps the AWS SDK v2 S3 client behind a small interface so
// workloads can be unit-tested against an in-memory fake (see fake/) without
// network I/O. It also enforces the run prefix guard: all object keys passed
// to the client have the configured prefix prepended exactly once.
package s3client

import (
	"context"
	"io"
	"strings"
)

// Client is the interface workloads consume. It is intentionally minimal; it
// grows as workloads need new verbs.
type Client interface {
	Put(ctx context.Context, key string, body io.Reader, size int64) error
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	RangeGet(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error)
	Head(ctx context.Context, key string) (int64, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix, delimiter, continuationToken string, maxKeys int32) (*ListResult, error)
	Copy(ctx context.Context, srcKey, dstKey string) error
	GetObjectTagging(ctx context.Context, key string) (map[string]string, error)
	PutObjectTagging(ctx context.Context, key string, tags map[string]string) error
	// MultipartUpload performs a full multipart upload of the given body,
	// using the configured part size and concurrency.
	MultipartUpload(ctx context.Context, key string, body io.Reader, size int64) error
}

// ListResult is a paginated LIST response.
type ListResult struct {
	Keys              []string
	CommonPrefixes    []string
	NextContinuation  string
	IsTruncated       bool
}

// joinPrefix prepends a run prefix exactly once, normalising double separators.
func joinPrefix(prefix, key string) string {
	key = strings.TrimPrefix(key, "/")
	if prefix == "" {
		return key
	}
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	if strings.HasPrefix(key, prefix) {
		return key
	}
	return prefix + key
}
