package safety

import (
	"context"
	"errors"
	"io"
	"sync/atomic"

	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/darrensoothill/s3aibench/internal/s3client"
	"github.com/darrensoothill/s3aibench/internal/s3client/fake"
)

// awsTypesNoSuchKey is a minimal alias for the SDK error used in tests.
type awsTypesNoSuchKey = s3types.NoSuchKey

// truncClient forces Cleanup to walk two pages so we cover the truncation
// branch. It returns one key per page and signals truncation on the first page.
type truncClient struct {
	s3client.Client
	calls int
}

func (t *truncClient) List(ctx context.Context, prefix, delim, token string, maxKeys int32) (*s3client.ListResult, error) {
	t.calls++
	switch t.calls {
	case 1:
		return &s3client.ListResult{Keys: []string{"s3aibench/a"}, IsTruncated: true, NextContinuation: "next"}, nil
	default:
		return &s3client.ListResult{Keys: []string{"s3aibench/b"}}, nil
	}
}

func (t *truncClient) Delete(ctx context.Context, key string) error { return nil }

type deleteErrorClient struct {
	s3client.Client
}

type listErrorClient struct {
	s3client.Client
}

func (l *listErrorClient) List(ctx context.Context, prefix, delim, token string, maxKeys int32) (*s3client.ListResult, error) {
	return nil, errors.New("down")
}

func (d *deleteErrorClient) List(ctx context.Context, prefix, delim, token string, maxKeys int32) (*s3client.ListResult, error) {
	return &s3client.ListResult{Keys: []string{prefix + "k"}}, nil
}

func (d *deleteErrorClient) Delete(ctx context.Context, key string) error {
	return errors.New("server down")
}

// compile-time assertion that truncClient satisfies Client.
var _ s3client.Client = (*truncClient)(nil)
var _ s3client.Client = (*listErrorClient)(nil)
var _ s3client.Client = (*deleteErrorClient)(nil)
var _ io.Reader = (*nopReader)(nil)

type nopReader struct{}

func (nopReader) Read(_ []byte) (int, error) { return 0, nil }

// notFoundDeleter wraps a fake client but returns a NoSuchKey-classified
// error on Delete — exercises the "skip missing key" branch in Cleanup.
type notFoundDeleter struct {
	s3client.Client
	listed bool
}

func (n *notFoundDeleter) List(ctx context.Context, prefix, delim, token string, maxKeys int32) (*s3client.ListResult, error) {
	if n.listed {
		return &s3client.ListResult{}, nil
	}
	n.listed = true
	return &s3client.ListResult{Keys: []string{prefix + "gone"}}, nil
}

func (n *notFoundDeleter) Delete(_ context.Context, _ string) error {
	return &awsTypesNoSuchKey{}
}

type versionCleaner struct {
	*fake.Client
	deleted int
	err     error
	called  bool
}

func (v *versionCleaner) DeleteVersions(_ context.Context, _ string) (int, error) {
	v.called = true
	if v.err != nil {
		return v.deleted, v.err
	}
	return v.deleted, nil
}

var _ s3client.VersionedCleaner = (*versionCleaner)(nil)

type batchTruncClient struct {
	*fake.Client
	listCalls   int
	deleteCalls atomic.Int64
}

func (b *batchTruncClient) List(ctx context.Context, prefix, delim, token string, maxKeys int32) (*s3client.ListResult, error) {
	b.listCalls++
	switch b.listCalls {
	case 1:
		return &s3client.ListResult{Keys: []string{"s3aibench/a"}, IsTruncated: true, NextContinuation: "next"}, nil
	default:
		return &s3client.ListResult{Keys: []string{"s3aibench/b"}}, nil
	}
}

func (b *batchTruncClient) DeleteMany(ctx context.Context, keys []string) (int, error) {
	b.deleteCalls.Add(1)
	return len(keys), nil
}

type onePageClient struct{}

func (onePageClient) Put(ctx context.Context, key string, body io.Reader, size int64) error {
	return nil
}

func (onePageClient) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return nil, nil
}

func (onePageClient) RangeGet(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	return nil, nil
}

func (onePageClient) Head(ctx context.Context, key string) (int64, error) {
	return 0, nil
}

func (onePageClient) Delete(ctx context.Context, key string) error {
	return nil
}

func (onePageClient) List(ctx context.Context, prefix, delimiter, continuationToken string, maxKeys int32) (*s3client.ListResult, error) {
	return &s3client.ListResult{Keys: []string{prefix + "k"}}, nil
}

func (onePageClient) Copy(ctx context.Context, srcKey, dstKey string) error {
	return nil
}

func (onePageClient) GetObjectTagging(ctx context.Context, key string) (map[string]string, error) {
	return nil, nil
}

func (onePageClient) PutObjectTagging(ctx context.Context, key string, tags map[string]string) error {
	return nil
}

func (onePageClient) MultipartUpload(ctx context.Context, key string, body io.Reader, size int64) error {
	return nil
}
