package safety

import (
	"context"
	"io"

	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/darrensoothill/s3aibench/internal/s3client"
	"github.com/darrensoothill/s3aibench/internal/s3client/fake"
)

// awsTypesNoSuchKey is a minimal alias for the SDK error used in tests.
type awsTypesNoSuchKey = s3types.NoSuchKey

// truncClient forces Cleanup to walk two pages so we cover the truncation
// branch. It returns one key per page and signals truncation on the first page.
type truncClient struct {
	*fake.Client
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

// compile-time assertion that truncClient satisfies Client.
var _ s3client.Client = (*truncClient)(nil)
var _ io.Reader = (*nopReader)(nil)

type nopReader struct{}

func (nopReader) Read(_ []byte) (int, error) { return 0, nil }

// notFoundDeleter wraps a fake client but returns a NoSuchKey-classified
// error on Delete — exercises the "skip missing key" branch in Cleanup.
type notFoundDeleter struct {
	*fake.Client
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
