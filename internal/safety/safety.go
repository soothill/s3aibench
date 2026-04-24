// Package safety implements the pre-flight guard that prevents s3aibench from
// writing to or cleaning up a bucket it doesn't solely own, and the idempotent
// cleanup walker used by the `cleanup` subcommand and by `run` on exit.
package safety

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/darrensoothill/s3aibench/internal/s3client"
)

// ErrSharedBucket is returned when the target bucket contains objects outside
// the tool's prefix and AllowSharedBucket is false.
var ErrSharedBucket = errors.New("bucket contains objects outside the tool prefix; pass --allow-shared-bucket to proceed")

// CheckBucket inspects the bucket root and returns ErrSharedBucket if any key
// is found whose prefix falls outside `prefix`. An empty bucket is allowed.
func CheckBucket(ctx context.Context, c s3client.Client, prefix string, allowShared bool) error {
	if allowShared {
		return nil
	}
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	res, err := c.List(ctx, "", "", "", 5)
	if err != nil {
		return fmt.Errorf("safety check list: %w", err)
	}
	for _, k := range res.Keys {
		if !strings.HasPrefix(k, prefix) {
			return fmt.Errorf("%w: found %q", ErrSharedBucket, k)
		}
	}
	return nil
}

// Cleanup deletes every object under `prefix`. It is idempotent: NoSuchKey
// errors are treated as success. Returns the number of keys deleted.
func Cleanup(ctx context.Context, c s3client.Client, prefix string) (int, error) {
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	var token string
	deleted := 0
	for {
		res, err := c.List(ctx, prefix, "", token, 1000)
		if err != nil {
			return deleted, fmt.Errorf("cleanup list: %w", err)
		}
		for _, k := range res.Keys {
			if err := c.Delete(ctx, k); err != nil {
				if !s3client.IsNotFound(err) {
					return deleted, fmt.Errorf("cleanup delete %q: %w", k, err)
				}
				continue
			}
			deleted++
		}
		if !res.IsTruncated {
			return deleted, nil
		}
		token = res.NextContinuation
	}
}
