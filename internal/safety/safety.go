// Package safety implements the pre-flight guard that prevents s3aibench from
// writing to or cleaning up a bucket it doesn't solely own, and the idempotent
// cleanup walker used by the `cleanup` subcommand and by `run` on exit.
package safety

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/darrensoothill/s3aibench/internal/s3client"
	"golang.org/x/sync/errgroup"
)

// ErrSharedBucket is returned when the target bucket contains objects outside
// the tool's prefix and AllowSharedBucket is false.
var ErrSharedBucket = errors.New("bucket contains objects outside the tool prefix; pass --allow-shared-bucket to proceed")

const (
	cleanupBatchSize     = 1000
	cleanupDeleteWorkers = 4
)

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
	deleted := 0
	if vc, ok := c.(s3client.VersionedCleaner); ok {
		n, err := vc.DeleteVersions(ctx, prefix)
		deleted += n
		if err != nil {
			return deleted, fmt.Errorf("cleanup versions: %w", err)
		}
	}
	if bd, ok := c.(s3client.BatchDeleter); ok {
		n, err := cleanupBatched(ctx, c, bd, prefix)
		deleted += n
		if err != nil {
			return deleted, err
		}
		return deleted, nil
	}
	n, err := cleanupOneByOne(ctx, c, prefix)
	deleted += n
	if err != nil {
		return deleted, err
	}
	return deleted, nil
}

func cleanupOneByOne(ctx context.Context, c s3client.Client, prefix string) (int, error) {
	deleted := 0
	var token string
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

func cleanupBatched(ctx context.Context, c s3client.Client, bd s3client.BatchDeleter, prefix string) (int, error) {
	g, ctx := errgroup.WithContext(ctx)
	batches := make(chan []string)
	var deleted atomic.Int64
	for range cleanupDeleteWorkers {
		g.Go(func() error {
			for keys := range batches {
				n, err := bd.DeleteMany(ctx, keys)
				deleted.Add(int64(n))
				if err != nil {
					return fmt.Errorf("cleanup delete batch: %w", err)
				}
			}
			return nil
		})
	}
	listErr := listCleanupBatches(ctx, c, prefix, batches)
	workerErr := g.Wait()
	total := int(deleted.Load())
	if listErr != nil {
		return total, listErr
	}
	if workerErr != nil {
		return total, workerErr
	}
	return total, nil
}

func listCleanupBatches(ctx context.Context, c s3client.Client, prefix string, batches chan<- []string) error {
	defer close(batches)
	var token string
	for {
		res, err := c.List(ctx, prefix, "", token, cleanupBatchSize)
		if err != nil {
			return fmt.Errorf("cleanup list: %w", err)
		}
		if len(res.Keys) > 0 {
			keys := append([]string(nil), res.Keys...)
			select {
			case batches <- keys:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if !res.IsTruncated {
			return nil
		}
		token = res.NextContinuation
	}
}
