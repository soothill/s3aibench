package s3client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"golang.org/x/sync/errgroup"
)

// ErrNotFound is the generic missing-object sentinel used by non-AWS client implementations.
var ErrNotFound = errors.New("s3client: not found")

const (
	defaultMultipartPartSize    int64 = 5 * 1024 * 1024
	defaultMultipartConcurrency       = 5
	maxDeleteObjectsBatch             = 1000
)

// AWSConfig bundles everything needed to instantiate an AWS-backed client.
type AWSConfig struct {
	Endpoint             string
	Region               string
	Bucket               string
	Prefix               string
	PathStyle            bool
	Credentials          aws.CredentialsProvider
	HTTPClient           *http.Client
	MultipartPartSize    int64
	MultipartConcurrency int
	// MaxRetries caps SDK retry attempts. Benchmarks default to 1 (no retries
	// beyond the first try) so transient SlowDown/503 responses don't skew
	// latency percentiles. Production callers can raise this explicitly.
	MaxRetries int
}

// AWS is a real S3-backed Client.
type AWS struct {
	api                  *s3.Client
	bucket               string
	prefix               string
	partSize             int64
	multipartConcurrency int
}

// NewAWS constructs a Client backed by the AWS SDK v2.
func NewAWS(cfg AWSConfig) *AWS {
	opts := s3.Options{
		Region:                     cfg.Region,
		UsePathStyle:               cfg.PathStyle,
		HTTPClient:                 cfg.HTTPClient,
		Credentials:                aws.NewCredentialsCache(cfg.Credentials),
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	}
	maxRetries := cfg.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 1
	}
	opts.Retryer = retry.NewStandard(func(o *retry.StandardOptions) {
		o.MaxAttempts = maxRetries
	})
	if cfg.Endpoint != "" {
		opts.BaseEndpoint = aws.String(cfg.Endpoint)
	}
	api := s3.New(opts)
	partSize := cfg.MultipartPartSize
	if partSize <= 0 {
		partSize = defaultMultipartPartSize
	}
	concurrency := cfg.MultipartConcurrency
	if concurrency <= 0 {
		concurrency = defaultMultipartConcurrency
	}
	return &AWS{
		api:                  api,
		bucket:               cfg.Bucket,
		prefix:               cfg.Prefix,
		partSize:             partSize,
		multipartConcurrency: concurrency,
	}
}

func (a *AWS) key(k string) string { return joinPrefix(a.prefix, k) }

func (a *AWS) Put(ctx context.Context, key string, body io.Reader, size int64) error {
	_, err := a.api.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(a.bucket),
		Key:           aws.String(a.key(key)),
		Body:          body,
		ContentLength: aws.Int64(size),
	})
	return err
}

func (a *AWS) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := a.api.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(a.bucket), Key: aws.String(a.key(key)),
	})
	if err != nil {
		return nil, err
	}
	return out.Body, nil
}

func (a *AWS) RangeGet(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	if offset < 0 || length <= 0 {
		return nil, fmt.Errorf("s3client: invalid range offset=%d length=%d", offset, length)
	}
	rng := fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	out, err := a.api.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(a.bucket), Key: aws.String(a.key(key)), Range: aws.String(rng),
	})
	if err != nil {
		return nil, err
	}
	return out.Body, nil
}

func (a *AWS) Head(ctx context.Context, key string) (int64, error) {
	out, err := a.api.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(a.bucket), Key: aws.String(a.key(key)),
	})
	if err != nil {
		return 0, err
	}
	if out.ContentLength == nil {
		return 0, nil
	}
	return *out.ContentLength, nil
}

func (a *AWS) Delete(ctx context.Context, key string) error {
	_, err := a.api.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(a.bucket), Key: aws.String(a.key(key)),
	})
	return err
}

// DeleteMany removes keys using S3 DeleteObjects batches.
func (a *AWS) DeleteMany(ctx context.Context, keys []string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	objects := make([]types.ObjectIdentifier, 0, len(keys))
	for _, key := range keys {
		objects = append(objects, types.ObjectIdentifier{Key: aws.String(a.key(key))})
	}
	return a.deleteObjects(ctx, objects, "delete object")
}

// DeleteVersions permanently removes all object versions and delete markers
// under prefix. It is used by cleanup for versioned S3-compatible buckets.
func (a *AWS) DeleteVersions(ctx context.Context, prefix string) (int, error) {
	fullPrefix := a.key(prefix)
	var keyMarker, versionMarker string
	deleted := 0
	for {
		in := &s3.ListObjectVersionsInput{
			Bucket:  aws.String(a.bucket),
			Prefix:  aws.String(fullPrefix),
			MaxKeys: aws.Int32(1000),
		}
		if keyMarker != "" {
			in.KeyMarker = aws.String(keyMarker)
		}
		if versionMarker != "" {
			in.VersionIdMarker = aws.String(versionMarker)
		}
		out, err := a.api.ListObjectVersions(ctx, in)
		if err != nil {
			if isUnsupportedVersionListing(err) {
				return deleted, nil
			}
			return deleted, err
		}
		objects := make([]types.ObjectIdentifier, 0, len(out.Versions)+len(out.DeleteMarkers))
		for _, v := range out.Versions {
			objects = append(objects, types.ObjectIdentifier{Key: v.Key, VersionId: v.VersionId})
		}
		for _, m := range out.DeleteMarkers {
			objects = append(objects, types.ObjectIdentifier{Key: m.Key, VersionId: m.VersionId})
		}
		if len(objects) > 0 {
			n, err := a.deleteObjects(ctx, objects, "delete object version")
			if err != nil {
				return deleted, err
			}
			deleted += n
		}
		if out.IsTruncated == nil || !*out.IsTruncated {
			return deleted, nil
		}
		keyMarker = aws.ToString(out.NextKeyMarker)
		versionMarker = aws.ToString(out.NextVersionIdMarker)
	}
}

func (a *AWS) deleteObjects(ctx context.Context, objects []types.ObjectIdentifier, op string) (int, error) {
	deleted := 0
	for len(objects) > 0 {
		batch := objects
		if len(batch) > maxDeleteObjectsBatch {
			batch = objects[:maxDeleteObjectsBatch]
		}
		n, err := a.deleteObjectBatch(ctx, batch, op)
		deleted += n
		if err != nil {
			return deleted, err
		}
		objects = objects[len(batch):]
	}
	return deleted, nil
}

func (a *AWS) deleteObjectBatch(ctx context.Context, objects []types.ObjectIdentifier, op string) (int, error) {
	out, err := a.api.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(a.bucket),
		Delete: &types.Delete{Objects: objects, Quiet: aws.Bool(true)},
	})
	if err != nil {
		return 0, err
	}
	if len(out.Errors) > 0 {
		e := out.Errors[0]
		return 0, fmt.Errorf("%s %q: %s: %s", op, aws.ToString(e.Key), aws.ToString(e.Code), aws.ToString(e.Message))
	}
	return len(objects), nil
}

func isUnsupportedVersionListing(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	switch apiErr.ErrorCode() {
	case "NotImplemented", "NotSupported", "MethodNotAllowed":
		return true
	default:
		return false
	}
}

func (a *AWS) List(ctx context.Context, prefix, delimiter, token string, maxKeys int32) (*ListResult, error) {
	in := &s3.ListObjectsV2Input{
		Bucket:  aws.String(a.bucket),
		Prefix:  aws.String(a.key(prefix)),
		MaxKeys: aws.Int32(maxKeys),
	}
	if delimiter != "" {
		in.Delimiter = aws.String(delimiter)
	}
	if token != "" {
		in.ContinuationToken = aws.String(token)
	}
	out, err := a.api.ListObjectsV2(ctx, in)
	if err != nil {
		return nil, err
	}
	res := &ListResult{IsTruncated: out.IsTruncated != nil && *out.IsTruncated}
	if out.NextContinuationToken != nil {
		res.NextContinuation = *out.NextContinuationToken
	}
	for _, o := range out.Contents {
		if o.Key != nil {
			res.Keys = append(res.Keys, *o.Key)
		}
	}
	for _, cp := range out.CommonPrefixes {
		if cp.Prefix != nil {
			res.CommonPrefixes = append(res.CommonPrefixes, *cp.Prefix)
		}
	}
	return res, nil
}

func (a *AWS) Copy(ctx context.Context, srcKey, dstKey string) error {
	_, err := a.api.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(a.bucket),
		Key:        aws.String(a.key(dstKey)),
		CopySource: aws.String(a.bucket + "/" + a.key(srcKey)),
	})
	return err
}

func (a *AWS) GetObjectTagging(ctx context.Context, key string) (map[string]string, error) {
	out, err := a.api.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
		Bucket: aws.String(a.bucket), Key: aws.String(a.key(key)),
	})
	if err != nil {
		return nil, err
	}
	tags := map[string]string{}
	for _, t := range out.TagSet {
		if t.Key != nil && t.Value != nil {
			tags[*t.Key] = *t.Value
		}
	}
	return tags, nil
}

func (a *AWS) PutObjectTagging(ctx context.Context, key string, tags map[string]string) error {
	var set []types.Tag
	for k, v := range tags {
		set = append(set, types.Tag{Key: aws.String(k), Value: aws.String(v)})
	}
	_, err := a.api.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
		Bucket:  aws.String(a.bucket),
		Key:     aws.String(a.key(key)),
		Tagging: &types.Tagging{TagSet: set},
	})
	return err
}

func (a *AWS) MultipartUpload(ctx context.Context, key string, body io.Reader, size int64) error {
	if size < 0 {
		return fmt.Errorf("s3client: invalid multipart upload size %d", size)
	}
	if size <= a.partSize {
		return a.Put(ctx, key, body, size)
	}
	fullKey := a.key(key)
	out, err := a.api.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(a.bucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		return err
	}
	uploadID := aws.ToString(out.UploadId)
	if uploadID == "" {
		return errors.New("s3client: create multipart upload returned empty upload id")
	}

	parts, err := a.uploadMultipartParts(ctx, fullKey, uploadID, body, size)
	if err != nil {
		return a.abortMultipartUploadAfterError(ctx, fullKey, uploadID, err)
	}
	_, err = a.api.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(a.bucket),
		Key:      aws.String(fullKey),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: parts,
		},
	})
	if err != nil {
		return a.abortMultipartUploadAfterError(ctx, fullKey, uploadID, err)
	}
	return nil
}

func (a *AWS) uploadMultipartParts(ctx context.Context, key, uploadID string, body io.Reader, size int64) ([]types.CompletedPart, error) {
	readerAt, ok := body.(io.ReaderAt)
	if !ok {
		return a.uploadMultipartPartsSequential(ctx, key, uploadID, body, size)
	}
	partCount := int((size + a.partSize - 1) / a.partSize)
	parts := make([]types.CompletedPart, partCount)
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(a.multipartConcurrency)
	for idx := range partCount {
		idx := idx
		offset := int64(idx) * a.partSize
		length := min(a.partSize, size-offset)
		partNumber := int32(idx + 1)
		g.Go(func() error {
			out, err := a.api.UploadPart(ctx, &s3.UploadPartInput{
				Bucket:        aws.String(a.bucket),
				Key:           aws.String(key),
				UploadId:      aws.String(uploadID),
				PartNumber:    aws.Int32(partNumber),
				Body:          io.NewSectionReader(readerAt, offset, length),
				ContentLength: aws.Int64(length),
			})
			if err != nil {
				return fmt.Errorf("upload part %d: %w", partNumber, err)
			}
			parts[idx] = types.CompletedPart{ETag: out.ETag, PartNumber: aws.Int32(partNumber)}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return parts, nil
}

func (a *AWS) uploadMultipartPartsSequential(ctx context.Context, key, uploadID string, body io.Reader, size int64) ([]types.CompletedPart, error) {
	partCount := int((size + a.partSize - 1) / a.partSize)
	parts := make([]types.CompletedPart, 0, partCount)
	remaining := size
	for partNumber := int32(1); remaining > 0; partNumber++ {
		partSize := min(a.partSize, remaining)
		buf := make([]byte, int(partSize))
		if _, err := io.ReadFull(body, buf); err != nil {
			return nil, fmt.Errorf("read part %d: %w", partNumber, err)
		}
		out, err := a.api.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:        aws.String(a.bucket),
			Key:           aws.String(key),
			UploadId:      aws.String(uploadID),
			PartNumber:    aws.Int32(partNumber),
			Body:          bytes.NewReader(buf),
			ContentLength: aws.Int64(partSize),
		})
		if err != nil {
			return nil, fmt.Errorf("upload part %d: %w", partNumber, err)
		}
		parts = append(parts, types.CompletedPart{ETag: out.ETag, PartNumber: aws.Int32(partNumber)})
		remaining -= partSize
	}
	return parts, nil
}

func (a *AWS) abortMultipartUploadAfterError(ctx context.Context, key, uploadID string, uploadErr error) error {
	abortErr := a.abortMultipartUpload(context.WithoutCancel(ctx), key, uploadID)
	if abortErr != nil {
		return errors.Join(uploadErr, fmt.Errorf("abort multipart upload: %w", abortErr))
	}
	return uploadErr
}

func (a *AWS) abortMultipartUpload(ctx context.Context, key, uploadID string) error {
	_, err := a.api.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(a.bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	})
	return err
}

// IsNotFound reports whether err represents a 404 / NoSuchKey from S3.
func IsNotFound(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrNotFound) {
		return true
	}
	var nk *types.NoSuchKey
	if errors.As(err, &nk) {
		return true
	}
	var respErr *smithyhttp.ResponseError
	if errors.As(err, &respErr) {
		return respErr.HTTPStatusCode() == http.StatusNotFound
	}
	return false
}
