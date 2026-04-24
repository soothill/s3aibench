// Package fake is an in-memory implementation of s3client.Client, used by
// workload unit tests so we can exercise every branch without a real S3.
package fake

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/darrensoothill/s3aibench/internal/s3client"
)

// Client is a thread-safe in-memory S3. Supports fault injection for negative
// tests and artificial latency so callers can exercise timing behaviour.
type Client struct {
	mu      sync.Mutex
	objects map[string][]byte
	tags    map[string]map[string]string
	fail    map[string]error
	delay   time.Duration
}

// New returns an empty Client.
func New() *Client {
	return &Client{
		objects: map[string][]byte{},
		tags:    map[string]map[string]string{},
		fail:    map[string]error{},
	}
}

// FailOp schedules the given error for the next call to the named op.
// Op names match the s3client.Op constants.
func (c *Client) FailOp(op string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.fail[op] = err
}

// SetDelay adds artificial latency to every operation.
func (c *Client) SetDelay(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.delay = d
}

// Objects returns a snapshot of the key→size map.
func (c *Client) Objects() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[string]int{}
	for k, v := range c.objects {
		out[k] = len(v)
	}
	return out
}

func (c *Client) checkFail(op string) error {
	if err, ok := c.fail[op]; ok {
		delete(c.fail, op)
		return err
	}
	return nil
}

func (c *Client) pause() {
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
}

func (c *Client) Put(ctx context.Context, key string, body io.Reader, size int64) error {
	c.mu.Lock()
	if err := c.checkFail("put"); err != nil {
		c.mu.Unlock()
		return err
	}
	c.mu.Unlock()
	c.pause()
	buf := &bytes.Buffer{}
	if _, err := io.Copy(buf, body); err != nil {
		return err
	}
	if size >= 0 && int64(buf.Len()) != size {
		return fmt.Errorf("fake: body size mismatch (got %d want %d)", buf.Len(), size)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.objects[key] = buf.Bytes()
	return nil
}

func (c *Client) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	c.mu.Lock()
	if err := c.checkFail("get"); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	b, ok := c.objects[key]
	c.mu.Unlock()
	c.pause()
	if !ok {
		return nil, ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (c *Client) RangeGet(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	c.mu.Lock()
	if err := c.checkFail("range_get"); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	b, ok := c.objects[key]
	c.mu.Unlock()
	c.pause()
	if !ok {
		return nil, ErrNotFound
	}
	end := offset + length
	if end > int64(len(b)) {
		end = int64(len(b))
	}
	if offset > int64(len(b)) {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	return io.NopCloser(bytes.NewReader(b[offset:end])), nil
}

func (c *Client) Head(ctx context.Context, key string) (int64, error) {
	c.mu.Lock()
	if err := c.checkFail("head"); err != nil {
		c.mu.Unlock()
		return 0, err
	}
	b, ok := c.objects[key]
	c.mu.Unlock()
	c.pause()
	if !ok {
		return 0, ErrNotFound
	}
	return int64(len(b)), nil
}

func (c *Client) Delete(ctx context.Context, key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkFail("delete"); err != nil {
		return err
	}
	delete(c.objects, key)
	return nil
}

func (c *Client) List(ctx context.Context, prefix, delimiter, token string, maxKeys int32) (*s3client.ListResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkFail("list"); err != nil {
		return nil, err
	}
	var keys []string
	for k := range c.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	if token != "" {
		i := sort.SearchStrings(keys, token)
		keys = keys[i:]
	}
	if maxKeys <= 0 {
		maxKeys = 1000
	}
	res := &s3client.ListResult{}
	if delimiter != "" {
		seen := map[string]bool{}
		var flat []string
		for _, k := range keys {
			rest := strings.TrimPrefix(k, prefix)
			if idx := strings.Index(rest, delimiter); idx >= 0 {
				cp := prefix + rest[:idx+len(delimiter)]
				if !seen[cp] {
					seen[cp] = true
					res.CommonPrefixes = append(res.CommonPrefixes, cp)
				}
				continue
			}
			flat = append(flat, k)
		}
		keys = flat
	}
	if int32(len(keys)) > maxKeys {
		res.Keys = keys[:maxKeys]
		res.NextContinuation = keys[maxKeys]
		res.IsTruncated = true
	} else {
		res.Keys = keys
	}
	return res, nil
}

func (c *Client) Copy(ctx context.Context, src, dst string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkFail("copy"); err != nil {
		return err
	}
	b, ok := c.objects[src]
	if !ok {
		return ErrNotFound
	}
	out := make([]byte, len(b))
	copy(out, b)
	c.objects[dst] = out
	return nil
}

func (c *Client) GetObjectTagging(ctx context.Context, key string) (map[string]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkFail("get_tagging"); err != nil {
		return nil, err
	}
	t, ok := c.tags[key]
	if !ok {
		return map[string]string{}, nil
	}
	out := map[string]string{}
	for k, v := range t {
		out[k] = v
	}
	return out, nil
}

func (c *Client) PutObjectTagging(ctx context.Context, key string, tags map[string]string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkFail("put_tagging"); err != nil {
		return err
	}
	t := map[string]string{}
	for k, v := range tags {
		t[k] = v
	}
	c.tags[key] = t
	return nil
}

func (c *Client) MultipartUpload(ctx context.Context, key string, body io.Reader, size int64) error {
	c.mu.Lock()
	if err := c.checkFail("multipart_complete"); err != nil {
		c.mu.Unlock()
		return err
	}
	c.mu.Unlock()
	c.pause()
	buf := &bytes.Buffer{}
	if _, err := io.Copy(buf, body); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.objects[key] = buf.Bytes()
	return nil
}

// ErrNotFound exposes the shared missing-object sentinel for tests.
var ErrNotFound = s3client.ErrNotFound
