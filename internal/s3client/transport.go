package s3client

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"
)

// TransportOptions mirrors the subset of http.Transport tuning the tool
// exposes to users. Workloads never construct this directly; the CLI
// translates resolved config into it.
type TransportOptions struct {
	MaxIdleConnsPerHost int
	TLSSkipVerify       bool
	ForceHTTP2          bool
}

// NewTransport builds an http.Transport tuned for high concurrency S3 traffic.
// HTTP/2 is off by default (most S3 endpoints are HTTP/1.1); keepalives enabled;
// no cap on total connections below the process ulimit.
func NewTransport(opt TransportOptions) *http.Transport {
	maxIdle := opt.MaxIdleConnsPerHost
	if maxIdle <= 0 {
		maxIdle = 4096
	}
	dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	return &http.Transport{
		MaxIdleConns:        0,
		MaxIdleConnsPerHost: maxIdle,
		MaxConnsPerHost:     0,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
		ForceAttemptHTTP2:   opt.ForceHTTP2,
		DialContext:         dialer.DialContext,
		WriteBufferSize:     256 << 10,
		ReadBufferSize:      256 << 10,
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: opt.TLSSkipVerify}, //nolint:gosec // tool-level opt-in
	}
}
