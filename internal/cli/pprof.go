package cli

import (
	"context"
	"net/http"
	_ "net/http/pprof" // side-effect: registers pprof handlers on DefaultServeMux
)

// startPprof spins up net/http/pprof on `addr` if non-empty. The listener is
// closed when ctx is canceled. Any listener error is logged via the provided
// logger rather than returned — the benchmark run should never abort because
// pprof couldn't bind.
var startPprof = func(ctx context.Context, addr string, logErr func(error)) {
	if addr == "" {
		return
	}
	srv := &http.Server{Addr: addr, Handler: http.DefaultServeMux}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logErr(err)
		}
	}()
}
