package report

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/darrensoothill/s3aibench/internal/metrics"
)

// Ticker prints a compact one-line live status every `Interval` while the
// context is open. It reads the collector's Snapshot each tick and writes
// cumulative counts/throughput. Cheap: driven by a single goroutine.
type Ticker struct {
	Recorder metrics.Recorder
	Interval time.Duration
	Out      io.Writer
}

// Run blocks until ctx is done, printing one status line per Interval.
func (t *Ticker) Run(ctx context.Context) {
	if t.Interval <= 0 {
		t.Interval = time.Second
	}
	tick := time.NewTicker(t.Interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			t.emit()
		}
	}
}

func (t *Ticker) emit() {
	totals := t.Recorder.Totals()
	dur := totals.End.Sub(totals.Start)
	if dur <= 0 {
		dur = time.Nanosecond
	}
	fmt.Fprintf(t.Out, "progress t=%s ops=%d err=%d throughput=%s\n",
		dur.Truncate(100*time.Millisecond), totals.Count, totals.Errors, humanBPS(float64(totals.Bytes)/dur.Seconds()))
}

// Sparkline renders a one-line ASCII sparkline from a throughput timeline.
// values is ops-per-bucket; the returned string has len(values) runes drawn
// from a fixed block ramp.
func Sparkline(values []int64) string {
	if len(values) == 0 {
		return ""
	}
	ramp := []rune("▁▂▃▄▅▆▇█")
	var max int64 = 1
	for _, v := range values {
		if v > max {
			max = v
		}
	}
	var b strings.Builder
	for _, v := range values {
		idx := int(int64(len(ramp)-1) * v / max)
		b.WriteRune(ramp[idx])
	}
	return b.String()
}

// humanBPS formats bytes-per-second with SI scaling.
func humanBPS(bps float64) string {
	abs := bps
	if abs < 0 {
		abs = -abs
	}
	switch {
	case abs >= 1e9:
		return fmt.Sprintf("%.2f GB/s", bps/1e9)
	case abs >= 1e6:
		return fmt.Sprintf("%.2f MB/s", bps/1e6)
	case abs >= 1e3:
		return fmt.Sprintf("%.2f kB/s", bps/1e3)
	}
	return fmt.Sprintf("%.0f B/s", bps)
}
