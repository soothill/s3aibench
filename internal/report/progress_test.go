package report

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/darrensoothill/s3aibench/internal/metrics"
)

func TestTickerEmits(t *testing.T) {
	c := metrics.NewCollector()
	c.Record("w", metrics.OpPut, 10*time.Millisecond, 1024, nil)
	c.Record("w", metrics.OpPut, 10*time.Millisecond, 1024, nil)
	var buf bytes.Buffer
	tk := &Ticker{Recorder: c, Interval: 5 * time.Millisecond, Out: &buf}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	tk.Run(ctx)
	if !strings.Contains(buf.String(), "ops=2") {
		t.Fatalf("missing ops: %s", buf.String())
	}
}

func TestTickerDefaultInterval(t *testing.T) {
	c := metrics.NewCollector()
	var buf bytes.Buffer
	tk := &Ticker{Recorder: c, Out: &buf} // zero interval triggers default
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	tk.Run(ctx)
}

func TestEmitZeroDurationGuard(t *testing.T) {
	c := metrics.NewCollector()
	// Replace the snapshot start/end so duration == 0; exercises nanosecond guard.
	var buf bytes.Buffer
	tk := &Ticker{Recorder: &fixedRecorder{totals: metrics.Totals{Start: time.Unix(0, 0), End: time.Unix(0, 0)}}, Out: &buf}
	tk.emit()
	if !strings.Contains(buf.String(), "ops=0") {
		t.Fatalf("expected ops=0, got %s", buf.String())
	}
	_ = c
}

type fixedRecorder struct {
	snap   metrics.Snapshot
	totals metrics.Totals
}

func (f *fixedRecorder) Record(string, metrics.Op, time.Duration, int64, error) {}
func (f *fixedRecorder) Snapshot() metrics.Snapshot                             { return f.snap }
func (f *fixedRecorder) Totals() metrics.Totals                                 { return f.totals }
func (f *fixedRecorder) ShardFor(int) metrics.Recorder                          { return f }

func TestSparkline(t *testing.T) {
	if Sparkline(nil) != "" {
		t.Fatal("empty should render empty")
	}
	s := Sparkline([]int64{1, 2, 4, 8, 0})
	if len(s) == 0 {
		t.Fatal("non-empty values should render")
	}
}

func TestHumanBPS(t *testing.T) {
	cases := []struct {
		in   float64
		want string
	}{
		{100, "100 B/s"},
		{2_000, "2.00 kB/s"},
		{5_000_000, "5.00 MB/s"},
		{7e9, "7.00 GB/s"},
		{-2000, "-2.00 kB/s"},
	}
	for _, c := range cases {
		if got := humanBPS(c.in); got != c.want {
			t.Errorf("humanBPS(%v)=%q want %q", c.in, got, c.want)
		}
	}
}
