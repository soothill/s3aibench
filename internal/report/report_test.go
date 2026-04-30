package report

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/darrensoothill/s3aibench/internal/config"
	"github.com/darrensoothill/s3aibench/internal/metrics"
	"github.com/darrensoothill/s3aibench/internal/plan"
	"github.com/darrensoothill/s3aibench/pkg/reportschema"
)

func mkSnap() metrics.Snapshot {
	c := metrics.NewCollector()
	c.Record("w", metrics.OpPut, 10*time.Millisecond, 1024, nil)
	c.Record("w", metrics.OpPut, 20*time.Millisecond, 1024, errors.New("boom"))
	return c.Snapshot()
}

func mkCfg() *config.Config {
	return &config.Config{
		PlanName: "plan", Endpoint: "https://e", Bucket: "b", Region: "r",
		ConfigHash: "sha256:abc",
		Sources:    map[string]config.Source{"endpoint": config.SourceEnv},
	}
}

func TestBuild(t *testing.T) {
	cfg := mkCfg()
	cfg.Workloads = []plan.Workload{{Name: "w", Type: "smallobject"}}
	r := Build(mkSnap(), cfg)
	if r.SchemaVersion != reportschema.SchemaVersion {
		t.Fatal("bad schema version")
	}
	if r.Run.Sources["endpoint"] != "env" {
		t.Fatalf("sources=%v", r.Run.Sources)
	}
	if len(r.Workloads) != 1 {
		t.Fatalf("workloads=%d", len(r.Workloads))
	}
	if len(r.Errors) == 0 {
		t.Fatal("expected errors")
	}
	if r.Workloads[0].Type != "smallobject" {
		t.Fatalf("type=%q", r.Workloads[0].Type)
	}
}

func TestBuildZeroDuration(t *testing.T) {
	// Explicitly-equal Start/End triggers the dur<=0 guard.
	t0 := time.Unix(0, 0)
	// Build a snapshot via a Collector to exercise the real HDR path.
	c := metrics.NewCollector()
	c.Record("w", metrics.OpPut, 5*time.Millisecond, 10, nil)
	snap := c.Snapshot()
	snap.Start, snap.End = t0, t0
	r := Build(snap, mkCfg())
	op := r.Workloads[0].Operations["put"]
	if op.ThroughputBps == 0 {
		t.Fatal("expected nonzero throughput under zero-duration guard")
	}
}

func TestWriteTextAndJSON(t *testing.T) {
	cfg := mkCfg()
	cfg.Timeline = true
	r := Build(mkSnap(), cfg)
	var txt, js bytes.Buffer
	if err := WriteText(&txt, r); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(txt.String(), "workload: w") {
		t.Fatalf("text: %s", txt.String())
	}
	if !strings.Contains(txt.String(), "top errors") {
		t.Fatalf("missing errors section: %s", txt.String())
	}
	if !strings.Contains(txt.String(), "p999=") {
		t.Fatalf("missing expanded latency fields: %s", txt.String())
	}
	if err := WriteJSON(&js, r); err != nil {
		t.Fatal(err)
	}
	var back reportschema.Report
	if err := json.Unmarshal(js.Bytes(), &back); err != nil {
		t.Fatal(err)
	}
	if back.SchemaVersion != r.SchemaVersion {
		t.Fatal("round trip mismatch")
	}
}

func TestBuildTimelineAndNestedWorkloadType(t *testing.T) {
	c := metrics.NewCollector()
	c.Record("child", metrics.OpGet, time.Millisecond, 1, nil)
	cfg := mkCfg()
	cfg.Timeline = true
	cfg.Duration = time.Second
	cfg.Workloads = []plan.Workload{{
		Name: "mix", Type: "mix", Params: map[string]interface{}{
			"workloads": []interface{}{
				map[string]interface{}{"name": "child", "type": "training_data"},
			},
		},
	}}
	r := Build(c.Snapshot(), cfg)
	if r.Workloads[0].Type != "training_data" {
		t.Fatalf("type=%q", r.Workloads[0].Type)
	}
	if len(r.Workloads[0].Operations["get"].Timeline) == 0 {
		t.Fatal("expected timeline")
	}
}

func TestNestedWorkloadsSkipsInvalidItems(t *testing.T) {
	w := plan.Workload{Params: map[string]interface{}{
		"workloads": []interface{}{
			"bad",
			map[string]interface{}{"name": "", "type": "x"},
			map[string]interface{}{"name": "ok", "type": "smallobject"},
		},
	}}
	got := nestedWorkloads(w)
	if len(got) != 1 || got[0].Name != "ok" {
		t.Fatalf("nested=%+v", got)
	}
}

func TestBuildTimelineEmpty(t *testing.T) {
	if got := buildTimeline(nil); got != nil {
		t.Fatalf("expected nil, got %+v", got)
	}
}

func TestBuildTimelineSorts(t *testing.T) {
	got := buildTimeline(map[int64]metrics.TimelineBucket{
		2: {Second: 2, Ops: 2},
		1: {Second: 1, Ops: 1},
	})
	if got[0].Second != 1 || got[1].Second != 2 {
		t.Fatalf("not sorted: %+v", got)
	}
}

func TestWriteTextNoErrorsSection(t *testing.T) {
	r := &reportschema.Report{SchemaVersion: "1.0.0", Workloads: []reportschema.Workload{{Name: "w", Type: "t", Operations: map[string]*reportschema.Operation{"put": {Count: 1}}}}}
	var buf bytes.Buffer
	if err := WriteText(&buf, r); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(buf.String(), "top errors") {
		t.Fatal("unexpected errors section")
	}
}

func TestWriteTextSortsAndTruncatesErrors(t *testing.T) {
	errs := make([]reportschema.Error, 0, 12)
	for i := 0; i < 12; i++ {
		errs = append(errs, reportschema.Error{Op: "op", Code: string(rune('a' + i)), Count: int64(i)})
	}
	errs = append(errs, reportschema.Error{Op: "aa", Code: "z", Count: 5})
	errs = append(errs, reportschema.Error{Op: "same", Code: "b", Count: 50})
	errs = append(errs, reportschema.Error{Op: "same", Code: "a", Count: 50})
	r := &reportschema.Report{
		SchemaVersion: "1.0.0",
		Errors:        errs,
		Workloads: []reportschema.Workload{{
			Name: "w", Type: "t",
			Operations: map[string]*reportschema.Operation{
				"put": {Count: 1, Timeline: []reportschema.TimelineBucket{{Second: 0, Ops: 1}}},
			},
		}},
	}
	var buf bytes.Buffer
	if err := WriteText(&buf, r); err != nil {
		t.Fatal(err)
	}
	if strings.Count(buf.String(), "op=") != 10 {
		t.Fatalf("expected 10 top errors: %s", buf.String())
	}
	if !strings.Contains(buf.String(), "timeline=") {
		t.Fatalf("missing timeline: %s", buf.String())
	}
}

func TestWriteTextTimelineWriterError(t *testing.T) {
	r := &reportschema.Report{SchemaVersion: "1.0.0", Workloads: []reportschema.Workload{{
		Name: "w", Type: "t",
		Operations: map[string]*reportschema.Operation{
			"put": {Count: 1, Timeline: []reportschema.TimelineBucket{{Second: 0, Ops: 1}}},
		},
	}}}
	counter := &failAtWriter{failOn: 99999}
	_ = WriteText(counter, r)
	if err := WriteText(&failAtWriter{failOn: counter.calls}, r); err == nil {
		t.Fatal("expected timeline write error")
	}
}

// failAtWriter errors on the Nth Write call (1-indexed). Each call to Fprintf
// produces at least one Write, so this lets us target individual statements.
type failAtWriter struct {
	failOn int
	calls  int
}

func (f *failAtWriter) Write(p []byte) (int, error) {
	f.calls++
	if f.calls >= f.failOn {
		return 0, io.ErrShortWrite
	}
	return len(p), nil
}

func TestWriteTextWriterErrors(t *testing.T) {
	r := Build(mkSnap(), mkCfg())
	// Run WriteText once to count the number of Write calls it makes.
	counter := &failAtWriter{failOn: 99999}
	_ = WriteText(counter, r)
	totalWrites := counter.calls
	if totalWrites < 6 {
		t.Fatalf("unexpected write count %d", totalWrites)
	}
	// Fail on every Write call from 1..total and require an error each time.
	for i := 1; i <= totalWrites; i++ {
		if err := WriteText(&failAtWriter{failOn: i}, r); err == nil {
			t.Fatalf("failOn=%d: expected error", i)
		}
	}
}

func TestWriteJSONWriterError(t *testing.T) {
	r := Build(mkSnap(), mkCfg())
	if err := WriteJSON(&failAtWriter{failOn: 1}, r); err == nil {
		t.Fatal("expected error")
	}
}
