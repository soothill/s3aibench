package report

import (
	"fmt"
	"io"
	"sort"

	"github.com/darrensoothill/s3aibench/pkg/reportschema"
)

// WriteText emits a human-readable plain-text report.
func WriteText(w io.Writer, r *reportschema.Report) error {
	if _, err := fmt.Fprintf(w, "# s3aibench report (schema %s)\n", r.SchemaVersion); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "plan:       %s\n", r.Run.PlanName); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "version:    %s\n", r.Run.ToolVersion); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "window:     %s -> %s\n", r.Run.StartedAt, r.Run.EndedAt); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "endpoint:   %s (bucket=%s region=%s)\n", r.Run.Endpoint, r.Run.Bucket, r.Run.Region); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "configHash: %s\n\n", r.Run.ConfigHash); err != nil {
		return err
	}
	for _, wl := range r.Workloads {
		if _, err := fmt.Fprintf(w, "## workload: %s (%s)\n", wl.Name, wl.Type); err != nil {
			return err
		}
		opNames := make([]string, 0, len(wl.Operations))
		for op := range wl.Operations {
			opNames = append(opNames, op)
		}
		sort.Strings(opNames)
		for _, op := range opNames {
			o := wl.Operations[op]
			if _, err := fmt.Fprintf(w, "  %-20s count=%d errors=%d bytes=%d throughput=%.1fB/s p50=%dns p99=%dns max=%dns\n",
				op, o.Count, o.Errors, o.Bytes, o.ThroughputBps,
				o.LatencyNS.P50, o.LatencyNS.P99, o.LatencyNS.Max); err != nil {
				return err
			}
		}
	}
	if len(r.Errors) > 0 {
		if _, err := fmt.Fprintln(w, "\n## top errors"); err != nil {
			return err
		}
		for _, e := range r.Errors {
			if _, err := fmt.Fprintf(w, "  %-20s op=%s count=%d msg=%s\n", e.Code, e.Op, e.Count, e.SampleMessage); err != nil {
				return err
			}
		}
	}
	return nil
}
