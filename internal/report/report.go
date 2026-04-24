// Package report turns a metrics.Snapshot into the PRD §8 text/JSON outputs.
package report

import (
	"time"

	"github.com/darrensoothill/s3aibench/internal/config"
	"github.com/darrensoothill/s3aibench/internal/metrics"
	"github.com/darrensoothill/s3aibench/internal/version"
	"github.com/darrensoothill/s3aibench/pkg/reportschema"
)

// Build assembles a reportschema.Report from a snapshot and resolved config.
func Build(snap metrics.Snapshot, cfg *config.Config) *reportschema.Report {
	r := &reportschema.Report{
		SchemaVersion: reportschema.SchemaVersion,
		Run: reportschema.Run{
			PlanName:    cfg.PlanName,
			ToolVersion: version.String(),
			StartedAt:   snap.Start.UTC().Format(time.RFC3339Nano),
			EndedAt:     snap.End.UTC().Format(time.RFC3339Nano),
			Endpoint:    cfg.Endpoint,
			Bucket:      cfg.Bucket,
			Region:      cfg.Region,
			ConfigHash:  cfg.ConfigHash,
			Sources:     sourcesAsStrings(cfg.Sources),
		},
	}
	dur := snap.End.Sub(snap.Start)
	if dur <= 0 {
		dur = time.Nanosecond
	}
	var errAgg []reportschema.Error
	for wlName, ops := range snap.Workloads {
		wl := reportschema.Workload{
			Name:       wlName,
			Type:       wlName, // workload type reconstructed at a higher layer if needed
			Operations: map[string]*reportschema.Operation{},
		}
		for opName, st := range ops {
			bounds, counts := st.Histogram()
			operation := &reportschema.Operation{
				Count:         st.Count,
				Errors:        st.Errors,
				Bytes:         st.Bytes,
				ThroughputBps: float64(st.Bytes) / dur.Seconds(),
				LatencyNS: reportschema.Latency{
					P50:    st.Percentile(0.50).Nanoseconds(),
					P90:    st.Percentile(0.90).Nanoseconds(),
					P95:    st.Percentile(0.95).Nanoseconds(),
					P99:    st.Percentile(0.99).Nanoseconds(),
					P999:   st.Percentile(0.999).Nanoseconds(),
					Max:    st.LatMax.Nanoseconds(),
					Mean:   st.Mean().Nanoseconds(),
					StdDev: st.StdDev().Nanoseconds(),
					Histogram: reportschema.Histogram{BucketsNS: bounds, Counts: counts},
				},
			}
			for code, agg := range st.ErrByCode {
				errAgg = append(errAgg, reportschema.Error{
					Op: string(opName), Code: code,
					Count: agg.Count, SampleMessage: agg.SampleMessage,
				})
			}
			wl.Operations[string(opName)] = operation
		}
		r.Workloads = append(r.Workloads, wl)
	}
	r.Errors = errAgg
	return r
}

func sourcesAsStrings(in map[string]config.Source) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = string(v)
	}
	return out
}
