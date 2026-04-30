// Package report turns a metrics.Snapshot into the PRD §8 text/JSON outputs.
package report

import (
	"sort"
	"time"

	"github.com/darrensoothill/s3aibench/internal/config"
	"github.com/darrensoothill/s3aibench/internal/metrics"
	"github.com/darrensoothill/s3aibench/internal/plan"
	"github.com/darrensoothill/s3aibench/internal/version"
	"github.com/darrensoothill/s3aibench/pkg/reportschema"
)

// Build assembles a reportschema.Report from a snapshot and resolved config.
func Build(snap metrics.Snapshot, cfg *config.Config) *reportschema.Report {
	workloadTypes := mapWorkloadTypes(cfg.Workloads)
	workloadDurations := mapWorkloadDurations(cfg.Workloads, cfg.Duration)
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
			Type:       workloadTypeFor(workloadTypes, wlName),
			Operations: map[string]*reportschema.Operation{},
		}
		opDur := dur
		if d := workloadDurations[wlName]; d > 0 {
			opDur = d
		}
		for opName, st := range ops {
			bounds, counts := st.Histogram()
			operation := &reportschema.Operation{
				Count:         st.Count,
				Errors:        st.Errors,
				Bytes:         st.Bytes,
				ThroughputBps: float64(st.Bytes) / opDur.Seconds(),
				LatencyNS: reportschema.Latency{
					P50:       st.Percentile(0.50).Nanoseconds(),
					P90:       st.Percentile(0.90).Nanoseconds(),
					P95:       st.Percentile(0.95).Nanoseconds(),
					P99:       st.Percentile(0.99).Nanoseconds(),
					P999:      st.Percentile(0.999).Nanoseconds(),
					Max:       st.LatMax.Nanoseconds(),
					Mean:      st.Mean().Nanoseconds(),
					StdDev:    st.StdDev().Nanoseconds(),
					Histogram: reportschema.Histogram{BucketsNS: bounds, Counts: counts},
				},
			}
			if cfg.Timeline {
				operation.Timeline = buildTimeline(st.Timeline)
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

func workloadTypeFor(types map[string]string, name string) string {
	if typ := types[name]; typ != "" {
		return typ
	}
	return name
}

func mapWorkloadTypes(wls []plan.Workload) map[string]string {
	out := map[string]string{}
	walk := func(items []plan.Workload) {
		for _, w := range items {
			out[w.Name] = w.Type
			for _, child := range nestedWorkloads(w) {
				out[child.Name] = child.Type
			}
		}
	}
	walk(wls)
	return out
}

func mapWorkloadDurations(wls []plan.Workload, def time.Duration) map[string]time.Duration {
	out := map[string]time.Duration{}
	for _, w := range wls {
		d := w.Duration.AsDuration()
		if d == 0 {
			d = def
		}
		out[w.Name] = d
		for _, child := range nestedWorkloads(w) {
			out[child.Name] = d
		}
	}
	return out
}

func nestedWorkloads(w plan.Workload) []plan.Workload {
	raw, ok := w.Params["workloads"].([]interface{})
	if !ok {
		return nil
	}
	children := make([]plan.Workload, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		name, _ := m["name"].(string)
		typ, _ := m["type"].(string)
		if name == "" || typ == "" {
			continue
		}
		children = append(children, plan.Workload{Name: name, Type: typ})
	}
	return children
}

func buildTimeline(in map[int64]metrics.TimelineBucket) []reportschema.TimelineBucket {
	if len(in) == 0 {
		return nil
	}
	keys := make([]int64, 0, len(in))
	for sec := range in {
		keys = append(keys, sec)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	out := make([]reportschema.TimelineBucket, 0, len(keys))
	for _, sec := range keys {
		b := in[sec]
		out = append(out, reportschema.TimelineBucket{
			Second: b.Second,
			Ops:    b.Ops,
			Bytes:  b.Bytes,
			Errors: b.Errors,
		})
	}
	return out
}

func sourcesAsStrings(in map[string]config.Source) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = string(v)
	}
	return out
}
