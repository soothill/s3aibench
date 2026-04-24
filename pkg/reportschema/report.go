// Package reportschema defines the stable, public JSON report emitted by s3aibench.
// Downstream CI gates and dashboards import these structs to parse reports without
// vendoring the whole tool.
package reportschema

const SchemaVersion = "1.0.0"

type Report struct {
	SchemaVersion string     `json:"schema_version"`
	Run           Run        `json:"run"`
	Workloads     []Workload `json:"workloads"`
	Errors        []Error    `json:"errors,omitempty"`
}

type Run struct {
	PlanName    string            `json:"plan_name"`
	ToolVersion string            `json:"tool_version"`
	StartedAt   string            `json:"started_at"`
	EndedAt     string            `json:"ended_at"`
	Endpoint    string            `json:"endpoint"`
	Bucket      string            `json:"bucket"`
	Region      string            `json:"region,omitempty"`
	ConfigHash  string            `json:"config_hash"`
	Sources     map[string]string `json:"config_sources,omitempty"`
}

type Workload struct {
	Name       string                `json:"name"`
	Type       string                `json:"type"`
	Operations map[string]*Operation `json:"operations"`
}

type Operation struct {
	Count         int64   `json:"count"`
	Errors        int64   `json:"errors"`
	Bytes         int64   `json:"bytes"`
	ThroughputBps float64 `json:"throughput_bps"`
	LatencyNS     Latency `json:"latency_ns"`
}

type Latency struct {
	P50       int64     `json:"p50"`
	P90       int64     `json:"p90"`
	P95       int64     `json:"p95"`
	P99       int64     `json:"p99"`
	P999      int64     `json:"p999"`
	Max       int64     `json:"max"`
	Mean      int64     `json:"mean"`
	StdDev    int64     `json:"stddev"`
	Histogram Histogram `json:"histogram"`
}

type Histogram struct {
	BucketsNS []int64 `json:"buckets_ns"`
	Counts    []int64 `json:"counts"`
}

type Error struct {
	Op            string `json:"op"`
	Code          string `json:"code"`
	Count         int64  `json:"count"`
	SampleMessage string `json:"sample_message,omitempty"`
}
