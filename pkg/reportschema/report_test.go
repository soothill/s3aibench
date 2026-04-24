package reportschema

import (
	"encoding/json"
	"testing"
)

func TestSchemaVersion(t *testing.T) {
	if SchemaVersion != "1.0.0" {
		t.Fatalf("unexpected schema version %q", SchemaVersion)
	}
}

func TestReportRoundTrip(t *testing.T) {
	r := Report{
		SchemaVersion: SchemaVersion,
		Run: Run{
			PlanName:    "p",
			ToolVersion: "dev",
			StartedAt:   "2026-04-23T10:00:00Z",
			EndedAt:     "2026-04-23T10:10:00Z",
			Endpoint:    "https://example",
			Bucket:      "b",
			ConfigHash:  "sha256:abc",
			Sources:     map[string]string{"endpoint": "flag"},
		},
		Workloads: []Workload{{
			Name: "n", Type: "smallobject",
			Operations: map[string]*Operation{
				"put": {Count: 1, Bytes: 2, LatencyNS: Latency{P50: 1, Histogram: Histogram{BucketsNS: []int64{1}, Counts: []int64{1}}}},
			},
		}},
		Errors: []Error{{Op: "put", Code: "SlowDown", Count: 1, SampleMessage: "slow"}},
	}
	b, err := json.Marshal(&r)
	if err != nil {
		t.Fatal(err)
	}
	var got Report
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got.SchemaVersion != r.SchemaVersion {
		t.Fatalf("round trip mismatch")
	}
	if got.Workloads[0].Operations["put"].Count != 1 {
		t.Fatalf("ops mismatch")
	}
	if got.Errors[0].Code != "SlowDown" {
		t.Fatalf("err mismatch")
	}
}
