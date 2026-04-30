// Package plan defines the on-disk YAML plan schema and validation rules.
// The plan is the user-authored description of what to benchmark; it is merged
// with env vars and CLI flags in internal/config to produce a resolved Config.
package plan

// Plan is the top-level YAML shape.
type Plan struct {
	Name           string     `json:"name,omitempty"`
	Endpoint       string     `json:"endpoint,omitempty"`
	Region         string     `json:"region,omitempty"`
	Bucket         string     `json:"bucket,omitempty"`
	AccessKey      string     `json:"access_key,omitempty"`
	SecretKey      string     `json:"secret_key,omitempty"`
	PathStyle      *bool      `json:"path_style,omitempty"`
	TLSSkipVerify  *bool      `json:"tls_skip_verify,omitempty"`
	ConnectionPool int        `json:"connection_pool_size,omitempty"`
	HTTP2          *bool      `json:"http2,omitempty"`
	Prepopulate    *bool      `json:"prepopulate,omitempty"`
	Cleanup        *bool      `json:"cleanup,omitempty"`
	RandomSeed     int64      `json:"random_seed,omitempty"`
	Defaults       Defaults   `json:"defaults,omitempty"`
	Workloads      []Workload `json:"workloads,omitempty"`
	Output         Output     `json:"output,omitempty"`
}

type Defaults struct {
	Duration             Duration `json:"duration,omitempty"`
	Warmup               Duration `json:"warmup,omitempty"`
	Threads              int      `json:"threads,omitempty"`
	MultipartPartSize    Size     `json:"multipart_part_size,omitempty"`
	MultipartConcurrency int      `json:"multipart_concurrency,omitempty"`
}

type Workload struct {
	Name       string                 `json:"name"`
	Type       string                 `json:"type"`
	Weight     int                    `json:"weight,omitempty"`
	ObjectSize Size                   `json:"object_size,omitempty"`
	Threads    int                    `json:"threads,omitempty"`
	Duration   Duration               `json:"duration,omitempty"`
	Params     map[string]interface{} `json:"params,omitempty"`
}

type Output struct {
	Text             string   `json:"text,omitempty"`
	JSON             string   `json:"json,omitempty"`
	Progress         bool     `json:"progress,omitempty"`
	ProgressInterval Duration `json:"progress_interval,omitempty"`
	Timeline         bool     `json:"timeline,omitempty"`
}
