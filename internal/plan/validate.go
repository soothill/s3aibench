package plan

import (
	"errors"
	"fmt"
	"log/slog"
)

// Validate returns nil if the plan has the minimum required fields. It also
// emits a loud WARN via logger (non-fatal) when credentials are embedded
// inline — per product decision, the tool warns but never refuses.
func Validate(p *Plan, logger *slog.Logger) error {
	if p == nil {
		return errors.New("plan is nil")
	}
	if p.Endpoint == "" {
		return errors.New("plan missing endpoint")
	}
	if p.Bucket == "" {
		return errors.New("plan missing bucket")
	}
	if len(p.Workloads) == 0 {
		return errors.New("plan must declare at least one workload")
	}
	seen := map[string]bool{}
	for i, w := range p.Workloads {
		if w.Name == "" {
			return fmt.Errorf("workload[%d] missing name", i)
		}
		if seen[w.Name] {
			return fmt.Errorf("duplicate workload name %q", w.Name)
		}
		seen[w.Name] = true
		if w.Type == "" {
			return fmt.Errorf("workload %q missing type", w.Name)
		}
	}
	if logger != nil && (p.AccessKey != "" || p.SecretKey != "") {
		logger.Warn("plan contains inline credentials; prefer AWS SDK environment variables or a credentials file",
			"plan_name", p.Name)
	}
	return nil
}
