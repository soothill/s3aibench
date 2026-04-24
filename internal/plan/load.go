package plan

import (
	"fmt"
	"os"

	"sigs.k8s.io/yaml"
)

// Load reads and parses a YAML plan file.
func Load(path string) (*Plan, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read plan: %w", err)
	}
	return Parse(b)
}

// Parse unmarshals raw YAML bytes into a Plan.
func Parse(b []byte) (*Plan, error) {
	var p Plan
	if err := yaml.Unmarshal(b, &p); err != nil {
		return nil, fmt.Errorf("parse plan: %w", err)
	}
	return &p, nil
}
