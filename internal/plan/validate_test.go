package plan

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func good() *Plan {
	return &Plan{
		Endpoint:  "https://example",
		Bucket:    "b",
		Workloads: []Workload{{Name: "w1", Type: "smallobject"}},
	}
}

func TestValidateOK(t *testing.T) {
	if err := Validate(good(), slog.Default()); err != nil {
		t.Fatal(err)
	}
}

func TestValidateNil(t *testing.T) {
	if err := Validate(nil, nil); err == nil {
		t.Fatal("expected error")
	}
}

func TestValidateErrors(t *testing.T) {
	cases := []struct {
		name string
		mod  func(*Plan)
	}{
		{"no endpoint", func(p *Plan) { p.Endpoint = "" }},
		{"no bucket", func(p *Plan) { p.Bucket = "" }},
		{"no workloads", func(p *Plan) { p.Workloads = nil }},
		{"missing workload name", func(p *Plan) { p.Workloads[0].Name = "" }},
		{"missing workload type", func(p *Plan) { p.Workloads[0].Type = "" }},
		{"duplicate workload name", func(p *Plan) {
			p.Workloads = append(p.Workloads, Workload{Name: "w1", Type: "smallobject"})
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := good()
			c.mod(p)
			if err := Validate(p, nil); err == nil {
				t.Fatalf("expected error for %s", c.name)
			}
		})
	}
}

func TestValidateInlineCredsWarns(t *testing.T) {
	p := good()
	p.AccessKey = "AKIA"
	p.SecretKey = "secret"
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	if err := Validate(p, logger); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "inline credentials") {
		t.Fatalf("expected warn, got %q", buf.String())
	}
}
