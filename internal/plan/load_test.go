package plan

import (
	"os"
	"path/filepath"
	"testing"
)

const sample = `
name: test
endpoint: https://example
bucket: b
defaults:
  duration: 10m
  threads: 64
  multipart_part_size: 16MiB
workloads:
  - name: small
    type: smallobject
    object_size: 4KiB
    threads: 8
output:
  json: ./out.json
`

func TestLoadAndParse(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "plan.yaml")
	if err := os.WriteFile(path, []byte(sample), 0o600); err != nil {
		t.Fatal(err)
	}
	p, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if p.Name != "test" {
		t.Fatalf("got name %q", p.Name)
	}
	if p.Defaults.Duration.AsDuration().Minutes() != 10 {
		t.Fatalf("got duration %v", p.Defaults.Duration.AsDuration())
	}
	if int64(p.Defaults.MultipartPartSize) != 16*1024*1024 {
		t.Fatalf("got part size %d", p.Defaults.MultipartPartSize)
	}
	if p.Workloads[0].ObjectSize != 4096 {
		t.Fatalf("got obj size %d", p.Workloads[0].ObjectSize)
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("expected error")
	}
}

func TestParseBadYAML(t *testing.T) {
	if _, err := Parse([]byte("bad:\n- [\n")); err == nil {
		t.Fatal("expected error")
	}
}
