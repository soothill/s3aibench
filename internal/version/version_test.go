package version

import (
	"strings"
	"testing"
)

func TestString(t *testing.T) {
	if !strings.Contains(String(), Version) {
		t.Fatalf("String() missing version: %s", String())
	}
	if !strings.Contains(String(), GitSHA) {
		t.Fatalf("String() missing git sha: %s", String())
	}
}
