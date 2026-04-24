package plan

import (
	"encoding/json"
	"testing"
	"time"
)

func TestSizeUnmarshalJSON(t *testing.T) {
	cases := []struct {
		in   string
		want int64
		err  bool
	}{
		{`"16MiB"`, 16 * 1024 * 1024, false},
		{`"1GiB"`, 1024 * 1024 * 1024, false},
		{`"1024"`, 1024, false},
		{`2048`, 2048, false},
		{`""`, 0, false},
		{`"not a size"`, 0, true},
		{`true`, 0, true},
		{`"1"`, 1, false},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			var s Size
			err := json.Unmarshal([]byte(c.in), &s)
			if c.err {
				if err == nil {
					t.Fatalf("expected error for %q", c.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if int64(s) != c.want {
				t.Fatalf("got %d, want %d", int64(s), c.want)
			}
		})
	}
}

func TestSizeMarshalJSON(t *testing.T) {
	b, err := json.Marshal(Size(1234))
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "1234" {
		t.Fatalf("got %s", b)
	}
}

func TestSizeUnmarshalJSON_BadStringInJSON(t *testing.T) {
	// Direct call with a byte slice that starts with `"` but is not a valid
	// JSON string — exercises the json.Unmarshal(b, &raw) error path.
	var s Size
	if err := s.UnmarshalJSON([]byte(`"unterminated`)); err == nil {
		t.Fatal("expected error")
	}
}

func TestDurationUnmarshalJSON(t *testing.T) {
	cases := []struct {
		in   string
		want time.Duration
		err  bool
	}{
		{`"10m"`, 10 * time.Minute, false},
		{`"30s"`, 30 * time.Second, false},
		{`""`, 0, false},
		{`1500000000`, 1500000000 * time.Nanosecond, false},
		{`"nope"`, 0, true},
		{`true`, 0, true},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			var d Duration
			err := json.Unmarshal([]byte(c.in), &d)
			if c.err {
				if err == nil {
					t.Fatalf("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if d.AsDuration() != c.want {
				t.Fatalf("got %v, want %v", d.AsDuration(), c.want)
			}
		})
	}
}

func TestDurationUnmarshalJSON_BadStringInJSON(t *testing.T) {
	var d Duration
	if err := d.UnmarshalJSON([]byte(`"unterminated`)); err == nil {
		t.Fatal("expected error")
	}
}

func TestDurationMarshalJSON(t *testing.T) {
	b, err := json.Marshal(Duration(5 * time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `"5m0s"` {
		t.Fatalf("got %s", b)
	}
}
