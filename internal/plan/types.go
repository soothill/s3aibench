package plan

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/dustin/go-humanize"
)

// Size is a byte count that unmarshals from strings like "16MiB", "1GiB",
// or a bare integer.
type Size int64

func (s Size) MarshalJSON() ([]byte, error) {
	return json.Marshal(int64(s))
}

func (s *Size) UnmarshalJSON(b []byte) error {
	// Accept either a JSON number or a JSON string.
	if len(b) > 0 && b[0] == '"' {
		var raw string
		if err := json.Unmarshal(b, &raw); err != nil {
			return err
		}
		return s.parse(raw)
	}
	var n int64
	if err := json.Unmarshal(b, &n); err != nil {
		return err
	}
	*s = Size(n)
	return nil
}

func (s *Size) parse(raw string) error {
	if raw == "" {
		*s = 0
		return nil
	}
	if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
		*s = Size(n)
		return nil
	}
	n, err := humanize.ParseBytes(raw)
	if err != nil {
		return fmt.Errorf("size %q: %w", raw, err)
	}
	*s = Size(n)
	return nil
}

// Duration is a time.Duration that unmarshals from Go duration strings ("10m").
type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	if len(b) > 0 && b[0] == '"' {
		var raw string
		if err := json.Unmarshal(b, &raw); err != nil {
			return err
		}
		if raw == "" {
			*d = 0
			return nil
		}
		dur, err := time.ParseDuration(raw)
		if err != nil {
			return fmt.Errorf("duration %q: %w", raw, err)
		}
		*d = Duration(dur)
		return nil
	}
	var n int64
	if err := json.Unmarshal(b, &n); err != nil {
		return err
	}
	*d = Duration(n)
	return nil
}

func (d Duration) AsDuration() time.Duration { return time.Duration(d) }
