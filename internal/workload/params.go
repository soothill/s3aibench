package workload

import (
	"strconv"
	"time"
)

// IntParam returns an int-valued workload parameter or def when absent.
func IntParam(p map[string]interface{}, key string, def int) int {
	if v, ok := p[key]; ok {
		if f, fok := v.(float64); fok {
			return int(f)
		}
		if i, iok := v.(int); iok {
			return i
		}
	}
	return def
}

// SizeParam returns an int64-valued size parameter or def when absent.
func SizeParam(p map[string]interface{}, key string, def int64) int64 {
	if v, ok := p[key]; ok {
		if f, fok := v.(float64); fok {
			return int64(f)
		}
		if i, iok := v.(int); iok {
			return int64(i)
		}
	}
	return def
}

// FloatParam returns a float64-valued parameter or def when absent.
func FloatParam(p map[string]interface{}, key string, def float64) float64 {
	if v, ok := p[key]; ok {
		if f, fok := v.(float64); fok {
			return f
		}
		if i, iok := v.(int); iok {
			return float64(i)
		}
		if s, sok := v.(string); sok {
			if parsed, err := strconv.ParseFloat(s, 64); err == nil {
				return parsed
			}
		}
	}
	return def
}

// StringParam returns a string-valued parameter or def when absent.
func StringParam(p map[string]interface{}, key, def string) string {
	if v, ok := p[key]; ok {
		if s, sok := v.(string); sok {
			return s
		}
	}
	return def
}

// BoolParam returns a bool-valued parameter or def when absent.
func BoolParam(p map[string]interface{}, key string, def bool) bool {
	if v, ok := p[key]; ok {
		if b, bok := v.(bool); bok {
			return b
		}
	}
	return def
}

// DurationParam parses a duration string parameter or returns def on failure.
func DurationParam(p map[string]interface{}, key string, def time.Duration) time.Duration {
	if v, ok := p[key]; ok {
		if s, sok := v.(string); sok {
			if d, err := time.ParseDuration(s); err == nil {
				return d
			}
		}
	}
	return def
}
