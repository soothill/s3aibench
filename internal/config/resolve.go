package config

import (
	"strconv"
	"strings"
	"time"

	"github.com/darrensoothill/s3aibench/internal/plan"
	"github.com/dustin/go-humanize"
)

func pickString(c *Config, key string, dst *string, flag *string, env Environ, envKey, planVal, def string) {
	if flag != nil {
		*dst = *flag
		c.Sources[key] = SourceFlag
		return
	}
	if v, ok := env(envKey); ok {
		*dst = v
		c.Sources[key] = SourceEnv
		return
	}
	if planVal != "" {
		*dst = planVal
		c.Sources[key] = SourcePlan
		return
	}
	*dst = def
	c.Sources[key] = SourceDefault
}

func pickBool(c *Config, key string, dst *bool, flag *bool, env Environ, envKey string, planVal *bool, def bool) {
	if flag != nil {
		*dst = *flag
		c.Sources[key] = SourceFlag
		return
	}
	if v, ok := env(envKey); ok {
		b, err := strconv.ParseBool(v)
		if err == nil {
			*dst = b
			c.Sources[key] = SourceEnv
			return
		}
	}
	if planVal != nil {
		*dst = *planVal
		c.Sources[key] = SourcePlan
		return
	}
	*dst = def
	c.Sources[key] = SourceDefault
}

func pickInt(c *Config, key string, dst *int, flag *int, env Environ, envKey string, planVal, def int) {
	if flag != nil {
		*dst = *flag
		c.Sources[key] = SourceFlag
		return
	}
	if v, ok := env(envKey); ok {
		n, err := strconv.Atoi(v)
		if err == nil {
			*dst = n
			c.Sources[key] = SourceEnv
			return
		}
	}
	if planVal != 0 {
		*dst = planVal
		c.Sources[key] = SourcePlan
		return
	}
	*dst = def
	c.Sources[key] = SourceDefault
}

func pickInt64(c *Config, key string, dst *int64, flag *int64, env Environ, envKey string, planVal, def int64) {
	if flag != nil {
		*dst = *flag
		c.Sources[key] = SourceFlag
		return
	}
	if v, ok := env(envKey); ok {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			*dst = n
			c.Sources[key] = SourceEnv
			return
		}
		if n, err := humanize.ParseBytes(v); err == nil {
			*dst = int64(n)
			c.Sources[key] = SourceEnv
			return
		}
	}
	if planVal != 0 {
		*dst = planVal
		c.Sources[key] = SourcePlan
		return
	}
	*dst = def
	c.Sources[key] = SourceDefault
}

func pickDuration(c *Config, key string, dst *time.Duration, flag *time.Duration, env Environ, envKey string, planVal, def time.Duration) {
	if flag != nil {
		*dst = *flag
		c.Sources[key] = SourceFlag
		return
	}
	if v, ok := env(envKey); ok {
		if d, err := time.ParseDuration(v); err == nil {
			*dst = d
			c.Sources[key] = SourceEnv
			return
		}
	}
	if planVal != 0 {
		*dst = planVal
		c.Sources[key] = SourcePlan
		return
	}
	*dst = def
	c.Sources[key] = SourceDefault
}

// NormalizeWorkloadName converts "lance-query" → "LANCE_QUERY" for env lookups.
func NormalizeWorkloadName(name string) string {
	return strings.ToUpper(strings.ReplaceAll(name, "-", "_"))
}

func applyWorkloadEnv(wls []plan.Workload, env Environ) {
	for i := range wls {
		n := NormalizeWorkloadName(wls[i].Name)
		if v, ok := env("S3AIBENCH_WORKLOAD_" + n + "_THREADS"); ok {
			if t, err := strconv.Atoi(v); err == nil {
				wls[i].Threads = t
			}
		}
		if v, ok := env("S3AIBENCH_WORKLOAD_" + n + "_DURATION"); ok {
			if d, err := time.ParseDuration(v); err == nil {
				wls[i].Duration = plan.Duration(d)
			}
		}
		if v, ok := env("S3AIBENCH_WORKLOAD_" + n + "_WEIGHT"); ok {
			if w, err := strconv.Atoi(v); err == nil {
				wls[i].Weight = w
			}
		}
	}
}
