package cli

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/darrensoothill/s3aibench/internal/config"
	"github.com/darrensoothill/s3aibench/internal/plan"
)

// loadAndResolve reads the plan file, runs validation (which warns on inline
// creds), applies overrides/env, and returns the resolved config. Resolve
// cannot fail — its canonical struct is JSON-safe by construction — so we
// ignore its error return.
func loadAndResolve(path string, overrides config.Overrides, env config.Environ, logger *slog.Logger) (*plan.Plan, *config.Config, error) {
	p, err := plan.Load(path)
	if err != nil {
		return nil, nil, err
	}
	if err := plan.Validate(p, logger); err != nil {
		return nil, nil, err
	}
	cfg, _ := config.Resolve(p, overrides, env)
	return p, cfg, nil
}

func osEnviron(key string) (string, bool) {
	return os.LookupEnv(key)
}

func parseLogLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func newLogger(level string, out *os.File) *slog.Logger {
	return slog.New(slog.NewTextHandler(out, &slog.HandlerOptions{Level: parseLogLevel(level)}))
}

// loggerFor returns a logger honoring cfg.LogLevel — called after the config
// is resolved. Callers use the bootstrap logger (info) from `newLogger` before
// the plan has been parsed.
func loggerFor(level string, out *os.File) *slog.Logger {
	return newLogger(level, out)
}

func printResolved(cfg *config.Config, w *os.File) {
	for _, line := range cfg.DumpSources() {
		fmt.Fprintln(w, line)
	}
	fmt.Fprintf(w, "config_hash=%s\n", cfg.ConfigHash)
}
