package report

import (
	"encoding/json"
	"io"

	"github.com/darrensoothill/s3aibench/pkg/reportschema"
)

// WriteJSON emits the report as indented JSON.
func WriteJSON(w io.Writer, r *reportschema.Report) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}
