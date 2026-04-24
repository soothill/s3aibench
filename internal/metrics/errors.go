package metrics

import (
	"errors"

	smithy "github.com/aws/smithy-go"
)

// classify returns a short code for an error suitable for grouping in reports.
// AWS SDK errors expose an ErrorCode via smithy; other errors fall back to
// "unknown".
func classify(err error) string {
	if err == nil {
		return ""
	}
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return "unknown"
}
