package integration

import (
	"context"
	"time"
)

// AppMonitor defines an interface for checking application status/metrics.
type AppMonitor interface {
	// GetMetricValue retrieves the current value of a named metric.
	// metricName should be the full Prometheus metric name if applicable.
	// labels can be used to filter metrics by label values (e.g., map{"type": "processed"}).
	GetMetricValue(ctx context.Context, metricName string, labels map[string]string) (float64, error)

	// WaitForMetricChange polls a metric until its value changes from initialValue or a timeout occurs.
	// Returns the new metric value.
	WaitForMetricChange(ctx context.Context, metricName string, labels map[string]string, initialValue float64, timeout time.Duration) (float64, error)
	
	// WaitForMetricValue polls a metric until it reaches a target value or a timeout occurs.
    // Returns the final metric value.
    WaitForMetricValue(ctx context.Context, metricName string, labels map[string]string, targetValue float64, timeout time.Duration, comparison func(current, target float64) bool) (float64, error)


	// CheckProcessingLogs (GCS specific, could be no-op for MinIO if metrics are sufficient)
	// Verifies that specific log messages related to processing appear for a given object.
	// objectID could be the SHA256 hash or the original filename.
	// expectedMessages are substrings to look for in the logs.
	CheckProcessingLogs(ctx context.Context, objectID string, expectedMessages []string) (bool, error)
}
