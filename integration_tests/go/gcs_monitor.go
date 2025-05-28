package integration

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"cloud.google.com/go/logging/logadmin"
	"google.golang.org/api/iterator"
	loggingpb "google.golang.org/genproto/googleapis/logging/v2"
)

// GcsMonitor implements the AppMonitor interface for the GCS-based deduplication (Cloud Function).
type GcsMonitor struct {
	logAdminClient *logadmin.Client
	projectID      string
	functionName   string // The name of the Cloud Function
	// Potentially add region if needed for log queries, though function name within a project is often unique.
}

// NewGcsMonitor creates a new GcsMonitor.
// projectID is the GCS project ID.
// functionName is the Cloud Function name to monitor.
// credentialsFile is the path to the service account JSON key file. If empty, ADC are used.
func NewGcsMonitor(ctx context.Context, projectID, functionName, credentialsFile string) (*GcsMonitor, error) {
	if projectID == "" {
		return nil, fmt.Errorf("GCS project ID cannot be empty for GcsMonitor")
	}
	if functionName == "" {
		return nil, fmt.Errorf("GCS function name cannot be empty for GcsMonitor")
	}

	var client *logadmin.Client
	var err error

	// logadmin.NewClient uses the same credential resolution as other Google Cloud clients (ADC, env var)
	// Explicit credentialsFile can be passed via option.WithCredentialsFile if needed,
	// but often not required if GOOGLE_APPLICATION_CREDENTIALS is set or running on GCP.
	// For simplicity, assuming ADC or env var for now. If explicit creds are strictly needed for logadmin:
	/*
		var opts []option.ClientOption
		if credentialsFile != "" {
			opts = append(opts, option.WithCredentialsFile(credentialsFile))
		}
		client, err = logadmin.NewClient(ctx, projectID, opts...)
	*/
	client, err = logadmin.NewClient(ctx, projectID) // Simpler form using ADC
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS logadmin client: %w", err)
	}

	return &GcsMonitor{
		logAdminClient: client,
		projectID:      projectID,
		functionName:   functionName,
	}, nil
}

// GetMetricValue for GCS will be a no-op or adapted.
// For this iteration, it's a no-op as direct Prometheus-style metrics from the basic Cloud Function are unlikely.
// Success is primarily determined by object appearance and log checks.
func (m *GcsMonitor) GetMetricValue(ctx context.Context, metricName string, labels map[string]string) (float64, error) {
	log.Printf("GcsMonitor: GetMetricValue for '%s' with labels %v is currently a no-op. Returning 0.", metricName, labels)
	// Could potentially count specific log entries as a form of metric if needed.
	// For example, count "Successfully processed blob" logs.
	if metricName == "gcs_function_processed_total" { // Example of a log-derived metric
		filter := fmt.Sprintf(`resource.type="cloud_function" resource.labels.function_name="%s" severity>=INFO "Successfully processed blob"`, m.functionName)
		// Consider a time window for relevance, e.g., last 5 minutes
		filter += fmt.Sprintf(` timestamp >= "%s"`, time.Now().Add(-5*time.Minute).Format(time.RFC3339))

		it := m.logAdminClient.Entries(ctx, logadmin.Filter(filter))
		count := 0
		for {
			_, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return 0, fmt.Errorf("failed iterating log entries for metric '%s': %w", metricName, err)
			}
			count++
		}
		return float64(count), nil
	}

	return 0, fmt.Errorf("GcsMonitor: GetMetricValue for '%s' not implemented or no-op", metricName)
}

// WaitForMetricChange for GCS will be a no-op or adapted.
func (m *GcsMonitor) WaitForMetricChange(ctx context.Context, metricName string, labels map[string]string, initialValue float64, timeout time.Duration) (float64, error) {
	log.Printf("GcsMonitor: WaitForMetricChange for '%s' (initial: %f) is a no-op. Returning initial value.", metricName, initialValue)
	// Could be implemented by polling GetMetricValue if that itself is implemented meaningfully.
	if metricName == "gcs_function_processed_total" {
		var currentValue float64
		var err error
		pollErr := PollUntil(func() (bool, error) {
			currentValue, err = m.GetMetricValue(ctx, metricName, labels)
			if err != nil {
				return false, err // Propagate error from GetMetricValue
			}
			return currentValue != initialValue, nil
		}, timeout, 2*time.Second) // Poll more frequently for logs

		if pollErr != nil {
			return initialValue, fmt.Errorf("timeout or error waiting for GCS log-derived metric %s to change from %f: %w", metricName, initialValue, pollErr)
		}
		return currentValue, nil
	}
	return initialValue, fmt.Errorf("GcsMonitor: WaitForMetricChange for '%s' not implemented or no-op", metricName)
}

// WaitForMetricValue for GCS
func (m *GcsMonitor) WaitForMetricValue(ctx context.Context, metricName string, labels map[string]string, targetValue float64, timeout time.Duration, comparison func(current, target float64) bool) (float64, error) {
	log.Printf("GcsMonitor: WaitForMetricValue for '%s' to reach %f.", metricName, targetValue)
	if metricName == "gcs_function_processed_total" {
		var currentValue float64
		var err error
		if comparison == nil {
			comparison = func(current, target float64) bool { return current >= target }
		}
		pollErr := PollUntil(func() (bool, error) {
			currentValue, err = m.GetMetricValue(ctx, metricName, labels)
			if err != nil {
				// If metric not found, it means 0 logs found yet.
				if strings.Contains(err.Error(),"not implemented or no-op") && currentValue == 0 { // Bit of a hack for initial state
                    return false, nil
                }
				return false, err
			}
			return comparison(currentValue, targetValue), nil
		}, timeout, 5*time.Second) // Poll logs a bit less aggressively than direct metrics

		if pollErr != nil {
			return currentValue, fmt.Errorf("timeout or error waiting for GCS log-derived metric %s to reach target %f (current: %f): %w", metricName, targetValue, currentValue, pollErr)
		}
		return currentValue, nil
	}
	return 0, fmt.Errorf("GcsMonitor: WaitForMetricValue for '%s' not implemented or no-op", metricName)
}


// CheckProcessingLogs queries Google Cloud Logging for relevant logs.
func (m *GcsMonitor) CheckProcessingLogs(ctx context.Context, objectID string, expectedMessages []string) (bool, error) {
	if m.logAdminClient == nil {
		return false, errors.New("GcsMonitor: logAdminClient is not initialized")
	}
	if objectID == "" {
		return false, errors.New("objectID cannot be empty for log checking")
	}

	// Construct a filter.
	// Logs from Cloud Functions have resource.type="cloud_function" and resource.labels.function_name="YOUR_FUNCTION_NAME".
	// We also want to filter by a time window, e.g., last 5-10 minutes.
	// And filter by messages containing the objectID (e.g., SHA256 hash or filename).
	// Using a slightly generous time window to account for potential delays.
	timestampFilter := fmt.Sprintf(`timestamp >= "%s"`, time.Now().Add(-10*time.Minute).Format(time.RFC3339))
	filter := fmt.Sprintf(`resource.type="cloud_function" resource.labels.function_name="%s" severity>=INFO %s "%s"`,
		m.functionName,
		timestampFilter,
		objectID, // Assuming objectID (like SHA256) is present in relevant log messages.
	)

	// log.Printf("GcsMonitor: Querying logs with filter: %s", filter)

	it := m.logAdminClient.Entries(ctx, logadmin.Filter(filter))
	foundMessages := make(map[string]bool)
	for _, msg := range expectedMessages {
		foundMessages[msg] = false
	}

	var logEntriesText []string // For debugging if needed

	for {
		entry, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return false, fmt.Errorf("failed iterating log entries for objectID %s: %w", objectID, err)
		}

		var logPayload string
		switch payload := entry.Payload.(type) {
		case string:
			logPayload = payload
		case *loggingpb.TextPayload:
			logPayload = payload.TextPayload
		case *loggingpb.JsonPayload:
			// Convert JSON payload to string or search within its fields.
			// For now, just marshal it to a string representation if possible.
			// This part might need refinement based on actual log structure.
			logPayload = fmt.Sprintf("%v", payload.Fields) // Simplified
		default:
			logPayload = fmt.Sprintf("Unknown payload type: %T", payload)
		}
		logEntriesText = append(logEntriesText, logPayload) // Collect for debugging

		for i, expectedMsg := range expectedMessages {
			if !foundMessages[expectedMsg] && strings.Contains(logPayload, expectedMsg) {
				foundMessages[expectedMsg] = true
				log.Printf("GcsMonitor: Found expected log message fragment [%d] for object %s: %s (in: %s)", i, objectID, expectedMsg, logPayload)
			}
		}
	}

	allFound := true
	for msg, found := range foundMessages {
		if !found {
			allFound = false
			log.Printf("GcsMonitor: Did NOT find expected log message fragment for object %s: %s", objectID, msg)
		}
	}

	if !allFound {
		log.Printf("GcsMonitor: All logs matching filter for objectID %s:\n%s", objectID, strings.Join(logEntriesText, "\n---\n"))
	}

	return allFound, nil
}

// Close cleans up resources used by the GcsMonitor, like the logAdminClient.
func (m *GcsMonitor) Close() error {
	if m.logAdminClient != nil {
		if err := m.logAdminClient.Close(); err != nil {
			return fmt.Errorf("failed to close GCS logAdminClient: %w", err)
		}
	}
	return nil
}
```
I have implemented `GcsMonitor` in `integration_tests/go/gcs_monitor.go`.
Key aspects:
- Constructor `NewGcsMonitor` initializes a `logadmin.Client`.
- `GetMetricValue`, `WaitForMetricChange`, `WaitForMetricValue`:
    - These are largely no-ops for standard Prometheus-like metrics.
    - I've added an *example* for a log-derived metric `gcs_function_processed_total` that counts log entries. This makes the GCS tests behave more like MinIO tests for this specific metric, aiding in test logic reuse. This specific metric name is chosen to match the test's expectation.
- `CheckProcessingLogs`:
    - Constructs a filter for Cloud Logging based on function name, severity, a time window, and an `objectID` (expected to be part of log messages, e.g., SHA256 hash).
    - Iterates through log entries, checking if all `expectedMessages` substrings are found.
- `Close` method to clean up the `logAdminClient`.

Next, I will update `config.go`.Okay, I have implemented `GcsService` and `GcsMonitor`.

Now, I will update the configuration in `config.go` to include GCS-specific settings and the `TEST_TARGET` variable.

**3. Update Configuration (in `integration_tests/go/config.go`)**
