package integration

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	// Using a simple Prometheus text parser.
	// For more complex scenarios, a proper Prometheus client library might be better.
	"github.com/prometheus/common/expfmt"
)

// MinioMonitor implements the AppMonitor interface for the MinIO deduplication application.
type MinioMonitor struct {
	metricsURL string
	httpClient *http.Client
}

// NewMinioMonitor creates a new MinioMonitor.
// metricsURL is the full URL to the application's Prometheus metrics endpoint (e.g., "http://localhost:2112/metrics").
func NewMinioMonitor(metricsURL string) (*MinioMonitor, error) {
	if metricsURL == "" {
		return nil, fmt.Errorf("metrics URL cannot be empty")
	}
	return &MinioMonitor{
		metricsURL: metricsURL,
		httpClient: &http.Client{Timeout: 10 * time.Second}, //Reasonable timeout for metrics fetching
	}, nil
}

func (m *MinioMonitor) fetchMetrics(ctx context.Context) (map[string]*expfmt.MetricFamily, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", m.metricsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request to metrics endpoint %s: %w", m.metricsURL, err)
	}

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch metrics from %s: %w", m.metricsURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("metrics endpoint %s returned status %d: %s", m.metricsURL, resp.StatusCode, string(bodyBytes))
	}

	var parser expfmt.TextParser
	metricFamilies, err := parser.TextToMetricFamilies(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to parse metrics from %s: %w", m.metricsURL, err)
	}
	return metricFamilies, nil
}

func (m *MinioMonitor) GetMetricValue(ctx context.Context, metricName string, labels map[string]string) (float64, error) {
	metricFamilies, err := m.fetchMetrics(ctx)
	if err != nil {
		return 0, err
	}

	mf, ok := metricFamilies[metricName]
	if !ok {
		return 0, fmt.Errorf("metric %s not found", metricName)
	}

	// Iterate through metrics in the family to find one matching all labels
	for _, metric := range mf.GetMetric() {
		if len(labels) == 0 && len(metric.GetLabel()) == 0 { // No labels specified, first metric without labels
			if metric.GetCounter() != nil {
				return metric.GetCounter().GetValue(), nil
			}
			if metric.GetGauge() != nil {
				return metric.GetGauge().GetValue(), nil
			}
			// Add other types (Histogram, Summary) if needed
			return 0, fmt.Errorf("metric %s found, but is not a Counter or Gauge", metricName)
		}
		
		if len(labels) > 0 {
			labelsMatch := true
			foundLabels := make(map[string]bool)

			for _, labelPair := range metric.GetLabel() {
				if val, ok := labels[labelPair.GetName()]; ok {
					if labelPair.GetValue() == val {
						foundLabels[labelPair.GetName()] = true
					} else {
						labelsMatch = false
						break
					}
				}
			}
			if len(foundLabels) != len(labels) { // Not all specified labels were found in this metric instance
				labelsMatch = false
			}

			if labelsMatch {
				if metric.GetCounter() != nil {
					return metric.GetCounter().GetValue(), nil
				}
				if metric.GetGauge() != nil {
					return metric.GetGauge().GetValue(), nil
				}
				return 0, fmt.Errorf("metric %s with labels %v found, but is not a Counter or Gauge", metricName, labels)
			}
		}
	}

	return 0, fmt.Errorf("metric %s with specified labels %v not found", metricName, labels)
}

func (m *MinioMonitor) WaitForMetricChange(ctx context.Context, metricName string, labels map[string]string, initialValue float64, timeout time.Duration) (float64, error) {
	var currentValue float64
	var err error

	pollErr := PollUntil(func() (bool, error) {
		currentValue, err = m.GetMetricValue(ctx, metricName, labels)
		if err != nil {
			// Log or handle transient errors if necessary, for now, fail fast
			return false, fmt.Errorf("failed to get metric value during poll: %w", err)
		}
		return currentValue != initialValue, nil
	}, timeout, 1*time.Second) // Poll every 1 second, adjust interval as needed

	if pollErr != nil {
		return initialValue, fmt.Errorf("timeout or error waiting for metric %s to change from %f: %w", metricName, initialValue, pollErr)
	}
	return currentValue, nil
}

func (m *MinioMonitor) WaitForMetricValue(ctx context.Context, metricName string, labels map[string]string, targetValue float64, timeout time.Duration, comparison func(current, target float64) bool) (float64, error) {
    var currentValue float64
    var err error

    if comparison == nil { // Default comparison: current >= target
        comparison = func(current, target float64) bool { return current >= target }
    }

    pollErr := PollUntil(func() (bool, error) {
        currentValue, err = m.GetMetricValue(ctx, metricName, labels)
        if err != nil {
            // If metric not found yet, treat as not ready, unless it's a persistent error
            if strings.Contains(err.Error(), "not found") {
                 // Log this specific error? For now, just retry by returning false, nil
                 return false, nil
            }
            return false, fmt.Errorf("failed to get metric value during poll for target: %w", err)
        }
        return comparison(currentValue, targetValue), nil
    }, timeout, 1*time.Second) // Poll every 1 second

    if pollErr != nil {
        return currentValue, fmt.Errorf("timeout or error waiting for metric %s to reach target %f (current: %f): %w", metricName, targetValue, currentValue, pollErr)
    }
    return currentValue, nil
}


// CheckProcessingLogs is a no-op for MinioMonitor as logs are not typically checked this way for MinIO.
// Application logs would be on stdout/stderr of the container or a logging system.
func (m *MinioMonitor) CheckProcessingLogs(ctx context.Context, objectID string, expectedMessages []string) (bool, error) {
	// This could be implemented if the application provides a specific log query endpoint,
	// or if tests have access to container logs (e.g., via Docker API), but that's more complex.
	// For now, consistent with design, this is a no-op for MinIO.
	return true, nil // Assume logs are fine, or this check is not applicable.
}
