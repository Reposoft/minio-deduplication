package integration

import (
	"os"
	"strconv"
	"time"
)

// Config holds the configuration for the integration tests.
type Config struct {
	TestTarget string // "minio" or "gcs"

	// MinIO specific
	MinioEndpoint        string
	MinioAccessKeyID     string
	MinioSecretAccessKey string
	MinioUseSSL          bool
	AppMetricsEndpoint   string // MinIO app's Prometheus metrics endpoint

	// GCS specific
	GcsProjectID                string
	GcsWriteBucketName          string // Specific GCS write bucket
	GcsReadBucketName           string // Specific GCS read bucket
	GcsFunctionName             string // For log monitoring
	GcsCredentialsFile          string // Path to ADC JSON file, optional
	GcsFunctionRegion           string // Optional, for more specific log queries or function interaction

	// Common bucket names (resolved based on TestTarget)
	WriteBucketName string // Actual write bucket to use for the test
	ReadBucketName  string // Actual read bucket to use for the test
	
	// Common application settings
	PreservedFolderDepth int // For application logic, used in GetExpectedShardedPath

	// Test behavior settings
	PollTimeout  time.Duration
	PollInterval time.Duration
}

// LoadConfig loads configuration from environment variables.
func LoadConfig() (*Config, error) {
	minioUseSSL, err := strconv.ParseBool(getEnv("MINIO_USE_SSL", "false"))
	if err != nil {
		minioUseSSL = false
	}

	preservedDepth, err := strconv.Atoi(getEnv("PRESERVED_FOLDER_DEPTH", "0"))
	if err != nil {
		preservedDepth = 0
	}

	pollTimeoutStr := getEnv("POLL_TIMEOUT_SECONDS", "120") // Default 2 minutes
	pollTimeoutSec, err := strconv.Atoi(pollTimeoutStr)
	if err != nil {
		pollTimeoutSec = 120
	}

	pollIntervalStr := getEnv("POLL_INTERVAL_SECONDS", "5") // Default 5 seconds for object polling
	pollIntervalSec, err := strconv.Atoi(pollIntervalStr)
	if err != nil {
		pollIntervalSec = 5
	}
	
	cfg := &Config{
		TestTarget:           strings.ToLower(getEnv("TEST_TARGET", "minio")),
		MinioEndpoint:        getEnv("MINIO_ENDPOINT", "localhost:9000"),
		MinioAccessKeyID:     getEnv("MINIO_ACCESS_KEY_ID", "minio"),
		MinioSecretAccessKey: getEnv("MINIO_SECRET_ACCESS_KEY", "minio123"),
		MinioUseSSL:          minioUseSSL,
		AppMetricsEndpoint:   getEnv("APP_METRICS_ENDPOINT", "http://localhost:2112/metrics"),

		GcsProjectID:       getEnv("GCS_PROJECT_ID", ""),
		GcsWriteBucketName: getEnv("GCS_WRITE_BUCKET_NAME", "gcs-dedup-write-bucket"), // Example default
		GcsReadBucketName:  getEnv("GCS_READ_BUCKET_NAME", "gcs-dedup-read-bucket"),   // Example default
		GcsFunctionName:    getEnv("GCS_FUNCTION_NAME", ""),                       // Must be provided for GCS tests
		GcsCredentialsFile: getEnv("GOOGLE_APPLICATION_CREDENTIALS", ""),           // Standard env var for ADC path
		GcsFunctionRegion:  getEnv("GCS_FUNCTION_REGION", ""),                     // e.g., "us-central1"

		PreservedFolderDepth: preservedDepth,
		PollTimeout:          time.Duration(pollTimeoutSec) * time.Second,
		PollInterval:         time.Duration(pollIntervalSec) * time.Second,
	}

	// Set the effective WriteBucketName and ReadBucketName based on TestTarget
	if cfg.TestTarget == "gcs" {
		cfg.WriteBucketName = cfg.GcsWriteBucketName
		cfg.ReadBucketName = cfg.GcsReadBucketName
		if cfg.GcsProjectID == "" {
			return nil, fmt.Errorf("GCS_PROJECT_ID must be set when TEST_TARGET is 'gcs'")
		}
		if cfg.GcsFunctionName == "" {
			// GCS Function name is crucial for monitoring via logs.
			// Allow if user explicitly wants no monitoring, but warn or make it an error for robust tests.
			fmt.Println("Warning: GCS_FUNCTION_NAME is not set. Log monitoring will be impaired.")
		}
	} else { // Default to MinIO settings
		cfg.WriteBucketName = getEnv("WRITE_BUCKET_NAME", "bucket-write") // Original MinIO bucket names
		cfg.ReadBucketName = getEnv("READ_BUCKET_NAME", "bucket-read")
	}
	
	return cfg, nil
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
