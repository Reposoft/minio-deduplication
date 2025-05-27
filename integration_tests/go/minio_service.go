package integration

import (
	"context"
	"fmt"
	"io"
	"log" // Using standard log for simplicity in tests, could be replaced with zap
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// MinioService implements the StorageService interface for MinIO.
type MinioService struct {
	client       *minio.Client
	config       *Config // Store config for easy access to bucket names, etc.
	isAWSSharded bool    // true if path style is AWS sharded (aa/bb/hash.ext), false for MinIO direct (hash.ext)
}

// NewMinioService creates a new MinioService instance.
func NewMinioService(cfg *Config) (*MinioService, error) {
	client, err := minio.New(cfg.MinioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.MinioAccessKeyID, cfg.MinioSecretAccessKey, ""),
		Secure: cfg.MinioUseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize MinIO client: %w", err)
	}

	// Simple check to see if the client is reachable
	// Note: ListBuckets might require permissions. A better health check might be needed.
	_, err = client.ListBuckets(context.Background())
	if err != nil {
		log.Printf("Warning: MinIO client might not be reachable or configured correctly: %v\nAttempting to proceed anyway...", err)
		// return nil, fmt.Errorf("MinIO client not reachable: %w", err)
	}

	return &MinioService{
		client:       client,
		config:       cfg,
		isAWSSharded: true, // By default, assume AWS sharded paths as per the app's main logic
	}, nil
}

// SetPathStyle allows overriding the default sharded path style for MinIO specific tests if needed.
// The main application uses sharded paths for MinIO as well.
func (s *MinioService) SetPathStyle(awsSharded bool) {
	s.isAWSSharded = awsSharded
}

func (s *MinioService) UploadObject(ctx context.Context, bucketName, objectName string, content io.Reader, size int64, contentType string, userMetadata map[string]string) error {
	opts := minio.PutObjectOptions{
		ContentType:  contentType,
		UserMetadata: userMetadata, // minio-go handles prefixing with X-Amz-Meta-
	}
	if size == -1 { // if size is unknown (e.g. for streams without upfront size)
		// Use -1 for unknown size, max 5TB part size. This is fine for most test files.
		// For very large files or specific stream scenarios, this might need adjustment.
		opts.PartSize = 0 // Let the client decide part size, default is 5MiB.
	}

	_, err := s.client.PutObject(ctx, bucketName, objectName, content, size, opts)
	if err != nil {
		return fmt.Errorf("failed to upload object %s/%s: %w", bucketName, objectName, err)
	}
	return nil
}

func (s *MinioService) GetObject(ctx context.Context, bucketName, objectName string) (io.ReadCloser, error) {
	obj, err := s.client.GetObject(ctx, bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get object %s/%s: %w", bucketName, objectName, err)
	}
	return obj, nil
}

func (s *MinioService) StatObject(ctx context.Context, bucketName, objectName string) (*ObjectInfo, error) {
	opts := minio.StatObjectOptions{}
	stat, err := s.client.StatObject(ctx, bucketName, objectName, opts)
	if err != nil {
		// Check if the error is "object not found"
		// minio-go error response for not found is an minio.ErrorResponse with Code "NoSuchKey"
		errResp, ok := err.(minio.ErrorResponse)
		if ok && (errResp.Code == "NoSuchKey" || errResp.Code == "NoSuchObject" || strings.Contains(errResp.Message, "The specified key does not exist")) {
			return nil, nil // Return nil, nil for "not found" to match interface expectation for polling
		}
		return nil, fmt.Errorf("failed to stat object %s/%s: %w", bucketName, objectName, err)
	}

	// Normalize user metadata keys (remove X-Amz-Meta- prefix)
	normUserMeta := make(map[string]string)
	for k, v := range stat.UserMetadata {
		normUserMeta[strings.TrimPrefix(strings.ToLower(k), "x-amz-meta-")] = v[0] // Assuming single value per meta key
	}


	return &ObjectInfo{
		Key:          stat.Key,
		Size:         stat.Size,
		ETag:         stat.ETag,
		LastModified: stat.LastModified,
		ContentType:  stat.ContentType,
		UserMetadata: normUserMeta,
	}, nil
}

func (s *MinioService) DeleteObject(ctx context.Context, bucketName, objectName string) error {
	err := s.client.RemoveObject(ctx, bucketName, objectName, minio.RemoveObjectOptions{})
	if err != nil {
		// Check if the error is because the object doesn't exist, which is not an error for DeleteObject
		errResp, ok := err.(minio.ErrorResponse)
		if ok && (errResp.Code == "NoSuchKey" || errResp.Code == "NoSuchObject") {
			return nil // Object not found is not an error for delete operation
		}
		return fmt.Errorf("failed to delete object %s/%s: %w", bucketName, objectName, err)
	}
	return nil
}

func (s *MinioService) ListObjects(ctx context.Context, bucketName, prefix string, recursive bool) ([]string, error) {
	var objectNames []string
	opts := minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: recursive,
	}
	objectCh := s.client.ListObjects(ctx, bucketName, opts)
	for object := range objectCh {
		if object.Err != nil {
			return nil, fmt.Errorf("failed during listing objects in %s with prefix %s: %w", bucketName, prefix, object.Err)
		}
		objectNames = append(objectNames, object.Key)
	}
	return objectNames, nil
}

func (s *MinioService) CreateBucket(ctx context.Context, bucketName string, region ...string) error {
	// Region is typically ignored by MinIO standalone, but can be passed.
	// Some MinIO gateways (like for Azure) might use it.
	var loc string
	if len(region) > 0 {
		loc = region[0]
	}
	err := s.client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{Region: loc, ObjectLocking: false})
	if err != nil {
		// Check if the bucket already exists
		exists, errBucketExists := s.client.BucketExists(ctx, bucketName)
		if errBucketExists == nil && exists {
			return nil // Bucket already exists, not an error
		}
		return fmt.Errorf("failed to create bucket %s: %w", bucketName, err)
	}
	return nil
}

func (s *MinioService) DeleteBucket(ctx context.Context, bucketName string) error {
	// Ensure bucket is empty first, as MinIO typically requires this.
	// This is a common pattern; some backends might offer force delete.
	err := s.EnsureBucketEmpty(ctx, bucketName)
	if err != nil {
		return fmt.Errorf("failed to empty bucket %s before deletion: %w", bucketName, err)
	}

	err = s.client.RemoveBucket(ctx, bucketName)
	if err != nil {
		// Check if the bucket doesn't exist, which might not be an error for delete.
		// However, standard behavior is to error if bucket not found.
		return fmt.Errorf("failed to delete bucket %s: %w", bucketName, err)
	}
	return nil
}

func (s *MinioService) EnsureBucketExists(ctx context.Context, bucketName string, region ...string) (bool, error) {
	exists, err := s.client.BucketExists(ctx, bucketName)
	if err != nil {
		return false, fmt.Errorf("failed to check if bucket %s exists: %w", bucketName, err)
	}
	if exists {
		return false, nil // Already existed
	}
	err = s.CreateBucket(ctx, bucketName, region...)
	if err != nil {
		return false, err // Error during creation
	}
	return true, nil // Was created
}

func (s *MinioService) EnsureBucketEmpty(ctx context.Context, bucketName string) error {
	objectsCh := s.client.ListObjects(ctx, bucketName, minio.ListObjectsOptions{Recursive: true})
	var errors []error
	var objectsToDelete []minio.ObjectInfo

	for object := range objectsCh {
		if object.Err != nil {
			errors = append(errors, fmt.Errorf("error listing object %s for deletion: %w", object.Key, object.Err))
			continue
		}
		objectsToDelete = append(objectsToDelete, object)
	}

	if len(errors) > 0 {
		// Combine errors if any occurred during listing
		var errorMessages []string
		for _, e := range errors {
			errorMessages = append(errorMessages, e.Error())
		}
		return fmt.Errorf("errors encountered while listing objects in bucket %s for emptying: %s", bucketName, strings.Join(errorMessages, "; "))
	}
	
	if len(objectsToDelete) == 0 {
		return nil // Bucket is already empty
	}

	// Convert []minio.ObjectInfo to a channel of minio.ObjectInfo for RemoveObjects API
	objectsInfoCh := make(chan minio.ObjectInfo)
	go func() {
		defer close(objectsInfoCh)
		for _, objInfo := range objectsToDelete {
			objectsInfoCh <- objInfo
		}
	}()

	// Remove all objects.
	// Note: RemoveObjects API takes a channel of ObjectInfo, not just keys.
	// It's simpler to list and then delete one by one if the list isn't too large for tests.
	// For robust large-scale emptying, RemoveObjects is better.
	// Let's use RemoveObjects for correctness.
	
	// If using RemoveObjects, it expects a channel of ObjectInfo struct.
	// The objectsCh from ListObjects already gives us this.
	// However, we need to collect them first to avoid modifying while iterating if there are issues,
	// or use RemoveObjects directly if we are confident.
	// The current objectsCh is already consumed. We need to re-list or use the collected list.

	errorCh := s.client.RemoveObjects(ctx, bucketName, objectsInfoCh, minio.RemoveObjectsOptions{GovernanceBypass: true})
	for e := range errorCh {
		errors = append(errors, fmt.Errorf("failed to delete object %s: %w", e.ObjectName, e.Err))
	}

	if len(errors) > 0 {
		var errorMessages []string
		for _, e := range errors {
			errorMessages = append(errorMessages, e.Error())
		}
		return fmt.Errorf("failed to empty bucket %s. Errors: %s", bucketName, strings.Join(errorMessages, "; "))
	}
	return nil
}

func (s *MinioService) PathSeparator() string {
	return "/"
}
```
