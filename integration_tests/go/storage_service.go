package integration

import (
	"context"
	"io"
	"time"
)

// ObjectInfo holds metadata about a storage object.
type ObjectInfo struct {
	Key          string            // Full object key/name
	Size         int64             // Size in bytes
	ETag         string            // Entity tag, often an MD5 hash of the object
	LastModified time.Time         // Last modified timestamp
	ContentType  string            // MIME type of the object
	UserMetadata map[string]string // User-defined metadata
}

// StorageService defines an interface for interacting with a storage backend (MinIO, GCS).
type StorageService interface {
	// UploadObject uploads an object with content and metadata.
	// objectName is the full path within the bucket.
	// userMetadata keys should be automatically prefixed if necessary by the implementation (e.g., for S3/MinIO).
	UploadObject(ctx context.Context, bucketName, objectName string, content io.Reader, size int64, contentType string, userMetadata map[string]string) error

	// GetObject retrieves an object's content.
	// Caller is responsible for closing the returned io.ReadCloser.
	GetObject(ctx context.Context, bucketName, objectName string) (io.ReadCloser, error)

	// StatObject retrieves metadata for an object without fetching its content.
	StatObject(ctx context.Context, bucketName, objectName string) (*ObjectInfo, error)

	// DeleteObject deletes an object.
	// Returns nil if the object was deleted or if the object did not exist.
	// Returns an error for other issues.
	DeleteObject(ctx context.Context, bucketName, objectName string) error

	// ListObjects lists object keys/names in a bucket, optionally filtered by a prefix.
	ListObjects(ctx context.Context, bucketName, prefix string, recursive bool) ([]string, error)

	// CreateBucket creates a new bucket.
	// region is an optional parameter, primarily for GCS or AWS S3.
	// Implementations for backends like MinIO might ignore it.
	CreateBucket(ctx context.Context, bucketName string, region ...string) error

	// DeleteBucket deletes a bucket.
	// Should ideally ensure the bucket is empty before deletion or handle non-empty bucket deletion if supported.
	DeleteBucket(ctx context.Context, bucketName string) error

	// EnsureBucketExists creates a bucket if it doesn't exist, or ensures it's accessible.
	// Returns true if the bucket was created, false if it already existed.
	EnsureBucketExists(ctx context.Context, bucketName string, region ...string) (bool, error)

	// EnsureBucketEmpty deletes all objects (and versions, if applicable) within a bucket.
	EnsureBucketEmpty(ctx context.Context, bucketName string) error

	// PathSeparator returns the conventional path separator for the storage service (typically "/").
	PathSeparator() string
}
