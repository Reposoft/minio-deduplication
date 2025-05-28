package gcs_transfer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	// "log" // Temporary, replace with Zap
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"cloud.google.com/go/storage"
	"go.uber.org/zap"

	"cloud.google.com/go/storage" // Import storage to access ObjectAttrs
)

// GCSClient defines the interface for GCS operations needed by the handler.
// This allows for mocking in tests.
type GCSClient interface {
	NewReader(ctx context.Context, bucketName, objectName string) (io.ReadCloser, error)
	CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string) error
	DeleteObject(ctx context.Context, bucketName, objectName string) error
	// Add other methods like GetObjectAttributes if needed
}

// StorageClientAdapter adapts a *storage.Client to the GCSClient interface.
type StorageClientAdapter struct {
	Client *storage.Client
}

func (s *StorageClientAdapter) NewReader(ctx context.Context, bucketName, objectName string) (io.ReadCloser, error) {
	return s.Client.Bucket(bucketName).Object(objectName).NewReader(ctx)
}

func (s *StorageClientAdapter) CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string) error {
	src := s.Client.Bucket(srcBucket).Object(srcObject)
	dst := s.Client.Bucket(dstBucket).Object(dstObject)
	_, err := dst.CopierFrom(src).Run(ctx)
	return err
}

func (s *StorageClientAdapter) DeleteObject(ctx context.Context, bucketName, objectName string) error {
	return s.Client.Bucket(bucketName).Object(objectName).Delete(ctx)
}


// GCSEvent is a placeholder for the actual GCS event structure.
// The real structure would be something like `storage.ObjectAttrs` or a dedicated event type
// if using Cloud Functions triggers directly with `google.golang.org/api/functions/v1`.
// For now, we can assume we get bucket and name.
type GCSEvent struct {
	Bucket string `json:"bucket"`
	Name   string `json:"name"`
}

func init() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		// Fallback to standard log if Zap fails to initialize
		log.Printf("Failed to initialize Zap logger: %v. Falling back to standard logger.", err)
		return
	}
	zap.ReplaceGlobals(logger)
}

// EventConfig holds configuration settings derived from environment variables.
type EventConfig struct {
	WriteBucketName      string
	ReadBucketName       string
	PreservedFolderDepth int
}

// ToExtension public version of toExtension for testing.
// It's generally better to test via the main function's behavior, but for focused unit tests of helpers,
// making them public or using build tags for testing is common.
func ToExtension(key string) string {
	ext := strings.ToLower(filepath.Ext(key))
	if ext == ".jpeg" {
		return ".jpg"
	}
	return ext
}

// processGCSEventInternal contains the core logic for processing the GCS event.
// It accepts configuration and a GCS client interface for better testability.
// It returns the calculated destination object name and any error encountered.
func processGCSEventInternal(
	ctx context.Context,
	config *EventConfig,
	event GCSEvent,
	gcsClient GCSClient, // Using the interface
	objectContentReader io.Reader, // Accept reader for content
	objectAttributes *storage.ObjectAttrs, // Pass object attributes for metadata access
) (string, error) {

	currentPreservedFolderDepth := config.PreservedFolderDepth // Default from env/config

	// Check for metadata override for PRESERVED_FOLDER_DEPTH
	// GCS metadata keys are case-insensitive but often stored as x-goog-meta-....
	// The client libraries typically handle this. When setting, use `x-goog-meta-` prefix for custom metadata.
	// When reading, the prefix might be stripped or normalized by some layers, but direct attribute access
	// from `objectAttributes.Metadata` map will have the keys as they are stored in GCS.
	// Standard GCS custom metadata prefix is `x-goog-meta-`.
	// Let's assume the key in the map is `preserved-depth-override` if set via `x-goog-meta-preserved-depth-override`.
	// The go client library for GCS stores metadata without the `x-goog-meta-` prefix in the ObjectAttrs.Metadata map.
	// So if you set `x-goog-meta-foo` to `bar`, `objectAttributes.Metadata["foo"]` will be `bar`.

	overrideDepthStr, ok := objectAttributes.Metadata["preserved-depth-override"] // Key without x-goog-meta-
	if ok {
		overrideDepth, err := strconv.Atoi(overrideDepthStr)
		if err == nil {
			zap.L().Info("Found 'preserved-depth-override' metadata",
				zap.String("value", overrideDepthStr),
				zap.Int("overrideValue", overrideDepth),
				zap.String("object", event.Name))
			currentPreservedFolderDepth = overrideDepth
		} else {
			zap.L().Warn("Failed to parse 'preserved-depth-override' metadata, using default/env value.",
				zap.String("value", overrideDepthStr),
				zap.Error(err),
				zap.String("object", event.Name))
		}
	}

	zap.L().Info("Processing GCS event internally",
		zap.String("bucket", event.Bucket),
		zap.String("name", event.Name),
		zap.String("writeBucket", config.WriteBucketName),
		zap.String("readBucket", config.ReadBucketName),
		zap.Int("effectivePreservedDepth", currentPreservedFolderDepth), // Log effective depth
		zap.Int("configPreservedDepth", config.PreservedFolderDepth),
	)

	if event.Bucket != config.WriteBucketName {
		zap.L().Warn("Event for incorrect bucket, skipping", zap.String("eventBucket", event.Bucket), zap.String("expectedBucket", config.WriteBucketName))
		return "", fmt.Errorf("event for bucket %s, expected %s", event.Bucket, config.WriteBucketName)
	}

	hasher := sha256.New()
	if _, err := io.Copy(hasher, objectContentReader); err != nil {
		zap.L().Error("Failed to hash object content", zap.String("object", event.Name), zap.Error(err))
		return "", fmt.Errorf("io.Copy to hasher: %w", err)
	}
	sha256Hex := hex.EncodeToString(hasher.Sum(nil))

	originalExtension := ToExtension(event.Name) // Use public version
	shardDir := ""
	if len(sha256Hex) >= 4 {
		shardDir = filepath.Join(sha256Hex[0:2], sha256Hex[2:4])
	} else {
		zap.L().Error("SHA256 hash too short for sharding", zap.String("hash", sha256Hex))
		return "", fmt.Errorf("SHA256 hash too short: %s", sha256Hex)
	}

	preservedPath := ""
	if currentPreservedFolderDepth > 0 { // Use the effective depth
		cleanedEventName := filepath.ToSlash(event.Name)
		parts := strings.Split(cleanedEventName, "/")
		if len(parts) > 0 && strings.Contains(parts[len(parts)-1], ".") {
			parts = parts[:len(parts)-1]
		}
		numPartsToPreserve := currentPreservedFolderDepth
		if numPartsToPreserve > len(parts) {
			numPartsToPreserve = len(parts)
		}
		preservedPath = strings.Join(parts[:numPartsToPreserve], "/")
	}

	destinationObjectName := filepath.Join(preservedPath, shardDir, sha256Hex+originalExtension)
	destinationObjectName = filepath.ToSlash(destinationObjectName)

	zap.L().Info("Blob processing complete",
		zap.String("sourceObject", event.Name),
		zap.String("sha256", sha256Hex),
		zap.String("destinationObject", destinationObjectName),
		zap.String("destinationBucket", config.ReadBucketName),
		zap.Int("usedPreservedDepth", currentPreservedFolderDepth),
	)

	// Actual GCS operations (using the client interface)
	// These are currently stubbed in the sense that HandleGCSEvent calls this internal function,
	// and the original HandleGCSEvent had stubs. The tests will mock the GCSClient interface
	// if they need to verify calls to these methods. For now, we're focused on destinationObjectName.

	// Example of how it would be used if not just stubbing:
	// err := gcsClient.CopyObject(ctx, event.Bucket, event.Name, config.ReadBucketName, destinationObjectName)
	// if err != nil {
	//    zap.L().Error("Failed to copy object", zap.Error(err))
	//    return "", fmt.Errorf("gcsClient.CopyObject: %w", err)
	// }
	// zap.L().Info("Successfully copied object", zap.String("destinationObject", destinationObjectName))
	//
	// err = gcsClient.DeleteObject(ctx, event.Bucket, event.Name)
	// if err != nil {
	//    zap.L().Error("Failed to delete original object", zap.Error(err))
	//    return "", fmt.Errorf("gcsClient.DeleteObject: %w", err)
	// }
	// zap.L().Info("Successfully deleted original object", zap.String("object", event.Name))

	// For the purpose of this refactoring, we are primarily interested in returning the
	// destinationObjectName for testing path logic. The actual GCS operations are stubbed out
	// in the sense that the tests won't perform them.
	zap.L().Info("[INFO] Stubbed GCS copy and delete would happen here using gcsClient.", // This log is from the internal func
		zap.String("srcBucket", event.Bucket),
		zap.String("srcObject", event.Name),
		zap.String("dstBucket", config.ReadBucketName),
		zap.String("dstObject", destinationObjectName),
	)


	return destinationObjectName, nil
}

// ProcessGCSEventInternalForTest is an exported wrapper for testing the internal logic.
func ProcessGCSEventInternalForTest(
	ctx context.Context,
	config *EventConfig,
	event GCSEvent,
	gcsClient GCSClient,
	objectContentReader io.Reader,
	objectAttributes *storage.ObjectAttrs,
) (string, error) {
	return processGCSEventInternal(ctx, config, event, gcsClient, objectContentReader, objectAttributes)
}


// HandleGCSEvent is the main entry point, now refactored to use processGCSEventInternal.
func HandleGCSEvent(ctx context.Context, event GCSEvent) error {
	zap.L().Info("Received GCS event", zap.String("bucket", event.Bucket), zap.String("name", event.Name))

	cfg := &EventConfig{} // Renamed config to cfg to avoid conflict with package name
	cfg.WriteBucketName = os.Getenv("GCS_WRITE_BUCKET_NAME")
	if cfg.WriteBucketName == "" {
		zap.L().Error("GCS_WRITE_BUCKET_NAME not set")
		return fmt.Errorf("GCS_WRITE_BUCKET_NAME not set")
	}
	cfg.ReadBucketName = os.Getenv("GCS_READ_BUCKET_NAME")
	if cfg.ReadBucketName == "" {
		zap.L().Error("GCS_READ_BUCKET_NAME not set")
		return fmt.Errorf("GCS_READ_BUCKET_NAME not set")
	}
	cfg.PreservedFolderDepth = 0 // Default
	preservedFolderDepthStr := os.Getenv("PRESERVED_FOLDER_DEPTH")
	if preservedFolderDepthStr != "" {
		parsedDepth, err := strconv.Atoi(preservedFolderDepthStr)
		if err != nil {
			zap.L().Error("Failed to parse PRESERVED_FOLDER_DEPTH, using default 0", zap.Error(err), zap.String("value", preservedFolderDepthStr))
			// Keep default 0
		} else {
			cfg.PreservedFolderDepth = parsedDepth
		}
	}

	// Initialize real GCS client
	storageClientImpl, err := storage.NewClient(ctx) // Renamed to avoid conflict
	if err != nil {
		zap.L().Error("Failed to create GCS client", zap.Error(err))
		return fmt.Errorf("storage.NewClient: %w", err)
	}
	defer storageClientImpl.Close()

	gcsClientAdapter := &StorageClientAdapter{Client: storageClientImpl}

	// Get object attributes to access metadata
	objHandle := storageClientImpl.Bucket(event.Bucket).Object(event.Name)
	attrs, err := objHandle.Attrs(ctx)
	if err != nil {
		zap.L().Error("Failed to get object attributes", zap.String("object", event.Name), zap.Error(err))
		// If object doesn't exist, Attrs will return storage.ErrObjectNotExist
		if errors.Is(err, storage.ErrObjectNotExist) {
			zap.L().Warn("Object not found, possibly already processed or deleted.", zap.String("object", event.Name))
			return nil // Or a specific error indicating this. For a trigger, this might mean it's a delete event or race.
		}
		return fmt.Errorf("objHandle.Attrs: %w", err)
	}


	// Get object reader
	reader, err := gcsClientAdapter.NewReader(ctx, event.Bucket, event.Name)
	if err != nil {
		zap.L().Error("Failed to get object reader", zap.String("object", event.Name), zap.Error(err))
		return fmt.Errorf("gcsClientAdapter.NewReader: %w", err)
	}
	defer reader.Close()

	// Call the internal processing function, now passing attributes
	destinationObjectName, err := processGCSEventInternal(ctx, cfg, event, gcsClientAdapter, reader, attrs)
	if err != nil {
		// Error already logged by processGCSEventInternal
		return err // Return the error to the caller (e.g., Cloud Function runtime)
	}

	// The actual GCS operations (copy/delete) would be part of processGCSEventInternal
	// if we weren't just focused on path logic for now.
	// Here, we're demonstrating that the real client and reader are passed.
	// For the current setup, processGCSEventInternal logs what it would do.
	// If processGCSEventInternal performed actual copy/delete, those would happen via gcsClientAdapter.

	// Example: Simulating the copy and delete logging here based on successful internal processing.
	// The actual GCS operations would typically be invoked here if processGCSEventInternal only returned paths/data
	// and not performed the operations itself.
	// However, with the current structure, processGCSEventInternal is expected to use the GCSClient
	// to perform these operations (even if they are just logged/stubbed for now).
	// The log lines "[STUBBED_IN_HANDLER]" are a bit confusing if processGCSEventInternal also logs.
	// For clarity, let's assume processGCSEventInternal is responsible for the GCS operations (via client)
	// and HandleGCSEvent is mostly for setup and calling it.

	// If processGCSEventInternal were to actually use the gcsClient to copy and delete:
	// No further GCS operations needed here, they would have been done in processGCSEventInternal.
	// The log "[INFO] Stubbed GCS copy and delete would happen here using gcsClient." from internal func covers this.

	// Removing the redundant stub logs from HandleGCSEvent as the internal function's log is sufficient.
	// If the internal function *didn't* perform operations, HandleGCSEvent would do them here.
	// For instance:
	// if err := gcsClientAdapter.CopyObject(ctx, event.Bucket, event.Name, config.ReadBucketName, destinationObjectName); err != nil { ... }
	// if err := gcsClientAdapter.DeleteObject(ctx, event.Bucket, event.Name); err != nil { ... }

	zap.L().Info("HandleGCSEvent completed processing.", zap.String("destinationObject", destinationObjectName))

	return nil
}
