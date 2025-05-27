package integration

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"
)

// GenerateRandomString creates a random hex string of a given byte length.
// The final string length will be 2 * byteLength.
func GenerateRandomString(byteLength int) (string, error) {
	b := make([]byte, byteLength)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// CalculateSHA256 calculates the SHA256 hash of content from an io.Reader.
func CalculateSHA256(r io.Reader) (string, error) {
	hasher := sha256.New()
	if _, err := io.Copy(hasher, r); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

// GetExpectedShardedPath calculates the expected destination path for an object
// based on its SHA256 sum, original extension, preserved folder depth, and original path.
// This logic must mirror the application's path generation.
func GetExpectedShardedPath(sha256sum, originalFilename string, preservedDepth int, originalFullObjectPath string) string {
	if sha256sum == "" || len(sha256sum) < 4 {
		// This case should ideally not happen if hashing is successful
		return ""
	}

	originalExtension := strings.ToLower(filepath.Ext(originalFilename))
	if originalExtension == ".jpeg" {
		originalExtension = ".jpg"
	}

	shardDir := filepath.Join(sha256sum[0:2], sha256sum[2:4])

	preservedPath := ""
	if preservedDepth > 0 {
		// Use filepath.ToSlash to ensure consistent path separators (forward slashes)
		// before splitting, as GCS/S3 uses forward slashes.
		cleanedOriginalFullObjectPath := filepath.ToSlash(originalFullObjectPath)
		parts := strings.Split(cleanedOriginalFullObjectPath, "/")

		// Remove filename from parts to only consider directory components
		if len(parts) > 0 { // Check if there are any parts
			// If the last part contains a dot, it's likely a file, so remove it.
			// Otherwise, assume all parts are directories (e.g. "a/b/c" with no file).
			if strings.Contains(parts[len(parts)-1], ".") {
				parts = parts[:len(parts)-1]
			}
		}
		
		numPartsToPreserve := preservedDepth
		if numPartsToPreserve > len(parts) {
			numPartsToPreserve = len(parts)
		}
		preservedPath = strings.Join(parts[:numPartsToPreserve], "/")
	}

	// Use filepath.Join for OS-agnostic path construction, then convert to forward slashes for storage paths.
	finalPath := filepath.Join(preservedPath, shardDir, sha256sum+originalExtension)
	return filepath.ToSlash(finalPath)
}

// PollUntil repeatedly calls the condition function until it returns true or the timeout is reached.
// Returns an error if the timeout is reached or if the condition function returns an error.
func PollUntil(condition func() (bool, error), timeout time.Duration, interval time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok, err := condition()
		if err != nil {
			return fmt.Errorf("condition check failed: %w", err) // Condition function indicated a hard error
		}
		if ok {
			return nil // Condition met
		}
		time.Sleep(interval)
	}
	return fmt.Errorf("timeout reached after %v", timeout)
}
