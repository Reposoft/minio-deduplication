package integration

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	// Assuming cfg, storageService, and appMonitor are initialized in TestMain as per basic_flow_test.go
)

func TestFolderDepthFeatureGCS(t *testing.T) {
	if cfg.TestTarget != "gcs" {
		t.Skip("Skipping folder depth tests because TEST_TARGET is not 'gcs'")
	}
	if storageService == nil || appMonitor == nil {
		t.Fatal("Test services not initialized. Ensure TestMain is correctly setting up for GCS.")
	}

	ctx := context.Background()

	type testCase struct {
		name                          string
		uploadPath                    string // e.g., "level1/level2/testfile.txt"
		metadataOverrideDepth         string // Value for "preserved-depth-override" metadata, e.g., "0", "1", "2", or "" for no override
		envPreservedFolderDepth       int    // To simulate or confirm behavior if metadata override is not primary
		expectedPreservedPortionInPath string // e.g., "level1/level2"
		fileContent                   string
		contentType                   string
	}

	testCases := []testCase{
		{
			name:                          "Depth 0 via Metadata",
			uploadPath:                    "level1/level2/fileA.txt",
			metadataOverrideDepth:         "0",
			envPreservedFolderDepth:       2, // Assume env is higher, metadata should override
			expectedPreservedPortionInPath: "", // No path preserved
			fileContent:                   "Content for file A - depth 0 metadata",
			contentType:                   "text/plain",
		},
		{
			name:                          "Depth 1 via Metadata",
			uploadPath:                    "level1/level2/fileB.txt",
			metadataOverrideDepth:         "1",
			envPreservedFolderDepth:       0, // Assume env is lower
			expectedPreservedPortionInPath: "level1",
			fileContent:                   "Content for file B - depth 1 metadata",
			contentType:                   "text/plain",
		},
		{
			name:                          "Depth 2 via Metadata",
			uploadPath:                    "level1/level2/fileC.txt",
			metadataOverrideDepth:         "2",
			envPreservedFolderDepth:       0,
			expectedPreservedPortionInPath: "level1/level2",
			fileContent:                   "Content for file C - depth 2 metadata",
			contentType:                   "text/plain",
		},
		{
			name:                          "Depth 3 via Metadata (more than available)",
			uploadPath:                    "level1/level2/fileD.txt",
			metadataOverrideDepth:         "3",
			envPreservedFolderDepth:       0,
			expectedPreservedPortionInPath: "level1/level2", // Preserves all available
			fileContent:                   "Content for file D - depth 3 metadata (more than available)",
			contentType:                   "text/plain",
		},
		{
			name:                          "No Metadata Override (use env depth if function respects it)",
			uploadPath:                    "level1/level2/level3/fileE.txt",
			metadataOverrideDepth:         "", // No override, rely on env var set for the Cloud Function
			envPreservedFolderDepth:       cfg.PreservedFolderDepth, // Use actual env depth from config
			// Calculate expected portion based on cfg.PreservedFolderDepth
			// This requires GetPreservedPortion helper or similar logic here
			expectedPreservedPortionInPath: getExpectedPreservedPortion("level1/level2/level3/fileE.txt", cfg.PreservedFolderDepth),
			fileContent:                   "Content for file E - no metadata override",
			contentType:                   "text/plain",
		},
		{
			name:                          "Root Upload with Depth 1 Metadata",
			uploadPath:                    "fileF.txt",
			metadataOverrideDepth:         "1",
			envPreservedFolderDepth:       0,
			expectedPreservedPortionInPath: "", // No path to preserve
			fileContent:                   "Content for file F - root upload, depth 1 metadata",
			contentType:                   "text/plain",
		},
		{
			name:                          "Invalid Metadata Override (should use env depth)",
			uploadPath:                    "level1/fileG.txt",
			metadataOverrideDepth:         "not-a-number",
			envPreservedFolderDepth:       cfg.PreservedFolderDepth,
			expectedPreservedPortionInPath: getExpectedPreservedPortion("level1/fileG.txt", cfg.PreservedFolderDepth),
			fileContent:                   "Content for file G - invalid metadata override",
			contentType:                   "text/plain",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			contentBytes := []byte(tc.fileContent)
			sha256sum, err := CalculateSHA256(bytes.NewReader(contentBytes))
			if err != nil {
				t.Fatalf("Failed to calculate SHA256: %v", err)
			}

			// Determine the actual preserved depth to use for calculating expected path
			// This mimics the logic in the Cloud Function: metadata first, then env.
			// For testing `expectedPreservedPortionInPath`, we directly use it.
			// The `GetExpectedShardedPath` helper itself takes `preservedDepth` as an argument,
			// but for constructing the final path, we need to know what the *application* will use.
			// The `expectedPreservedPortionInPath` is what we assert.
			// The `helpers.GetExpectedShardedPath` will use `tc.expectedPreservedPortionInPath`
			// indirectly by how the final path is formed.

			// Simplified: helpers.GetExpectedShardedPath directly takes the full original path and applies depth.
			// We need to ensure `tc.expectedPreservedPortionInPath` is correctly used.
			// Let's adjust `GetExpectedShardedPath` or how we call it.
			// The current `helpers.GetExpectedShardedPath` takes `originalFullObjectPath` and `preservedDepth`.
			// We need to calculate the `preservedDepth` that results in `tc.expectedPreservedPortionInPath`.
			// Or, more simply, construct the expected path directly.

			shardDir := fmt.Sprintf("%s/%s", sha256sum[0:2], sha256sum[2:4])
			baseName := sha256sum + strings.ToLower(GetExtension(tc.uploadPath)) // GetExtension is a simplified helper
			
			expectedDestinationPath := baseName
			if tc.expectedPreservedPortionInPath != "" {
				expectedDestinationPath = fmt.Sprintf("%s/%s/%s", tc.expectedPreservedPortionInPath, shardDir, baseName)
			} else {
				expectedDestinationPath = fmt.Sprintf("%s/%s", shardDir, baseName)
			}
			expectedDestinationPath = strings.ReplaceAll(expectedDestinationPath, "//", "/") // Clean up potential double slashes if portion is empty


			t.Logf("Test Case: %s", tc.name)
			t.Logf("  Upload Path: %s", tc.uploadPath)
			t.Logf("  Metadata Override Depth: '%s'", tc.metadataOverrideDepth)
			t.Logf("  Content SHA256: %s", sha256sum)
			t.Logf("  Expected Preserved Portion: '%s'", tc.expectedPreservedPortionInPath)
			t.Logf("  Expected Destination Path in Read Bucket: %s", expectedDestinationPath)

			userMeta := make(map[string]string)
			if tc.metadataOverrideDepth != "" {
				// IMPORTANT: GCS Go client library expects metadata keys WITHOUT "x-goog-meta-" prefix.
				// It adds the prefix when sending the request.
				userMeta["preserved-depth-override"] = tc.metadataOverrideDepth
			}

			// Upload the file
			err = storageService.UploadObject(ctx, cfg.WriteBucketName, tc.uploadPath, bytes.NewReader(contentBytes), int64(len(contentBytes)), tc.contentType, userMeta)
			if err != nil {
				t.Fatalf("Failed to upload %s: %v", tc.uploadPath, err)
			}

			// Poll for the object in the read bucket
			var retrievedObjectInfo *ObjectInfo
			pollErr := PollUntil(func() (bool, error) {
				objInfo, statErr := storageService.StatObject(ctx, cfg.ReadBucketName, expectedDestinationPath)
				if statErr != nil {
					t.Logf("Polling: Error stating object %s (retrying): %v", expectedDestinationPath, statErr)
					return false, nil // Continue polling on error
				}
				if objInfo != nil {
					retrievedObjectInfo = objInfo
					return true, nil
				}
				return false, nil
			}, cfg.PollTimeout, cfg.PollInterval)

			if pollErr != nil {
				t.Fatalf("Object %s did not appear in read bucket at %s: %v", tc.uploadPath, expectedDestinationPath, pollErr)
			}
			t.Logf("Found object in read bucket: %s", retrievedObjectInfo.Key)

			// Verify content
			objReader, getErr := storageService.GetObject(ctx, cfg.ReadBucketName, expectedDestinationPath)
			if getErr != nil {
				t.Fatalf("Failed to get object %s for content verification: %v", expectedDestinationPath, getErr)
			}
			defer objReader.Close()
			retrievedContent, readErr := io.ReadAll(objReader)
			if readErr != nil {
				t.Fatalf("Failed to read content of %s: %v", expectedDestinationPath, readErr)
			}
			if !bytes.Equal(contentBytes, retrievedContent) {
				t.Errorf("Content mismatch for %s. Expected '%s', got '%s'", expectedDestinationPath, string(contentBytes), string(retrievedContent))
			}

			// Verify logs (check for effective preserved depth if possible)
			// The GCS function logs "effectivePreservedDepth" and "usedPreservedDepth".
			// We can check for this.
			effectiveDepthToLog := tc.metadataOverrideDepth
			if tc.metadataOverrideDepth == "" || tc.metadataOverrideDepth == "not-a-number" {
				// If no override or invalid, it uses env var.
				// The log will show the value from env (cfg.PreservedFolderDepth)
				effectiveDepthToLog = fmt.Sprintf("%d", cfg.PreservedFolderDepth) 
			}
			// If override is "3" for "l1/l2/f.txt", effective becomes 2. The "usedPreservedDepth" log reflects this.
			// The logic inside the cloud function for numPartsToPreserve handles this clipping.
			// For "level1/level2/fileD.txt" (depth 3 override), parts are ["level1", "level2"], numPartsToPreserve = min(3,2) = 2.
			// So "usedPreservedDepth" should be 2.
			// Let's calculate the *actual* depth that would have been used by the function for the "usedPreservedDepth" log.
			
			calculatedUsedDepth := cfg.PreservedFolderDepth // Start with ENV
			if ovDepth, convErr := strconv.Atoi(tc.metadataOverrideDepth); convErr == nil {
				calculatedUsedDepth = ovDepth // Metadata override is valid
			}
			pathParts := strings.Split(strings.Trim(filepath.Dir(tc.uploadPath), "/"), "/")
			if tc.uploadPath == filepath.Base(tc.uploadPath) { // Root upload
				pathParts = []string{}
			}
			if calculatedUsedDepth > len(pathParts) {
				calculatedUsedDepth = len(pathParts)
			}
			if calculatedUsedDepth < 0 { // Depth cannot be negative
				calculatedUsedDepth = 0
			}


			expectedLogMessages := []string{
				fmt.Sprintf("Successfully processed blob %s", sha256sum),
				"destinationObject=" + expectedDestinationPath,
				// Check for the log showing the depth used by the function
				// Example: "usedPreservedDepth":calculatedUsedDepth (actual log might vary slightly)
				// The log in gcs_transfer.go is: zap.Int("usedPreservedDepth", currentPreservedFolderDepth)
				// where currentPreservedFolderDepth is *after* clipping to available parts.
				fmt.Sprintf(`"usedPreservedDepth":%d`, calculatedUsedDepth),
			}
			if tc.metadataOverrideDepth != "" && tc.metadataOverrideDepth != "not-a-number" {
				// Also check the log that shows the override was attempted/parsed
				expectedLogMessages = append(expectedLogMessages, fmt.Sprintf(`"overrideValue":%s`, tc.metadataOverrideDepth))
			}


			logCheckPassed, logErr := appMonitor.CheckProcessingLogs(ctx, sha256sum, expectedLogMessages)
			if logErr != nil {
				t.Errorf("Error checking GCS processing logs for %s: %v", sha256sum, logErr)
			} else if !logCheckPassed {
				t.Errorf("Expected GCS processing logs for %s (depth override '%s', used depth %d) not found or incomplete.", sha256sum, tc.metadataOverrideDepth, calculatedUsedDepth)
			} else {
				t.Logf("GCS processing logs verification for %s (depth override '%s', used depth %d) succeeded.", sha256sum, tc.metadataOverrideDepth, calculatedUsedDepth)
			}

			// Cleanup
			if err := storageService.DeleteObject(ctx, cfg.ReadBucketName, expectedDestinationPath); err != nil {
				t.Logf("Warning: Failed to delete object from read bucket: %v", err)
			}
			// The write bucket object should have been deleted by the function.
			// We can verify this too.
			sourceObjectInfo, _ := storageService.StatObject(ctx, cfg.WriteBucketName, tc.uploadPath)
			if sourceObjectInfo != nil {
				t.Errorf("Source object %s still exists in write bucket %s", tc.uploadPath, cfg.WriteBucketName)
				if err := storageService.DeleteObject(ctx, cfg.WriteBucketName, tc.uploadPath); err != nil {
					t.Logf("Warning: Failed to delete source object from write bucket: %v", err)
				}
			}
		})
	}
}

// getExpectedPreservedPortion calculates the expected preserved path string.
// Example: fullPath="a/b/c/file.txt", depth=2 -> "a/b"
// Example: fullPath="a/file.txt", depth=2 -> "a"
// Example: fullPath="file.txt", depth=1 -> ""
func getExpectedPreservedPortion(fullPath string, depth int) string {
	if depth <= 0 {
		return ""
	}
	// Ensure forward slashes, then get directory part, then split.
	dir := filepath.ToSlash(filepath.Dir(fullPath))
	if dir == "." || dir == "/" { // filepath.Dir of "file.txt" is "."
		return ""
	}
	parts := strings.Split(dir, "/")
	if depth > len(parts) {
		depth = len(parts)
	}
	return strings.Join(parts[:depth], "/")
}

// GetExtension is a simplified helper for tests, assuming basic extensions.
func GetExtension(path string) string {
	ext := strings.ToLower(filepath.Ext(path))
	if ext == ".jpeg" {
		return ".jpg"
	}
	return ext
}

// Note: This test file assumes TestMain in basic_flow_test.go or similar correctly
// initializes `cfg`, `storageService`, and `appMonitor` for GCS when TEST_TARGET=gcs.
```

**Summary of `folder_depth_test.go`:**
- Skips if `TEST_TARGET` is not "gcs".
- Defines a table-driven test `TestFolderDepthFeatureGCS`.
- Each test case specifies:
    - `uploadPath`: Original path of the file in the write bucket.
    - `metadataOverrideDepth`: Value for `preserved-depth-override` GCS metadata (or "" for no override).
    - `expectedPreservedPortionInPath`: The string expected to be preserved (e.g., "level1/level2").
- Uploads files with the specified path and metadata override.
- Calculates the `expectedDestinationPath` based on the `expectedPreservedPortionInPath`, SHA256 hash, and sharding logic.
- Polls for the object in the read bucket and verifies its content.
- **Crucially, it verifies logs using `CheckProcessingLogs` to confirm that the GCS function used the correct `preservedFolderDepth` (either from metadata or environment).** This involves checking for specific log messages like `"usedPreservedDepth":X`.
- Performs cleanup of created objects.
- Includes a helper `getExpectedPreservedPortion` to calculate the expected path part based on depth, which is used for setting up test case expectations.

This test suite, combined with the modification to `gcs/gcs_transfer.go`, should thoroughly test the configurable folder depth feature for the GCS implementation.

Final check of deliverables:
- New `folder_depth_test.go` file with comprehensive test cases. (Done)
- Modifications to `gcs/gcs_transfer.go` if Option B is chosen for controlling `PRESERVED_FOLDER_DEPTH` per invocation. (Done in the previous step)
- Confirmation that tests pass against a GCS environment. (This will be done by running the tests in a GCS environment).

The implementation seems complete.I have modified `gcs/gcs_transfer.go` to read the `preserved-depth-override` metadata and created `integration_tests/go/folder_depth_test.go` with test cases for the folder depth feature.

Key aspects of `folder_depth_test.go`:
-   The test suite `TestFolderDepthFeatureGCS` only runs if `cfg.TestTarget == "gcs"`.
-   It uses a table-driven approach with various scenarios for `uploadPath` and `metadataOverrideDepth`.
-   For each test case, it calculates the `expectedDestinationPath` based on the `expectedPreservedPortionInPath` (which itself is derived from the depth setting being tested).
-   It uploads a file with the specified `uploadPath` and sets the `preserved-depth-override` metadata on the GCS object.
-   It polls for the object in the read bucket and verifies its content.
-   It uses `appMonitor.CheckProcessingLogs` to verify that the GCS Cloud Function logged the correct `usedPreservedDepth`, confirming that the metadata override (or fallback to environment variable) worked as expected.
-   It includes cleanup steps for objects in the read and write buckets.
-   A helper `getExpectedPreservedPortion` assists in defining test case expectations.

This completes the implementation for testing the configurable folder depth feature in GCS. The next step would be to run these tests in a configured GCS environment with the updated Cloud Function deployed.
