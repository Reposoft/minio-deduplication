# Serverless GCS Deduplication Function

## Overview

This project provides a Google Cloud Function for hash-based deduplication and sharded storage of files uploaded to a Google Cloud Storage (GCS) bucket. It is a Python-based port of an existing Go application, enhanced with new features.

The function automatically processes new files, calculates their SHA256 hash, and stores them in a structured, sharded manner in a separate GCS bucket. This helps in saving storage space by avoiding redundant copies of identical files and organizes the archive efficiently.

**Key Features:**

*   **SHA256 Hashing:** Uniquely identifies files based on their content.
*   **Directory Sharding:** Organizes archived files into subdirectories based on hash prefixes (e.g., `ab/cd/`).
*   **Metadata Preservation & Update:**
    *   Tracks original upload paths (`Uploadpaths`).
    *   Tracks original upload directories (`Uploaddir`).
    *   Preserves and sets appropriate `Content-Type` and `Content-Disposition`.
*   **Configurable Folder Depth Preservation:** A new feature allowing preservation of a specified number of leading directory levels from the original file path in the archive.

*Note: The working directory `gcp_minio_dedup_cf` retains 'minio' in its name due to its origin as a port of a Minio-based Go project. The actual implementation is fully GCP native and does not use Minio.*

## How it Works

The deduplication process follows these steps:

1.  A file is uploaded to a designated "write" GCS bucket.
2.  This upload event triggers the `gcs_deduplicate_on_upload_handler` Cloud Function.
3.  The function downloads the file and calculates its SHA256 hash.
4.  It determines the destination path in a separate "read" (archive) GCS bucket. This path includes:
    *   A sharded structure based on the hash (e.g., `<hash_prefix1>/<hash_prefix2>/`).
    *   Optionally, a prefix from the original file's folder structure, based on the `PRESERVED_FOLDER_DEPTH` configuration.
    *   The full SHA256 hash as the filename, followed by the original file extension.
5.  The function checks if an object with the same SHA256 hash already exists at the destination path in the "read" bucket.
    *   **If it exists (duplicate found):** The existing object's metadata is updated (e.g., `Uploadpaths` and `Uploaddir` are appended with the new source information). The original uploaded file's content is not re-copied.
    *   **If it does not exist (new file):** The uploaded file is copied to the destination path in the "read" bucket with new metadata.
6.  The original file is then deleted from the "write" bucket.

## New Feature: Configurable Folder Depth Preservation

The `PRESERVED_FOLDER_DEPTH` environment variable allows you to control how many leading directory levels from the original file's path are preserved in the archive destination path.

**Examples:**

Assume original file path in "write" bucket: `uploads/project_alpha/images/pic.png`
Archive destination filename (hash + ext): `0123456789abcdef.png`
Sharded part: `01/23/`

*   **`PRESERVED_FOLDER_DEPTH=0` (Default):**
    *   Original: `write_bucket/uploads/project_alpha/images/pic.png`
    *   Archived: `read_bucket/01/23/0123456789abcdef.png`

*   **`PRESERVED_FOLDER_DEPTH=1`:**
    *   Original: `write_bucket/uploads/project_alpha/images/pic.png`
    *   Archived: `read_bucket/uploads/01/23/0123456789abcdef.png`

*   **`PRESERVED_FOLDER_DEPTH=2`:**
    *   Original: `write_bucket/uploads/project_alpha/images/pic.png`
    *   Archived: `read_bucket/uploads/project_alpha/01/23/0123456789abcdef.png`

*   **`PRESERVED_FOLDER_DEPTH=3` (and actual depth is 3 like in example):**
    *   Original: `write_bucket/uploads/project_alpha/images/pic.png`
    *   Archived: `read_bucket/uploads/project_alpha/images/01/23/0123456789abcdef.png`

If `PRESERVED_FOLDER_DEPTH` is greater than the actual depth of the original path, all available directory levels will be preserved.

## Deployment and Configuration

For detailed instructions on:
*   Provisioning necessary GCP resources (GCS buckets, Service Accounts).
*   Deploying the Cloud Function.
*   Configuring environment variables (`READ_BUCKET_NAME`, `PRESERVED_FOLDER_DEPTH`).

Please refer to the [DEPLOYMENT.MD](DEPLOYMENT.md) file.

## Testing

This project includes a comprehensive suite of tests:

*   **Unit Tests:** Located in `tests/test_main.py`, these tests cover individual functions in `core_logic.py` and `metadata_utils.py`. They use `pytest` and mock objects where necessary.
*   **Mocked Integration Tests:**
    *   `tests/test_ported_basic_flow.py`: Simulates the basic flow of uploading new, unique files.
    *   `tests/test_ported_deduplication.py`: Simulates the deduplication flow where a file with identical content to an existing one is uploaded.
    *   `tests/test_ported_folder_depth.py`: Tests the folder depth preservation feature with various configurations.
    These tests use `pytest` and a mocked `GCSUtil` to simulate GCS interactions without needing live resources.
*   **End-to-End Integration Test:**
    *   `tests/test_integration_e2e.py`: This test runs against live GCP resources. It uploads a file to a real "write" GCS bucket, waits for the deployed Cloud Function to process it, and then verifies the outcome in a real "read" GCS bucket.
    *   **Setup for E2E Tests:** Requires specific environment variables (`TEST_WRITE_BUCKET_NAME`, `TEST_READ_BUCKET_NAME`, `GOOGLE_APPLICATION_CREDENTIALS`, `TEST_PRESERVED_FOLDER_DEPTH`) to be configured. Refer to [DEPLOYMENT.MD](DEPLOYMENT.MD) for instructions on setting up credentials for these tests.

**Running Tests:**

From the parent directory of `gcp_minio_dedup_cf`:
```bash
pytest gcp_minio_dedup_cf/tests
```

Alternatively, if your current directory is `gcp_minio_dedup_cf`:
```bash
pytest tests
```
The end-to-end test (`test_integration_e2e.py`) will be skipped automatically if the required environment variables are not set.
```
