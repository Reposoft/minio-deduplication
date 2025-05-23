import pytest
import os
import time
import uuid
import io
import logging

from gcp_minio_dedup_cf.gcs_utils import GCSUtil
from gcp_minio_dedup_cf import core_logic

# Configure basic logging for test output
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment variables needed for this test
TEST_WRITE_BUCKET_NAME = os.environ.get("TEST_WRITE_BUCKET_NAME")
TEST_READ_BUCKET_NAME = os.environ.get("TEST_READ_BUCKET_NAME")
GOOGLE_APPLICATION_CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
TEST_PRESERVED_FOLDER_DEPTH_STR = os.environ.get("TEST_PRESERVED_FOLDER_DEPTH", "0")

skip_reason = "Missing environment variables for integration test: "
skip_test = False
if not TEST_WRITE_BUCKET_NAME:
    skip_reason += "TEST_WRITE_BUCKET_NAME "
    skip_test = True
if not TEST_READ_BUCKET_NAME:
    skip_reason += "TEST_READ_BUCKET_NAME "
    skip_test = True
if not GOOGLE_APPLICATION_CREDENTIALS: # GCSUtil relies on this or default ADC
    skip_reason += "GOOGLE_APPLICATION_CREDENTIALS "
    skip_test = True

@pytest.mark.skipif(skip_test, reason=skip_reason.strip())
def test_end_to_end_basic_flow():
    """
    Tests the end-to-end basic flow:
    1. Upload a unique file to the "write" bucket.
    2. Wait for the Cloud Function to process it.
    3. Verify the file is archived correctly in the "read" bucket with expected metadata.
    4. Verify the original file is deleted from the "write" bucket.
    5. Cleanup created resources.
    """
    gcs = GCSUtil() # Real GCSUtil
    
    test_file_original_key = f"test-data/integration-test-{uuid.uuid4()}.txt"
    # Ensure GCS paths use forward slashes
    test_file_original_key = test_file_original_key.replace(os.sep, '/') 
    
    file_content = f"This is an end-to-end integration test content for {test_file_original_key}".encode('utf-8')
    original_custom_meta_key = "custom-test-meta" # GCS custom metadata keys are case-insensitive
    original_custom_meta_value = f"e2e-value-{uuid.uuid4()}"
    
    # Metadata to be set on the uploaded object.
    # GCSUtil.upload_object_from_stream will handle 'content_type' separately
    # and other keys as custom metadata.
    original_upload_metadata = {
        'content_type': 'text/plain; charset=utf-8',
        original_custom_meta_key: original_custom_meta_value
    }

    try:
        preserved_depth = int(TEST_PRESERVED_FOLDER_DEPTH_STR)
    except ValueError:
        logger.warning(f"Invalid TEST_PRESERVED_FOLDER_DEPTH: '{TEST_PRESERVED_FOLDER_DEPTH_STR}'. Defaulting to 0.")
        preserved_depth = 0

    logger.info(f"Starting E2E test for object: gs://{TEST_WRITE_BUCKET_NAME}/{test_file_original_key}")
    logger.info(f"Read bucket: gs://{TEST_READ_BUCKET_NAME}, Preserved depth: {preserved_depth}")

    # Calculate Expected Outcome (using core_logic)
    sha256_hash = core_logic.calculate_sha256(io.BytesIO(file_content))
    original_ext = core_logic.get_original_extension(test_file_original_key)
    expected_archive_object_path = core_logic.construct_destination_path(
        test_file_original_key, sha256_hash, original_ext, preserved_depth
    )
    expected_archive_object_path = expected_archive_object_path.replace(os.sep, '/')
    logger.info(f"Expected archive path: gs://{TEST_READ_BUCKET_NAME}/{expected_archive_object_path}")

    # Upload to "Write" Bucket
    try:
        logger.info(f"Uploading gs://{TEST_WRITE_BUCKET_NAME}/{test_file_original_key}...")
        gcs.upload_object_from_stream(
            TEST_WRITE_BUCKET_NAME, 
            test_file_original_key, 
            io.BytesIO(file_content), 
            original_upload_metadata
        )
        logger.info(f"Upload complete.")

        # Wait for Cloud Function Processing
        # Increased sleep time for GCF cold starts and processing.
        # For robust CI, polling or log checking is better.
        processing_time = 60 
        logger.info(f"Waiting {processing_time} seconds for Cloud Function processing...")
        time.sleep(processing_time)

        # Verification in "Read" Bucket
        logger.info(f"Verifying object in read bucket: gs://{TEST_READ_BUCKET_NAME}/{expected_archive_object_path}")
        archived_object_stat = gcs.stat_object(TEST_READ_BUCKET_NAME, expected_archive_object_path)
        assert archived_object_stat is not None, \
            f"Archived object gs://{TEST_READ_BUCKET_NAME}/{expected_archive_object_path} not found."
        logger.info(f"Archived object found. Verifying content and metadata...")

        stream, archived_metadata_full = gcs.get_object_stream_and_metadata(
            TEST_READ_BUCKET_NAME, expected_archive_object_path
        )
        retrieved_content = stream.read()
        stream.close()

        assert retrieved_content == file_content, "Archived file content does not match original."
        logger.info(f"Content matched.")

        # Verify standard metadata
        assert archived_metadata_full.get('Content-Type') == original_upload_metadata['content_type'], \
            f"Content-Type mismatch. Expected {original_upload_metadata['content_type']}, Got {archived_metadata_full.get('Content-Type')}"
        
        expected_filename = os.path.basename(test_file_original_key)
        expected_disposition = f'attachment; filename="{expected_filename}"'
        # Content-Disposition might have quotes escaped differently by GCS, so a 'in' check is safer
        assert expected_filename in archived_metadata_full.get('Content-Disposition', ''), \
            f"Content-Disposition filename part mismatch. Expected '{expected_filename}' in '{archived_metadata_full.get('Content-Disposition', '')}'"
        logger.info(f"Standard metadata (Content-Type, Content-Disposition) verified.")
        
        # Verify custom metadata (GCSUtil flattens blob.metadata into the main dict)
        assert archived_metadata_full.get(original_custom_meta_key.lower()) == original_custom_meta_value, \
             f"Custom metadata '{original_custom_meta_key}' mismatch or missing."
        
        # Verify Uploadpaths (this is also custom metadata)
        # For a new file, Uploadpaths should be just the original key.
        # metadata_utils.prepare_new_metadata encodes path components in Uploadpaths.
        encoded_original_key = core_logic.metadata_utils._encode_path_component(test_file_original_key)
        assert archived_metadata_full.get('Uploadpaths') == encoded_original_key, \
            f"Uploadpaths mismatch. Expected '{encoded_original_key}', Got '{archived_metadata_full.get('Uploadpaths')}'"
        logger.info(f"Custom metadata (custom test meta, Uploadpaths) verified.")


        # Verification in "Write" Bucket (original should be deleted)
        logger.info(f"Verifying original object deletion from write bucket: gs://{TEST_WRITE_BUCKET_NAME}/{test_file_original_key}")
        original_object_stat_after = gcs.stat_object(TEST_WRITE_BUCKET_NAME, test_file_original_key)
        assert original_object_stat_after is None, \
            f"Original object gs://{TEST_WRITE_BUCKET_NAME}/{test_file_original_key} was not deleted."
        logger.info(f"Original object successfully deleted from write bucket.")

    finally:
        # Cleanup
        logger.info("Starting cleanup...")
        try:
            logger.info(f"Attempting to delete archived object: gs://{TEST_READ_BUCKET_NAME}/{expected_archive_object_path}")
            gcs.delete_object(TEST_READ_BUCKET_NAME, expected_archive_object_path)
            logger.info(f"Deleted archived object.")
        except Exception as e:
            logger.error(f"Error during cleanup of read bucket object: {e}")
        
        try:
            # Attempt to delete from write bucket in case it wasn't processed and deleted by function
            logger.info(f"Attempting to delete original object (if exists): gs://{TEST_WRITE_BUCKET_NAME}/{test_file_original_key}")
            gcs.delete_object(TEST_WRITE_BUCKET_NAME, test_file_original_key)
            logger.info(f"Deleted original object (if it still existed).")
        except Exception as e:
            logger.error(f"Error during cleanup of write bucket object: {e}")
        logger.info("Cleanup finished.")

```
