import pytest
from unittest.mock import MagicMock, call
import io
import os # For os.sep

# Assuming project structure allows these imports
from gcp_minio_dedup_cf.gcs_utils import GCSUtil
from gcp_minio_dedup_cf import core_logic
from gcp_minio_dedup_cf import metadata_utils

# PRESERVED_FOLDER_DEPTH for these tests
PRESERVED_FOLDER_DEPTH = 0

@pytest.fixture
def mock_gcs_util(mocker):
    """Provides a mocked GCSUtil instance."""
    mocked_gcs_util_instance = mocker.MagicMock(spec=GCSUtil)
    
    # Mock the methods that will be called
    mocked_gcs_util_instance.get_object_stream_and_metadata = mocker.MagicMock()
    mocked_gcs_util_instance.stat_object = mocker.MagicMock()
    mocked_gcs_util_instance.upload_object_from_stream = mocker.MagicMock() # Though not directly in basic_flow, good to have
    mocked_gcs_util_instance.copy_object = mocker.MagicMock()
    mocked_gcs_util_instance.delete_object = mocker.MagicMock()
    
    # Patch the GCSUtil class to return this instance when instantiated
    # This is useful if the main code creates an instance of GCSUtil internally.
    # For this test, we might pass the instance directly if the main_handler takes it.
    # For now, we'll assume we can directly use this mocked instance in our test setup.
    # If main.py instantiates GCSUtil(), then we'd do:
    # mocker.patch('gcp_minio_dedup_cf.main.GCSUtil', return_value=mocked_gcs_util_instance)
    # Or if GCSUtil is instantiated elsewhere and passed:
    # mocker.patch('gcp_minio_dedup_cf.gcs_utils.GCSUtil', return_value=mocked_gcs_util_instance)
    
    return mocked_gcs_util_instance


def test_basic_flow_two_files(mock_gcs_util):
    """
    Tests the basic flow of uploading two distinct files,
    simulating the logic from basic-flow.sh.
    """
    write_bucket = "test_write_bucket"
    read_bucket = "test_read_bucket"

    # --- Simulate processing for test1.txt ---
    file_content_1 = b"content of test1"
    original_key_1 = "test1.txt"
    # As per GCSUtil, metadata includes 'Content-Type' and custom metadata is flat
    original_gcs_metadata_1 = {'Content-Type': 'text/testing1', 'custom_key': 'custom_value1'}

    # 1. Mock GCSUtil.get_object_stream_and_metadata for file1
    # The mock_gcs_util is already an instance, so we set return_value on its methods
    mock_gcs_util.get_object_stream_and_metadata.return_value = (io.BytesIO(file_content_1), original_gcs_metadata_1)

    # 2. Calculate hash and paths (using actual core_logic)
    # Rewind stream for multiple reads if calculate_sha256 doesn't do it. io.BytesIO can be re-read.
    sha256_hash_1 = core_logic.calculate_sha256(io.BytesIO(file_content_1))
    extension_1 = core_logic.get_original_extension(original_key_1)
    archive_path_1 = core_logic.construct_destination_path(
        original_path=original_key_1, # For PRESERVED_FOLDER_DEPTH=0, this is fine
        sha256_hash=sha256_hash_1,
        original_extension=extension_1,
        preserved_depth=PRESERVED_FOLDER_DEPTH
    )
    # Ensure paths use os.sep for consistency in assertions if needed, though GCS uses '/'
    archive_path_1 = archive_path_1.replace(os.sep, '/')


    # 3. Mock GCSUtil.stat_object for checking existence in archive (file not found first time)
    # We'll use side_effect to handle different calls for file1 and file2 if hashes were same
    # For distinct files, simple return_value is okay per file.
    mock_gcs_util.stat_object.return_value = None 

    # 4. Prepare expected metadata for archive (using actual metadata_utils)
    # metadata_utils.prepare_new_metadata expects original_object_metadata to be the dict
    # that GCSUtil's get_object_stream_and_metadata returns as its second element.
    expected_archive_metadata_1 = metadata_utils.prepare_new_metadata(
        original_object_key=original_key_1,
        original_object_metadata=original_gcs_metadata_1, 
        existing_archive_object_metadata=None 
    )

    # --- Conceptual main_handler logic for test1.txt (simulated by direct calls) ---
    # This part simulates what a future main_handler function would execute.
    
    # a. Get object stream and metadata
    stream_1, meta_1 = mock_gcs_util.get_object_stream_and_metadata(write_bucket, original_key_1)
    # b. Calculate hash (already did for setup, but handler would do it)
    # c. Construct archive path (already did for setup)
    # d. Check if archive object exists
    existing_meta_1 = mock_gcs_util.stat_object(read_bucket, archive_path_1)
    # e. Prepare metadata for the new archive object (already did for setup)
    # f. Copy object
    mock_gcs_util.copy_object(
        source_bucket_name=write_bucket,
        source_blob_name=original_key_1,
        dest_bucket_name=read_bucket,
        dest_blob_name=archive_path_1,
        new_metadata=expected_archive_metadata_1
    )
    # g. Delete original object
    mock_gcs_util.delete_object(write_bucket, original_key_1)
    
    # Assertions for test1.txt
    mock_gcs_util.get_object_stream_and_metadata.assert_called_with(write_bucket, original_key_1)
    mock_gcs_util.stat_object.assert_called_with(read_bucket, archive_path_1)
    mock_gcs_util.copy_object.assert_called_with(
        write_bucket, original_key_1, read_bucket, archive_path_1, new_metadata=expected_archive_metadata_1
    )
    mock_gcs_util.delete_object.assert_called_with(write_bucket, original_key_1)

    # --- Reset mocks for the next file if necessary, or use call_args_list ---
    # For this test, we'll check call_args_list at the end for aggregate counts,
    # but specific calls for file2 will need new return_values setup for mocks like get_object_stream_and_metadata and stat_object.

    # --- Simulate processing for test2.txt ---
    file_content_2 = b"content of test2 which is different" # Different content
    original_key_2 = "test2.txt" # Different key
    original_gcs_metadata_2 = {'Content-Type': 'text/testing2', 'custom_key': 'custom_value2'}

    # 1. Mock GCSUtil.get_object_stream_and_metadata for file2
    mock_gcs_util.get_object_stream_and_metadata.return_value = (io.BytesIO(file_content_2), original_gcs_metadata_2)
    
    # 2. Calculate hash and paths for file2
    sha256_hash_2 = core_logic.calculate_sha256(io.BytesIO(file_content_2))
    extension_2 = core_logic.get_original_extension(original_key_2)
    archive_path_2 = core_logic.construct_destination_path(
        original_path=original_key_2,
        sha256_hash=sha256_hash_2,
        original_extension=extension_2,
        preserved_depth=PRESERVED_FOLDER_DEPTH
    )
    archive_path_2 = archive_path_2.replace(os.sep, '/')

    # 3. Mock GCSUtil.stat_object for file2 (also not found first time, as hash is different)
    mock_gcs_util.stat_object.return_value = None 

    # 4. Prepare expected metadata for archive for file2
    expected_archive_metadata_2 = metadata_utils.prepare_new_metadata(
        original_object_key=original_key_2,
        original_object_metadata=original_gcs_metadata_2,
        existing_archive_object_metadata=None
    )

    # --- Conceptual main_handler logic for test2.txt ---
    stream_2, meta_2 = mock_gcs_util.get_object_stream_and_metadata(write_bucket, original_key_2)
    existing_meta_2 = mock_gcs_util.stat_object(read_bucket, archive_path_2)
    mock_gcs_util.copy_object(
        source_bucket_name=write_bucket,
        source_blob_name=original_key_2,
        dest_bucket_name=read_bucket,
        dest_blob_name=archive_path_2,
        new_metadata=expected_archive_metadata_2
    )
    mock_gcs_util.delete_object(write_bucket, original_key_2)

    # Assertions for test2.txt (specific calls)
    # Use call_count or assert_any_call / assert_has_calls for more specific checks if mocks are not reset
    # For simplicity here, we rely on the sequence and later check overall counts.
    # A more robust way is to use mocker.resetall() or check call_args_list carefully.

    # Example of checking the second set of calls specifically:
    # We expect get_object_stream_and_metadata to be called twice.
    # The second call should be with (write_bucket, original_key_2).
    mock_gcs_util.get_object_stream_and_metadata.assert_any_call(write_bucket, original_key_2)
    # The second call to stat_object should be with (read_bucket, archive_path_2)
    mock_gcs_util.stat_object.assert_any_call(read_bucket, archive_path_2)
    # The second call to copy_object
    mock_gcs_util.copy_object.assert_any_call(
         write_bucket, original_key_2, read_bucket, archive_path_2, new_metadata=expected_archive_metadata_2
    )
    # The second call to delete_object
    mock_gcs_util.delete_object.assert_any_call(write_bucket, original_key_2)


    # --- Final Assertions ---
    assert mock_gcs_util.get_object_stream_and_metadata.call_count == 2
    assert mock_gcs_util.stat_object.call_count == 2
    assert mock_gcs_util.copy_object.call_count == 2
    assert mock_gcs_util.delete_object.call_count == 2

    # Verify the sequence of calls if necessary, e.g. using call_args_list or manager mocks.
    # Example: Check that delete is called after copy for each file.
    # This is implicitly handled by the structure but can be made more explicit.
    
    # Check specific arguments of all calls made to copy_object
    copy_calls = mock_gcs_util.copy_object.call_args_list
    assert len(copy_calls) == 2
    # Call 1 for test1.txt
    args, kwargs = copy_calls[0]
    assert args == (write_bucket, original_key_1, read_bucket, archive_path_1)
    assert kwargs['new_metadata'] == expected_archive_metadata_1
    
    # Call 2 for test2.txt
    args, kwargs = copy_calls[1]
    assert args == (write_bucket, original_key_2, read_bucket, archive_path_2)
    assert kwargs['new_metadata'] == expected_archive_metadata_2

    # Check specific arguments of all calls made to delete_object
    delete_calls = mock_gcs_util.delete_object.call_args_list
    assert len(delete_calls) == 2
    assert delete_calls[0] == call(write_bucket, original_key_1)
    assert delete_calls[1] == call(write_bucket, original_key_2)
```
