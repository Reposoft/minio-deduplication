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
    mocked_gcs_util_instance.get_object_stream_and_metadata = mocker.MagicMock()
    mocked_gcs_util_instance.stat_object = mocker.MagicMock()
    mocked_gcs_util_instance.copy_object = mocker.MagicMock()
    mocked_gcs_util_instance.delete_object = mocker.MagicMock()
    return mocked_gcs_util_instance

def test_deduplication_flow(mock_gcs_util):
    """
    Tests the deduplication flow:
    1. Upload fileA.
    2. Upload fileB with the same content as fileA but different path/metadata.
    3. Verify fileA is archived, fileB is processed, metadata of existing
       archive object is updated, and both original files are deleted.
    """
    write_bucket = "test_write_bucket"
    read_bucket = "test_read_bucket"

    common_content = b"common_content_for_deduplication"

    # --- Simulate processing for fileA.txt ---
    original_key_A = "path/to/fileA.txt"
    original_gcs_metadata_A = {'Content-Type': 'text/plain', 'source_file': 'fileA'}
    
    # Mock get_object_stream for fileA
    mock_gcs_util.get_object_stream_and_metadata.return_value = (
        io.BytesIO(common_content), original_gcs_metadata_A
    )

    # Calculate hash and paths for fileA
    sha256_hash_A = core_logic.calculate_sha256(io.BytesIO(common_content))
    extension_A = core_logic.get_original_extension(original_key_A)
    archive_path_A = core_logic.construct_destination_path(
        original_path=original_key_A,
        sha256_hash=sha256_hash_A,
        original_extension=extension_A,
        preserved_depth=PRESERVED_FOLDER_DEPTH
    ).replace(os.sep, '/')

    # Mock stat_object: first call (for fileA) -> None (archive miss)
    # Second call (for fileB, same hash) -> metadata_A_final (archive hit)
    # We will set this up using side_effect before fileB processing.
    mock_gcs_util.stat_object.return_value = None 

    metadata_A_final = metadata_utils.prepare_new_metadata(
        original_object_key=original_key_A,
        original_object_metadata=original_gcs_metadata_A,
        existing_archive_object_metadata=None
    )

    # --- Conceptual main_handler logic for fileA.txt ---
    mock_gcs_util.get_object_stream_and_metadata(write_bucket, original_key_A)
    mock_gcs_util.stat_object(read_bucket, archive_path_A) # First call to stat_object
    mock_gcs_util.copy_object(
        source_bucket_name=write_bucket,
        source_blob_name=original_key_A,
        dest_bucket_name=read_bucket,
        dest_blob_name=archive_path_A,
        new_metadata=metadata_A_final
    )
    mock_gcs_util.delete_object(write_bucket, original_key_A)

    # --- Simulate processing for fileB.txt ---
    original_key_B = "another/path/fileB.txt" # Different path
    original_gcs_metadata_B = {'Content-Type': 'text/different', 'source_file': 'fileB'} # Different metadata
    
    # Mock get_object_stream for fileB (same content stream, different metadata)
    mock_gcs_util.get_object_stream_and_metadata.return_value = (
        io.BytesIO(common_content), original_gcs_metadata_B
    )

    # Calculate hash and paths for fileB (hash and archive path will be same as fileA)
    sha256_hash_B = core_logic.calculate_sha256(io.BytesIO(common_content))
    assert sha256_hash_A == sha256_hash_B
    extension_B = core_logic.get_original_extension(original_key_B)
    archive_path_B = core_logic.construct_destination_path(
        original_path=original_key_B, # Original path is different
        sha256_hash=sha256_hash_B,
        original_extension=extension_B, # Extension might be different if keyB has different ext
        preserved_depth=PRESERVED_FOLDER_DEPTH
    ).replace(os.sep, '/')
    assert archive_path_A == archive_path_B # Critical for deduplication test

    # Mock stat_object: second call (for fileB) should return metadata_A_final
    # This simulates that the object (by hash) already exists.
    # We use side_effect to manage different return values for sequential calls to stat_object
    # if they were targeting different objects. But since it's the *same* archive_path_A,
    # we can just change the return_value before the second conceptual call.
    mock_gcs_util.stat_object.return_value = metadata_A_final

    metadata_B_updated = metadata_utils.prepare_new_metadata(
        original_object_key=original_key_B,
        original_object_metadata=original_gcs_metadata_B,
        existing_archive_object_metadata=metadata_A_final # Pass fileA's final metadata
    )
    
    # --- Conceptual main_handler logic for fileB.txt ---
    mock_gcs_util.get_object_stream_and_metadata(write_bucket, original_key_B)
    mock_gcs_util.stat_object(read_bucket, archive_path_A) # Second call to stat_object for the same archive path
    mock_gcs_util.copy_object(
        source_bucket_name=write_bucket,
        source_blob_name=original_key_B,
        dest_bucket_name=read_bucket,
        dest_blob_name=archive_path_A, # Copy to the SAME archive path
        new_metadata=metadata_B_updated # With updated metadata
    )
    mock_gcs_util.delete_object(write_bucket, original_key_B)

    # --- Assertions ---
    # get_object_stream_and_metadata calls
    assert mock_gcs_util.get_object_stream_and_metadata.call_count == 2
    mock_gcs_util.get_object_stream_and_metadata.assert_any_call(write_bucket, original_key_A)
    mock_gcs_util.get_object_stream_and_metadata.assert_any_call(write_bucket, original_key_B)
    
    # stat_object calls
    assert mock_gcs_util.stat_object.call_count == 2
    # All calls to stat_object were for archive_path_A
    mock_gcs_util.stat_object.assert_has_calls([
        call(read_bucket, archive_path_A), # First call
        call(read_bucket, archive_path_A)  # Second call
    ])
    # To verify the *return values* of stat_object for each call is harder without more complex mock setup
    # or inspecting internal mock call objects. The current setup where `return_value` is changed
    # means the *last* set `return_value` (metadata_A_final) is what the mock remembers for
    # all calls if not using side_effect.
    # For a more robust check of `stat_object`'s different return values, `side_effect` is better.
    # Let's adjust `stat_object` mocking to use side_effect for this test.

    # Re-configure mock_gcs_util.stat_object with side_effect for more precise assertion
    mock_gcs_util.stat_object.reset_mock() # Reset previous calls and return_value
    mock_gcs_util.stat_object.side_effect = [
        None,              # First call (for fileA's archive check)
        metadata_A_final   # Second call (for fileB's archive check, finding fileA's archive)
    ]
    # Re-simulate the stat_object calls as the handler would
    actual_stat_return_A = mock_gcs_util.stat_object(read_bucket, archive_path_A) # Simulates first call
    actual_stat_return_B = mock_gcs_util.stat_object(read_bucket, archive_path_A) # Simulates second call
    assert actual_stat_return_A is None
    assert actual_stat_return_B == metadata_A_final
    assert mock_gcs_util.stat_object.call_count == 2


    # copy_object calls
    assert mock_gcs_util.copy_object.call_count == 2
    expected_copy_calls = [
        call(write_bucket, original_key_A, read_bucket, archive_path_A, new_metadata=metadata_A_final),
        call(write_bucket, original_key_B, read_bucket, archive_path_A, new_metadata=metadata_B_updated)
    ]
    mock_gcs_util.copy_object.assert_has_calls(expected_copy_calls, any_order=False)

    # delete_object calls
    assert mock_gcs_util.delete_object.call_count == 2
    expected_delete_calls = [
        call(write_bucket, original_key_A),
        call(write_bucket, original_key_B)
    ]
    mock_gcs_util.delete_object.assert_has_calls(expected_delete_calls, any_order=False)

    # Metadata content assertions
    # Check Uploadpaths in metadata_B_updated
    upload_paths_in_B = metadata_B_updated.get('Uploadpaths', "")
    assert original_key_A in upload_paths_in_B
    assert original_key_B in upload_paths_in_B
    
    # Check Uploaddir in metadata_B_updated
    # path/to/ and another/path/
    # metadata_utils.append_to_metadata_list_string ensures no duplicates and proper formatting
    expected_dir_A = os.path.dirname(original_key_A) + '/' if os.path.dirname(original_key_A) else ""
    expected_dir_B = os.path.dirname(original_key_B) + '/' if os.path.dirname(original_key_B) else ""
    
    upload_dirs_in_B = metadata_B_updated.get('Uploaddir', "")
    
    if expected_dir_A:
      assert metadata_utils._encode_path_component(expected_dir_A) in upload_dirs_in_B
    if expected_dir_B:
      assert metadata_utils._encode_path_component(expected_dir_B) in upload_dirs_in_B

    # Ensure the order of appended items if it matters (it does for string comparison)
    # metadata_utils.append_to_metadata_list_string appends.
    # So, encoded dir_A should be before encoded dir_B if both are non-empty.
    if expected_dir_A and expected_dir_B and expected_dir_A != expected_dir_B:
        assert upload_dirs_in_B.startswith(metadata_utils._encode_path_component(expected_dir_A))
        assert metadata_utils._encode_path_component(expected_dir_B) in upload_dirs_in_B.split("; ")[1:]
    elif expected_dir_A and not expected_dir_B:
        assert upload_dirs_in_B == metadata_utils._encode_path_component(expected_dir_A)
    elif not expected_dir_A and expected_dir_B:
         # This case implies metadata_A_final had no Uploaddir, which is possible if original_key_A was at root
        assert upload_dirs_in_B == metadata_utils._encode_path_component(expected_dir_B)


```
