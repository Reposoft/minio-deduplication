import io
import pytest
import os

# Assuming your project structure allows this import path
from gcp_minio_dedup_cf import core_logic, metadata_utils

# Tests for core_logic.py functions

def test_calculate_sha256_known_string():
  """Test SHA256 calculation with a known string."""
  data = b"some data"
  stream = io.BytesIO(data)
  # Known SHA256 hash for "some data"
  expected_hash = "1307990e6ba5ca145eb35e99182a9bec46531bc54ddf656a602c780fa0240dee"
  assert core_logic.calculate_sha256(stream) == expected_hash

def test_calculate_sha256_empty_stream():
  """Test SHA256 calculation with an empty stream."""
  stream = io.BytesIO(b"")
  # Known SHA256 hash for an empty string
  expected_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
  assert core_logic.calculate_sha256(stream) == expected_hash

def test_get_sharded_path_valid_hash():
  """Test sharded path generation with a valid hash."""
  sha256_hash = "abcdef1234567890"
  expected_path = "ab/cd/"
  assert core_logic.get_sharded_path(sha256_hash) == expected_path

def test_get_sharded_path_short_hash():
  """Test sharded path generation with a hash shorter than 4 characters."""
  sha256_hash = "abc"
  with pytest.raises(ValueError, match="SHA256 hash is too short for sharding."):
    core_logic.get_sharded_path(sha256_hash)

def test_get_sharded_path_exact_4_chars_hash():
  """Test sharded path generation with a hash exactly 4 characters long."""
  sha256_hash = "abcd"
  expected_path = "ab/cd/"
  assert core_logic.get_sharded_path(sha256_hash) == expected_path


# Test cases for construct_destination_path
# Using a consistent long hash for easier comparison
SAMPLE_HASH = "abcdef1234ghijklmnopqr7890stuvwxyz" 
SHARDED_PART = f"{SAMPLE_HASH[0:2]}/{SAMPLE_HASH[2:4]}/" # "ab/cd/"

# Base tests (re-verified)
def test_construct_destination_path_depth_0():
  original_path = "folder/subfolder/file.txt"
  ext = ".txt"
  depth = 0
  expected = SHARDED_PART + SAMPLE_HASH + ext
  assert core_logic.construct_destination_path(original_path, SAMPLE_HASH, ext, depth) == expected.replace('/', os.sep)

def test_construct_destination_path_depth_1():
  original_path = "folder/subfolder/file.txt" # No leading/trailing slash
  ext = ".txt"
  depth = 1
  expected = "folder/" + SHARDED_PART + SAMPLE_HASH + ext
  assert core_logic.construct_destination_path(original_path, SAMPLE_HASH, ext, depth) == expected.replace('/', os.sep)

def test_construct_destination_path_depth_2():
  original_path = "folder/subfolder/file.txt" # No leading/trailing slash
  ext = ".txt"
  depth = 2
  expected = "folder/subfolder/" + SHARDED_PART + SAMPLE_HASH + ext
  assert core_logic.construct_destination_path(original_path, SAMPLE_HASH, ext, depth) == expected.replace('/', os.sep)

def test_construct_destination_path_depth_gt_actual():
  original_path = "folder/file.txt"
  ext = ".txt"
  depth = 3 # Greater than actual depth of 1 (folder)
  expected = "folder/" + SHARDED_PART + SAMPLE_HASH + ext
  assert core_logic.construct_destination_path(original_path, SAMPLE_HASH, ext, depth) == expected.replace('/', os.sep)

# Tests for specific feedback points
def test_construct_destination_path_leading_slash_depth_1():
  original_path = "/folder/file.txt"
  ext = ".txt"
  depth = 1
  expected = "folder/" + SHARDED_PART + SAMPLE_HASH + ext
  assert core_logic.construct_destination_path(original_path, SAMPLE_HASH, ext, depth) == expected.replace('/', os.sep)

def test_construct_destination_path_double_slash():
  original_path = "folder//subfolder/file.txt"
  ext = ".txt"
  depth = 2
  expected = "folder/subfolder/" + SHARDED_PART + SAMPLE_HASH + ext
  assert core_logic.construct_destination_path(original_path, SAMPLE_HASH, ext, depth) == expected.replace('/', os.sep)

def test_construct_destination_path_filename_only_depth_1():
  original_path = "file.txt"
  ext = ".txt"
  depth = 1
  expected = SHARDED_PART + SAMPLE_HASH + ext # No prefix part
  assert core_logic.construct_destination_path(original_path, SAMPLE_HASH, ext, depth) == expected.replace('/', os.sep)

def test_construct_destination_path_empty_original_path():
  original_path = ""
  ext = ".txt"
  depth = 1
  expected = SHARDED_PART + SAMPLE_HASH + ext # No prefix part
  assert core_logic.construct_destination_path(original_path, SAMPLE_HASH, ext, depth) == expected.replace('/', os.sep)
  
  # Also test with depth = 0 for empty original_path
  assert core_logic.construct_destination_path("", SAMPLE_HASH, ".txt", 0) == (SHARDED_PART + SAMPLE_HASH + ".txt").replace('/', os.sep)

# Additional tests for robustness
def test_construct_destination_path_trailing_slash_input():
  original_path = "folder/subfolder/" 
  ext = ".txt"
  depth = 2
  expected = "folder/subfolder/" + SHARDED_PART + SAMPLE_HASH + ext
  assert core_logic.construct_destination_path(original_path, SAMPLE_HASH, ext, depth) == expected.replace('/', os.sep)

def test_construct_destination_path_trailing_slash_depth_1():
    original_path = "folder/subfolder/"
    ext = ".txt"
    depth = 1
    expected = "folder/" + SHARDED_PART + SAMPLE_HASH + ext
    assert core_logic.construct_destination_path(original_path, SAMPLE_HASH, ext, depth) == expected.replace('/', os.sep)


def test_construct_destination_path_root_path_input():
  original_path = "/"
  ext = ".txt"
  depth = 1 
  expected = SHARDED_PART + SAMPLE_HASH + ext # No prefix from root path
  assert core_logic.construct_destination_path(original_path, SAMPLE_HASH, ext, depth) == expected.replace('/', os.sep)

def test_construct_destination_path_relative_path_input():
  original_path = "./folder/file.txt"
  ext = ".txt"
  depth = 1
  expected = "folder/" + SHARDED_PART + SAMPLE_HASH + ext
  assert core_logic.construct_destination_path(original_path, SAMPLE_HASH, ext, depth) == expected.replace('/', os.sep)

def test_construct_destination_path_multiple_leading_slashes_input():
  original_path = "///folder/file.txt"
  ext = ".txt"
  depth = 1
  expected = "folder/" + SHARDED_PART + SAMPLE_HASH + ext
  assert core_logic.construct_destination_path(original_path, SAMPLE_HASH, ext, depth) == expected.replace('/', os.sep)

def test_construct_destination_path_path_with_dots():
    original_path = "folder.with.dots/file.txt"
    ext = ".txt"
    depth = 1
    expected = "folder.with.dots/" + SHARDED_PART + SAMPLE_HASH + ext
    assert core_logic.construct_destination_path(original_path, SAMPLE_HASH, ext, depth) == expected.replace('/', os.sep)

def test_construct_destination_path_original_path_is_just_dots():
    original_path = "."
    ext = ".txt"
    depth = 1
    expected = SHARDED_PART + SAMPLE_HASH + ext
    assert core_logic.construct_destination_path(original_path, SAMPLE_HASH, ext, depth) == expected.replace('/', os.sep)

    original_path_2 = ".."
    expected_2 = SHARDED_PART + SAMPLE_HASH + ext # ".." also normalizes to no prefix if it's the whole path
    assert core_logic.construct_destination_path(original_path_2, SAMPLE_HASH, ext, depth) == expected_2.replace('/', os.sep)


def test_construct_destination_path_original_path_no_file_depth_1():
    original_path = "folder/subfolder/" 
    ext = ".dat" 
    depth = 1
    expected = "folder/" + SHARDED_PART + SAMPLE_HASH + ext
    assert core_logic.construct_destination_path(original_path, SAMPLE_HASH, ext, depth) == expected.replace('/', os.sep)

def test_construct_destination_path_original_path_no_file_depth_0():
    original_path = "folder/subfolder/" 
    ext = ".dat" 
    depth = 0
    expected = SHARDED_PART + SAMPLE_HASH + ext # Depth 0, no prefix
    assert core_logic.construct_destination_path(original_path, SAMPLE_HASH, ext, depth) == expected.replace('/', os.sep)


def test_get_original_extension():
  # Updated tests for get_original_extension
  assert core_logic.get_original_extension("file.txt") == ".txt"
  assert core_logic.get_original_extension("archive.tar.gz") == ".gz" 
  assert core_logic.get_original_extension("image.JPEG") == ".jpg" 
  assert core_logic.get_original_extension("image.jpeg") == ".jpg" 
  assert core_logic.get_original_extension("file.with.dots.ext") == ".ext"
  
  # Test cases based on feedback
  assert core_logic.get_original_extension(".bashrc") == ".bashrc"
  assert core_logic.get_original_extension("path/to/.configfile") == ".configfile"
  assert core_logic.get_original_extension("nodotextension") == "" 
  assert core_logic.get_original_extension("some.file.ext") == ".ext"

  # Additional test cases
  assert core_logic.get_original_extension("file") == "" 
  assert core_logic.get_original_extension("") == "" 
  assert core_logic.get_original_extension("file.endingindot.") == "." 
  assert core_logic.get_original_extension("path/to/archive.tar.gz") == ".gz" 
  assert core_logic.get_original_extension("path/to/nodot") == "" 
  assert core_logic.get_original_extension("a/b/c/.hiddenfile") == ".hiddenfile" 
  assert core_logic.get_original_extension("a/b/c/normal.file.ext") == ".ext"
  assert core_logic.get_original_extension("file.") == "." # filename ending with dot
  assert core_logic.get_original_extension("path/to/file.") == "." # path to filename ending with dot
  assert core_logic.get_original_extension(".onlydot") == ".onlydot" # filename is just ".something"

# Tests for metadata_utils.py functions

def test_encode_path_component():
  assert metadata_utils._encode_path_component("abc;def") == "abc%3Bdef"
  assert metadata_utils._encode_path_component("nochanges") == "nochanges"
  assert metadata_utils._encode_path_component(None) == ""
  assert metadata_utils._encode_path_component("") == ""

def test_append_to_metadata_list_string():
  assert metadata_utils.append_to_metadata_list_string("", "item1") == "item1"
  assert metadata_utils.append_to_metadata_list_string(None, "item1") == "item1"
  assert metadata_utils.append_to_metadata_list_string("item1", "item2") == "item1; item2"
  assert metadata_utils.append_to_metadata_list_string("item1; item2", "item1") == "item1; item2"
  assert metadata_utils.append_to_metadata_list_string("item1", "item;2") == "item1; item%3B2"
  assert metadata_utils.append_to_metadata_list_string("item1", "") == "item1" # Appending empty string
  assert metadata_utils.append_to_metadata_list_string("", "") == "" # Current and new are empty
  assert metadata_utils.append_to_metadata_list_string(None, None) == "" # Current and new are None (new_item=None is handled by "if not new_item")

def test_prepare_new_metadata_scenario1_new_object():
  original_object_key = "uploads/data/file1.txt"
  original_object_metadata = {'Content-Type': 'text/plain', 'custom_meta': 'value1'}
  existing_archive_object_metadata = None

  new_meta = metadata_utils.prepare_new_metadata(
      original_object_key, original_object_metadata, existing_archive_object_metadata
  )

  assert new_meta.get('Content-Type') == 'text/plain'
  assert new_meta.get('Content-Disposition') == 'attachment; filename="file1.txt"'
  assert new_meta.get('Uploadpaths') == "uploads/data/file1.txt"
  assert new_meta.get('Uploaddir') == "uploads/data/"
  assert new_meta.get('custom_meta') == "value1" 

def test_prepare_new_metadata_scenario2_deduplication():
  original_object_key = "other_uploads/data/new_file.txt"
  original_object_metadata = {'Content-Type': 'text/markdown', 'new_custom': 'value2'}
  existing_archive_object_metadata = {
      'Content-Type': 'text/plain', 
      'Content-Disposition': 'attachment; filename="file1.txt"', 
      'Uploadpaths': 'uploads/data/file1.txt',
      'Uploaddir': 'uploads/data/',
      'existing_custom': 'value_old' 
  }

  new_meta = metadata_utils.prepare_new_metadata(
      original_object_key, original_object_metadata, existing_archive_object_metadata
  )

  assert new_meta.get('Content-Type') == 'text/markdown' 
  assert new_meta.get('Content-Disposition') == 'attachment; filename="new_file.txt"' 
  expected_uploadpaths = "uploads/data/file1.txt; other_uploads/data/new_file.txt"
  expected_uploaddir = "uploads/data/; other_uploads/data/"
  
  assert new_meta.get('Uploadpaths') == expected_uploadpaths
  assert new_meta.get('Uploaddir') == expected_uploaddir
  assert new_meta.get('existing_custom') == 'value_old'
  assert new_meta.get('new_custom') == 'value2'


def test_prepare_new_metadata_scenario3_root_object_no_existing():
  original_object_key = "file_at_root.dat"
  original_object_metadata = {'Content-Type': 'application/octet-stream'}
  existing_archive_object_metadata = None

  new_meta = metadata_utils.prepare_new_metadata(
      original_object_key, original_object_metadata, existing_archive_object_metadata
  )

  assert new_meta.get('Content-Type') == 'application/octet-stream'
  assert new_meta.get('Content-Disposition') == 'attachment; filename="file_at_root.dat"'
  assert new_meta.get('Uploadpaths') == "file_at_root.dat"
  assert 'Uploaddir' not in new_meta


def test_prepare_new_metadata_scenario4_custom_metadata_preservation():
  original_object_key = "uploads/data/file1.txt"
  original_object_metadata = {
      'Content-Type': 'text/plain', 
      'custom1': 'val1', 
      'custom2': 'val2',
      'Cache-Control': 'no-cache' 
  }
  existing_archive_object_metadata = None

  new_meta = metadata_utils.prepare_new_metadata(
      original_object_key, original_object_metadata, existing_archive_object_metadata
  )
  assert new_meta.get('custom1') == 'val1'
  assert new_meta.get('custom2') == 'val2'
  assert new_meta.get('Content-Type') == 'text/plain'
  assert 'Cache-Control' not in new_meta 


def test_prepare_new_metadata_scenario5_content_type_default():
  original_object_key = "key.bin"
  original_object_metadata = {} 
  existing_archive_object_metadata = None

  new_meta = metadata_utils.prepare_new_metadata(
      original_object_key, original_object_metadata, existing_archive_object_metadata
  )
  assert new_meta.get('Content-Type') == 'application/octet-stream' 
  assert new_meta.get('Content-Disposition') == 'attachment; filename="key.bin"'
  assert new_meta.get('Uploadpaths') == "key.bin"
  assert 'Uploaddir' not in new_meta

def test_uploaddir_trailing_slash_and_no_duplicates():
    meta1 = metadata_utils.prepare_new_metadata(
        "dir1/file1.txt", {'Content-Type': 'text/plain'}, None
    )
    assert meta1['Uploaddir'] == "dir1/"

    meta2 = metadata_utils.prepare_new_metadata(
        "dir1/file2.txt", {'Content-Type': 'text/plain'}, meta1
    )
    assert meta2['Uploaddir'] == "dir1/" 

    meta3 = metadata_utils.prepare_new_metadata(
        "dir2/file3.txt", {'Content-Type': 'text/plain'}, meta2
    )
    assert meta3['Uploaddir'] == "dir1/; dir2/" 

    meta4 = metadata_utils.prepare_new_metadata(
        "rootfile.txt", {'Content-Type': 'text/plain'}, meta3
    )
    assert meta4['Uploaddir'] == "dir1/; dir2/"
    assert meta4['Uploadpaths'] == "dir1/file1.txt; dir1/file2.txt; dir2/file3.txt; rootfile.txt"
```
