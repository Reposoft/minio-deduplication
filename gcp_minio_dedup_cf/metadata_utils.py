import os
import urllib.parse

def _encode_path_component(path_component: str) -> str:
  """
  Encodes a path component by replacing semicolons with percent encoding.

  Args:
    path_component: The path component string.

  Returns:
    The encoded string.
  """
  if path_component is None:
    return ""
  return path_component.replace(";", "%3B")

def append_to_metadata_list_string(current_list_str: str | None, new_item: str) -> str:
  """
  Appends a new item to a semicolon-space separated list string, ensuring no duplicates.

  Args:
    current_list_str: The current list string (e.g., "item1; item2").
                      Can be None or empty.
    new_item: The new item to add.

  Returns:
    The updated list string.
  """
  if not new_item: # Do not append empty items
      return current_list_str if current_list_str is not None else ""

  encoded_new_item = _encode_path_component(new_item)

  if current_list_str is None or current_list_str == "":
    return encoded_new_item

  # Split by "; " to check for existence
  items = current_list_str.split("; ")
  if encoded_new_item in items:
    return current_list_str
  
  return f"{current_list_str}; {encoded_new_item}"

def prepare_new_metadata(
    original_object_key: str, 
    original_object_metadata: dict, 
    existing_archive_object_metadata: dict | None = None
) -> dict:
  """
  Prepares the metadata for a new or updated archive object.

  Args:
    original_object_key: The full key of the object uploaded (e.g., "uploads/file.txt").
    original_object_metadata: Metadata of the uploaded object.
    existing_archive_object_metadata (optional): Metadata of the existing archive object, if any.

  Returns:
    A dictionary containing the new metadata.
  """
  new_metadata = {}

  # 1. Propagate all original_object_metadata (custom metadata) to new_metadata.
  #    The GCSUtil handles the actual `blob.metadata` which is a flat dict.
  #    If original_object_metadata comes from GCSUtil's get_object_stream_and_metadata,
  #    it will be a flat dict of custom metadata combined with some HTTP headers.
  #    We should be careful not to mix these two concepts here.
  #    Let's assume original_object_metadata primarily contains custom metadata items
  #    and specific http headers are accessed via their known keys like 'Content-Type'.
  
  # Start with a copy of original custom metadata.
  # GCS blob.metadata is flat, so we just copy the dict.
  if original_object_metadata:
    # We only want to copy actual custom metadata. Standard headers are handled explicitly.
    # Standard GCS headers in the dict from get_object_stream_and_metadata are:
    # 'Content-Type', 'Content-Encoding', 'Content-Disposition', 'Cache-Control', 'crc32c', 'md5Hash'
    # We should exclude these from being copied as "custom" then re-evaluate.
    # The Go code copies `uploaded.UserMetadata`. For GCS, `blob.metadata` is user metadata.
    for k, v in original_object_metadata.items():
        # A simple check: if not one of the known standard header keys extracted by GCSUtil.
        # This is a bit heuristic. A better way might be if GCSUtil provided separate dicts.
        if k not in ['Content-Type', 'Content-Encoding', 'Content-Disposition', 
                       'Cache-Control', 'crc32c', 'md5Hash', 'size', 'updated']:
            new_metadata[k] = v


  # 2. Set Content-Type
  #    Priority: original_object_metadata 'Content-Type', then default.
  #    The GCSUtil stores 'Content-Type' (capitalized) from blob.content_type.
  new_metadata['Content-Type'] = original_object_metadata.get('Content-Type', 'application/octet-stream')

  # 3. Set Content-Disposition
  filename = os.path.basename(original_object_key)
  # Ensure filename is quoted and any internal quotes are handled if necessary,
  # though os.path.basename usually gives a clean name.
  # HTTP header quoting for filename is complex. For simplicity, basic quoting:
  encoded_filename = filename.replace('"', '\\"') # Basic escaping for quotes
  new_metadata['Content-Disposition'] = f"attachment; filename=\"{encoded_filename}\""

  # 4. Calculate Uploadpaths
  current_upload_paths = ""
  if existing_archive_object_metadata and 'Uploadpaths' in existing_archive_object_metadata:
    current_upload_paths = existing_archive_object_metadata['Uploadpaths']
  
  # original_object_key itself is added to Uploadpaths
  new_metadata['Uploadpaths'] = append_to_metadata_list_string(current_upload_paths, original_object_key)

  # 5. Calculate Uploaddir
  current_upload_dirs = ""
  if existing_archive_object_metadata and 'Uploaddir' in existing_archive_object_metadata:
    current_upload_dirs = existing_archive_object_metadata['Uploaddir']
  
  upload_dir = os.path.dirname(original_object_key)
  if upload_dir: # Only add if not empty
    # Ensure it ends with a '/' if it's a directory path
    if not upload_dir.endswith('/'):
      upload_dir += '/'
    
    updated_upload_dirs = append_to_metadata_list_string(current_upload_dirs, upload_dir)
    if updated_upload_dirs: # Only add the key if the resulting string is not empty
        new_metadata['Uploaddir'] = updated_upload_dirs
  elif current_upload_dirs: # If upload_dir is empty, but there were existing dirs, keep them.
    new_metadata['Uploaddir'] = current_upload_dirs
  # If upload_dir is empty AND current_upload_dirs is empty, 'Uploaddir' will not be in new_metadata.
  
  return new_metadata

```
