import hashlib
import os

def calculate_sha256(file_stream):
  """
  Calculates the hexadecimal SHA256 hash of a file stream.

  Args:
    file_stream: A file-like object (stream).

  Returns:
    The hexadecimal SHA256 hash string.
  """
  sha256 = hashlib.sha256()
  # Read in chunks to handle large files
  for chunk in iter(lambda: file_stream.read(4096), b""):
    sha256.update(chunk)
  return sha256.hexdigest()

def get_sharded_path(sha256_hash):
  """
  Generates a sharded directory path from a SHA256 hash.

  Args:
    sha256_hash: The SHA256 hash string.

  Returns:
    A string representing the sharded directory path (e.g., "ab/cd/").
  """
  if len(sha256_hash) < 4:
    raise ValueError("SHA256 hash is too short for sharding.")
  return f"{sha256_hash[0:2]}/{sha256_hash[2:4]}/"

def construct_destination_path(original_path, sha256_hash, original_extension, preserved_depth):
  """
  Constructs the destination path for a deduplicated file.

  Args:
    original_path: The full original path of the uploaded file.
    sha256_hash: The SHA256 hash of the file.
    original_extension: The original file extension including the dot.
    preserved_depth: Number of leading directory levels from original_path to preserve.

  Returns:
    The constructed destination path string.
  """
  sharded_path_part = get_sharded_path(sha256_hash)
  final_object_name = sha256_hash + original_extension

  if preserved_depth == 0:
    return sharded_path_part + final_object_name
  else:
    # Normalize path to remove double slashes and handle relative paths correctly.
    # Strip leading/trailing slashes to ensure consistent splitting.
    normalized_path = os.path.normpath(original_path.strip(os.sep))

    if not normalized_path or normalized_path == '.': # Handle cases like "" or "/" or "./"
        return sharded_path_part + final_object_name

    path_parts = normalized_path.split(os.sep)
    
    # Determine if the last part is a file or directory.
    # If original_path ended with os.sep, or if the last part is "." or "..",
    # it's likely a directory path or we should treat it as such for prefix purposes.
    # A simple check: if original_path (before stripping) ends with sep, or last part is empty after split.
    
    # If original_path was "foo/bar/", normalized_path="foo/bar", path_parts=["foo", "bar"]
    # If original_path was "foo/bar/file.txt", normalized_path="foo/bar/file.txt", path_parts=["foo", "bar", "file.txt"]

    # Consider the parts that represent directories.
    # If the original path string seemed to point to a directory (e.g. "a/b/"),
    # or if it's just a directory name ("a/b" without a file), all parts are dir_parts.
    # If it has a file-like component at the end, exclude that.
    
    # A robust way to get directory parts:
    # 1. Get directory name of the normalized path.
    # 2. If the original path doesn't look like a directory, then split os.path.dirname(normalized_path)
    # This can be tricky. Let's stick to splitting and then deciding.

    # If the original path ends with a separator, or the last component of normalized_path is empty (e.g. from "a/b//")
    # or ".." or ".", then all path_parts are considered directory parts for preservation.
    # Otherwise, if there's a file-like component, exclude it.
    
    has_filename_component = True # Assume last part is a file unless proven otherwise
    if original_path.endswith(os.sep):
        has_filename_component = False
    elif not path_parts: # Should not happen if normalized_path is not empty
        has_filename_component = False
    elif path_parts[-1] == "." or path_parts[-1] == ".." : # e.g. path/to/..
        has_filename_component = False
    elif os.path.splitext(path_parts[-1])[1] == "": # No extension, could be dir or file
        # This is ambiguous. Let's assume if no extension, it's a directory component unless original_path implies it's a file.
        # For simplicity, if original_path doesn't end with sep, and last part has no ext,
        # we might still consider it a file if it's the only part. e.g. "myfile"
        # The original logic: `if '.' in path_parts[-1]` was a very basic check.
        # A more common approach: if original_path doesn't end with '/', last part is file.
        # Let's refine: if original_path does not end with os.sep, then the last path_part is a filename.
        if original_path.strip(os.sep) == path_parts[-1] and not original_path.endswith(os.sep): # e.g. "file"
             pass # It's a file
        elif not original_path.endswith(os.sep) and len(path_parts) > 0 : # e.g. "folder/file"
             pass # It's a file
        else: # e.g. "folder/subfolder" or "folder/subfolder/"
             has_filename_component = False


    if has_filename_component and path_parts:
        dir_parts = path_parts[:-1]
    else:
        dir_parts = path_parts

    if not dir_parts:
        return sharded_path_part + final_object_name

    preserved_elements = dir_parts[:preserved_depth]

    if not preserved_elements:
        return sharded_path_part + final_object_name

    # Join the preserved elements. This will form something like "a/b" or "a".
    preserved_prefix_path = os.path.join(*preserved_elements)
    
    # Ensure it ends with a separator to be a proper prefix.
    if preserved_prefix_path: # If not empty string
        preserved_prefix = preserved_prefix_path + os.sep
    else: # if preserved_elements was empty or os.path.join resulted in ""
        preserved_prefix = ""

    return preserved_prefix + sharded_path_part + final_object_name

def get_original_extension(file_key):
  """
  Extracts the file extension from a file key.
  Maps ".jpeg" to ".jpg".

  Args:
    file_key: The object key (filename, e.g., "path/to/file.tar.gz").

  Returns:
    The file extension, including the dot (e.g., ".gz", ".jpg").
  """
  if not file_key:
    return ""

  # Special case for ".jpeg" -> ".jpg"
  # os.path.splitext will give ".jpeg", so we handle this after.
  
  basename = os.path.basename(file_key)
  # For ".bashrc", splitext gives (".bashrc", "").
  # For "file.ext", splitext gives ("file", ".ext").
  # For "archive.tar.gz", splitext gives ("archive.tar", ".gz").
  # For "nodot", splitext gives ("nodot", "").
  
  _root, ext = os.path.splitext(basename)

  # If the basename starts with a dot and splitext returns an empty extension,
  # it means the entire filename is the extension (e.g., ".bashrc").
  if basename.startswith('.') and not ext and _root == basename : # e.g. .bashrc -> _root = .bashrc, ext = ""
      ext = basename
  
  # Handle the ".jpeg" to ".jpg" mapping
  if ext.lower() == ".jpeg":
    return ".jpg"
  
  return ext
