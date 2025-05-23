import io
from google.cloud import storage
from google.cloud.exceptions import NotFound

class GCSUtil:
  """
  A utility class for interacting with Google Cloud Storage.
  """
  def __init__(self):
    """
    Initializes the Google Cloud Storage client.
    """
    self.client = storage.Client()

  def get_object_stream_and_metadata(self, bucket_name, blob_name):
    """
    Downloads a blob's content into a memory stream and retrieves its metadata.

    Args:
      bucket_name: The name of the GCS bucket.
      blob_name: The name of the blob.

    Returns:
      A tuple (file_stream, metadata_dict).
      file_stream: io.BytesIO object containing the blob's content.
      metadata_dict: Dictionary of the blob's metadata.

    Raises:
      google.cloud.exceptions.NotFound: If the blob is not found.
    """
    bucket = self.client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    try:
      # Download content into bytes
      content_bytes = blob.download_as_bytes()
      file_stream = io.BytesIO(content_bytes)
      file_stream.seek(0) # Ensure stream is at the beginning

      # Reload metadata as download_as_bytes() might not update all properties
      blob.reload() 
      metadata_dict = {}
      if blob.metadata:
        metadata_dict.update(blob.metadata)
      
      # Add standard properties, ensuring they are not None
      if blob.content_type:
        metadata_dict['Content-Type'] = blob.content_type
      if blob.content_encoding:
        metadata_dict['Content-Encoding'] = blob.content_encoding
      if blob.content_disposition:
        metadata_dict['Content-Disposition'] = blob.content_disposition
      if blob.cache_control:
        metadata_dict['Cache-Control'] = blob.cache_control
      if blob.crc32c:
          metadata_dict['crc32c'] = blob.crc32c
      if blob.md5_hash:
          metadata_dict['md5Hash'] = blob.md5_hash
      
      return file_stream, metadata_dict
    except NotFound:
      raise

  def stat_object(self, bucket_name, blob_name):
    """
    Gets a blob's metadata without downloading its content.

    Args:
      bucket_name: The name of the GCS bucket.
      blob_name: The name of the blob.

    Returns:
      A dictionary of the blob's metadata if it exists, None otherwise.
    """
    bucket = self.client.bucket(bucket_name)
    blob = bucket.get_blob(blob_name)

    if blob:
      metadata_dict = {}
      if blob.metadata:
        metadata_dict.update(blob.metadata)
      
      # Add standard properties
      if blob.content_type:
        metadata_dict['Content-Type'] = blob.content_type
      if blob.content_encoding:
        metadata_dict['Content-Encoding'] = blob.content_encoding
      if blob.content_disposition:
        metadata_dict['Content-Disposition'] = blob.content_disposition
      if blob.cache_control:
        metadata_dict['Cache-Control'] = blob.cache_control
      if blob.size:
          metadata_dict['size'] = blob.size
      if blob.updated:
          metadata_dict['updated'] = blob.updated.isoformat()
      if blob.crc32c:
          metadata_dict['crc32c'] = blob.crc32c
      if blob.md5_hash:
          metadata_dict['md5Hash'] = blob.md5_hash
      return metadata_dict
    else:
      return None

  def upload_object_from_stream(self, bucket_name, blob_name, stream, metadata):
    """
    Uploads content from a stream to a GCS blob and sets metadata.

    Args:
      bucket_name: The name of the GCS bucket.
      blob_name: The name for the new blob.
      stream: A file-like object to read from.
      metadata: A dictionary of metadata to set on the object.
                Custom metadata keys will be preserved.
                'Content-Type' will be set from metadata if present.
    """
    bucket = self.client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    stream.seek(0) # Ensure stream is at the beginning

    # Separate standard http headers from custom metadata
    custom_metadata = {}
    content_type = None

    if metadata:
        for k, v in metadata.items():
            if k.lower() == 'content-type':
                content_type = v
            # Add other standard HTTP headers here if needed
            # else: # Assume it's custom metadata
            custom_metadata[k] = v
    
    blob.metadata = custom_metadata # Set custom metadata
    
    # The client library might infer content_type, but explicit is better.
    # Pass content_type separately to upload_from_file.
    # If content_type is in custom_metadata, it will be overwritten by explicit param.
    blob.upload_from_file(stream, content_type=content_type)


  def copy_object(self, source_bucket_name, source_blob_name, dest_bucket_name, dest_blob_name, new_metadata=None):
    """
    Copies a GCS object, optionally setting new metadata on the destination.

    Args:
      source_bucket_name: The name of the source GCS bucket.
      source_blob_name: The name of the source blob.
      dest_bucket_name: The name of the destination GCS bucket.
      dest_blob_name: The name for the destination blob.
      new_metadata (optional dict): If provided, this metadata is set on the
                                    destination object, replacing any source metadata.
                                    If None, source metadata is copied.

    Raises:
      google.cloud.exceptions.NotFound: If the source blob is not found.
    """
    source_bucket = self.client.bucket(source_bucket_name)
    source_blob = source_bucket.blob(source_blob_name)

    if not source_blob.exists():
      raise NotFound(f"Source object {source_blob_name} not found in bucket {source_bucket_name}")

    dest_bucket = self.client.bucket(dest_bucket_name)
    
    # Create a new blob object for the destination to set its properties
    dest_blob = dest_bucket.blob(dest_blob_name)

    if new_metadata is not None:
        custom_metadata_to_set = {}
        content_type_to_set = None
        for k, v in new_metadata.items():
            if k.lower() == 'content-type':
                content_type_to_set = v
            # Add other standard HTTP headers here if needed (e.g. Content-Encoding)
            # else: # Assume it's custom metadata
            custom_metadata_to_set[k] = v
        
        dest_blob.metadata = custom_metadata_to_set
        if content_type_to_set:
            dest_blob.content_type = content_type_to_set
        # If other standard properties need to be set, do it here on dest_blob
        # e.g., dest_blob.content_encoding = new_metadata.get('Content-Encoding')

    # The rewrite method is generally preferred for copying, especially large objects.
    # copy_blob is simpler for same-location, same-KMS-key copies.
    # For potentially different locations or KMS keys, rewrite is more robust.
    # However, copy_blob also supports setting metadata on the new_blob.
    
    # If new_metadata is None, the client library copies metadata by default.
    # If new_metadata is provided, we've set it on dest_blob.
    # The copy_blob method takes the destination blob object (dest_blob)
    # which already has the desired metadata (either blank for copy, or new).
    
    # The GCS Python client's bucket.copy_blob() copies metadata by default.
    # To apply *new* metadata, we pass a `new_blob` (our `dest_blob`) 
    # with its metadata attributes already set.
    
    token = None
    while True: # Handle potential multi-part copy for large objects
        token, bytes_rewritten, total_bytes = dest_blob.rewrite(source_blob, token=token)
        if token is None:
            break
    
    # An alternative using copy_blob, which is simpler but might be less flexible for very large files or cross-location:
    # dest_blob_returned = source_bucket.copy_blob(source_blob, dest_bucket, dest_blob_name, new_blob=dest_blob if new_metadata is not None else None)
    # Note: if new_metadata is None, we pass None to new_blob so it copies source metadata.
    # If new_metadata is not None, our dest_blob (which has new_metadata set) is passed.

  def delete_object(self, bucket_name, blob_name):
    """
    Deletes a GCS blob. Does not raise an error if the blob doesn't exist.

    Args:
      bucket_name: The name of the GCS bucket.
      blob_name: The name of the blob to delete.
    """
    bucket = self.client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    # ignore_not_found=True makes it idempotent
    blob.delete(ignore_not_found=True)

```
