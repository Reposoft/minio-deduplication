import os
import logging
import traceback
import functions_framework

# Custom modules
from .gcs_utils import GCSUtil
from .core_logic import calculate_sha256, get_original_extension, construct_destination_path
from .metadata_utils import prepare_new_metadata
from google.cloud.exceptions import NotFound # For specific error handling if needed, though GCSUtil might abstract this

# Configure basic logging
logging.basicConfig(level=logging.INFO)

# Environment Variables:
# READ_BUCKET_NAME: Name of the GCS bucket to store archived/deduplicated files.
# PRESERVED_FOLDER_DEPTH: Integer (0 or more) for preserving source folder structure. Defaults to 0.

@functions_framework.cloud_event
def gcs_deduplicate_on_upload_handler(cloud_event):
    """
    Triggered by a new object creation in a GCS bucket (the 'write_bucket').
    Deduplicates the object based on its SHA256 hash and stores it in the
    'read_bucket' (archive/deduplicated store). Updates metadata if the
    object (by hash) already exists.

    Args:
        cloud_event (functions_framework.CloudEvent): The event payload.
            - cloud_event.data['bucket']: The GCS bucket where the file was uploaded.
            - cloud_event.data['name']: The object key (path) of the uploaded file.
    """
    try:
        bucket_name = cloud_event.data['bucket']
        object_key = cloud_event.data['name'] # Path of the uploaded file

        logging.info(f"Processing new object: gs://{bucket_name}/{object_key}")

        # --- Configuration ---
        read_bucket_name = os.environ.get("READ_BUCKET_NAME")
        if not read_bucket_name:
            logging.error("READ_BUCKET_NAME environment variable is not set.")
            # For background functions, raising an error is often the best way
            # to signal a terminal failure for this event if retries are not desired
            # or if it's a permanent configuration issue.
            raise ValueError("Configuration error: READ_BUCKET_NAME is not set.")

        preserved_depth_str = os.environ.get("PRESERVED_FOLDER_DEPTH", "0")
        try:
            preserved_depth = int(preserved_depth_str)
            if preserved_depth < 0:
                logging.warning(
                    f"PRESERVED_FOLDER_DEPTH is negative ({preserved_depth}). "
                    f"Defaulting to 0."
                )
                preserved_depth = 0
        except ValueError:
            logging.warning(
                f"Invalid PRESERVED_FOLDER_DEPTH: '{preserved_depth_str}'. "
                f"Must be an integer. Defaulting to 0."
            )
            preserved_depth = 0
        
        logging.info(f"Archive (read) bucket: {read_bucket_name}")
        logging.info(f"Preserved folder depth: {preserved_depth}")

        gcs = GCSUtil()

        # --- Main Processing Logic ---
        logging.info(f"Step 1: Getting object stream and metadata for gs://{bucket_name}/{object_key}")
        # GCSUtil.get_object_stream_and_metadata returns a BytesIO stream and a metadata dict.
        # The BytesIO stream is a copy of the object data.
        stream, original_metadata = gcs.get_object_stream_and_metadata(bucket_name, object_key)
        logging.info(f"Successfully retrieved stream and metadata for gs://{bucket_name}/{object_key}")
        logging.debug(f"Original metadata: {original_metadata}")

        logging.info(f"Step 2: Calculating SHA256 hash for gs://{bucket_name}/{object_key}")
        # calculate_sha256 will read the stream. Since it's BytesIO, it can be re-read if needed,
        # but for this flow, it's only needed for hash calculation.
        sha256_hash = calculate_sha256(stream)
        stream.close() # Good practice to close the stream when done
        logging.info(f"Calculated SHA256 hash: {sha256_hash}")

        original_ext = get_original_extension(object_key)
        logging.info(f"Original extension: '{original_ext}'")

        archive_object_path = construct_destination_path(
            original_path=object_key, # construct_destination_path handles path normalization
            sha256_hash=sha256_hash,
            original_extension=original_ext,
            preserved_depth=preserved_depth
        )
        # Ensure GCS paths use forward slashes
        archive_object_path = archive_object_path.replace(os.sep, '/')
        logging.info(f"Constructed archive object path: gs://{read_bucket_name}/{archive_object_path}")

        logging.info(f"Step 3: Checking for existing object in archive: gs://{read_bucket_name}/{archive_object_path}")
        existing_archive_metadata = gcs.stat_object(read_bucket_name, archive_object_path)

        if existing_archive_metadata:
            logging.info(f"Object with hash {sha256_hash} found in archive. Deduplicating.")
            logging.debug(f"Existing archive metadata: {existing_archive_metadata}")
        else:
            logging.info(f"Object with hash {sha256_hash} not found in archive. This is a new unique file.")

        logging.info(f"Step 4: Preparing new/updated metadata for archive object.")
        final_archive_metadata = prepare_new_metadata(
            original_object_key=object_key, # Pass the original key as is
            original_object_metadata=original_metadata,
            existing_archive_object_metadata=existing_archive_metadata
        )
        logging.debug(f"Final archive metadata to be set: {final_archive_metadata}")

        logging.info(
            f"Step 5: Copying object from gs://{bucket_name}/{object_key} to "
            f"gs://{read_bucket_name}/{archive_object_path} with new metadata."
        )
        gcs.copy_object(
            source_bucket_name=bucket_name,
            source_blob_name=object_key,
            dest_bucket_name=read_bucket_name,
            dest_blob_name=archive_object_path,
            new_metadata=final_archive_metadata
        )
        logging.info(f"Successfully copied object to archive.")

        logging.info(f"Step 6: Deleting original object from inbox: gs://{bucket_name}/{object_key}")
        gcs.delete_object(bucket_name, object_key)
        logging.info(f"Successfully deleted original object: gs://{bucket_name}/{object_key}")

        logging.info(f"Successfully processed and deduplicated gs://{bucket_name}/{object_key}")

    except Exception as e:
        logging.error(f"Error processing gs://{cloud_event.data.get('bucket')}/{cloud_event.data.get('name')}: {e}")
        logging.error(traceback.format_exc())
        # Re-raise the exception to allow GCF to handle retries/failures
        # and to make the failure visible in Cloud Logging for the function execution.
        raise
```
