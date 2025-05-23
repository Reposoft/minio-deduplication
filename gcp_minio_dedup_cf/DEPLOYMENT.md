# Google Cloud Function Deployment: GCS Deduplication

This document provides instructions for provisioning the necessary Google Cloud Platform (GCP) resources and deploying the Cloud Function for GCS object deduplication.

## 1. Prerequisites

Before you begin, ensure you have the following:

*   **Google Cloud SDK (`gcloud`)**: Installed and configured. You should be able to run `gcloud` commands and have authenticated with your Google Cloud account.
    *   Installation: [Google Cloud SDK Documentation](https://cloud.google.com/sdk/docs/install)
*   **Google Cloud Project**: A GCP project with billing enabled.
*   **Permissions**: Sufficient IAM permissions in your project to:
    *   Create GCS buckets (`roles/storage.admin`)
    *   Create Cloud Functions (`roles/cloudfunctions.admin`)
    *   Create Service Accounts (`roles/iam.serviceAccountAdmin`)
    *   Assign IAM roles to service accounts and buckets (`roles/iam.securityAdmin` or `roles/owner` for project-level, or specific admin roles for resources).
    *   Alternatively, the `roles/owner` role on the project grants all necessary permissions.

## 2. Resource Provisioning

### a. Create GCS Buckets

You need two GCS buckets:
*   A **"write" bucket**: New files are uploaded here, triggering the Cloud Function.
*   A **"read" bucket**: Deduplicated files (archive objects) are stored here.

**Instructions:**

1.  Choose a globally unique name for your buckets. It's a good practice to include your project ID.
2.  Choose a region (e.g., `US-CENTRAL1`). For best performance and to avoid cross-region data transfer costs, deploy your Cloud Function in the same region as your buckets.
3.  Choose a storage class (e.g., Standard, Nearline, Coldline). Standard is recommended for frequently accessed "write" buckets and potentially the "read" bucket if access is frequent.

**Example `gcloud` commands:**

Replace `your-project-id` and `US-CENTRAL1` with your actual project ID and desired region.

```bash
# Create the "write" bucket
gcloud storage buckets create gs://your-project-id-dedup-write --project=your-project-id --location=US-CENTRAL1 --default-storage-class=STANDARD

# Create the "read" bucket (archive)
gcloud storage buckets create gs://your-project-id-dedup-read --project=your-project-id --location=US-CENTRAL1 --default-storage-class=STANDARD
```

Let's define these bucket names for later use:
*   `YOUR_WRITE_BUCKET_NAME="gs://your-project-id-dedup-write"`
*   `YOUR_READ_BUCKET_NAME="gs://your-project-id-dedup-read"`

### b. Create a Service Account for the Cloud Function

The Cloud Function requires a dedicated service account with specific permissions to interact with GCS.

**Instructions:**

1.  Create a new service account.
2.  Grant this service account `roles/storage.objectAdmin` on both the "write" and "read" buckets. This role allows the function to read, write, and delete objects, as well as manage object metadata.

**Example `gcloud` commands:**

Replace `your-project-id`, `function-sa`, `YOUR_WRITE_BUCKET_NAME_NO_GS` (e.g., `your-project-id-dedup-write`), and `YOUR_READ_BUCKET_NAME_NO_GS` (e.g., `your-project-id-dedup-read`).

```bash
# 1. Create the service account
gcloud iam service-accounts create function-sa \
    --description="Service account for GCS deduplication Cloud Function" \
    --display-name="GCS Deduplication Function SA" \
    --project=your-project-id

# Define service account email for convenience
FUNCTION_SA_EMAIL="function-sa@your-project-id.iam.gserviceaccount.com" 

# 2. Grant permissions to the "write" bucket
gcloud storage buckets add-iam-policy-binding gs://YOUR_WRITE_BUCKET_NAME_NO_GS \
    --member="serviceAccount:${FUNCTION_SA_EMAIL}" \
    --role="roles/storage.objectAdmin" \
    --project=your-project-id

# 3. Grant permissions to the "read" bucket
gcloud storage buckets add-iam-policy-binding gs://YOUR_READ_BUCKET_NAME_NO_GS \
    --member="serviceAccount:${FUNCTION_SA_EMAIL}" \
    --role="roles/storage.objectAdmin" \
    --project=your-project-id
```
*Note: `YOUR_WRITE_BUCKET_NAME_NO_GS` is the bucket name without the `gs://` prefix.*

## 3. Cloud Function Deployment

### a. Prepare for Deployment

1.  Ensure your `requirements.txt` file in the `gcp_minio_dedup_cf` directory lists all necessary dependencies. It should include:
    ```
    google-cloud-storage
    functions-framework
    ```
2.  Navigate your terminal to the root directory of the function, `gcp_minio_dedup_cf` (i.e., the directory containing `main.py` and `requirements.txt`).

### b. Deploy the Function

Use the `gcloud functions deploy` command.

**Command Parameters:**

*   **Function Name**: A name for your function (e.g., `gcs-deduplicate-on-upload`).
*   **Runtime**: Specify a recent stable Python runtime (e.g., `python311`, `python310`, `python39`).
*   **Trigger**:
    *   `--trigger-resource`: Your "write" bucket (e.g., `your-project-id-dedup-write`).
    *   `--trigger-event`: `google.storage.object.finalize` (triggers on new object creation/overwrite).
*   **Entry Point**: The name of the Python function in `main.py` to execute (i.e., `gcs_deduplicate_on_upload_handler`).
*   **Service Account**: The email address of the service account created in step 2b.
*   **Region**: The GCP region for deployment (e.g., `us-central1`). Ideally, this should match the region of your GCS buckets.
*   **Environment Variables**:
    *   `READ_BUCKET_NAME`: The name of your "read" bucket (e.g., `your-project-id-dedup-read`).
    *   `PRESERVED_FOLDER_DEPTH`: Set to `0` for default behavior (no prefix preservation) or your desired depth.

**Example `gcloud` command:**

Replace placeholders with your actual values. Remember `YOUR_WRITE_BUCKET_NAME_NO_GS` and `YOUR_READ_BUCKET_NAME_NO_GS` are the bucket names without the `gs://` prefix.

```bash
gcloud functions deploy gcs-deduplicate-on-upload \
    --gen2 \
    --runtime=python311 \
    --project=your-project-id \
    --region=us-central1 \
    --source=. \
    --entry-point=gcs_deduplicate_on_upload_handler \
    --trigger-resource=YOUR_WRITE_BUCKET_NAME_NO_GS \
    --trigger-event=google.storage.object.finalize \
    --service-account=${FUNCTION_SA_EMAIL} \
    --set-env-vars READ_BUCKET_NAME=YOUR_READ_BUCKET_NAME_NO_GS,PRESERVED_FOLDER_DEPTH=0
```
*Note: `--gen2` deploys the function as a 2nd generation Cloud Function, which is recommended. Adjust runtime as needed.*

## 4. Integration Test Authorization (Local Machine / CI/CD)

To run integration tests that interact with your GCP resources (e.g., upload files, verify deduplication), you'll need a separate service account with appropriate permissions.

### a. Create a Service Account for Testing

**Instructions:**

Create a new service account specifically for testing purposes.

**Example `gcloud` command:**

```bash
gcloud iam service-accounts create integration-tester-sa \
    --description="Service account for GCS deduplication integration tests" \
    --display-name="Integration Tester SA" \
    --project=your-project-id
```

Let's define this service account email:
*   `TESTER_SA_EMAIL="integration-tester-sa@your-project-id.iam.gserviceaccount.com"`

### b. Grant Roles to the Testing Service Account

This service account needs permissions to:
*   Upload to the "write" bucket.
*   Read and delete from the "write" bucket (for test setup and cleanup).
*   Read and delete from the "read" bucket (to verify results and cleanup).

**Example `gcloud` commands:**

```bash
# Permissions for the "write" bucket
gcloud storage buckets add-iam-policy-binding gs://YOUR_WRITE_BUCKET_NAME_NO_GS \
    --member="serviceAccount:${TESTER_SA_EMAIL}" \
    --role="roles/storage.objectAdmin" \
    --project=your-project-id

# Permissions for the "read" bucket
gcloud storage buckets add-iam-policy-binding gs://YOUR_READ_BUCKET_NAME_NO_GS \
    --member="serviceAccount:${TESTER_SA_EMAIL}" \
    --role="roles/storage.objectAdmin" \
    --project=your-project-id

# Optional: If tests manage the function itself (e.g., deploying for an integration test run)
# gcloud projects add-iam-policy-binding your-project-id \
#     --member="serviceAccount:${TESTER_SA_EMAIL}" \
#     --role="roles/cloudfunctions.developer"
```

### c. Download a Service Account Key

**Instructions:**

Download a JSON key file for this testing service account. This key allows applications to authenticate as the service account.

**Example `gcloud` command:**

```bash
gcloud iam service-accounts keys create ./integration-tester-key.json \
    --iam-account=${TESTER_SA_EMAIL} \
    --project=your-project-id
```
This command will download a key file named `integration-tester-key.json` to your current directory.

**Security Note:**
*   **Keep this key file secure.**
*   **Do NOT commit this key file to your version control system (e.g., Git).** Add it to your `.gitignore` file.
*   Grant this key file only the necessary permissions and restrict its access.

### d. Configure Tests to Use the Key

Your integration tests will need to authenticate using this service account key. The most common way is to set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable.

**Instructions:**

Set the environment variable to the absolute path of the downloaded JSON key file.

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/integration-tester-key.json"
```

Replace `/path/to/your/` with the actual path to the key file on your system. Your testing framework or CI/CD environment might have specific ways to manage environment variables.
```
