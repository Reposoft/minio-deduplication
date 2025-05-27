# Google Cloud Storage (GCS) Backend for Deduplication

This document outlines the Google Cloud Platform (GCP) services required to run the GCS version of the deduplication application and its integration tests.

## 1. Required Google Cloud Services

### 1.1. Google Cloud Storage (GCS)

Two GCS buckets are essential for the application's operation:

*   **Write Bucket (Source):** This bucket receives initial file uploads. The Cloud Function is triggered by new objects in this bucket.
    *   Configured for the Cloud Function via `GCS_WRITE_BUCKET_NAME` environment variable.
    *   Configured for integration tests via `GCS_WRITE_BUCKET_NAME` (as per current `config.go`, which uses this for the GCS target).
*   **Read Bucket (Destination):** Processed (deduplicated) files are stored here, organized by their hash and potentially preserved original path components.
    *   Configured for the Cloud Function via `GCS_READ_BUCKET_NAME` environment variable.
    *   Configured for integration tests via `GCS_READ_BUCKET_NAME`.

Bucket names should be globally unique.

### 1.2. Google Cloud Functions

A Google Cloud Function processes blob uploads from the "write" bucket.

*   **Entry Point:** The Go function `gcs_transfer.HandleGCSEvent` (in `gcs_transfer.go`) is the entry point for the Cloud Function.
*   **Trigger Type:** Google Cloud Storage.
*   **Event Type:** "Finalize/Create" (officially `google.storage.object.finalize`). This triggers the function when a new object is successfully created in the write bucket.
*   **Runtime:** Go (e.g., `go121` or a newer supported version).
*   **Required Environment Variables for the Cloud Function:**
    *   `GCS_WRITE_BUCKET_NAME`: Name of the source GCS bucket.
    *   `GCS_READ_BUCKET_NAME`: Name of the destination GCS bucket.
    *   `PRESERVED_FOLDER_DEPTH`: (Integer) Specifies how many leading path components from the original object path should be preserved in the destination path.
        *   Defaults to `0` if not set.
        *   Can be overridden for specific uploads during testing by setting the `preserved-depth-override` custom metadata field on the uploaded object.
    *   `GCLOUD_PROJECT`: (Optional) The Google Cloud Project ID. Often inferred from the execution environment if not explicitly set.

### 1.3. IAM (Identity and Access Management)

The Cloud Function executes under a specific service account. This service account requires the following IAM roles:

*   **On the "Read" Bucket (Destination):**
    *   `roles/storage.objectCreator` (Storage Object Creator): To write processed objects.
    *   Alternatively, `roles/storage.objectAdmin` (Storage Object Admin) provides broader permissions including creation.
*   **On the "Write" Bucket (Source):**
    *   `roles/storage.objectViewer` (Storage Object Viewer): To read the uploaded object's content and metadata.
    *   `roles/storage.objectUser` (Storage Legacy Object User): This role grants `storage.objects.get` and `storage.objects.list` on the bucket, and `storage.objects.delete` on objects. It's a common role for functions that need to read and then delete source objects.
    *   Alternatively, if the function only reads and does not delete, `objectViewer` is sufficient for reading. If it deletes, `Storage Object Admin` or a custom role with `storage.objects.delete` is needed. The current GCS function implementation in `gcs_transfer.go` does not explicitly perform the delete; this is usually handled by the application logic calling the function or by a separate cleanup process. For the described deduplication flow, the source object is typically deleted after successful processing.
*   **On the Project:**
    *   `roles/logging.logWriter` (Logs Writer): To write execution logs to Google Cloud Logging. This is typically granted by default to Cloud Function service accounts.

**Note:** If the Cloud Function itself were responsible for creating the GCS buckets (which is not the current design for this application), its service account would need `roles/storage.admin` (Storage Admin) at the project level.

### 1.4. Google Cloud Logging

*   The Cloud Function automatically sends its logs (written via `go.uber.org/zap` or standard Go `log`) to Google Cloud Logging.
*   Integration tests (specifically `GcsMonitor`) use Cloud Logging to query and verify log messages, confirming that the function processed objects as expected and used the correct settings (e.g., `preservedFolderDepth`).

## 2. Integration Test Authorization

The Go integration tests in `integration_tests/go/` require authorization to interact with GCP services when `TEST_TARGET=gcs`.

### 2.1. Service Account for Tests

A dedicated Google Cloud service account should be used for running the integration tests. This service account needs the following IAM roles:

*   **On the Test GCS Buckets (both write and read buckets used by tests):**
    *   `roles/storage.admin` (Storage Admin): Recommended if tests manage their own bucket lifecycle (create/delete buckets) or need to modify bucket ACLs/CORS settings.
    *   Alternatively, if buckets are pre-created and tests only manage objects: `roles/storage.objectAdmin` (Storage Object Admin) on both test buckets is sufficient for uploading, reading, deleting, and listing objects.
*   **On the Project:**
    *   `roles/logging.viewer` (Logs Viewer): To allow `GcsMonitor` to read Cloud Function logs from Cloud Logging.
    *   `roles/cloudfunctions.invoker` (Cloud Functions Invoker): Only if tests were designed to invoke the Cloud Function directly via HTTP/RPC (not the current design, which relies on GCS triggers).
    *   `roles/resourcemanager.projectViewer` (Project Viewer) or a role granting `resourcemanager.projects.get`: This might be needed for the Cloud Logging client to correctly scope log queries if the project ID is not implicitly available or to list available projects if the client needs to discover it. Usually, providing `GCS_PROJECT_ID` is sufficient.

### 2.2. Authentication Methods

*   **Application Default Credentials (ADC):** This is the recommended method for ease of use, especially when running tests within GCP environments (e.g., Google Compute Engine, Google Kubernetes Engine, Cloud Build). The Go client libraries automatically find credentials in these environments.
*   **Service Account Key File (JSON):** For local development or CI environments outside of GCP, you can use a service account key file.
    1.  Create a service account and grant it the roles listed above.
    2.  Download its key as a JSON file.
    3.  Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to the absolute path of this JSON key file.
        ```bash
        export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your-service-account-key.json"
        ```

### 2.3. Required Environment Variables for Tests

When running integration tests with `TEST_TARGET=gcs`, ensure the following environment variables are set (refer to `integration_tests/go/config.go` for authoritative names):

*   `TEST_TARGET=gcs`
*   `GCS_PROJECT_ID`: Your Google Cloud Project ID.
*   `GCS_WRITE_BUCKET_NAME`: Name of the GCS bucket for test uploads (source).
*   `GCS_READ_BUCKET_NAME`: Name of the GCS bucket for test results (destination).
*   `GCS_FUNCTION_NAME`: The name of the deployed Cloud Function being tested.
*   `GCS_FUNCTION_REGION`: The region where the Cloud Function is deployed (e.g., `us-central1`). This helps in uniquely identifying the function for log monitoring.
*   `PRESERVED_FOLDER_DEPTH` (Optional): Sets the default depth for the application if not overridden by metadata. The tests themselves can override this via metadata.
*   `GOOGLE_APPLICATION_CREDENTIALS` (Conditional): Path to your service account key JSON file, if not using ADC or if ADC cannot find credentials.

## 3. Deployment Snippet for Cloud Function

Here's a sample `gcloud` command to deploy the Go Cloud Function. This command should be run from the root of the repository.

```bash
# Ensure you are in the repository root directory
# The --source argument points to the directory containing the function's Go code.

gcloud functions deploy YOUR_FUNCTION_NAME \
  --gen2 \ # Optional: Use 2nd generation Cloud Functions for more features
  --runtime go121 \ # Or your desired Go runtime, e.g., go1.21
  --trigger-resource YOUR_GCS_WRITE_BUCKET_NAME \
  --trigger-event google.storage.object.finalize \
  --entry-point HandleGCSEvent \
  --source gcs/ \
  --region YOUR_DEPLOY_REGION \
  --service-account YOUR_FUNCTION_SERVICE_ACCOUNT_EMAIL \
  --set-env-vars GCS_WRITE_BUCKET_NAME=YOUR_GCS_WRITE_BUCKET_NAME,GCS_READ_BUCKET_NAME=YOUR_GCS_READ_BUCKET_NAME,PRESERVED_FOLDER_DEPTH=0
```

**Replace placeholders:**

*   `YOUR_FUNCTION_NAME`: A unique name for your Cloud Function (e.g., `gcs-deduplicator`).
*   `YOUR_GCS_WRITE_BUCKET_NAME`: The name of the GCS bucket that will trigger the function.
*   `YOUR_DEPLOY_REGION`: The GCP region where you want to deploy the function (e.g., `us-central1`).
*   `YOUR_FUNCTION_SERVICE_ACCOUNT_EMAIL`: The email address of the service account the function will use.
*   Adjust `PRESERVED_FOLDER_DEPTH=0` as needed for the default behavior.

**Note on `--source`:** The path `gcs/` assumes your `gcloud` command is run from the root of this repository, and your Go files for the function are within the `gcs` subdirectory. If your function had external dependencies not vendored, you might need to ensure `go.mod` and `go.sum` are present in the `gcs/` directory or adjust source packaging.

## 4. CI/CD with GitHub Actions

A GitHub Actions workflow is configured to automate testing for this project.

### 4.1. Workflow Overview

*   **Location:** `.github/workflows/go-tests.yml`
*   **Triggers:** The workflow is triggered on `push` and `pull_request` events to the `main` and `feat/gcs-serverless-port` branches.
*   **Jobs:**
    *   `unit-tests`:
        *   Checks out the code and sets up Go.
        *   Runs unit tests in the root module (if Go files and tests exist).
        *   Runs unit tests located within the `gcs/` module.
    *   `integration-tests-minio`:
        *   Runs after `unit-tests`.
        *   Sets up Go and uses Docker Compose (`docker-compose.test.yml`) to start MinIO and the application (`app0`) containers.
        *   Waits for services to become healthy.
        *   Executes integration tests against the MinIO backend.
        *   Stops containers after tests.
    *   `integration-tests-gcs`:
        *   Runs after `unit-tests`.
        *   Sets up Go and authenticates to Google Cloud using Workload Identity Federation.
        *   Executes integration tests against a live GCS backend and a deployed Google Cloud Function.

### 4.2. Running Integration Tests

*   Integration tests are tagged with the build constraint `//go:build integration`.
*   To run them locally, navigate to the `integration_tests/go` directory and use the command:
    ```bash
    go test -v -tags=integration .
    ```
*   Ensure all necessary environment variables are set as described in the "Integration Test Authorization" section and in `integration_tests/go/config.go` (e.g., `TEST_TARGET`, credentials, bucket names).

### 4.3. Secrets for GCS Integration Tests

For the `integration-tests-gcs` job to run successfully in GitHub Actions, the following secrets must be configured in your GitHub repository settings (under "Settings" > "Secrets and variables" > "Actions"):

*   `GCP_PROJECT_ID`: Your Google Cloud Project ID.
*   `GCP_SA_EMAIL`: The email address of the Google Cloud Service Account that GitHub Actions will impersonate (e.g., `your-service-account@your-project-id.iam.gserviceaccount.com`). This service account needs the IAM roles specified in the "Integration Test Authorization" section.
*   `TEST_GCS_WRITE_BUCKET`: Name of the GCS bucket for uploads (write operations) during tests. (Note: The workflow uses `GCS_WRITE_BUCKET_NAME` which is derived from this or a default in `config.go` if this secret is not directly used under this exact name in the workflow env var section).
*   `TEST_GCS_READ_BUCKET`: Name of the GCS bucket for processed files (read operations) during tests. (Note: Similar to above, workflow uses `GCS_READ_BUCKET_NAME`).
*   `GCS_FUNCTION_NAME`: The name of the deployed Google Cloud Function being tested.
*   `GCS_FUNCTION_REGION`: The region where the Google Cloud Function is deployed.

**Note on Workload Identity Federation:**
The workflow uses `google-github-actions/auth@v2` for authentication, leveraging Workload Identity Federation. This requires you to:
1.  Set up a Workload Identity Pool and Provider in your GCP project.
2.  Grant the GitHub repository (or specific branches/tags) permission to impersonate the specified service account (`GCP_SA_EMAIL`) through this pool.
The example workflow uses a provider path like `projects/${{ secrets.GCP_PROJECT_ID }}/locations/global/workloadIdentityPools/github-actions-pool/providers/github-actions-provider`. You may need to adjust this path in the `.github/workflows/go-tests.yml` file if your pool/provider names differ.
For more details, refer to the [Google Cloud documentation on Workload Identity Federation](https://cloud.google.com/iam/docs/workload-identity-federation).

### 4.4. Secrets for MinIO Integration Tests

*   The `integration-tests-minio` job runs using Docker Compose.
*   It uses the default credentials (`minioadmin/minioadmin`) and bucket names (e.g., `bucket-write`, `bucket-read`) as defined in `docker-compose.test.yml` and the default test configuration (`integration_tests/go/config.go`).
*   Typically, no GitHub secrets are required for this job unless you intend to override these defaults with custom MinIO settings via environment variables in the workflow.

### 4.5. Interpreting Test Results

*   Test results, including logs and error messages, can be viewed in the "Actions" tab of your GitHub repository.
*   Select the specific workflow run, then navigate to the job (e.g., `integration-tests-gcs`) to see its output.
```

I have created the `gcs/README.md` file with the requested documentation.
It covers:
- Required GCP services (GCS, Cloud Functions, IAM, Cloud Logging).
- Details for the Cloud Function (entry point, trigger, environment variables).
- IAM roles for the Cloud Function's service account.
- Authorization for integration tests (service account, roles, authentication methods).
- Required environment variables for running tests against GCS.
- A sample `gcloud` deployment command for the Cloud Function.
- Markdown formatting for readability.

This fulfills the requirements of the subtask.Okay, I have created `gcs/README.md` with the detailed documentation covering GCP service requirements, Cloud Function setup, IAM roles, integration test authorization, and a sample deployment command.

This completes the subtask.
