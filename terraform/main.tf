provider "google" {
  project     = var.project_id
  region      = var.region
  credentials = file("gcp_credentials.json")
}

# main.tf - Defines all the GCP resources for the project.

# --- GCS Bucket ---
# A bucket to store raw data files and for Dataflow staging.
resource "google_storage_bucket" "data_bucket_ing_1990" {
  name          = var.bucket_name
  project       = var.project_id
  location      = var.region
  force_destroy = true # Set to false in production

  uniform_bucket_level_access = true
}

# --- BigQuery Dataset ---
resource "google_bigquery_dataset" "analytics_dataset" {
  dataset_id = var.dataset_id
  project    = var.project_id
  location   = var.region
  description = "Dataset for sales analytics data"
}

# --- BigQuery Raw Tables ---

# Customers table with partitioning
resource "google_bigquery_table" "customers_raw" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.analytics_dataset.dataset_id
  table_id   = "customers_raw"

  schema = file("${path.module}/schema/customer.json")

  time_partitioning {
    type  = "MONTH"
    field = "registration_date"
  }

  deletion_protection = false # Set to true in production
}

# Transactions table with partitioning and clustering
resource "google_bigquery_table" "transactions_raw" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.analytics_dataset.dataset_id
  table_id   = "transactions_raw"

  schema = file("${path.module}/schema/transaction.json")

  time_partitioning {
    type  = "MONTH"
    field = "transaction_timestamp"
  }

  clustering = ["customer_id"]

  deletion_protection = false # Set to true in production
}


# --- IAM Bindings ---
# Grant necessary roles to the Dataflow service account so it can
# access GCS and BigQuery.

# Find the default Dataflow service account
data "google_project" "project" {
  project_id = var.project_id
}

locals {
  dataflow_service_account = "service-${data.google_project.project.number}@dataflow-service-producer-prod.iam.gserviceaccount.com"
}

# Role: Dataflow Worker
resource "google_project_iam_member" "dataflow_worker" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:terraform-sa@totemic-inquiry-475408-u0.iam.gserviceaccount.com"
}

# Role: GCS Bucket Access
resource "google_project_iam_member" "storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:terraform-sa@totemic-inquiry-475408-u0.iam.gserviceaccount.com"
}

# Role: BigQuery Data Editor
resource "google_project_iam_member" "bigquery_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:terraform-sa@totemic-inquiry-475408-u0.iam.gserviceaccount.com"
}

# Role: BigQuery Job User
resource "google_project_iam_member" "bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:terraform-sa@totemic-inquiry-475408-u0.iam.gserviceaccount.com"
}


# Define the Pub/Sub topic (the "notification channel")
resource "google_pubsub_topic" "data_ing_inbound_notification" {
  name = "gcs-bucket-notifications"
}
# 3. Get the GCS service account email
# This special service account is used by GCS to publish notifications.
data "google_storage_project_service_account" "gcs_account" {
}

# 4. Grant the GCS service account permission to publish to the topic
resource "google_pubsub_topic_iam_binding" "binding" {
  topic   = google_pubsub_topic.data_ing_inbound_notification.id
  role    = "roles/pubsub.publisher"
  members = [
    "serviceAccount:${data.google_storage_project_service_account.gcs_account.email_address}",
  ]
}

# 5. Create the notification configuration
# This links the bucket to the topic.
resource "google_storage_notification" "notification" {
  bucket         = google_storage_bucket.data_bucket_ing_1990.name
  topic          = google_pubsub_topic.data_ing_inbound_notification.id
  payload_format = "JSON_API_V1"

  # (Optional) Specify which events to listen for.
  # If omitted, it defaults to all event types.
  event_types = ["OBJECT_FINALIZE", "OBJECT_DELETE"]

  # (Optional) Filter notifications to objects with a specific prefix
  # object_name_prefix = "uploads/"

  # Ensures the IAM binding is created before the notification
  depends_on = [
    google_pubsub_topic_iam_binding.binding,
  ]
}

# -----------------------------------------------------------------------------
# 2. CREATE A DEDICATED SERVICE ACCOUNT
# -----------------------------------------------------------------------------
resource "google_service_account" "composer_sa" {
  project      = var.project_id
  account_id   = "composer-ing-sa"
  display_name = "Composer Environment Service Account"
}

# -----------------------------------------------------------------------------
# 3. ASSIGN IAM ROLES TO THE SERVICE ACCOUNT
# -----------------------------------------------------------------------------
resource "google_project_iam_member" "composer_worker_role" {
  project = var.project_id
  role    = "roles/composer.worker"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

resource "google_project_iam_member" "composer_service_agent_ext" {
  project = var.project_id
  role    = "roles/composer.ServiceAgentV2Ext"
  member  = "serviceAccount:service-${data.google_project.project.number}@cloudcomposer-accounts.iam.gserviceaccount.com"

  # This must wait for the API to be enabled, which is when
  # Google creates the service agent.

}

resource "google_composer_environment" "composer_env" {
  provider = google
  project  = var.project_id
  name     = "data-ingestion-env"
  region   = var.region

  # This block contains the main configuration for the environment
  config {

    # Specifies a Composer 2 environment and a specific Airflow image.
    # Find the latest image versions here:
    # https://cloud.google.com/composer/docs/concepts/versioning/composer-versions
    software_config {
      image_version = "composer-2.14.4-airflow-2.9.3"
    }

    # Configures the underlying GKE Autopilot cluster
    environment_size = "ENVIRONMENT_SIZE_SMALL"

    # Assign the dedicated service account
    # This resource must wait for the IAM binding to be created
    node_config {
      service_account = google_service_account.composer_sa.email
    }
  }
  depends_on = [
    google_project_iam_member.composer_worker_role,
    google_project_iam_member.composer_service_agent_ext
  ]
}
