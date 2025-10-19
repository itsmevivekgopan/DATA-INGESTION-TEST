variable "project_id" {
  description = "The GCP project ID."
  type        = string
  default     = "totemic-inquiry-475408-u0"
}

variable "region" {
  description = "The GCP region for resources."
  type        = string
  default     = "europe-west2"
}

variable "bucket_name" {
  description = "The name for the GCS bucket. Must be globally unique."
  type        = string
  default     = "data_bucket_ing_1990"
}

variable "dataset_id" {
  description = "The ID for the BigQuery dataset."
  type        = string
  default     = "transaction_analytics"
}
