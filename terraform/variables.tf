variable "project_id" {
  description = "The GCP project that runs the Wikipedia services."
  type        = string
  default     = "data-integration-473613"
}

variable "region" {
  description = "The region for the Cloud Run services."
  type        = string
  default     = "us-central1"
}

variable "web_image" {
  description = "The immutable wikipedia-web container image."
  type        = string
}

variable "ingester_image" {
  description = "The immutable wikipedia-ingester container image."
  type        = string
}

variable "query_api_key_secret" {
  description = "The Secret Manager secret with a query-only turbopuffer key."
  type        = string
  default     = "wikipedia-turbopuffer-query-api-key"
}

variable "write_api_key_secret" {
  description = "The Secret Manager secret with a write-only turbopuffer key."
  type        = string
  default     = "wikipedia-turbopuffer-write-api-key"
}

variable "turbopuffer_namespace" {
  description = "The turbopuffer namespace for Wikipedia documents."
  type        = string
  default     = "wikipedia-en-live"
}

variable "turbopuffer_region" {
  description = "The turbopuffer region for the Wikipedia namespace."
  type        = string
  default     = "gcp-us-central1"
}

variable "wikimedia_user_agent" {
  description = "A Wikimedia-compliant user agent with a contact address."
  type        = string
}
