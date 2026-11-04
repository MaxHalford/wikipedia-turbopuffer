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

variable "job_image" {
  description = "The immutable image used by the backfill and catch-up jobs. Defaults to the live ingester image."
  type        = string
  default     = null
}

variable "query_api_key_secret" {
  description = "The Secret Manager secret with a query-only turbopuffer key."
  type        = string
  default     = "wikipedia-turbopuffer-query-api-key"
}

variable "write_api_key_secret" {
  description = "The Secret Manager secret with a turbopuffer ingestion key that can query and write."
  type        = string
  default     = "wikipedia-turbopuffer-write-api-key"
}

variable "job_write_api_key_secret" {
  description = "The Secret Manager secret used by the backfill and catch-up jobs."
  type        = string
  default     = "wikipedia-turbopuffer-write-api-key"
}

variable "turbopuffer_namespace" {
  description = "The turbopuffer namespace queried by the web service."
  type        = string
  default     = "wikipedia-en-live"
}

variable "turbopuffer_ingester_namespace" {
  description = "The turbopuffer namespace updated by the live ingester."
  type        = string
  default     = "wikipedia-en-live"
}

variable "turbopuffer_backfill_namespace" {
  description = "The staging namespace for the Wikipedia dump backfill."
  type        = string
  default     = "wikipedia-en-20261101"
}

variable "wikipedia_dump_date" {
  description = "The YYYYMMDD date of the Wikimedia CirrusSearch dump."
  type        = string
  default     = "20261101"
}

variable "wikipedia_dump_shards" {
  description = "The number of files in the Wikimedia CirrusSearch dump."
  type        = number
  default     = 66
}

variable "wikipedia_backfill_parallelism" {
  description = "The maximum number of concurrent dump workers."
  type        = number
  default     = 8
}

variable "wikipedia_catchup_start" {
  description = "The earliest revision timestamp to reconcile after the dump."
  type        = string
  default     = "2026-11-01T00:00:00Z"
}

variable "wikipedia_catchup_interval" {
  description = "How often the live ingester reconciles recent changes."
  type        = string
  default     = "5m"
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
