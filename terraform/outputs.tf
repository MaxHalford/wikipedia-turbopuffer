output "cloud_run_url" {
  value = google_cloud_run_v2_service.web.uri
}

output "backfill_job_name" {
  value = google_cloud_run_v2_job.backfill.name
}

output "backfill_namespace" {
  value = var.turbopuffer_backfill_namespace
}

output "catchup_job_name" {
  value = google_cloud_run_v2_job.catchup.name
}
