data "google_secret_manager_secret" "query_api_key" {
  secret_id = var.query_api_key_secret
}

data "google_secret_manager_secret" "write_api_key" {
  secret_id = var.write_api_key_secret
}

data "google_secret_manager_secret" "job_write_api_key" {
  secret_id = var.job_write_api_key_secret
}

locals {
  job_image = coalesce(var.job_image, var.ingester_image)
}

resource "google_service_account" "web" {
  account_id   = "wikipedia-web"
  display_name = "Wikipedia search web"
}

resource "google_service_account" "ingester" {
  account_id   = "wikipedia-ingester"
  display_name = "Wikipedia Redpanda Connect ingester"
}

resource "google_secret_manager_secret_iam_member" "web_query_api_key" {
  secret_id = data.google_secret_manager_secret.query_api_key.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.web.email}"
}

resource "google_secret_manager_secret_iam_member" "ingester_write_api_key" {
  secret_id = data.google_secret_manager_secret.write_api_key.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.ingester.email}"
}

resource "google_secret_manager_secret_iam_member" "ingester_job_write_api_key" {
  secret_id = data.google_secret_manager_secret.job_write_api_key.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.ingester.email}"
}

resource "google_cloud_run_v2_service" "web" {
  name                = "wikipedia-web"
  location            = var.region
  ingress             = "INGRESS_TRAFFIC_ALL"
  deletion_protection = false

  template {
    service_account                  = google_service_account.web.email
    timeout                          = "15s"
    max_instance_request_concurrency = 80

    scaling {
      min_instance_count = 0
      max_instance_count = 2
    }

    containers {
      image = var.web_image

      ports {
        container_port = 8080
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "256Mi"
        }
        cpu_idle          = true
        startup_cpu_boost = true
      }

      env {
        name  = "TURBOPUFFER_REGION"
        value = var.turbopuffer_region
      }

      env {
        name  = "TURBOPUFFER_NAMESPACE"
        value = var.turbopuffer_namespace
      }

      env {
        name = "TURBOPUFFER_API_KEY"
        value_source {
          secret_key_ref {
            secret  = data.google_secret_manager_secret.query_api_key.secret_id
            version = "latest"
          }
        }
      }

      startup_probe {
        initial_delay_seconds = 0
        timeout_seconds       = 2
        period_seconds        = 2
        failure_threshold     = 10
        http_get {
          path = "/healthz"
          port = 8080
        }
      }
    }
  }

  depends_on = [google_secret_manager_secret_iam_member.web_query_api_key]

  lifecycle {
    ignore_changes = [scaling]
  }
}

resource "google_cloud_run_v2_service_iam_member" "public_web" {
  name     = google_cloud_run_v2_service.web.name
  location = google_cloud_run_v2_service.web.location
  role     = "roles/run.invoker"
  member   = "allUsers"
}

resource "google_cloud_run_v2_service" "ingester" {
  name                = "wikipedia-ingester"
  location            = var.region
  ingress             = "INGRESS_TRAFFIC_INTERNAL_ONLY"
  deletion_protection = false

  template {
    service_account                  = google_service_account.ingester.email
    timeout                          = "300s"
    max_instance_request_concurrency = 1

    scaling {
      min_instance_count = 1
      max_instance_count = 1
    }

    containers {
      image = var.ingester_image

      ports {
        container_port = 8080
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
        cpu_idle          = false
        startup_cpu_boost = true
      }

      env {
        name  = "TURBOPUFFER_REGION"
        value = var.turbopuffer_region
      }

      env {
        name  = "TURBOPUFFER_NAMESPACE"
        value = var.turbopuffer_ingester_namespace
      }

      env {
        name  = "WIKIMEDIA_USER_AGENT"
        value = var.wikimedia_user_agent
      }

      env {
        name  = "WIKIPEDIA_CATCHUP_START"
        value = var.wikipedia_catchup_start
      }

      env {
        name  = "WIKIPEDIA_CATCHUP_INTERVAL"
        value = var.wikipedia_catchup_interval
      }

      env {
        name = "TURBOPUFFER_API_KEY"
        value_source {
          secret_key_ref {
            secret  = data.google_secret_manager_secret.write_api_key.secret_id
            version = "latest"
          }
        }
      }

      startup_probe {
        initial_delay_seconds = 0
        timeout_seconds       = 2
        period_seconds        = 2
        failure_threshold     = 15
        http_get {
          path = "/ready"
          port = 8080
        }
      }
    }
  }

  depends_on = [google_secret_manager_secret_iam_member.ingester_write_api_key]

  lifecycle {
    ignore_changes = [scaling]
  }
}

resource "google_cloud_run_v2_job" "backfill" {
  name                = "wikipedia-backfill"
  location            = var.region
  deletion_protection = false

  template {
    task_count  = var.wikipedia_dump_shards
    parallelism = var.wikipedia_backfill_parallelism

    template {
      service_account = google_service_account.ingester.email
      timeout         = "604800s"
      max_retries     = 2

      containers {
        image = local.job_image
        args  = ["wikipedia-backfill"]

        resources {
          limits = {
            cpu    = "2"
            memory = "2Gi"
          }
        }

        env {
          name  = "TURBOPUFFER_REGION"
          value = var.turbopuffer_region
        }

        env {
          name  = "TURBOPUFFER_NAMESPACE"
          value = var.turbopuffer_backfill_namespace
        }

        env {
          name  = "TURBOPUFFER_MAX_IN_FLIGHT"
          value = tostring(var.turbopuffer_backfill_max_in_flight)
        }

        env {
          name  = "WIKIPEDIA_DUMP_DATE"
          value = var.wikipedia_dump_date
        }

        env {
          name  = "WIKIMEDIA_USER_AGENT"
          value = var.wikimedia_user_agent
        }

        env {
          name = "TURBOPUFFER_API_KEY"
          value_source {
            secret_key_ref {
              secret  = data.google_secret_manager_secret.job_write_api_key.secret_id
              version = "latest"
            }
          }
        }
      }
    }
  }

  depends_on = [google_secret_manager_secret_iam_member.ingester_job_write_api_key]
}

resource "google_cloud_run_v2_job" "catchup" {
  name                = "wikipedia-catchup"
  location            = var.region
  deletion_protection = false

  template {
    task_count  = 1
    parallelism = 1

    template {
      service_account = google_service_account.ingester.email
      timeout         = "86400s"
      max_retries     = 1

      containers {
        image = local.job_image
        args  = ["wikipedia-catchup"]

        resources {
          limits = {
            cpu    = "2"
            memory = "1Gi"
          }
        }

        env {
          name  = "TURBOPUFFER_REGION"
          value = var.turbopuffer_region
        }

        env {
          name  = "TURBOPUFFER_NAMESPACE"
          value = var.turbopuffer_backfill_namespace
        }

        env {
          name  = "WIKIPEDIA_CATCHUP_START"
          value = var.wikipedia_catchup_start
        }

        env {
          name  = "WIKIMEDIA_USER_AGENT"
          value = var.wikimedia_user_agent
        }

        env {
          name = "TURBOPUFFER_API_KEY"
          value_source {
            secret_key_ref {
              secret  = data.google_secret_manager_secret.job_write_api_key.secret_id
              version = "latest"
            }
          }
        }
      }
    }
  }

  depends_on = [google_secret_manager_secret_iam_member.ingester_job_write_api_key]
}
