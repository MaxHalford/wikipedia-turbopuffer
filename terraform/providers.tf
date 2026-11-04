terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.19.0"
    }
  }

  required_version = ">= 1.10.0"
}

provider "google" {
  project = var.project_id
  region  = var.region
}


