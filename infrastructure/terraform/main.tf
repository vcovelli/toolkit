provider "aws" {
  region = var.aws_region
}

# Example resource template (e.g., S3 bucket)
resource "aws_s3_bucket" "example" {
  bucket = var.bucket_name
}

