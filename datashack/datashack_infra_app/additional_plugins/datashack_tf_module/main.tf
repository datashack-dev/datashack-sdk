resource "aws_s3_bucket" "module_bucket" {
  bucket = var.bucket_name
}