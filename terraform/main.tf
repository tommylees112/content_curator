terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# Variables
variable "aws_region" {
  description = "AWS region to deploy resources"
  default     = "us-east-1"
}

variable "s3_bucket_name" {
  description = "Name of the S3 bucket to store content"
  default     = "content-curator-bucket"
}

variable "dynamodb_table_name" {
  description = "Name of the DynamoDB table to store metadata"
  default     = "content-curator-metadata"
}

# S3 bucket for content storage
resource "aws_s3_bucket" "content_bucket" {
  bucket = var.s3_bucket_name

  tags = {
    Name        = "Content Curator Storage"
    Environment = "Production"
  }
}

# S3 bucket server-side encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "content_bucket_encryption" {
  bucket = aws_s3_bucket.content_bucket.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# S3 bucket versioning
resource "aws_s3_bucket_versioning" "content_bucket_versioning" {
  bucket = aws_s3_bucket.content_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

# S3 bucket private access
resource "aws_s3_bucket_public_access_block" "content_bucket_access" {
  bucket = aws_s3_bucket.content_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# DynamoDB table for content metadata
resource "aws_dynamodb_table" "content_metadata" {
  name         = var.dynamodb_table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "guid"

  attribute {
    name = "guid"
    type = "S"
  }

  tags = {
    Name        = "Content Curator Metadata"
    Environment = "Production"
  }
}

# Outputs
output "s3_bucket_name" {
  value       = aws_s3_bucket.content_bucket.id
  description = "Name of the created S3 bucket"
}

output "dynamodb_table_name" {
  value       = aws_dynamodb_table.content_metadata.id
  description = "Name of the created DynamoDB table"
}

output "s3_bucket_arn" {
  value       = aws_s3_bucket.content_bucket.arn
  description = "ARN of the created S3 bucket"
}

output "dynamodb_table_arn" {
  value       = aws_dynamodb_table.content_metadata.arn
  description = "ARN of the created DynamoDB table"
} 