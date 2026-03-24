# s3_trigger_setup.tf
# =====================
# Terraform to wire up: S3 bucket → EventBridge → Lambda → ADLS bronze
#
# Prerequisites:
#   terraform init
#   AWS credentials in env (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION)
#
# Usage:
#   terraform apply -var="azure_sas_token=<your-sas-token>"

terraform {
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
}

variable "azure_sas_token" {
  description = "Azure SAS token for ADLS write access (bronze/)"
  sensitive   = true
}

variable "s3_bucket_name" {
  default = "nz-road-pipeline-raw"
}

variable "azure_storage_account" {
  default = "nzetlpipeline"
}

variable "azure_container" {
  default = "medallion"
}

variable "aws_region" {
  default = "ap-southeast-2"   # Sydney — closest to NZ
}

provider "aws" {
  region = var.aws_region
}

# ── S3 Raw Landing Bucket ────────────────────────────────────────────────────
resource "aws_s3_bucket" "raw" {
  bucket = var.s3_bucket_name
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration { status = "Enabled" }
}

# Enable EventBridge notifications on the bucket
resource "aws_s3_bucket_notification" "raw_eventbridge" {
  bucket      = aws_s3_bucket.raw.id
  eventbridge = true
}

# ── IAM Role for Lambda ──────────────────────────────────────────────────────
resource "aws_iam_role" "lambda_exec" {
  name = "road-pipeline-lambda-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "lambda_s3_read" {
  role = aws_iam_role.lambda_exec.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = "${aws_s3_bucket.raw.arn}/*"
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

# ── Lambda Function ──────────────────────────────────────────────────────────
resource "aws_lambda_function" "s3_to_adls" {
  function_name = "road-pipeline-s3-to-adls"
  role          = aws_iam_role.lambda_exec.arn
  runtime       = "python3.12"
  handler       = "s3_to_adls_lambda.lambda_handler"
  filename      = "lambda_package.zip"   # built by build_lambda.sh
  timeout       = 300                    # 5 min — enough for large parquet files
  memory_size   = 512

  environment {
    variables = {
      AZURE_STORAGE_ACCOUNT = var.azure_storage_account
      AZURE_CONTAINER       = var.azure_container
      AZURE_SAS_TOKEN       = var.azure_sas_token   # from tfvars / CI secret
      S3_BUCKET             = var.s3_bucket_name
    }
  }
}

# ── EventBridge Rule — fires on any new S3 object ───────────────────────────
resource "aws_cloudwatch_event_rule" "s3_object_created" {
  name        = "road-pipeline-s3-new-object"
  description = "Fires when a new object lands in the raw S3 bucket"
  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = { name = [var.s3_bucket_name] }
    }
  })
}

resource "aws_cloudwatch_event_target" "lambda" {
  rule      = aws_cloudwatch_event_rule.s3_object_created.name
  target_id = "s3ToAdlsLambda"
  arn       = aws_lambda_function.s3_to_adls.arn
}

resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.s3_to_adls.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.s3_object_created.arn
}

# ── Outputs ──────────────────────────────────────────────────────────────────
output "s3_bucket" {
  value = aws_s3_bucket.raw.bucket
}

output "lambda_function" {
  value = aws_lambda_function.s3_to_adls.function_name
}
