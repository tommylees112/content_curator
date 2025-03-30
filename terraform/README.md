# Terraform Configuration for Content Curator

TODO: create terraform.tfvars

This directory contains the Terraform configuration to set up the required AWS resources for the Content Curator application.

## Resources Created

- **S3 Bucket**: For storing Markdown files in organized directories
  - Server-side encryption enabled
  - Versioning enabled
  - Public access blocked

- **DynamoDB Table**: For storing metadata about content items
  - Partition key: `guid` (String)
  - Pay-per-request billing mode

## Prerequisites

- [Terraform](https://www.terraform.io/downloads) installed (version >= 1.0.0)
- AWS credentials configured via environment variables or AWS CLI

## Usage

1. Optionally customize the variables in `main.tf` or create a `terraform.tfvars` file:

   ```hcl
   aws_region          = "us-west-2"  # Change to your preferred region
   s3_bucket_name      = "your-custom-bucket-name"
   dynamodb_table_name = "your-custom-table-name"
   ```

2. Initialize Terraform:

   ```bash
   terraform init
   ```

3. Preview the changes:

   ```bash
   terraform plan
   ```

4. Apply the changes:

   ```bash
   terraform apply
   ```

5. After creation, update the `.env` file in your project root with the output values:

   ```
   AWS_S3_BUCKET_NAME=<output value from terraform>
   AWS_DYNAMODB_TABLE_NAME=<output value from terraform>
   AWS_REGION=<your region>
   ```

## Cleanup

To remove all created resources:

```bash
terraform destroy
```

⚠️ **Warning**: This will delete all content stored in the S3 bucket and all metadata stored in the DynamoDB table.

## Notes

- The S3 bucket name must be globally unique across all AWS accounts
- Make sure the AWS credentials used have permissions to create S3 buckets and DynamoDB tables 