# AWS S3 → ADLS Bronze Ingestion

This folder contains the AWS-side ingestion layer for the Road Network ETL Pipeline.

## Architecture

```
[Local / GitHub Actions]          [AWS ap-southeast-2]           [Azure]
  upload_to_s3.py           →    S3 raw landing bucket
  (OSM + Waka Kotahi data)            ↓ EventBridge
                                  Lambda function          →    ADLS Gen2
                                  s3_to_adls_lambda.py          bronze/ layer
                                                                    ↓
                                                           Databricks silver/gold
```

## Files

| File | Purpose |
|------|---------|
| `upload_to_s3.py` | Fetches OSM + Waka Kotahi data and uploads to S3 (run locally or in CI) |
| `s3_to_adls_lambda.py` | Lambda triggered on S3 ObjectCreated → streams file to ADLS bronze |
| `s3_trigger_setup.tf` | Terraform: S3 bucket + EventBridge rule + Lambda + IAM |
| `build_lambda.sh` | Packages Lambda + dependencies into `lambda_package.zip` |

## Setup steps

### 1. Azure — generate a SAS token for ADLS
In the Azure Portal:
- Storage account `nzetlpipeline` → **Shared access signature**
- Allowed services: Blob
- Allowed resource types: Object, Container
- Permissions: Write, Create
- Expiry: set to your project duration
- Copy the **SAS token** (starts with `?sv=...`)

### 2. AWS — deploy Lambda + S3 via Terraform
```bash
# Set credentials
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=ap-southeast-2

# Build Lambda package
./build_lambda.sh

# Deploy infrastructure
terraform init
terraform apply -var="azure_sas_token=<your-sas-token>"
```

### 3. Databricks — set up Secret Scope (if not done already)
```bash
databricks secrets create-scope --scope adls
databricks secrets put-secret --scope adls --key storage-account-key
# paste your ADLS storage account key (not the SAS token — this is for Spark)
```

### 4. Run the ingestion
```bash
export S3_BUCKET=nz-road-pipeline-raw
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...

python upload_to_s3.py --date 2026-03-20
```

This will:
1. Download OSM road data for Christchurch via `osmnx`
2. Download Waka Kotahi TMS counts + sites
3. Upload both as Parquet files to S3
4. EventBridge detects the new objects and invokes Lambda
5. Lambda streams each file from S3 to ADLS `bronze/`
6. Your existing Databricks silver/gold notebooks read from `bronze/` as normal

## S3 folder structure
```
s3://nz-road-pipeline-raw/
  osm/christchurch/2026-03-20/
    road_segments.parquet
    road_nodes.parquet
  waka_kotahi/2026-03-20/
    tms_daily_traffic_counts.parquet
    tms_monitoring_sites.parquet
```

These map 1:1 to ADLS `bronze/` paths — the Lambda just prepends `bronze/`.

## Security notes
- The SAS token is passed to Lambda via an environment variable set in Terraform
- For production, move it to **AWS Secrets Manager** and fetch at runtime
- The ADLS storage key used by Databricks is stored in a **Databricks Secret Scope**, never in notebook code
- Never commit `.tfvars` files containing secrets to Git — add them to `.gitignore`
