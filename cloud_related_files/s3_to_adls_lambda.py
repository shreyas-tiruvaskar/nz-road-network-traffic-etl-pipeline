"""
AWS Lambda — S3 to ADLS Bronze Ingestion
=========================================
Triggered by S3 ObjectCreated events (EventBridge or S3 notification).
Copies the new file to ADLS Gen2 (bronze layer) using the Azure Blob
REST API directly via requests — no azure-storage-blob SDK needed,
which avoids glibc/cffi compatibility issues on Lambda.

Environment variables (set in Lambda console, never hardcoded):
  AZURE_STORAGE_ACCOUNT  — e.g. nzetlpipeline
  AZURE_CONTAINER        — e.g. medallion
  AZURE_SAS_TOKEN        — SAS token with write access to bronze/
  S3_BUCKET              — source bucket name

Dependencies: boto3 (built into Lambda), requests (pure Python — no C extensions)
"""

import os
import logging
import boto3
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Config from environment ──────────────────────────────────────────────────
AZURE_STORAGE_ACCOUNT = os.environ["AZURE_STORAGE_ACCOUNT"]
AZURE_CONTAINER       = os.environ["AZURE_CONTAINER"]
AZURE_SAS_TOKEN       = os.environ["AZURE_SAS_TOKEN"]
S3_BUCKET             = os.environ["S3_BUCKET"]
DATABRICKS_HOST       = os.environ["DATABRICKS_HOST"].rstrip("/")
DATABRICKS_JOB_ID     = int(os.environ["DATABRICKS_JOB_ID"])
DATABRICKS_TOKEN      = os.environ["DATABRICKS_TOKEN"]

# Strip leading ? from SAS token if present
SAS = AZURE_SAS_TOKEN.lstrip("?")

ADLS_BASE = (
    f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net"
    f"/{AZURE_CONTAINER}"
)

# All four bronze files expected per pipeline run — only trigger Databricks
# once all four have landed in ADLS bronze
EXPECTED_FILES = {
    "osm/christchurch",        # matches road_segments + road_nodes
    "waka_kotahi",             # matches tms_counts + tms_sites
}


def s3_key_to_bronze_path(s3_key: str) -> str:
    return f"bronze/{s3_key}"


def extract_date_from_key(s3_key: str) -> str:
    """
    Extract the date partition from an S3 key.
    e.g. osm/christchurch/2026-03-25/road_segments.parquet -> 2026-03-25
         waka_kotahi/2026-03-25/tms_daily_traffic_counts.parquet -> 2026-03-25
    """
    parts = s3_key.split("/")
    for part in parts:
        if len(part) == 10 and part[4] == "-" and part[7] == "-":
            return part
    return None


def copy_s3_object_to_adls(s3_key: str) -> None:
    """Stream an S3 object directly to ADLS via the Blob REST API."""
    s3_client   = boto3.client("s3")
    bronze_path = s3_key_to_bronze_path(s3_key)

    logger.info(
        f"Copying s3://{S3_BUCKET}/{s3_key} "
        f"-> adls://{AZURE_CONTAINER}/{bronze_path}"
    )

    s3_response    = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
    data           = s3_response["Body"].read()
    content_length = len(data)
    content_type   = s3_response.get("ContentType", "application/octet-stream")

    url     = f"{ADLS_BASE}/{bronze_path}?{SAS}"
    headers = {
        "x-ms-blob-type": "BlockBlob",
        "Content-Type":   content_type,
        "Content-Length": str(content_length),
    }

    response = requests.put(url, data=data, headers=headers)

    if response.status_code not in (200, 201):
        raise RuntimeError(
            f"ADLS upload failed for {bronze_path}: "
            f"HTTP {response.status_code} — {response.text}"
        )

    logger.info(
        f"Successfully uploaded {content_length:,} bytes -> {bronze_path}"
    )


def trigger_databricks_job(bronze_date: str) -> None:
    """Trigger the Databricks silver→gold workflow with the bronze date."""
    url     = f"{DATABRICKS_HOST}/api/2.1/jobs/run-now"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type":  "application/json",
    }
    payload = {
        "job_id": DATABRICKS_JOB_ID,
        "notebook_params": {
            "bronze_date": bronze_date,
        },
    }

    response = requests.post(url, json=payload, headers=headers)

    if response.status_code == 200:
        run_id = response.json().get("run_id")
        logger.info(
            f"Databricks job triggered successfully. "
            f"run_id={run_id}, bronze_date={bronze_date}"
        )
    else:
        raise RuntimeError(
            f"Failed to trigger Databricks job: "
            f"HTTP {response.status_code} — {response.text}"
        )


def lambda_handler(event: dict, context) -> dict:
    """
    Entry point. Copies S3 objects to ADLS bronze, then triggers the
    Databricks silver→gold workflow once all files are copied.
    """
    records = event.get("Records", [])
    if not records:
        detail = event.get("detail", {})
        bucket = detail.get("bucket", {}).get("name")
        key    = detail.get("object", {}).get("key")
        if bucket and key:
            records = [{"s3": {"bucket": {"name": bucket}, "object": {"key": key}}}]

    if not records:
        logger.warning("No S3 records found in event — nothing to do.")
        return {"statusCode": 200, "body": "No records"}

    copied, failed = [], []
    bronze_date    = None

    for record in records:
        s3_info = record.get("s3", {})
        bucket  = s3_info.get("bucket", {}).get("name", "")
        key     = s3_info.get("object", {}).get("key", "")

        if bucket != S3_BUCKET:
            logger.warning(f"Unexpected bucket {bucket!r} — skipping.")
            continue

        # Extract date from the first file we process
        if bronze_date is None:
            bronze_date = extract_date_from_key(key)

        try:
            copy_s3_object_to_adls(key)
            copied.append(key)
        except Exception as exc:
            logger.error(f"Failed to copy {key!r}: {exc}", exc_info=True)
            failed.append(key)

    if failed:
        raise RuntimeError(f"Failed to copy {len(failed)} file(s): {failed}")

    # Only trigger Databricks job when the traffic counts file lands —
    # that's always the last file uploaded by upload_to_s3.py.
    # This avoids triggering 4 runs (one per file).
    TRIGGER_FILE = "tms_daily_traffic_counts.parquet"

    if copied and bronze_date:
        if any(TRIGGER_FILE in key for key in copied):
            logger.info(
                f"Trigger file detected ({TRIGGER_FILE}). "
                f"Firing Databricks job for bronze_date={bronze_date}"
            )
            try:
                trigger_databricks_job(bronze_date)
            except Exception as exc:
                logger.error(f"Databricks trigger failed: {exc}", exc_info=True)
        else:
            logger.info(
                f"Copied {copied} — not the trigger file, skipping Databricks job."
            )

    return {
        "statusCode": 200,
        "body": f"Copied {len(copied)} file(s) to ADLS bronze layer.",
    }