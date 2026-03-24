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

# Strip leading ? from SAS token if present
SAS = AZURE_SAS_TOKEN.lstrip("?")

ADLS_BASE = (
    f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net"
    f"/{AZURE_CONTAINER}"
)


def s3_key_to_bronze_path(s3_key: str) -> str:
    """Map S3 key to ADLS bronze path — just prepend bronze/."""
    return f"bronze/{s3_key}"


def copy_s3_object_to_adls(s3_key: str) -> None:
    """Stream an S3 object directly to ADLS via the Blob REST API."""
    s3_client = boto3.client("s3")
    bronze_path = s3_key_to_bronze_path(s3_key)

    logger.info(
        f"Copying s3://{S3_BUCKET}/{s3_key} "
        f"-> adls://{AZURE_CONTAINER}/{bronze_path}"
    )

    # Stream from S3
    s3_response     = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
    data            = s3_response["Body"].read()
    content_length  = len(data)
    content_type    = s3_response.get("ContentType", "application/octet-stream")

    # Upload to ADLS via Blob REST API (Put Blob)
    url = f"{ADLS_BASE}/{bronze_path}?{SAS}"
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


def lambda_handler(event: dict, context) -> dict:
    """
    Entry point. Handles both direct S3 event notifications
    and EventBridge events wrapping S3 notifications.
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

    for record in records:
        s3_info = record.get("s3", {})
        bucket  = s3_info.get("bucket", {}).get("name", "")
        key     = s3_info.get("object", {}).get("key", "")

        if bucket != S3_BUCKET:
            logger.warning(f"Unexpected bucket {bucket!r} — skipping.")
            continue

        try:
            copy_s3_object_to_adls(key)
            copied.append(key)
        except Exception as exc:
            logger.error(f"Failed to copy {key!r}: {exc}", exc_info=True)
            failed.append(key)

    if failed:
        raise RuntimeError(f"Failed to copy {len(failed)} file(s): {failed}")

    return {
        "statusCode": 200,
        "body": f"Copied {len(copied)} file(s) to ADLS bronze layer.",
    }