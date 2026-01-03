"""
AWS Lambda handlers for image processing pipeline.

This module contains Lambda functions for:
1. Ingesting S3 upload events and queueing them to SQS
2. Extracting image metadata and storing as JSON in S3
"""

import json
import os
import io
import urllib.parse
import logging
from typing import Dict, Any
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError
from PIL import Image
from PIL.ExifTags import TAGS

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3 = boto3.client("s3")

# Environment variables with defaults
INPUT_PREFIX = os.environ.get("INPUT_PREFIX", "incoming/")
METADATA_PREFIX = os.environ.get("METADATA_PREFIX", "metadata/")
SUPPORTED_EXTENSIONS = (".jpg", ".jpeg", ".png")


def is_valid_image(key: str) -> bool:
    """
    Validate if the file is a supported image type.
    Args:
        key: S3 object key
    Returns:
        bool: True if file has a supported image extension
    """
    return key.lower().endswith(SUPPORTED_EXTENSIONS)


def send_sqs_message(sqs_client, queue_url: str, message: Dict[str, Any]) -> None:
    """Send a message to SQS queue..."""
    response = sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(message)
    )
    logger.info(f"Message sent to SQS. MessageId: {response.get('MessageId')}")


def ingest_lambda_handler(event, context):
    """S3 → SQS: Validate and queue image uploads..."""
    sqs = boto3.client("sqs")
    queue_url = os.environ["QUEUE_URL"]

    processed = skipped = errors = 0

    for record in event.get("Records", []):
        try:
            bucket = record["s3"]["bucket"]["name"]
            key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
            etag = record["s3"]["object"]["eTag"]

            # Skip non-incoming files
            if not key.startswith(INPUT_PREFIX) or not is_valid_image(key):
                skipped += 1
                continue

            # Send to SQS
            message = {"bucket": bucket, "key": key, "etag": etag}
            send_sqs_message(sqs, queue_url, message)
            processed += 1

        except Exception as e:
            logger.error(f"Error processing record: {e}")
            errors += 1

    logger.info(f"Ingestion: processed={processed}, skipped={skipped}, errors={errors}")

    return {
        "status": "ok" if errors == 0 else "partial_failure",
        "processed": processed,
        "skipped": skipped,
        "errors": errors
    }



def metadata_exists(bucket: str, key: str) -> bool:
    """Check if metadata file already exists (idempotency)..."""

    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        return e.response['Error']['Code'] != '404'


def extract_image_metadata(image: Image.Image, image_data: bytes) -> Dict[str, Any]:
    """Extract metadata from an image..."""
    metadata = {
        "format": image.format,
        "width": image.width,
        "height": image.height,
        "file_size_bytes": len(image_data),
    }

    # EXIF data if available
    try:
        exif_data = image.getexif()
        if exif_data:
            metadata["exif"] = {TAGS.get(tag, tag): str(value) for tag, value in exif_data.items()}
    except Exception as e:
        logger.warning(f"Could not extract EXIF: {e}")

    return metadata


def metadata_extraction_lambda_handler(event, context):
    """SQS → S3: Extract and store image metadata..."""
    if not event.get('Records'):
        return {"status": "ok", "processed": 0, "skipped": 0, "errors": 0}

    processed = skipped = errors = 0

    for record in event.get("Records", []):
        try:
            # Parse message
            msg = json.loads(record["body"])
            bucket, key = msg["bucket"], msg["key"]

            # Check idempotency
            metadata_key = f"{METADATA_PREFIX}{os.path.splitext(os.path.basename(key))[0]}.json"
            if metadata_exists(bucket, metadata_key):
                logger.info(f"Metadata exists: {metadata_key}, skipping")
                skipped += 1
                continue

            # Download and process image
            image_data = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            image = Image.open(io.BytesIO(image_data))
            metadata = extract_image_metadata(image, image_data)

            # Upload metadata
            s3.put_object(
                Bucket=bucket,
                Key=metadata_key,
                Body=json.dumps(metadata, indent=2),
                ContentType="application/json"
            )
            logger.info(f"Uploaded: {metadata_key}")
            processed += 1

        except Exception as e:
            logger.error(f"Error processing record: {e}")
            errors += 1

    logger.info(f"Metadata extraction: processed={processed}, skipped={skipped}, errors={errors}")
    return {
        "status": "ok" if errors == 0 else "partial_failure",
        "processed": processed,
        "skipped": skipped,
        "errors": errors
    }
