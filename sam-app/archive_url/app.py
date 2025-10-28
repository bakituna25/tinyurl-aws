#!/usr/bin/env python3
"""
Archiver Lambda for TinyURL

Behavior:
 - Scans the DynamoDB table for items whose `lastAccessed` is older than cutoff_days.
 - Writes each item as JSON to S3 under key: archive/<prefix>/<shortCode>.json
 - If DRY_RUN=true -> it only writes to S3 (or just logs depending on DRY_RUN_MODE)
 - If DRY_RUN=false -> after successful S3 put it will conditionally delete the item from DynamoDB:
       Delete only if lastAccessed < cutoff_ts (protects from races)
 - Emits CloudWatch-friendly log lines and basic counts.

Environment variables:
 - TABLE_NAME (default: url-mappings)
 - ARCHIVE_BUCKET (required to write to S3)
 - CUTOFF_DAYS (default: 90)    # items with lastAccessed older than this will be archived
 - DRY_RUN (default: "true")    # "true" => do not delete from DDB; just write or simulate
 - BATCH_SIZE (default: 25)     # how many items to process per DynamoDB page
 - MAX_ITEMS (default: 500)     # max items to process per invocation (safety)
 - LOG_LEVEL (default: INFO)

IAM notes (needed for Lambda role):
 - dynamodb:Query, dynamodb:Scan, dynamodb:GetItem, dynamodb:DeleteItem on the table
 - s3:PutObject on the archive bucket
 - logs:CreateLogGroup/CreateLogStream/PutLogEvents (Lambda default)

Deploy and test:
 - First run with DRY_RUN=true, CUTOFF_DAYS small (e.g., 0 or 1) to create S3 objects without deleting DDB items.
 - Verify S3 keys and contents.
 - When comfortable, set DRY_RUN=false and run with low MAX_ITEMS to move items in small batches.
"""

import os
import json
import time
import math
import logging
import traceback
from typing import Dict, Any, Iterator, List

import boto3
from botocore.exceptions import ClientError, BotoCoreError
from boto3.dynamodb.conditions import Attr

# -----------------------
# Configuration via env
# -----------------------
TABLE_NAME = os.environ.get("TABLE_NAME", "url-mappings")
ARCHIVE_BUCKET = os.environ.get("ARCHIVE_BUCKET")  # REQUIRED
CUTOFF_DAYS = int(os.environ.get("CUTOFF_DAYS", "90"))
DRY_RUN = os.environ.get("DRY_RUN", "true").strip().lower() in ("1", "true", "yes", "on")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "25"))
MAX_ITEMS = int(os.environ.get("MAX_ITEMS", "500"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

# Safety checks
if not ARCHIVE_BUCKET:
    raise RuntimeError("ARCHIVE_BUCKET environment variable must be set")

# -----------------------
# Setup logging & clients
# -----------------------
logger = logging.getLogger("archiver")
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(handler)

dynamodb = boto3.resource("dynamodb")
ddb_table = dynamodb.Table(TABLE_NAME)
s3 = boto3.client("s3")

# -----------------------
# Helpers
# -----------------------
def _now_ms() -> int:
    return int(time.time() * 1000)

def _cutoff_ts_ms(cutoff_days: int) -> int:
    return int((time.time() - cutoff_days * 86400) * 1000)

def _archive_key(short_code: str) -> str:
    # prefix by first two chars to reduce list-bucket hotspots
    prefix = (short_code[:2] if len(short_code) >= 2 else "_")
    return f"archive/{prefix}/{short_code}.json"

def _safe_json(item: Dict[str, Any]) -> str:
    # Convert DynamoDB attribute values (if passed raw) into plain json-friendly dict.
    # Expecting item already in normal form (from Scan/Query Items) — but ensure serializable.
    return json.dumps(item, default=str, ensure_ascii=False)

def _put_s3_with_retry(bucket: str, key: str, body: bytes, max_attempts: int = 3, base_sleep: float = 0.5):
    attempt = 0
    while True:
        attempt += 1
        try:
            s3.put_object(Bucket=bucket, Key=key, Body=body)
            return
        except (ClientError, BotoCoreError) as e:
            logger.warning("S3 put_object failed (attempt %d/%d) key=%s: %s", attempt, max_attempts, key, e)
            if attempt >= max_attempts:
                logger.error("S3 put_object final failure key=%s: %s", key, traceback.format_exc())
                raise
            sleep = base_sleep * (2 ** (attempt - 1))
            time.sleep(sleep)

def _delete_ddb_conditional(short_code: str, cutoff_ts: int):
    # Delete only if lastAccessed still < cutoff_ts
    try:
        ddb_table.delete_item(
            Key={"shortCode": short_code},
            ConditionExpression=Attr("lastAccessed").lt(cutoff_ts)
        )
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("ConditionalCheckFailedException", "ConditionalCheckFailed"):
            # someone updated lastAccessed -> do not delete
            logger.info("Skipping delete for %s: condition failed (recent access)", short_code)
            return False
        else:
            logger.error("DDB delete_item error for %s: %s", short_code, traceback.format_exc())
            raise

def _scan_old_items(cutoff_ts: int, page_size: int) -> Iterator[List[Dict[str, Any]]]:
    """
    Scan DynamoDB for items with lastAccessed < cutoff_ts. Yields pages of items (list).
    Uses pagination and a filter expression.
    """
    logger.info("Starting scan for items older than cutoff_ts=%d (page=%d)", cutoff_ts, page_size)
    # Use Scan with FilterExpression - note: Scan can be expensive — we rely on small pages and occasional runs.
    kwargs = {
        "FilterExpression": Attr("lastAccessed").lt(cutoff_ts),
        "ProjectionExpression": "shortCode, longUrl, createdAt, lastAccessed",
        "Limit": page_size
    }

    done = False
    last_evaluated_key = None
    attempts = 0
    while not done:
        if last_evaluated_key:
            kwargs["ExclusiveStartKey"] = last_evaluated_key
        try:
            resp = ddb_table.scan(**kwargs)
        except (ClientError, BotoCoreError) as e:
            attempts += 1
            logger.error("DDB scan error attempt %d: %s", attempts, e)
            if attempts > 3:
                raise
            time.sleep(0.5 * attempts)
            continue

        items = resp.get("Items", [])
        if items:
            yield items

        last_evaluated_key = resp.get("LastEvaluatedKey")
        if not last_evaluated_key:
            done = True

# -----------------------
# Main handler
# -----------------------
def lambda_handler(event, context):
    t_start = time.time()
    invocation_id = getattr(context, "aws_request_id", "-")
    logger.info("Archiver invocation id=%s, DRY_RUN=%s, CUTOFF_DAYS=%d", invocation_id, DRY_RUN, CUTOFF_DAYS)

    cutoff_ts = _cutoff_ts_ms(CUTOFF_DAYS)
    processed = 0
    archived = 0
    deleted = 0
    errors = 0

    try:
        # iterate pages
        for page in _scan_old_items(cutoff_ts=cutoff_ts, page_size=BATCH_SIZE):
            if processed >= MAX_ITEMS:
                logger.info("Reached MAX_ITEMS (%d). Stopping scan for this invocation.", MAX_ITEMS)
                break

            for item in page:
                if processed >= MAX_ITEMS:
                    break
                processed += 1
                short_code = item.get("shortCode")
                if not short_code:
                    logger.warning("Skipping item without shortCode: %s", item)
                    continue

                key = _archive_key(short_code)
                body = _safe_json(item).encode("utf-8")

                try:
                    # write to s3 (retry)
                    _put_s3_with_retry(ARCHIVE_BUCKET, key, body)
                    archived += 1
                    logger.info("Archived %s -> s3://%s/%s", short_code, ARCHIVE_BUCKET, key)
                except Exception:
                    errors += 1
                    logger.exception("Failed to put %s to S3", short_code)
                    continue

                # if not dry-run, attempt conditional delete
                if not DRY_RUN:
                    try:
                        ok = _delete_ddb_conditional(short_code, cutoff_ts)
                        if ok:
                            deleted += 1
                        else:
                            # not deleted because lastAccessed updated
                            pass
                    except Exception:
                        errors += 1
                        logger.exception("Failed deleting item %s from DDB", short_code)

            # loop end for page

        elapsed = time.time() - t_start
        logger.info("Archiver finished: processed=%d archived=%d deleted=%d errors=%d elapsed=%.2fs",
                    processed, archived, deleted, errors, elapsed)

        # Return structured result which is useful when testing
        return {
            "statusCode": 200,
            "body": json.dumps({
                "processed": processed,
                "archived": archived,
                "deleted": deleted,
                "errors": errors,
                "dry_run": DRY_RUN,
                "cutoff_ts": cutoff_ts
            })
        }

    except Exception:
        logger.exception("Archiver fatal error")
        return {"statusCode": 500, "body": json.dumps({"message": "archiver error"})}
