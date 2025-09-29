import os
import json
import time
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError

TABLE_NAME      = os.getenv("TABLE_NAME", "url-mappings")
ARCHIVE_BUCKET  = os.environ["ARCHIVE_BUCKET"]  # zorunlu
OLDER_THAN_DAYS = int(os.getenv("OLDER_THAN_DAYS", "120"))
MAX_ITEMS       = int(os.getenv("MAX_ITEMS", "1000"))
DRY_RUN         = os.getenv("DRY_RUN", "false").lower() == "true"

ddb   = boto3.resource("dynamodb")
table = ddb.Table(TABLE_NAME)
s3    = boto3.client("s3")

def _archive_key(short_code: str) -> str:
    p = short_code[:2] if len(short_code) >= 2 else "_"
    return f"archive/{p}/{short_code}.json"

def lambda_handler(event, context):
    now_ms = int(time.time() * 1000)
    cutoff_ms = now_ms - OLDER_THAN_DAYS * 24 * 60 * 60 * 1000

    print(f"[CONFIG] TABLE_NAME={TABLE_NAME} BUCKET={ARCHIVE_BUCKET} "
          f"OLDER_THAN_DAYS={OLDER_THAN_DAYS} MAX_ITEMS={MAX_ITEMS} DRY_RUN={DRY_RUN}")
    print(f"[INFO] cutoff_ms={cutoff_ms} ({datetime.now(timezone.utc) - timedelta(days=OLDER_THAN_DAYS)})")

    moved = 0
    last_evaluated_key = None

    while moved < MAX_ITEMS:
        scan_kwargs = {
            # Filtre: lastAccessed <= cutoff_ms
            "FilterExpression": "attribute_exists(lastAccessed) AND lastAccessed <= :cut",
            "ExpressionAttributeValues": {":cut": cutoff_ms},
            # Okunacak alanları kısıtlamak istersen:
            # "ProjectionExpression": "shortCode,longUrl,createdAt,lastAccessed"
        }
        # >>> HATA KAYNAĞI DÜZELTİLDİ: Sadece varsa gönder <<<
        if last_evaluated_key:
            scan_kwargs["ExclusiveStartKey"] = last_evaluated_key

        page = table.scan(**scan_kwargs)
        items = page.get("Items", [])
        last_evaluated_key = page.get("LastEvaluatedKey")

        print(f"[SCAN] fetched={len(items)} moved_so_far={moved} has_more={bool(last_evaluated_key)}")

        if not items:
            # Sayfada yoksa ve devam anahtarı da yoksa bitti
            if not last_evaluated_key:
                break
        for it in items:
            if moved >= MAX_ITEMS:
                break

            code = it.get("shortCode")
            long_url = it.get("longUrl")
            created_at = int(it.get("createdAt", 0)) if it.get("createdAt") is not None else 0
            last_accessed = int(it.get("lastAccessed", 0)) if it.get("lastAccessed") is not None else 0

            if not code or not long_url:
                print(f"[SKIP] invalid item: {it}")
                continue

            payload = {
                "shortCode": code,
                "longUrl": long_url,
                "createdAt": created_at,
                "lastAccessed": last_accessed,
            }
            key = _archive_key(code)

            print(f"[MOVE] {code} -> s3://{ARCHIVE_BUCKET}/{key}")

            if not DRY_RUN:
                # 1) S3’e yaz
                s3.put_object(
                    Bucket=ARCHIVE_BUCKET,
                    Key=key,
                    Body=json.dumps(payload).encode("utf-8"),
                    ContentType="application/json",
                )
                # 2) DDB’den sil
                table.delete_item(Key={"shortCode": code})

            moved += 1

        # Devam edilecek sayfa yoksa döngüden çık
        if not last_evaluated_key:
            break

    print(f"[RESULT] moved={moved} (limit={MAX_ITEMS}) cutoff_days={OLDER_THAN_DAYS}")
    return {
        "statusCode": 200,
        "body": json.dumps({"moved": moved, "limit": MAX_ITEMS, "cutoff_days": OLDER_THAN_DAYS})
    }
