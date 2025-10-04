import os
import json
import time
import traceback

import boto3
from botocore.exceptions import ClientError

# ====== Env vars ======
TABLE_NAME     = os.environ.get("TABLE_NAME", "url-mappings")
ARCHIVE_BUCKET = os.environ.get("ARCHIVE_BUCKET")  # opsiyonel
REDIS_HOST     = os.environ.get("REDIS_HOST")      # opsiyonel
REDIS_PORT     = int(os.environ.get("REDIS_PORT", "6379"))
CACHE_TTL_SEC  = int(os.environ.get("CACHE_TTL_SEC", "86400"))
REDIS_SSL_ENV  = os.environ.get("REDIS_SSL")       # "true"/"false"/None

def _bool_from_env(v):
    if v is None:
        return None
    return str(v).strip().lower() in ("1", "true", "yes", "on")

REDIS_SSL = _bool_from_env(REDIS_SSL_ENV)
if REDIS_SSL is None:
    REDIS_SSL = (REDIS_PORT == 6380)

# ====== Clients ======
ddb   = boto3.resource("dynamodb")
table = ddb.Table(TABLE_NAME)
s3    = boto3.client("s3")

# Redis (opsiyonel)
USE_REDIS = False
r = None
try:
    import redis  # type: ignore
    if REDIS_HOST:
        r = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            ssl=REDIS_SSL,
            socket_connect_timeout=1.0,
            socket_timeout=1.0,
            decode_responses=True,
        )
        USE_REDIS = True
except Exception:
    # redis paketi yoksa ya da host verilmemişse sessizce Redis'i pas geç
    r = None
    USE_REDIS = False

def _archive_key(short_code: str) -> str:
    p = short_code[:2] if len(short_code) >= 2 else "_"
    return f"archive/{p}/{short_code}.json"

def lambda_handler(event, context):
    t0 = time.perf_counter()
    req_id = getattr(context, "aws_request_id", "-")
    print(f"[INVOCATION] req_id={req_id}")
    print(f"[EVENT] pathParameters={(event or {}).get('pathParameters')}")

    short_code = ((event or {}).get("pathParameters") or {}).get("shortCode")
    if not short_code:
        print("[ERROR] shortCode missing in pathParameters")
        return {
            "statusCode": 400,
            "headers": {"content-type": "application/json"},
            "body": json.dumps({"message": "shortCode parametresi gerekli"})
        }

    # ---------- 1) REDIS ----------
    if USE_REDIS and r is not None:
        try:
            t = time.perf_counter()
            url = r.get(short_code)
            dt = (time.perf_counter() - t) * 1000
            if url:
                print(f"[HIT:REDIS] code={short_code} -> {url} (redis {dt:.2f} ms)")
                print(f"[TOTAL] {(time.perf_counter()-t0)*1000:.2f} ms")
                return {"statusCode": 302, "headers": {"Location": url}, "body": ""}
            else:
                print(f"[MISS:REDIS] code={short_code} (redis {dt:.2f} ms)")
        except Exception as e:
            print(f"[REDIS:ERROR] {type(e).__name__}: {e}")
            print(traceback.format_exc())

    # ---------- 2) DYNAMODB ----------
    try:
        t = time.perf_counter()
        resp = table.get_item(Key={"shortCode": short_code})
        dt = (time.perf_counter() - t) * 1000
        item = resp.get("Item")
        if item:
            url = item["longUrl"]
            print(f"[HIT:DDB] code={short_code} -> {url} (ddb get {dt:.2f} ms)")

            # lastAccessed (best-effort)
            try:
                t2 = time.perf_counter()
                table.update_item(
                    Key={"shortCode": short_code},
                    UpdateExpression="SET lastAccessed = :ts",
                    ExpressionAttributeValues={":ts": int(time.time() * 1000)},
                )
                print(f"[DDB:update] lastAccessed ok ({(time.perf_counter()-t2)*1000:.2f} ms)")
            except Exception as e:
                print(f"[DDB:update:WARN] {e}")

            # Redis'e yaz (best-effort)
            if USE_REDIS and r is not None:
                try:
                    t3 = time.perf_counter()
                    r.setex(short_code, CACHE_TTL_SEC, url)
                    print(f"[CACHE:REDIS:setex] ttl={CACHE_TTL_SEC}s ({(time.perf_counter()-t3)*1000:.2f} ms)")
                except Exception as e:
                    print(f"[CACHE:REDIS:WARN] {e}")

            print(f"[TOTAL] {(time.perf_counter()-t0)*1000:.2f} ms")
            return {"statusCode": 302, "headers": {"Location": url}, "body": ""}
        else:
            print(f"[MISS:DDB] code={short_code} (ddb get {dt:.2f} ms)")
    except Exception as e:
        print(f"[DDB:ERROR] {type(e).__name__}: {e}")
        print(traceback.format_exc())

    # ---------- 3) S3 ARCHIVE (opsiyonel) ----------
    if ARCHIVE_BUCKET:
        key = _archive_key(short_code)
        try:
            t = time.perf_counter()
            obj = s3.get_object(Bucket=ARCHIVE_BUCKET, Key=key)
            dt = (time.perf_counter() - t) * 1000
            payload = json.loads(obj["Body"].read())
            url = payload["longUrl"]
            print(f"[HIT:S3] s3://{ARCHIVE_BUCKET}/{key} -> {url} (s3 {dt:.2f} ms)")

            # Rehydrate to DDB (best-effort)
            try:
                now = int(time.time() * 1000)
                table.put_item(
                    Item={
                        "shortCode": short_code,
                        "longUrl": url,
                        "createdAt": int(payload.get("createdAt", now)),
                        "lastAccessed": now,
                    },
                    ConditionExpression="attribute_not_exists(shortCode)",
                )
                print("[REHYDRATE:DDB] ok")
            except Exception as e:
                print(f"[REHYDRATE:DDB:WARN] {e}")

            # Redis’e yaz (best-effort)
            if USE_REDIS and r is not None:
                try:
                    r.setex(short_code, CACHE_TTL_SEC, url)
                    print(f"[CACHE:REDIS:setex] from S3 ttl={CACHE_TTL_SEC}s")
                except Exception as e:
                    print(f"[CACHE:REDIS:WARN] {e}")

            print(f"[TOTAL] {(time.perf_counter()-t0)*1000:.2f} ms")
            return {"statusCode": 302, "headers": {"Location": url}, "body": ""}
        except s3.exceptions.NoSuchKey:
            print(f"[MISS:S3] no key s3://{ARCHIVE_BUCKET}/{key}")
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            print(f"[S3:ERROR] {code} on s3://{ARCHIVE_BUCKET}/{key}")
        except Exception as e:
            print(f"[S3:ERROR] {type(e).__name__}: {e}")

    # ---------- 4) NOT FOUND ----------
    print(f"[MISS:ALL] code={short_code}")
    print(f"[TOTAL] {(time.perf_counter()-t0)*1000:.2f} ms")
    return {
        "statusCode": 404,
        "headers": {"content-type": "application/json"},
        "body": json.dumps({"message": "Kayıt bulunamadı"}),
    }
