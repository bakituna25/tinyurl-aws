import os, json, time, re, secrets
import boto3
from botocore.exceptions import ClientError

# --- Redis opsiyonel: layer ekli ve env var varsa kullan ---
USE_REDIS = False
r = None
try:
    import redis
    if os.getenv("REDIS_HOST"):
        USE_REDIS = True
except Exception:
    pass

# ====== Env vars ======
TABLE_NAME   = os.environ.get("TABLE_NAME", "url-mappings")
BASE_URL     = (os.environ.get("BASE_URL") or "").rstrip("/")  # normalize
REDIS_HOST   = os.environ.get("REDIS_HOST")
REDIS_PORT   = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_SSL    = os.environ.get("REDIS_SSL", "false").lower() == "true"
CACHE_TTL    = int(os.environ.get("CACHE_TTL_SEC", "86400"))

ddb   = boto3.resource("dynamodb")
table = ddb.Table(TABLE_NAME)

if USE_REDIS:
    r = redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT, ssl=REDIS_SSL,
        socket_connect_timeout=1.0, socket_timeout=1.0, decode_responses=True
    )

SHORT_RE = re.compile(r"^[a-zA-Z0-9_-]{3,32}$")

# ---------- Helpers ----------
def _cors_headers():
    return {
        "content-type": "application/json",
        "access-control-allow-origin": "*",
        "access-control-allow-methods": "POST,OPTIONS",
        "access-control-allow-headers": "Content-Type,Authorization"
    }

def _random_code(n=7):
    alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    return "".join(secrets.choice(alphabet) for _ in range(n))

def _bad_request(msg):
    return {
        "statusCode": 400,
        "headers": _cors_headers(),
        "body": json.dumps({"message": msg})
    }

def _conflict(msg):
    return {
        "statusCode": 409,
        "headers": _cors_headers(),
        "body": json.dumps({"message": msg})
    }

def _server_error(msg):
    return {
        "statusCode": 500,
        "headers": _cors_headers(),
        "body": json.dumps({"message": msg})
    }

# ---------- Handler ----------
def lambda_handler(event, context):
    # Preflight CORS
    if (event.get("requestContext", {}).get("http", {}).get("method") == "OPTIONS" 
        or event.get("httpMethod") == "OPTIONS"):
        return {"statusCode": 204, "headers": _cors_headers(), "body": ""}

    try:
        body = event.get("body") or ""
        data = json.loads(body) if body else {}
    except Exception:
        return _bad_request("JSON body required")

    long_url = data.get("long_url") or data.get("longUrl")
    custom   = data.get("custom_alias") or data.get("customAlias")

    if not long_url or not isinstance(long_url, str):
        return _bad_request("long_url is required")

    if not long_url.startswith(("http://", "https://")):
        return _bad_request("long_url must start with http:// or https://")

    now_ms = int(time.time()*1000)

    # ---------- 1) custom_alias geldiyse ----------
    if custom:
        if not SHORT_RE.match(custom):
            return _bad_request("custom_alias must be 3-32 chars [a-zA-Z0-9_-]")
        try:
            table.put_item(
                Item={
                    "shortCode": custom,
                    "longUrl": long_url,
                    "createdAt": now_ms,
                    "lastAccessed": now_ms,
                },
                ConditionExpression="attribute_not_exists(shortCode)"
            )
            if USE_REDIS:
                try: r.setex(custom, CACHE_TTL, long_url)
                except Exception: pass
            short_url = f"{BASE_URL}/{custom}" if BASE_URL else custom
            return {
                "statusCode": 201,
                "headers": _cors_headers(),
                "body": json.dumps({"short_code": custom, "short_url": short_url})
            }
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ("ConditionalCheckFailedException",):
                return _conflict("custom_alias already taken")
            raise

    # ---------- 2) random kısa kod üret ----------
    for _ in range(6):
        code = _random_code(7)
        try:
            table.put_item(
                Item={
                    "shortCode": code,
                    "longUrl": long_url,
                    "createdAt": now_ms,
                    "lastAccessed": now_ms,
                },
                ConditionExpression="attribute_not_exists(shortCode)"
            )
            if USE_REDIS:
                try: r.setex(code, CACHE_TTL, long_url)
                except Exception: pass
            short_url = f"{BASE_URL}/{code}" if BASE_URL else code
            return {
                "statusCode": 201,
                "headers": _cors_headers(),
                "body": json.dumps({"short_code": code, "short_url": short_url})
            }
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                continue
            raise

    return _server_error("Could not allocate a unique code")
