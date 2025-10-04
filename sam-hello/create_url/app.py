import os, json, time, re, secrets
import boto3
from botocore.exceptions import ClientError

# --- Opsiyonel Redis (env varsa kullanır) ---
USE_REDIS = False
r = None
try:
    import redis  # type: ignore
    if os.getenv("REDIS_HOST"):
        USE_REDIS = True
except Exception:
    pass

TABLE_NAME = os.environ.get("TABLE_NAME", "url-mappings")
BASE_URL   = os.environ.get("BASE_URL")  # Varsa kısa URL'yi buna göre döndür
REDIS_HOST = os.environ.get("REDIS_HOST")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
CACHE_TTL  = int(os.environ.get("CACHE_TTL_SEC", "86400"))

ddb   = boto3.resource("dynamodb")
table = ddb.Table(TABLE_NAME)

if USE_REDIS:
    r = redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT,
        socket_connect_timeout=1.0, socket_timeout=1.0,
        decode_responses=True
    )

# custom alias validasyonu
SHORT_RE = re.compile(r"^[a-zA-Z0-9_-]{3,32}$")

# --------- Base62 yardımcıları ----------
_B62_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

def _b62_encode(n: int) -> str:
    if n == 0:
        return _B62_ALPHABET[0]
    s = []
    while n > 0:
        n, r = divmod(n, 62)
        s.append(_B62_ALPHABET[r])
    return "".join(reversed(s))

def _random_code(n: int = 7) -> str:
    """
    64-bit rastgele sayı üretip Base62'ye çevirir, sonra n karaktere uyarlar.
    62^7 ~ 3.5e12 kombinasyon -> çakışma olasılığı çok düşüktür.
    """
    x = secrets.randbits(64)
    c = _b62_encode(x)
    if len(c) < n:
        # eksikse rastgele Base62 karakterler ekle
        c += "".join(_B62_ALPHABET[secrets.randbelow(62)] for _ in range(n - len(c)))
    return c[:n]
# -----------------------------------------

def _bad(msg, code=400):
    return {
        "statusCode": code,
        "headers": {"content-type": "application/json"},
        "body": json.dumps({"message": msg})
    }

def lambda_handler(event, context):
    try:
        body = event.get("body") or ""
        data = json.loads(body) if body else {}
    except Exception:
        return _bad("JSON body required")

    long_url = data.get("long_url") or data.get("longUrl")
    custom   = data.get("custom_alias") or data.get("customAlias")

    if not long_url or not isinstance(long_url, str):
        return _bad("long_url is required")

    if not long_url.startswith(("http://", "https://")):
        return _bad("long_url must start with http:// or https://")

    now_ms = int(time.time() * 1000)

    # 1) custom alias istenmişse önce onu deneyelim
    if custom:
        if not SHORT_RE.match(custom):
            return _bad("custom_alias must be 3-32 chars [a-zA-Z0-9_-]")
        try:
            table.put_item(
                Item={
                    "shortCode": custom,
                    "longUrl": long_url,
                    "createdAt": now_ms,
                    "lastAccessed": now_ms
                },
                ConditionExpression="attribute_not_exists(shortCode)"
            )
            if USE_REDIS:
                try:
                    r.setex(custom, CACHE_TTL, long_url)
                except Exception:
                    pass
            short_url = f"{BASE_URL}/{custom}" if BASE_URL else custom
            return {
                "statusCode": 201,
                "headers": {"content-type": "application/json"},
                "body": json.dumps({"short_code": custom, "short_url": short_url})
            }
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                return _bad("custom_alias already taken", 409)
            raise

    # 2) random Base62 kısa kod (çakışma olursa tekrar dener)
    for _ in range(8):
        code = _random_code(7)
        try:
            table.put_item(
                Item={
                    "shortCode": code,
                    "longUrl": long_url,
                    "createdAt": now_ms,
                    "lastAccessed": now_ms
                },
                ConditionExpression="attribute_not_exists(shortCode)"
            )
            if USE_REDIS:
                try:
                    r.setex(code, CACHE_TTL, long_url)
                except Exception:
                    pass
            short_url = f"{BASE_URL}/{code}" if BASE_URL else code
            return {
                "statusCode": 201,
                "headers": {"content-type": "application/json"},
                "body": json.dumps({"short_code": code, "short_url": short_url})
            }
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                continue
            raise

    return _bad("Could not allocate a unique code", 500)
