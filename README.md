This section outlines the project's milestone-based progress and the quick verification tests that validate each phase.

### Status Overview
- ✅ **Milestone 1 — CI/CD Pipeline (MVP)**  
  - GitHub Actions workflow (`sam build` + `sam deploy`) deploys the SAM stack to AWS on every push to `main`.  
  - Stack outputs (API base URL, archiver function name) are printed at the end of each run.

- ✅ **Milestone 2 — Foundational Shortening Service (MVP)**  
  - DynamoDB table: **url-mappings** (PK: `shortCode`)  
  - `POST /url` creates a short link and stores it in DynamoDB.  
  - Short codes are 7-character Base62 strings generated from 64-bit random values; uniqueness is guaranteed with a conditional write (`attribute_not_exists`) and up to 8 retries on collision.  
  - Basic validations:  
    - `long_url` is required and must start with `http://` or `https://`  
    - (Optional) `custom_alias` must be 3–32 characters long, matching `[a-zA-Z0-9_-]`  
  - Example response: `{"short_code":"ABC1234","short_url":"ABC1234"}`

- ✅ **Milestone 3 — Redirection (MVP)**  
  - `GET /{shortCode}` → **302 Location** header pointing to the original URL  
  - If the code is not found in any layer → **404** with a JSON error message  
  - The `lastAccessed` field is updated on each redirect.

- ✅ **Milestone 4 — Caching with Redis (MMP)**  
  - **Implemented** a cache-aside read path in the redirect Lambda: **Redis → DynamoDB → S3 archive**. On a DynamoDB or S3 hit, the URL is written back to Redis with a configurable TTL (`CACHE_TTL_SEC`, default 24h). Newly created links are cached on creation.  
  - Caching is controlled by configuration (`REDIS_HOST`, `REDIS_PORT`, `REDIS_SSL`). If Redis is not configured or unreachable (1 s timeouts), requests fall back to DynamoDB, so the cache can never break redirects.  
  - The ElastiCache cluster is **kept disabled by default** after weighing idle cluster cost against current traffic; it can be re-enabled by setting `REDIS_HOST`, with no code changes.  
  - Each layer logs its hit/miss and latency to CloudWatch (`[HIT:REDIS]`, `[HIT:DDB]`, `[HIT:S3]`).

- ✅ **Milestone 5 — Storage Tiering & Optimization (MMP)**  
  - A Lambda scheduled daily with EventBridge archives links not accessed for 90+ days (`CUTOFF_DAYS`) to S3.  
  - DynamoDB items are removed with a conditional delete (`lastAccessed < cutoff`), so a link accessed during archiving is never lost.  
  - Archived links are re-hydrated back into DynamoDB on their next access.  
  - Safe by default: `DRY_RUN` mode, batch size and per-run item limits are configurable.

- ✅ **Milestone 6 — Custom URLs (MLP)**  
  - Supports `custom_alias` creation; returns **409 Conflict** if the alias already exists.
