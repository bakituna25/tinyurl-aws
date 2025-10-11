
This section outlines the project’s milestone-based progress and the quick verification tests that validate each phase.

### Status Overview
- ✅ **Milestone 1 — CI/CD Pipeline (MVP)**  
  - Automated GitHub Actions workflow (`sam build` + `sam deploy`) is fully functional — every push triggers a deployment.  
  - Result: `GET /hello` via API Gateway returns **200 OK**.

- ✅ **Milestone 2 — Foundational Shortening Service (MVP)**  
  - DynamoDB table: **url-mappings** (PK: `shortCode`)  
  - `POST /url` endpoint creates a short link (random Base62) and stores it in DynamoDB.  
  - Basic validations:  
    - `long_url` is required and must start with `http://` or `https://`  
    - (Optional) `custom_alias` must be 3–32 characters long, matching `[a-zA-Z0-9_-]`  
  - Example response: `{"short_code":"ABC1234","short_url":"ABC1234"}`

- ✅ **Milestone 3 — Redirection (MVP)**  
  - `GET /{shortCode}` → **302 Location** header pointing to the original URL  
  - If not found → **404 JSON:** `{"message":"Record not found"}`  
  - The `lastAccessed` field is automatically updated on each redirect.

- ⏳ **Milestone 4 — Caching with Redis (MMP)**  
  - Not yet implemented. Planned: cache-aside strategy (Redis → DynamoDB → Redis).

- ⏳ **Milestone 5 — Storage Tiering & Optimization (MMP)**  
  - Not yet implemented. Planned: archive infrequently accessed data to S3 and rehydrate on demand.

- ✅ **Milestone 6 — Custom URLs (MLP)**  
  - Supports `custom_alias` creation; returns **409 Conflict** if alias already exists.
