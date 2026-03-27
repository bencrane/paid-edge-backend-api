# Creative Engine X API — Developer Guide

## Authentication

All API requests require a JWT Bearer token from Supabase Auth.

```bash
# Sign up
curl -X POST https://api.example.com/auth/signup \
  -H "Content-Type: application/json" \
  -d '{"email": "dev@example.com", "password": "your-password", "full_name": "Dev User"}'

# Login — returns access_token and refresh_token
curl -X POST https://api.example.com/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email": "dev@example.com", "password": "your-password"}'
```

Use the `access_token` in all subsequent requests:

```bash
curl -H "Authorization: Bearer <access_token>" \
     -H "X-Organization-Id: <org_id>" \
     https://api.example.com/assets/generate
```

The `X-Organization-Id` header selects which organization context to use. If omitted, defaults to the user's first organization.

---

## Quick Start

### 1. Create an organization

```bash
curl -X POST https://api.example.com/orgs \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"name": "My Company"}'
```

### 2. Create a campaign

```bash
curl -X POST https://api.example.com/campaigns \
  -H "Authorization: Bearer <token>" \
  -H "X-Organization-Id: <org_id>" \
  -H "Content-Type: application/json" \
  -d '{"name": "Q1 Launch", "objective": "lead_generation"}'
```

### 3. Generate creative assets

```bash
curl -X POST https://api.example.com/assets/generate \
  -H "Authorization: Bearer <token>" \
  -H "X-Organization-Id: <org_id>" \
  -H "Content-Type: application/json" \
  -d '{
    "campaign_id": "<campaign_id>",
    "asset_types": ["ad_copy", "lead_magnet"],
    "platforms": ["linkedin", "meta"],
    "angle": "pain-point-focused",
    "tone": "professional",
    "cta": "Download the Guide"
  }'
```

---

## Asset Types

### Renderable (produce PDF/HTML files)

| Type | Description | Key Options |
|------|-------------|-------------|
| `lead_magnet` | Multi-page PDFs (checklists, guides, reports) | `lead_magnet_format`: checklist, ultimate_guide, benchmark_report, template_toolkit, state_of_industry |
| `document_ad` | Carousel/document ads for LinkedIn/Meta | `document_ad_pattern`: problem_solution, before_after, step_by_step |
| `landing_page` | Hosted landing pages with form capture | `landing_page_template`: lead_magnet_download, demo_request, webinar_registration, free_trial |
| `case_study_page` | Customer success story pages | — |

### Text-only (return JSON content)

| Type | Description | Key Options |
|------|-------------|-------------|
| `ad_copy` | Platform-specific ad copy with headlines, descriptions, CTAs | `platforms`: linkedin, meta, google |
| `email_copy` | Multi-email sequences | `email_trigger`: lead_magnet_download, demo_request, trial_signup |
| `video_script` | Video ad scripts with scenes and timing | `video_duration`: 15s, 30s, 60s |
| `image_brief` | Creative briefs for image production | `platforms`: linkedin_sponsored, meta_feed |

---

## Rate Limits

All authenticated requests are rate-limited per user:

- **Default**: 60 requests per minute
- **429 Too Many Requests** returned when exceeded with `Retry-After` header

Every response includes rate limit headers:

```
X-RateLimit-Limit: 60
X-RateLimit-Remaining: 45
X-RateLimit-Reset: 1711500000
```

---

## Error Handling

All errors return a consistent JSON shape:

```json
{
  "error": {
    "type": "validation_error",
    "message": "Human-readable description",
    "details": { ... },
    "request_id": "req_a1b2c3d4e5f67890"
  }
}
```

### Error Types

| Type | HTTP Status | Description |
|------|-------------|-------------|
| `validation_error` | 422 | Invalid request schema |
| `not_found` | 404 | Resource not found |
| `auth_error` | 401 | Invalid or missing token |
| `forbidden` | 403 | Insufficient permissions |
| `rate_limit_exceeded` | 429 | Rate limit exceeded |
| `generation_error` | 500 | AI generation failure |
| `render_error` | 500 | Asset rendering failure |
| `provider_error` | 502 | External provider failure |
| `internal_error` | 500 | Unexpected server error |

The `request_id` field is included in every error response and response header (`X-Request-Id`) for debugging.

---

## Webhooks

Webhook payloads are signed with HMAC-SHA256. Verify the signature to ensure authenticity:

```python
import hmac
import hashlib

def verify_webhook(payload_bytes: bytes, signature_header: str, secret: str) -> bool:
    # signature_header format: "sha256=<hex_digest>"
    expected = hmac.new(secret.encode(), payload_bytes, hashlib.sha256).hexdigest()
    received = signature_header.removeprefix("sha256=")
    return hmac.compare_digest(expected, received)
```

### Webhook Headers

| Header | Description |
|--------|-------------|
| `X-CEX-Signature` | `sha256=<HMAC-SHA256 hex digest>` |
| `X-CEX-Event` | Event type (e.g., `job.completed`) |
| `X-CEX-Delivery-Id` | Unique delivery attempt ID |
| `X-CEX-Timestamp` | Unix epoch timestamp |

Failed deliveries are retried up to 3 times with exponential backoff (10s, 60s, 300s).

---

## Usage Tracking

Track API usage per organization:

```bash
curl -H "Authorization: Bearer <token>" \
     -H "X-Organization-Id: <org_id>" \
     "https://api.example.com/usage?start_date=2025-01-01&end_date=2025-01-31&asset_type=ad_copy"
```

Response includes a summary and event list:

```json
{
  "summary": {
    "total_generations": 100,
    "successful": 95,
    "failed": 5,
    "total_tokens_input": 50000,
    "total_tokens_output": 30000
  },
  "events": [...]
}
```

---

## Health Checks

| Endpoint | Purpose | Auth Required |
|----------|---------|---------------|
| `GET /health` | Basic health check | No |
| `GET /health/live` | Liveness probe (always 200) | No |
| `GET /health/ready` | Readiness probe (checks DB + Claude API) | No |

`/health/ready` returns 503 if any dependency is unreachable:

```json
{
  "status": "degraded",
  "checks": {
    "database": {"status": "ok", "latency_ms": 42},
    "claude_api": {"status": "error", "latency_ms": 5000, "error": "timeout"}
  }
}
```

---

## API Reference

Interactive API documentation is available at:

- **Swagger UI**: `/docs`
- **ReDoc**: `/redoc`
- **OpenAPI JSON**: `/openapi.json`
