# Phase 7: Production Readiness

**Version:** v0.7.0.0
**Date:** 2026-03-27
**Branch:** `bencrane/linear-access-check`
**PR:** [#21](https://github.com/bencrane/paid-engine-x-api/pull/21) (merged to `main`)
**Linear Issues:** CEX-39 through CEX-45

---

## Overview

Phase 7 hardened the Creative Engine X API for production deployment. Seven issues were implemented covering rate limiting, usage tracking, webhook reliability, structured error handling, monitoring, API documentation, and CI/CD with Railway deployment configuration.

All 7 commits map 1:1 to Linear issues. The phase introduced **55 new tests** (1,080 lines of test code) with a 100% pass rate.

---

## Issues Implemented

### CEX-39: Rate Limiting — Per-User Sliding Window Throttling

**Commit:** `0d11582`
**Files:**
- `app/auth/rate_limit.py` (new — 104 lines)
- `app/config.py` (modified — added `RATE_LIMIT_RPM`)
- `tests/conftest.py` (modified — added default env var)
- `tests/test_rate_limit.py` (new — 10 tests)

**Implementation:**

A `RateLimitMiddleware` using the `BaseHTTPMiddleware` pattern that enforces a per-user sliding window rate limit. Key design decisions:

- **Keyed on `user_id`** (set by `JWTAuthMiddleware`) rather than API key, since the project has no API key system. Unauthenticated requests pass through — the JWT middleware handles rejection.
- **Sliding window counter** using an in-memory `dict[str, list[float]]` of monotonic timestamps. Timestamps outside the 60-second window are pruned on each request.
- **Periodic cleanup** of stale user entries every 5 minutes to prevent unbounded memory growth.
- **Skips** `PUBLIC_PATHS`, `PUBLIC_PREFIXES`, and `OPTIONS` requests.
- Returns **429** with structured error body and `Retry-After` header when limit exceeded.
- Attaches `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset` headers to every authenticated response.

**Configuration:**

| Variable | Default | Description |
|----------|---------|-------------|
| `RATE_LIMIT_RPM` | `60` | Requests per minute per user |

**Scaling note:** The in-memory counter is suitable for single-instance deployments. For multi-instance (horizontal scaling), swap for a Redis-backed counter.

---

### CEX-40: Usage Tracking — Per-Request Metrics and Cost Attribution

**Commit:** `8aa0238`
**Files:**
- `app/shared/usage.py` (new — 36 lines)
- `app/usage/__init__.py` (new)
- `app/usage/router.py` (new — 98 lines)
- `migrations/004_usage_events.sql` (new — 19 lines)
- `app/assets/service.py` (modified — added usage recording)
- `app/assets/generation_router.py` (modified — passes `user_id`)
- `app/main.py` (modified — includes `usage_router`)
- `tests/test_usage.py` (new — 8 tests)

**Implementation:**

Two components:

1. **`record_usage_event()`** — A fire-and-forget function that inserts a `UsageEvent` into Supabase. Wrapped in `try/except` so tracking failures **never break generation**. Called from `app/assets/service.py` on both success and failure paths with timing data.

2. **`GET /usage`** endpoint — Returns usage summary and event list for the authenticated organization. Supports query parameters:
   - `start_date` / `end_date` — ISO date range filter
   - `asset_type` — filter by asset type

**Response shape:**
```json
{
  "summary": {
    "total_generations": 42,
    "successful": 40,
    "failed": 2,
    "total_tokens_input": 125000,
    "total_tokens_output": 45000
  },
  "events": [...]
}
```

**Database migration (`004_usage_events.sql`):**

```sql
CREATE TABLE usage_events (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    org_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    asset_type TEXT NOT NULL,
    status TEXT NOT NULL,  -- "success" | "failed"
    duration_ms INTEGER,
    claude_tokens_input INTEGER,
    claude_tokens_output INTEGER,
    provider_costs JSONB,
    request_id TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);
```

Indexes on `(org_id, created_at)` and `(user_id, created_at)` for efficient filtering.

---

### CEX-41: Webhook Reliability — Retry with HMAC-SHA256 Signing

**Commit:** `627382d`
**Files:**
- `app/shared/webhooks.py` (new — 188 lines)
- `app/config.py` (modified — added `WEBHOOK_SIGNING_SECRET`)
- `tests/test_webhooks.py` (new — 12 tests)

**Implementation:**

A `WebhookDelivery` class that delivers payloads to external URLs with:

- **HMAC-SHA256 signing** — Payloads are signed with the `WEBHOOK_SIGNING_SECRET`. Signature sent as `X-CEX-Signature: sha256={hex_digest}`.
- **Up to 3 retries** with exponential backoff delays: 10s, 60s, 300s.
- **10-second timeout** per delivery attempt.
- **4xx = no retry** — Client errors are considered permanent failures.
- **5xx / timeout = retry** — Server errors and timeouts trigger retry with backoff.

**Headers sent with every webhook:**

| Header | Description |
|--------|-------------|
| `X-CEX-Event` | Event type (e.g., `asset.generated`) |
| `X-CEX-Signature` | `sha256={hmac_hex_digest}` |
| `X-CEX-Delivery-Id` | Unique UUID per delivery attempt chain |
| `X-CEX-Timestamp` | Unix timestamp of initial delivery |

**Return type:** `WebhookDeliveryResult` with `delivery_id`, `status` ("delivered" | "failed"), `attempts`, `final_status_code`, and `total_duration_ms`.

**Testing:** Uses a hookable `_sleep()` method to avoid real delays in tests.

---

### CEX-42: Structured Error Handling + Request ID Middleware

**Commit:** `685e767`
**Files:**
- `app/shared/request_id.py` (new — 26 lines)
- `app/shared/errors.py` (modified — refactored to structured base)
- `app/shared/error_handlers.py` (new — 110 lines)
- `app/main.py` (modified — registers handlers + middleware)
- `tests/test_error_handlers.py` (new — 8 tests)
- `tests/test_request_id.py` (new — 5 tests)

**Implementation:**

**Request ID Middleware:**

`RequestIDMiddleware` generates a unique `req_{uuid_hex[:16]}` for every request, stores it on `request.state.request_id`, and adds `X-Request-Id` to the response. This is the outermost middleware (first to execute) so all downstream middleware and handlers have access to the request ID.

**Structured Error Types:**

All error classes extend `_StructuredHTTPException` which adds `error_type` and `details` attributes:

| Error Class | Status | `error_type` |
|-------------|--------|--------------|
| `NotFoundError` | 404 | `not_found` |
| `ForbiddenError` | 403 | `forbidden` |
| `UnauthorizedError` | 401 | `auth_error` |
| `BadRequestError` | 400 | `bad_request` |
| `ConflictError` | 409 | `conflict` |
| `ValidationError` | 422 | `validation_error` |
| `RateLimitError` | 429 | `rate_limit_exceeded` |
| `GenerationError` | 500 | `generation_error` |
| `RenderError` | 500 | `render_error` |
| `ProviderError` | 502 | `provider_error` |

**Error Handlers:**

`register_error_handlers(app)` installs four exception handlers:

1. **`_StructuredHTTPException`** → returns structured JSON with `error_type` and `details`
2. **`RequestValidationError`** → converts Pydantic validation errors to `validation_error` with field-level detail
3. **`StarletteHTTPException`** → maps status codes to `error_type` strings
4. **Generic `Exception`** → logs full traceback, returns `internal_error` with **no internal detail leakage**

**Consistent error response shape:**
```json
{
  "error": {
    "type": "not_found",
    "message": "Resource not found",
    "details": null,
    "request_id": "req_a1b2c3d4e5f67890"
  }
}
```

---

### CEX-43: Monitoring — Health Checks + Structured JSON Logging

**Commit:** `0fc606a`
**Files:**
- `app/shared/logging_config.py` (new — 65 lines)
- `app/shared/models.py` (modified — added `CheckResult`, `ReadinessResponse`)
- `app/main.py` (modified — health endpoints + logging init)
- `app/auth/middleware.py` (modified — added health paths to `PUBLIC_PATHS`)
- `tests/test_health_checks.py` (new — 12 tests)

**Health Check Endpoints:**

| Endpoint | Purpose | Auth Required |
|----------|---------|---------------|
| `GET /health` | Basic liveness — returns `{"status": "ok"}` | No |
| `GET /health/live` | Kubernetes/Railway liveness probe | No |
| `GET /health/ready` | Readiness probe — checks DB + Claude API | No |

**Readiness probe** (`/health/ready`) checks:
1. **Database** — executes `SELECT id FROM organizations LIMIT 1` against Supabase
2. **Claude API** — sends GET to `https://api.anthropic.com/v1/messages` (even 401 confirms reachability)

Returns `200` with `"status": "ok"` if all checks pass, or `503` with `"status": "degraded"` and per-check error details if any fail. Each check includes `latency_ms`.

**Structured JSON Logging:**

`JSONFormatter` produces single-line JSON log entries with:
- `timestamp`, `level`, `logger`, `message`
- Optional contextual fields when available: `request_id`, `method`, `path`, `status_code`, `duration_ms`, `asset_type`, `model`, `input_tokens`, `output_tokens`
- Exception tracebacks included as `exception` field

`configure_logging()` sets up the root logger with JSON output and quiets noisy third-party loggers (`httpx`, `httpcore`, `uvicorn.access`).

---

### CEX-44: API Documentation — OpenAPI Tags + Developer Guide

**Commit:** `72015fd`
**Files:**
- `docs/API_GUIDE.md` (new — 226 lines)
- `app/main.py` (modified — OpenAPI metadata + tags)

**Implementation:**

- Added `openapi_tags` to the FastAPI app for all endpoint groups (auth, assets, campaigns, audiences, analytics, attribution, landing_pages, organizations, usage, health).
- Created comprehensive developer guide covering:
  - Authentication (JWT via Supabase)
  - Quick start (cURL examples)
  - All asset types with their configurable parameters
  - Rate limiting behavior and headers
  - Error response format with all error types
  - Webhook delivery, signing, and verification
  - Usage tracking API
  - Health check endpoints

---

### CEX-45: Railway Deployment + CI/CD + Lint Fixes

**Commit:** `831ebea`
**Files:**
- `.github/workflows/ci.yml` (new — 30 lines)
- `railway.toml` (modified — added health check config)

**CI Pipeline (`.github/workflows/ci.yml`):**

Triggers on push to `main` and pull requests targeting `main`. Two jobs:

1. **`lint`** — `ruff check .` and `ruff format --check .`
2. **`test`** — `pip install ".[dev]"` then `pytest tests/ -v`

Both jobs run on `ubuntu-latest` with Python 3.12.

**Railway Configuration:**

```toml
[deploy]
healthcheckPath = "/health/live"
healthcheckTimeout = 30
restartPolicyType = "on_failure"
restartPolicyMaxRetries = 3
```

**Lint fixes applied:**
- Removed unused variable `resp` in `app/main.py`
- Renamed ambiguous variable `l` → `part` in `app/shared/error_handlers.py`
- Changed deprecated `timezone.utc` → `UTC` import in `app/shared/logging_config.py`
- Removed unused imports (`datetime`, `timezone`, `Field`) in `app/shared/usage.py`
- Removed unused import (`datetime`) in `app/usage/router.py`

---

## Middleware Stack

Starlette convention: last `add_middleware()` call = first to execute on request.

```
Request → RequestIDMiddleware → CORSMiddleware → JWTAuthMiddleware → RateLimitMiddleware → Route Handler
```

This ordering ensures:
- Request ID is available to all downstream middleware and error handlers
- CORS headers are set before auth rejection
- JWT populates `request.state.user_id` before rate limiter reads it
- Rate limiter only acts on authenticated requests

---

## Test Summary

| Test File | Tests | Lines | Covers |
|-----------|-------|-------|--------|
| `test_rate_limit.py` | 10 | 186 | Sliding window, 429 response, headers, cleanup, public paths |
| `test_usage.py` | 8 | 163 | Fire-and-forget insert, failure isolation, endpoint filtering |
| `test_webhooks.py` | 12 | 260 | Signing, retry logic, timeout handling, 4xx abort, backoff |
| `test_error_handlers.py` | 8 | 195 | All error types, validation errors, generic handler, no leakage |
| `test_request_id.py` | 5 | 66 | ID generation, header propagation, format validation |
| `test_health_checks.py` | 12 | 210 | Liveness, readiness, DB/API failure scenarios, 503 degraded |
| **Total** | **55** | **1,080** | |

Full suite result: **1,153 passed**, 27 failed (all pre-existing, unrelated to Phase 7).

---

## Configuration Reference

| Variable | Default | Added In | Description |
|----------|---------|----------|-------------|
| `RATE_LIMIT_RPM` | `60` | CEX-39 | Max requests per minute per user |
| `WEBHOOK_SIGNING_SECRET` | `""` | CEX-41 | HMAC-SHA256 key for webhook payloads |

---

## Architecture Decisions

1. **In-memory rate limiting** — Chosen for simplicity in single-instance Railway deployment. The `_counters` dict is bounded by periodic cleanup. Redis migration path is straightforward when horizontal scaling is needed.

2. **Fire-and-forget usage tracking** — Usage events are inserted synchronously but wrapped in `try/except` so failures never propagate to the caller. This trades strict delivery guarantees for zero impact on generation latency.

3. **Hookable webhook sleep** — The `WebhookDelivery._sleep()` method can be monkey-patched in tests to avoid real delays, keeping the test suite fast (~4 seconds total).

4. **Structured error base class** — `_StructuredHTTPException` extends FastAPI's `HTTPException` with `error_type` and `details`. The underscore prefix signals it's internal; consumers use the named subclasses.

5. **Generic exception handler safety** — The catch-all `Exception` handler logs the full traceback server-side but returns only `"An unexpected error occurred"` to the client, preventing internal detail leakage (verified by test).

6. **Health check as public path** — `/health/live` and `/health/ready` are added to `PUBLIC_PATHS` in the JWT middleware so Railway's health checks don't need authentication.

---

## Files Changed (Summary)

| Category | New | Modified |
|----------|-----|----------|
| Application code | 7 | 5 |
| Tests | 6 | 1 |
| Migrations | 1 | 0 |
| CI/CD | 1 | 1 |
| Documentation | 1 | 0 |
| **Total** | **16** | **7** |
