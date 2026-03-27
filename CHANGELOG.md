# Changelog

All notable changes to Creative Engine X API will be documented in this file.

## [0.7.0.0] - 2026-03-26

### Added
- Per-user sliding-window rate limiting with configurable RPM (CEX-39)
- Usage tracking: per-request metrics recording and `GET /usage` reporting endpoint (CEX-40)
- Webhook delivery engine with HMAC-SHA256 signing, retry with exponential backoff, and 4xx short-circuit (CEX-41)
- Structured error responses with consistent JSON shape and request ID propagation (CEX-42)
- Request ID middleware generating unique `req_*` identifiers on every request (CEX-42)
- Health check endpoints: `/health/live` (liveness), `/health/ready` (readiness with DB + Claude API checks) (CEX-43)
- Structured JSON logging with contextual fields (request_id, duration_ms, status_code) (CEX-43)
- API developer guide covering auth, asset types, rate limits, errors, webhooks, and usage (CEX-44)
- OpenAPI tags and endpoint documentation for all route groups (CEX-44)
- GitHub Actions CI pipeline with ruff lint + pytest (CEX-45)
- Railway deployment config with liveness healthcheck and restart policy (CEX-45)
- Usage events database migration with org_id and user_id composite indexes (CEX-40)

### Changed
- Error responses now use `{"error": {"type", "message", "details", "request_id"}}` format across all endpoints
- Rate limit 429 responses match the structured error format
- Health check error messages sanitized to prevent internal detail leakage
