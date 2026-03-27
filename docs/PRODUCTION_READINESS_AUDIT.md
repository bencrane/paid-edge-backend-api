# PaidEdge Backend API — Production Readiness Audit

**Date:** 2026-03-27
**Auditor:** Claude Opus 4.6 (eng review + outside voice)
**Branch:** `bencrane/linear-api-test`
**Version at audit:** 0.7.0.0 (commit `8c0585f`)
**Scope:** Full codebase audit — architecture, security, production readiness, test coverage, integration quality

---

## Executive Summary

This is a 3-day sprint that built an entire multi-tenant B2B paid advertising backend from scratch. 21 PRs merged. ~18,700 lines of application code, ~22,400 lines of test code. Multiple AI agents ran in parallel on different feature branches while the human orchestrator managed merges and production hardening.

The architecture is sound. FastAPI + Supabase (transactional) + ClickHouse (analytics) is the right two-database pattern for this workload. The code is well-organized, follows FastAPI best practices, and has genuine production infrastructure (structured errors, request tracing, rate limiting, health checks, webhook delivery).

23 issues were identified across two review passes (14 from eng review, 9 from independent outside voice). All P0 issues have been fixed in this PR.

---

## What Was Built

### Tech Stack
- **API Framework:** FastAPI (Python 3.12+, async)
- **Primary Database:** Supabase (PostgreSQL 17) — auth, CRUD, config
- **Analytics Database:** ClickHouse Cloud — campaign metrics, CRM data, attribution
- **AI/LLM:** Anthropic Claude API (Sonnet 4 for speed, Opus 4 for quality)
- **Secrets:** Doppler (injected at runtime)
- **Deployment:** Railway via Docker
- **CI:** GitHub Actions (ruff lint + pytest)

### Architecture

```
CORE LOOP:
  Audiences → Assets → Campaigns → Analytics → Attribution

  ┌──────────┐    ┌──────────┐    ┌──────────┐
  │ Audience  │───▶│  Asset   │───▶│ Campaign │
  │ Building  │    │  Gen AI  │    │  Launch  │
  └──────────┘    └──────────┘    └────┬─────┘
       ▲                               │
       │                               ▼
  ┌──────────┐                   ┌──────────┐
  │ Revenue  │◀──────────────────│Analytics │
  │ Attrib.  │                   │ClickHouse│
  └──────────┘                   └──────────┘
```

### API Surface

| Group | Prefix | Endpoints | Auth |
|---|---|---|---|
| Auth | `/auth` | signup, login, logout, me, refresh, OAuth callbacks | Public (signup/login), JWT (rest) |
| Organizations | `/orgs` | CRUD, members, provider configs | JWT + membership/admin |
| Assets | `/assets`, `/render` | generate, get, update, render landing page/PDF | JWT + tenant |
| Audiences | `/audiences` | CRUD, signals, export, push to LinkedIn/Meta | JWT + tenant |
| Campaigns | `/campaigns` | CRUD, launch to platforms | JWT + tenant |
| Analytics | `/analytics` | overview KPIs, time series, platform comparison | JWT + tenant |
| Attribution | `/attribution` | funnel, cost-per-opp, pipeline influenced | JWT + tenant |
| Landing Pages | `/lp` | serve HTML, form submission | Public |
| Usage | `/usage` | usage summary and events | JWT + tenant |
| Health | `/health` | liveness, readiness | Public |

### Consumer Integration

```
Authentication:
  POST /auth/login → { access_token, refresh_token }
  All requests: Authorization: Bearer {access_token}
  Tenant selection: X-Organization-Id: {org_uuid} (optional)

Error format (all endpoints):
  {
    "error": {
      "type": "not_found | auth_error | validation_error | ...",
      "message": "Human-readable description",
      "details": null | { "errors": [...] },
      "request_id": "req_abc123"
    }
  }

Rate limit headers (every authenticated response):
  X-RateLimit-Limit: 60
  X-RateLimit-Remaining: 57
  X-RateLimit-Reset: 1711497600
```

### Deployment

```
Railway:
  Dockerfile → python:3.12-slim + Doppler CLI
  CMD: doppler run -- uvicorn app.main:app --host 0.0.0.0 --port 8080
  Health: /health/live (liveness), /health/ready (readiness with DB + Claude checks)
  Restart: on_failure, max 3 retries
  CI: GitHub Actions (ruff lint + pytest on push to main / PRs)
```

---

## Issues Found

### Review Pass 1: Engineering Review (14 issues)

| # | Priority | Category | Issue | File(s) |
|---|---|---|---|---|
| 1 | P2 | Architecture | In-memory rate limiting won't work multi-instance | `rate_limit.py` |
| 2 | **P0** | **Performance** | **Sync Claude client blocks event loop 30-120s** | `claude_ai.py` |
| 3 | P2 | DX | No CLAUDE.md file | repo root |
| 4 | P3 | Naming | "Creative Engine X" vs "PaidEdge" identity confusion | `main.py`, CHANGELOG |
| 5 | P3 | Docs | Port 8080 vs 8000 doc inconsistency | Dockerfile, agent ops |
| 6 | P3 | Infra | No database migration runner | `migrations/` |
| 7 | **P0** | **Performance** | **`time.sleep()` in async code paths** | `claude_ai.py` |
| 8 | P2 | Config | Empty string defaults for required secrets | `config.py` |
| 9 | P2 | Memory | Unbounded rate limit counter growth | `rate_limit.py` |
| 10 | P1 | Metadata | FastAPI version shows 0.1.0, actual is 0.7.0.0 | `main.py` |
| 11 | P2 | Testing | Tests can't verify async retry behavior | `test_claude_client.py` |
| 12 | **P0** | **Performance** | **Event loop blocking (critical for production)** | `claude_ai.py` |
| 13 | P2 | Perf | ClickHouse connection pooling unclear | `db/clickhouse.py` |
| 14 | P3 | Perf | No request-level timeout middleware | `main.py` |

### Review Pass 2: Outside Voice (9 additional issues)

| # | Priority | Category | Issue | File(s) |
|---|---|---|---|---|
| 15 | **P0** | **Security** | **OAuth tokens returned in plaintext via API** | `tenants/router.py` |
| 16 | **P0** | **Security** | **Stored XSS via Jinja2 autoescape=False** | `assets/router.py`, `landing_pages/router.py` |
| 17 | P1 | Security | User enumeration via invite error message | `tenants/router.py` |
| 18 | P2 | Infra | Trigger.dev tasks are dead code (no SDK/scheduler) | `trigger/` |
| 19 | P1 | Security | Landing page form spam (no rate limit on public endpoint) | `landing_pages/router.py` |
| 20 | **P0** | **Security** | **ClickHouse prod host hardcoded as default** | `config.py` |
| 21 | P2 | Testing | Tests mock everything, zero integration tests | `tests/` |
| 22 | P2 | Perf | Usage aggregation done client-side (should be SQL) | `usage/router.py` |
| 23 | P2 | Perf | ClaudeClient instantiated per request (no connection reuse) | `dependencies.py` |

---

## Fixes Applied in This PR

### Security P0s

**1. OAuth token exfiltration (Issue #15)**
- Added `require_membership()` to `list_providers` endpoint — any authenticated user could previously read any org's provider configs by guessing `org_id`
- Added `mask_provider_config()` that redacts `access_token`, `refresh_token`, `client_secret`, `api_key`, `app_secret` values in API responses (shows first 4 and last 4 chars only)
- Files: `app/tenants/router.py`, `app/tenants/service.py`, `app/tenants/models.py`

**2. Stored XSS (Issue #16)**
- Changed `autoescape=False` to `autoescape=True` on both Jinja2 `Environment` instances
- Previously, user-controlled content from the database was rendered directly into publicly-served HTML without escaping
- Files: `app/assets/router.py`, `app/landing_pages/router.py`

**3. ClickHouse prod host default (Issue #20)**
- Removed hardcoded production ClickHouse host (`gf9xtjjqyl.us-east-1.aws.clickhouse.cloud`) as the default value
- Running locally without env vars would previously connect to production ClickHouse
- File: `app/config.py`

### Performance P0

**4. Async Claude client (Issues #2, #7, #12)**
- Switched from `anthropic.Anthropic` (synchronous) to `anthropic.AsyncAnthropic`
- Added `await` on `self._client.messages.create()`
- Replaced all 4 instances of `time.sleep()` with `await asyncio.sleep()`
- The event loop is no longer blocked during asset generation (30-120 seconds per call)
- File: `app/integrations/claude_ai.py`

### P1 Fixes

**5. User enumeration (Issue #17)**
- Changed invite error from `"No user found with email {email}"` to generic `"Unable to invite user. They may need to create an account first."`
- File: `app/tenants/router.py`

**6. Landing page form spam (Issue #19)**
- Added per-IP rate limiting (10 submissions per minute) on the public `POST /lp/{slug}/submit` endpoint
- Includes periodic cleanup of stale IP entries
- File: `app/landing_pages/router.py`

### P2 Fixes

**7. FastAPI version metadata (Issue #10)**
- Fixed version from `"0.1.0"` to `"0.7.0.0"`
- Fixed title from `"Creative Engine X API"` to `"PaidEdge API"`
- File: `app/main.py`

**8. Rate limit memory leak (Issue #9)**
- Added `_cleanup_stale_entries()` that runs every 5 minutes
- Removes user entries from the counter dict when they have no recent timestamps
- File: `app/auth/rate_limit.py`

---

## What's Well-Designed (highlights)

1. **Two-database architecture** — Supabase for CRUD, ClickHouse for analytics. Correct separation of transactional vs analytical workloads.

2. **Webhook delivery engine** (`shared/webhooks.py`) — HMAC-SHA256 signing, exponential backoff (10s/60s/300s), 4xx short-circuit, hookable `_sleep` for testing. Production-grade.

3. **Claude structured output pattern** (`integrations/claude_ai.py`) — XML tag prompting with schema enforcement, JSON parse recovery with re-prompt at lower temperature. Works reliably in production.

4. **Structured error handling** — Custom exception hierarchy, consistent JSON response shape, request ID propagation, validation error field mapping, generic handler that never leaks internals.

5. **Request tracing** — `RequestIDMiddleware` generates unique `req_*` identifiers, propagated to all error responses and structured logs.

6. **Test coverage for platform integrations** — Every LinkedIn and Meta API operation (campaigns, audiences, creatives, metrics, conversions, leads, OAuth) has dedicated tests with edge cases.

---

## Remaining Issues (not fixed in this PR)

| # | Priority | Issue | Recommendation |
|---|---|---|---|
| 1 | P2 | In-memory rate limiting (single-instance only) | Add Redis when scaling to multiple instances |
| 3 | P2 | No CLAUDE.md | Create one based on PAIDEDGE_AGENT_OPS-2.md |
| 6 | P3 | No migration runner | Use `supabase db push` or add Alembic |
| 11 | P2 | Tests can't verify async retry behavior | Now fixable with AsyncAnthropic |
| 13 | P2 | ClickHouse connection pooling | Verify `get_clickhouse_client()` reuses connections |
| 14 | P3 | No request-level timeout middleware | Add uvicorn timeout or middleware |
| 18 | P2 | Trigger.dev tasks are dead code | Need Trigger.dev SDK + deployment config |
| 21 | P2 | Tests mock everything | Add integration tests with FastAPI TestClient |
| 22 | P2 | Usage aggregation client-side | Move to SQL aggregation in ClickHouse |
| 23 | P2 | ClaudeClient per-request instantiation | Cache client instance in dependency |

---

## Linear Tracking Note

The CEX workspace has 9 issues (CEX-1 through CEX-9) covering Phase 1 Foundation. The vast majority of actual work is tracked in BJC (BJC-46 through BJC-192) and PEX (PEX-67 through PEX-74) workspaces. Git commit history is the authoritative record of what was built.

---

## Files Changed in This PR

| File | Change |
|---|---|
| `app/integrations/claude_ai.py` | Async client + asyncio.sleep |
| `app/tenants/router.py` | Membership check, token masking, generic invite error |
| `app/tenants/service.py` | Added `require_membership()` |
| `app/tenants/models.py` | Added `mask_provider_config()` |
| `app/assets/router.py` | `autoescape=True` |
| `app/landing_pages/router.py` | `autoescape=True`, per-IP form rate limiting |
| `app/auth/rate_limit.py` | Stale entry cleanup |
| `app/config.py` | Removed prod ClickHouse default |
| `app/main.py` | Fixed version + title |
| `docs/PRODUCTION_READINESS_AUDIT.md` | This document |
