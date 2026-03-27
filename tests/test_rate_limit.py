"""Tests for per-user rate limiting middleware (CEX-39)."""

from __future__ import annotations

import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.testclient import TestClient

from app.auth.rate_limit import RateLimitMiddleware


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_request(path: str = "/assets/generate", user_id: str | None = "user-1") -> Request:
    """Build a minimal Starlette Request with optional user_id on state."""
    scope = {
        "type": "http",
        "method": "GET",
        "path": path,
        "query_string": b"",
        "headers": [],
    }
    req = Request(scope)
    if user_id is not None:
        req.state.user_id = user_id
    return req


async def _ok_response(request: Request) -> Response:
    return JSONResponse({"ok": True})


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------


class TestRateLimitMiddleware:
    """Sliding-window rate limiter keyed on user_id."""

    @pytest.fixture()
    def middleware(self):
        # Use a low RPM for fast testing
        mw = RateLimitMiddleware(app=AsyncMock(), rpm=5)
        return mw

    async def test_allows_requests_under_limit(self, middleware: RateLimitMiddleware):
        request = _make_request()
        response = await middleware.dispatch(request, _ok_response)
        assert response.status_code == 200
        assert response.headers["X-RateLimit-Limit"] == "5"
        assert response.headers["X-RateLimit-Remaining"] == "4"
        assert "X-RateLimit-Reset" in response.headers

    async def test_returns_429_when_limit_exceeded(self, middleware: RateLimitMiddleware):
        request = _make_request()

        # Exhaust the limit
        for _ in range(5):
            resp = await middleware.dispatch(request, _ok_response)
            assert resp.status_code == 200

        # 6th request should be rejected
        resp = await middleware.dispatch(request, _ok_response)
        assert resp.status_code == 429
        assert "Retry-After" in resp.headers
        assert resp.headers["X-RateLimit-Remaining"] == "0"

    async def test_independent_limits_per_user(self, middleware: RateLimitMiddleware):
        """Different users have independent rate limit counters."""
        req_a = _make_request(user_id="user-a")
        req_b = _make_request(user_id="user-b")

        # Exhaust user-a's limit
        for _ in range(5):
            await middleware.dispatch(req_a, _ok_response)

        # user-a should be blocked
        resp_a = await middleware.dispatch(req_a, _ok_response)
        assert resp_a.status_code == 429

        # user-b should still be allowed
        resp_b = await middleware.dispatch(req_b, _ok_response)
        assert resp_b.status_code == 200

    async def test_retry_after_header_value(self, middleware: RateLimitMiddleware):
        """Retry-After header gives a positive integer of seconds to wait."""
        request = _make_request()

        for _ in range(5):
            await middleware.dispatch(request, _ok_response)

        resp = await middleware.dispatch(request, _ok_response)
        retry_after = int(resp.headers["Retry-After"])
        assert retry_after > 0
        assert retry_after <= 61  # at most the full window + 1

    async def test_public_paths_bypass_rate_limit(self, middleware: RateLimitMiddleware):
        """Public paths (health, docs, auth) skip rate limiting."""
        for path in ["/health", "/docs", "/auth/login", "/lp/some-slug"]:
            request = _make_request(path=path, user_id="user-1")
            resp = await middleware.dispatch(request, _ok_response)
            assert resp.status_code == 200
            assert "X-RateLimit-Limit" not in resp.headers

    async def test_options_requests_bypass(self, middleware: RateLimitMiddleware):
        """OPTIONS (CORS preflight) requests bypass rate limiting."""
        scope = {
            "type": "http",
            "method": "OPTIONS",
            "path": "/assets/generate",
            "query_string": b"",
            "headers": [],
        }
        request = Request(scope)
        request.state.user_id = "user-1"
        resp = await middleware.dispatch(request, _ok_response)
        assert resp.status_code == 200
        assert "X-RateLimit-Limit" not in resp.headers

    async def test_unauthenticated_requests_pass_through(self, middleware: RateLimitMiddleware):
        """Requests without user_id pass through (JWT middleware handles rejection)."""
        request = _make_request(user_id=None)
        resp = await middleware.dispatch(request, _ok_response)
        assert resp.status_code == 200
        assert "X-RateLimit-Limit" not in resp.headers

    async def test_sliding_window_expires_old_timestamps(self, middleware: RateLimitMiddleware):
        """After the window elapses, old timestamps are pruned and new requests allowed."""
        request = _make_request()

        # Exhaust limit
        for _ in range(5):
            await middleware.dispatch(request, _ok_response)

        # Manually age timestamps beyond the window
        user_timestamps = middleware._counters["user-1"]
        aged = time.monotonic() - 61  # 61 seconds ago
        middleware._counters["user-1"] = [aged] * 5

        # Now the request should succeed (old timestamps pruned)
        resp = await middleware.dispatch(request, _ok_response)
        assert resp.status_code == 200

    async def test_rate_limit_headers_present_on_success(self, middleware: RateLimitMiddleware):
        """Every authenticated response includes all three rate-limit headers."""
        request = _make_request()
        resp = await middleware.dispatch(request, _ok_response)

        assert "X-RateLimit-Limit" in resp.headers
        assert "X-RateLimit-Remaining" in resp.headers
        assert "X-RateLimit-Reset" in resp.headers

        # Remaining should decrease with each request
        remaining1 = int(resp.headers["X-RateLimit-Remaining"])

        resp2 = await middleware.dispatch(request, _ok_response)
        remaining2 = int(resp2.headers["X-RateLimit-Remaining"])

        assert remaining2 == remaining1 - 1

    async def test_429_response_body_structure(self, middleware: RateLimitMiddleware):
        """429 response body includes detail and retry_after fields."""
        request = _make_request()

        for _ in range(5):
            await middleware.dispatch(request, _ok_response)

        resp = await middleware.dispatch(request, _ok_response)
        assert resp.status_code == 429

        import json

        body = json.loads(resp.body.decode())
        assert "detail" in body
        assert "retry_after" in body
        assert isinstance(body["retry_after"], int)
