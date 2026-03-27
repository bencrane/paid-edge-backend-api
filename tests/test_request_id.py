"""Tests for request ID middleware (CEX-42)."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from app.shared.request_id import RequestIDMiddleware


async def _ok_response(request: Request) -> Response:
    return JSONResponse({"ok": True})


class TestRequestIDMiddleware:
    @pytest.fixture()
    def middleware(self):
        return RequestIDMiddleware(app=AsyncMock())

    async def test_adds_request_id_to_state(self, middleware: RequestIDMiddleware):
        scope = {"type": "http", "method": "GET", "path": "/test", "query_string": b"", "headers": []}
        request = Request(scope)

        await middleware.dispatch(request, _ok_response)

        assert hasattr(request.state, "request_id")
        assert request.state.request_id.startswith("req_")

    async def test_adds_header_to_response(self, middleware: RequestIDMiddleware):
        scope = {"type": "http", "method": "GET", "path": "/test", "query_string": b"", "headers": []}
        request = Request(scope)

        response = await middleware.dispatch(request, _ok_response)

        assert "X-Request-Id" in response.headers
        assert response.headers["X-Request-Id"].startswith("req_")

    async def test_request_id_format(self, middleware: RequestIDMiddleware):
        scope = {"type": "http", "method": "GET", "path": "/test", "query_string": b"", "headers": []}
        request = Request(scope)

        await middleware.dispatch(request, _ok_response)

        rid = request.state.request_id
        assert rid.startswith("req_")
        assert len(rid) == 20  # "req_" + 16 hex chars

    async def test_unique_ids_per_request(self, middleware: RequestIDMiddleware):
        ids = set()
        for _ in range(10):
            scope = {"type": "http", "method": "GET", "path": "/test", "query_string": b"", "headers": []}
            request = Request(scope)
            await middleware.dispatch(request, _ok_response)
            ids.add(request.state.request_id)
        assert len(ids) == 10

    async def test_response_header_matches_state(self, middleware: RequestIDMiddleware):
        scope = {"type": "http", "method": "GET", "path": "/test", "query_string": b"", "headers": []}
        request = Request(scope)

        response = await middleware.dispatch(request, _ok_response)

        assert response.headers["X-Request-Id"] == request.state.request_id
