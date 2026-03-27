"""Tests for structured error handling (CEX-42)."""

from __future__ import annotations

import json

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from app.shared.error_handlers import register_error_handlers
from app.shared.errors import (
    BadRequestError,
    ConflictError,
    ForbiddenError,
    GenerationError,
    NotFoundError,
    ProviderError,
    RateLimitError,
    RenderError,
    UnauthorizedError,
    ValidationError,
)
from app.shared.request_id import RequestIDMiddleware


# ---------------------------------------------------------------------------
# Fixture: minimal FastAPI app with error handlers + request ID
# ---------------------------------------------------------------------------


@pytest.fixture()
def app():
    test_app = FastAPI()
    register_error_handlers(test_app)
    test_app.add_middleware(RequestIDMiddleware)

    @test_app.get("/raise-not-found")
    async def raise_not_found():
        raise NotFoundError("Thing not found")

    @test_app.get("/raise-unauthorized")
    async def raise_unauthorized():
        raise UnauthorizedError()

    @test_app.get("/raise-forbidden")
    async def raise_forbidden():
        raise ForbiddenError()

    @test_app.get("/raise-bad-request")
    async def raise_bad_request():
        raise BadRequestError("Invalid input")

    @test_app.get("/raise-conflict")
    async def raise_conflict():
        raise ConflictError()

    @test_app.get("/raise-validation")
    async def raise_validation():
        raise ValidationError(
            "Invalid field",
            details={"field": "name", "reason": "too short"},
        )

    @test_app.get("/raise-rate-limit")
    async def raise_rate_limit():
        raise RateLimitError("Rate limit exceeded. Try again in 15 seconds.")

    @test_app.get("/raise-generation")
    async def raise_generation():
        raise GenerationError("Claude API timed out")

    @test_app.get("/raise-render")
    async def raise_render():
        raise RenderError("PDF rendering failed")

    @test_app.get("/raise-provider")
    async def raise_provider():
        raise ProviderError("ElevenLabs API unavailable")

    @test_app.get("/raise-generic")
    async def raise_generic():
        raise RuntimeError("unexpected crash with secret_key=abc123")

    return test_app


@pytest.fixture()
def client(app):
    return TestClient(app)


# ---------------------------------------------------------------------------
# Tests: structured error shape
# ---------------------------------------------------------------------------


class TestStructuredErrorShape:
    def test_error_has_type_message_details_request_id(self, client: TestClient):
        resp = client.get("/raise-not-found")
        body = resp.json()

        assert "error" in body
        error = body["error"]
        assert error["type"] == "not_found"
        assert error["message"] == "Thing not found"
        assert "request_id" in error
        assert error["request_id"].startswith("req_")

    def test_request_id_in_response_header(self, client: TestClient):
        resp = client.get("/raise-not-found")
        assert "X-Request-Id" in resp.headers
        assert resp.headers["X-Request-Id"] == resp.json()["error"]["request_id"]


# ---------------------------------------------------------------------------
# Tests: each error type returns correct status + type
# ---------------------------------------------------------------------------


class TestErrorTypes:
    @pytest.mark.parametrize(
        "path, expected_status, expected_type",
        [
            ("/raise-not-found", 404, "not_found"),
            ("/raise-unauthorized", 401, "auth_error"),
            ("/raise-forbidden", 403, "forbidden"),
            ("/raise-bad-request", 400, "bad_request"),
            ("/raise-conflict", 409, "conflict"),
            ("/raise-validation", 422, "validation_error"),
            ("/raise-rate-limit", 429, "rate_limit_exceeded"),
            ("/raise-generation", 500, "generation_error"),
            ("/raise-render", 500, "render_error"),
            ("/raise-provider", 502, "provider_error"),
        ],
    )
    def test_error_status_and_type(
        self, client: TestClient, path: str, expected_status: int, expected_type: str
    ):
        resp = client.get(path)
        assert resp.status_code == expected_status
        assert resp.json()["error"]["type"] == expected_type


# ---------------------------------------------------------------------------
# Tests: details field
# ---------------------------------------------------------------------------


class TestErrorDetails:
    def test_validation_error_includes_details(self, client: TestClient):
        resp = client.get("/raise-validation")
        body = resp.json()
        assert body["error"]["details"] == {"field": "name", "reason": "too short"}

    def test_not_found_details_is_none(self, client: TestClient):
        resp = client.get("/raise-not-found")
        body = resp.json()
        assert body["error"]["details"] is None


# ---------------------------------------------------------------------------
# Tests: generic exception does not leak internals
# ---------------------------------------------------------------------------


class TestGenericExceptionSafety:
    def test_generic_exception_returns_500(self, app):
        with TestClient(app, raise_server_exceptions=False) as c:
            resp = c.get("/raise-generic")
        assert resp.status_code == 500

    def test_generic_exception_does_not_leak_details(self, app):
        with TestClient(app, raise_server_exceptions=False) as c:
            resp = c.get("/raise-generic")
        body = resp.json()
        assert body["error"]["type"] == "internal_error"
        assert body["error"]["message"] == "An unexpected error occurred"
        # Must NOT contain the secret from the RuntimeError
        assert "secret_key" not in json.dumps(body)
        assert "abc123" not in json.dumps(body)


# ---------------------------------------------------------------------------
# Tests: 404 from unknown routes (Starlette handler)
# ---------------------------------------------------------------------------


class TestUnknownRoutes:
    def test_unknown_route_returns_structured_404(self, client: TestClient):
        resp = client.get("/this-does-not-exist")
        assert resp.status_code == 404
        body = resp.json()
        assert body["error"]["type"] == "not_found"
        assert "request_id" in body["error"]
