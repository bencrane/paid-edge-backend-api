"""Tests for health check endpoints and structured logging (CEX-43)."""

from __future__ import annotations

import json
import logging
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.shared.logging_config import JSONFormatter, configure_logging
from app.shared.models import CheckResult, ReadinessResponse


# ---------------------------------------------------------------------------
# Health endpoint tests (using the real app)
# ---------------------------------------------------------------------------


class TestHealthLive:
    def test_live_returns_200(self):
        from app.main import app

        with TestClient(app) as client:
            resp = client.get("/health/live")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ok"

    def test_health_returns_200(self):
        from app.main import app

        with TestClient(app) as client:
            resp = client.get("/health")
        assert resp.status_code == 200


class TestHealthReady:
    def test_ready_returns_200_when_all_ok(self):
        from app.main import app

        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.limit.return_value.execute.return_value = (
            MagicMock(data=[{"id": "org-1"}])
        )

        with (
            patch("app.main.get_supabase_client", return_value=mock_supabase),
            patch("app.main.httpx.AsyncClient") as mock_httpx,
        ):
            mock_client = AsyncMock()
            mock_client.get.return_value = MagicMock(status_code=401)
            mock_httpx.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_httpx.return_value.__aexit__ = AsyncMock(return_value=False)

            with TestClient(app) as client:
                resp = client.get("/health/ready")

        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ok"
        assert "database" in body["checks"]
        assert "claude_api" in body["checks"]
        assert body["checks"]["database"]["status"] == "ok"
        assert body["checks"]["claude_api"]["status"] == "ok"

    def test_ready_returns_503_when_db_down(self):
        from app.main import app

        with (
            patch("app.main.get_supabase_client", side_effect=Exception("Connection refused")),
            patch("app.main.httpx.AsyncClient") as mock_httpx,
        ):
            mock_client = AsyncMock()
            mock_client.get.return_value = MagicMock(status_code=401)
            mock_httpx.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_httpx.return_value.__aexit__ = AsyncMock(return_value=False)

            with TestClient(app) as client:
                resp = client.get("/health/ready")

        assert resp.status_code == 503
        body = resp.json()
        assert body["status"] == "degraded"
        assert body["checks"]["database"]["status"] == "error"

    def test_ready_includes_latency_ms(self):
        from app.main import app

        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.limit.return_value.execute.return_value = (
            MagicMock(data=[{"id": "org-1"}])
        )

        with (
            patch("app.main.get_supabase_client", return_value=mock_supabase),
            patch("app.main.httpx.AsyncClient") as mock_httpx,
        ):
            mock_client = AsyncMock()
            mock_client.get.return_value = MagicMock(status_code=401)
            mock_httpx.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_httpx.return_value.__aexit__ = AsyncMock(return_value=False)

            with TestClient(app) as client:
                resp = client.get("/health/ready")

        body = resp.json()
        assert "latency_ms" in body["checks"]["database"]
        assert isinstance(body["checks"]["database"]["latency_ms"], int)


# ---------------------------------------------------------------------------
# Structured logging tests
# ---------------------------------------------------------------------------


class TestJSONFormatter:
    def test_produces_valid_json(self):
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0, msg="hello", args=(), exc_info=None
        )
        output = formatter.format(record)
        data = json.loads(output)
        assert data["message"] == "hello"
        assert data["level"] == "INFO"
        assert data["logger"] == "test"
        assert "timestamp" in data

    def test_includes_extra_fields(self):
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0, msg="req", args=(), exc_info=None
        )
        record.request_id = "req_abc123"
        record.duration_ms = 150
        record.status_code = 200

        output = formatter.format(record)
        data = json.loads(output)
        assert data["request_id"] == "req_abc123"
        assert data["duration_ms"] == 150
        assert data["status_code"] == 200

    def test_excludes_none_extra_fields(self):
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0, msg="test", args=(), exc_info=None
        )
        output = formatter.format(record)
        data = json.loads(output)
        assert "request_id" not in data
        assert "duration_ms" not in data

    def test_includes_exception_info(self):
        formatter = JSONFormatter()
        try:
            raise ValueError("test error")
        except ValueError:
            import sys

            exc_info = sys.exc_info()

        record = logging.LogRecord(
            name="test", level=logging.ERROR, pathname="", lineno=0, msg="failed", args=(), exc_info=exc_info
        )
        output = formatter.format(record)
        data = json.loads(output)
        assert "exception" in data
        assert "ValueError" in data["exception"]


class TestConfigureLogging:
    def test_configure_sets_root_handler(self):
        configure_logging()
        root = logging.getLogger()
        assert len(root.handlers) > 0
        assert isinstance(root.handlers[0].formatter, JSONFormatter)


# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


class TestReadinessResponse:
    def test_model_structure(self):
        resp = ReadinessResponse(
            status="ok",
            checks={
                "database": CheckResult(status="ok", latency_ms=42),
                "claude_api": CheckResult(status="ok", latency_ms=150),
            },
        )
        data = resp.model_dump()
        assert data["status"] == "ok"
        assert data["checks"]["database"]["latency_ms"] == 42

    def test_degraded_with_error(self):
        resp = ReadinessResponse(
            status="degraded",
            checks={
                "database": CheckResult(status="error", latency_ms=0, error="Connection refused"),
            },
        )
        data = resp.model_dump()
        assert data["status"] == "degraded"
        assert data["checks"]["database"]["error"] == "Connection refused"
