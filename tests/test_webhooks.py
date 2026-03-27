"""Tests for webhook delivery engine (CEX-41)."""

from __future__ import annotations

import hashlib
import hmac
import json
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from app.shared.webhooks import (
    DELIVERY_TIMEOUT,
    RETRY_DELAYS,
    WebhookDelivery,
    WebhookDeliveryResult,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SIGNING_SECRET = "test-webhook-secret-key"
WEBHOOK_URL = "https://example.com/webhook"
EVENT_TYPE = "job.completed"
PAYLOAD = {"job_id": "abc-123", "status": "completed", "artifact_type": "ad_copy"}


def _make_delivery() -> WebhookDelivery:
    delivery = WebhookDelivery(signing_secret=SIGNING_SECRET)
    # Disable actual sleep in tests
    delivery._sleep = AsyncMock()
    return delivery


# ---------------------------------------------------------------------------
# Signature
# ---------------------------------------------------------------------------


class TestSignature:
    def test_hmac_sha256_signature(self):
        delivery = WebhookDelivery(signing_secret=SIGNING_SECRET)
        payload_bytes = json.dumps(PAYLOAD).encode()
        sig = delivery.sign(payload_bytes)

        expected = hmac.new(
            SIGNING_SECRET.encode(), payload_bytes, hashlib.sha256
        ).hexdigest()
        assert sig == expected

    def test_different_payloads_produce_different_signatures(self):
        delivery = WebhookDelivery(signing_secret=SIGNING_SECRET)
        sig1 = delivery.sign(b'{"a": 1}')
        sig2 = delivery.sign(b'{"a": 2}')
        assert sig1 != sig2

    def test_different_secrets_produce_different_signatures(self):
        d1 = WebhookDelivery(signing_secret="secret-1")
        d2 = WebhookDelivery(signing_secret="secret-2")
        payload = b'{"data": "test"}'
        assert d1.sign(payload) != d2.sign(payload)


# ---------------------------------------------------------------------------
# Delivery — success
# ---------------------------------------------------------------------------


class TestDeliverySuccess:
    async def test_successful_delivery_on_first_attempt(self):
        delivery = _make_delivery()

        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch("app.shared.webhooks.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_response
            mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            result = await delivery.deliver(WEBHOOK_URL, EVENT_TYPE, PAYLOAD)

        assert result.status == "delivered"
        assert result.attempts == 1
        assert result.final_status_code == 200
        assert result.url == WEBHOOK_URL
        assert result.event_type == EVENT_TYPE

    async def test_headers_include_signature_and_event(self):
        delivery = _make_delivery()

        mock_response = MagicMock()
        mock_response.status_code = 200
        captured_headers = {}

        async def capture_post(url, content, headers):
            captured_headers.update(headers)
            return mock_response

        with patch("app.shared.webhooks.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post = capture_post
            mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            await delivery.deliver(WEBHOOK_URL, EVENT_TYPE, PAYLOAD)

        assert "X-CEX-Signature" in captured_headers
        assert captured_headers["X-CEX-Signature"].startswith("sha256=")
        assert captured_headers["X-CEX-Event"] == EVENT_TYPE
        assert "X-CEX-Delivery-Id" in captured_headers
        assert "X-CEX-Timestamp" in captured_headers
        assert captured_headers["Content-Type"] == "application/json"


# ---------------------------------------------------------------------------
# Delivery — retry on failure
# ---------------------------------------------------------------------------


class TestDeliveryRetry:
    async def test_retries_on_500_status(self):
        delivery = _make_delivery()

        fail_response = MagicMock()
        fail_response.status_code = 500
        ok_response = MagicMock()
        ok_response.status_code = 200

        with patch("app.shared.webhooks.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.side_effect = [fail_response, fail_response, ok_response]
            mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            result = await delivery.deliver(WEBHOOK_URL, EVENT_TYPE, PAYLOAD)

        assert result.status == "delivered"
        assert result.attempts == 3
        assert result.final_status_code == 200

    async def test_retries_on_timeout(self):
        delivery = _make_delivery()

        ok_response = MagicMock()
        ok_response.status_code = 200

        with patch("app.shared.webhooks.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.side_effect = [
                httpx.TimeoutException("timeout"),
                ok_response,
            ]
            mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            result = await delivery.deliver(WEBHOOK_URL, EVENT_TYPE, PAYLOAD)

        assert result.status == "delivered"
        assert result.attempts == 2

    async def test_all_retries_exhausted(self):
        delivery = _make_delivery()

        fail_response = MagicMock()
        fail_response.status_code = 500

        with patch("app.shared.webhooks.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = fail_response
            mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            result = await delivery.deliver(WEBHOOK_URL, EVENT_TYPE, PAYLOAD)

        assert result.status == "failed"
        assert result.attempts == 4  # 1 initial + 3 retries
        assert result.final_status_code == 500

    async def test_retry_delays_respected(self):
        """Verify sleep is called with correct backoff delays."""
        delivery = _make_delivery()

        fail_response = MagicMock()
        fail_response.status_code = 500

        with patch("app.shared.webhooks.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = fail_response
            mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            await delivery.deliver(WEBHOOK_URL, EVENT_TYPE, PAYLOAD)

        sleep_calls = [c.args[0] for c in delivery._sleep.call_args_list]
        assert sleep_calls == RETRY_DELAYS


# ---------------------------------------------------------------------------
# Delivery — error handling
# ---------------------------------------------------------------------------


class TestDeliveryErrors:
    async def test_http_error_triggers_retry(self):
        delivery = _make_delivery()

        ok_response = MagicMock()
        ok_response.status_code = 200

        with patch("app.shared.webhooks.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.side_effect = [
                httpx.ConnectError("connection refused"),
                ok_response,
            ]
            mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            result = await delivery.deliver(WEBHOOK_URL, EVENT_TYPE, PAYLOAD)

        assert result.status == "delivered"
        assert result.attempts == 2


# ---------------------------------------------------------------------------
# WebhookDeliveryResult model
# ---------------------------------------------------------------------------


class TestWebhookDeliveryResult:
    def test_result_model(self):
        result = WebhookDeliveryResult(
            delivery_id="test-id",
            url=WEBHOOK_URL,
            event_type=EVENT_TYPE,
            status="delivered",
            attempts=1,
            final_status_code=200,
            total_duration_ms=150,
        )
        assert result.delivery_id == "test-id"
        assert result.status == "delivered"

    def test_result_failed_no_status_code(self):
        """Failed delivery with no response (e.g., all timeouts)."""
        result = WebhookDeliveryResult(
            delivery_id="test-id",
            url=WEBHOOK_URL,
            event_type=EVENT_TYPE,
            status="failed",
            attempts=4,
            final_status_code=None,
            total_duration_ms=5000,
        )
        assert result.final_status_code is None
