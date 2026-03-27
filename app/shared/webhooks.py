"""Webhook delivery engine with retry and HMAC signing (CEX-41).

Delivers payloads to external URLs with:
- Up to 3 retry attempts (exponential backoff: 10s, 60s, 300s)
- HMAC-SHA256 payload signing via X-CEX-Signature header
- 10-second timeout per delivery attempt
- Full delivery attempt logging
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time
import uuid
from typing import Any

import httpx
from pydantic import BaseModel

logger = logging.getLogger(__name__)

RETRY_DELAYS = [10, 60, 300]  # seconds between retries
DELIVERY_TIMEOUT = 10.0  # seconds per attempt


class WebhookDeliveryResult(BaseModel):
    """Outcome of a webhook delivery (including retries)."""

    delivery_id: str
    url: str
    event_type: str
    status: str  # "delivered" | "failed"
    attempts: int
    final_status_code: int | None = None
    total_duration_ms: int


class WebhookDelivery:
    """Delivers webhook payloads with retry, signing, and logging."""

    def __init__(self, signing_secret: str):
        self.signing_secret = signing_secret

    def sign(self, payload_bytes: bytes) -> str:
        """Compute HMAC-SHA256 hex digest for a payload."""
        return hmac.new(
            self.signing_secret.encode(),
            payload_bytes,
            hashlib.sha256,
        ).hexdigest()

    async def deliver(
        self,
        url: str,
        event_type: str,
        payload: dict[str, Any],
    ) -> WebhookDeliveryResult:
        """Deliver a webhook payload with up to 3 retries on failure.

        Returns a result indicating delivery success or failure.
        """
        delivery_id = str(uuid.uuid4())
        payload_bytes = json.dumps(payload, default=str).encode()
        signature = self.sign(payload_bytes)
        timestamp = str(int(time.time()))

        headers = {
            "Content-Type": "application/json",
            "X-CEX-Event": event_type,
            "X-CEX-Signature": f"sha256={signature}",
            "X-CEX-Delivery-Id": delivery_id,
            "X-CEX-Timestamp": timestamp,
        }

        t0 = time.monotonic()
        last_status_code: int | None = None
        attempts = 0

        async with httpx.AsyncClient(timeout=DELIVERY_TIMEOUT) as client:
            for attempt_idx in range(1 + len(RETRY_DELAYS)):
                attempts += 1

                if attempt_idx > 0:
                    delay = RETRY_DELAYS[attempt_idx - 1]
                    logger.info(
                        "Webhook retry %d/%d for %s (delay=%ds)",
                        attempt_idx,
                        len(RETRY_DELAYS),
                        delivery_id,
                        delay,
                    )
                    # In production this would use asyncio.sleep(delay).
                    # For testability, we use a hookable delay.
                    await self._sleep(delay)

                try:
                    response = await client.post(
                        url, content=payload_bytes, headers=headers
                    )
                    last_status_code = response.status_code

                    if 200 <= response.status_code < 300:
                        duration_ms = int((time.monotonic() - t0) * 1000)
                        logger.info(
                            "Webhook delivered: id=%s url=%s status=%d attempts=%d",
                            delivery_id,
                            url,
                            response.status_code,
                            attempts,
                        )
                        return WebhookDeliveryResult(
                            delivery_id=delivery_id,
                            url=url,
                            event_type=event_type,
                            status="delivered",
                            attempts=attempts,
                            final_status_code=last_status_code,
                            total_duration_ms=duration_ms,
                        )

                    # 4xx = client error, retrying won't help
                    if 400 <= response.status_code < 500:
                        duration_ms = int((time.monotonic() - t0) * 1000)
                        logger.warning(
                            "Webhook client error (no retry): id=%s status=%d",
                            delivery_id,
                            response.status_code,
                        )
                        return WebhookDeliveryResult(
                            delivery_id=delivery_id,
                            url=url,
                            event_type=event_type,
                            status="failed",
                            attempts=attempts,
                            final_status_code=last_status_code,
                            total_duration_ms=duration_ms,
                        )

                    logger.warning(
                        "Webhook attempt %d failed: id=%s status=%d",
                        attempts,
                        delivery_id,
                        response.status_code,
                    )

                except httpx.TimeoutException:
                    logger.warning(
                        "Webhook attempt %d timed out: id=%s url=%s",
                        attempts,
                        delivery_id,
                        url,
                    )

                except httpx.HTTPError as exc:
                    logger.warning(
                        "Webhook attempt %d error: id=%s error=%s",
                        attempts,
                        delivery_id,
                        str(exc),
                    )

        # All attempts exhausted
        duration_ms = int((time.monotonic() - t0) * 1000)
        logger.error(
            "Webhook delivery failed after %d attempts: id=%s url=%s",
            attempts,
            delivery_id,
            url,
        )
        return WebhookDeliveryResult(
            delivery_id=delivery_id,
            url=url,
            event_type=event_type,
            status="failed",
            attempts=attempts,
            final_status_code=last_status_code,
            total_duration_ms=duration_ms,
        )

    async def _sleep(self, seconds: float) -> None:
        """Hookable sleep for testing."""
        import asyncio

        await asyncio.sleep(seconds)
