"""Claude API client for structured content generation.

BJC-166: Model selection, structured output, retries, token management.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from typing import TypeVar

import anthropic
from pydantic import BaseModel

from app.config import settings

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)

# ---------------------------------------------------------------------------
# Model constants
# ---------------------------------------------------------------------------

MODEL_FAST = "claude-sonnet-4-20250514"  # Ad copy, email, image briefs, video scripts
MODEL_QUALITY = "claude-opus-4-20250514"  # Lead magnets, landing pages, case studies, carousels

QUALITY_ASSET_TYPES = {"lead_magnet", "landing_page", "case_study_page", "document_ad"}
FAST_ASSET_TYPES = {"ad_copy", "email_copy", "video_script", "image_brief"}

# Timeouts per model (seconds)
_TIMEOUTS = {
    MODEL_QUALITY: 120.0,
    MODEL_FAST: 30.0,
}

# Retry config
_MAX_RETRIES = 3
_RETRY_BASE_DELAY = 1.0  # seconds — exponential backoff: 1s, 2s, 4s
_RETRYABLE_STATUS_CODES = {429, 500, 529}


def get_model_for_asset_type(asset_type: str) -> str:
    """Return the appropriate Claude model for a given asset type."""
    if asset_type in QUALITY_ASSET_TYPES:
        return MODEL_QUALITY
    if asset_type in FAST_ASSET_TYPES:
        return MODEL_FAST
    # Default to fast for unknown types
    logger.warning("Unknown asset type %r — defaulting to MODEL_FAST", asset_type)
    return MODEL_FAST


# ---------------------------------------------------------------------------
# Response parsing utilities
# ---------------------------------------------------------------------------

def parse_json_from_response(text: str) -> dict:
    """Extract JSON from Claude's response.

    Handles:
    - JSON wrapped in <output>...</output> XML tags
    - JSON in markdown code fences (```json ... ```)
    - Raw JSON
    """
    # 1. Try XML tags first
    xml_match = re.search(r"<output>\s*(.*?)\s*</output>", text, re.DOTALL)
    if xml_match:
        text = xml_match.group(1)

    # 2. Try markdown code fences
    fence_match = re.search(r"```(?:json)?\s*\n?(.*?)\n?\s*```", text, re.DOTALL)
    if fence_match:
        text = fence_match.group(1)

    # 3. Try to find JSON object/array boundaries
    text = text.strip()
    if not text.startswith(("{", "[")):
        # Look for first { or [
        start = min(
            (text.find("{"), text.find("[")),
            key=lambda x: x if x >= 0 else float("inf"),
        )
        if isinstance(start, float):
            raise ValueError(f"No JSON found in response: {text[:200]}")
        text = text[start:]

    return json.loads(text)


def validate_against_schema(data: dict, schema: type[T]) -> T:
    """Validate parsed JSON against a Pydantic model."""
    return schema.model_validate(data)


# ---------------------------------------------------------------------------
# Claude client
# ---------------------------------------------------------------------------

class ClaudeClient:
    """Wrapper around the Anthropic SDK with retry, structured output, and token tracking."""

    MODEL_FAST = MODEL_FAST
    MODEL_QUALITY = MODEL_QUALITY

    def __init__(self, api_key: str | None = None):
        key = api_key or settings.ANTHROPIC_API_KEY
        self._client = anthropic.AsyncAnthropic(api_key=key)

    # --- Public API -------------------------------------------------------

    async def generate_structured(
        self,
        model: str,
        system_prompt: str,
        user_prompt: str,
        output_schema: type[T],
        temperature: float = 0.3,
        max_tokens: int = 4096,
        asset_type: str = "unknown",
    ) -> T:
        """Call Claude and return a validated Pydantic model instance.

        Uses XML tag prompting for reliable structured output.
        If initial JSON parsing fails, retries once with explicit instructions.
        """
        # Append output enforcement to system prompt
        schema_json = json.dumps(output_schema.model_json_schema(), indent=2)
        full_system = (
            f"{system_prompt}\n\n"
            "OUTPUT RULES:\n"
            "- Return your response inside <output> XML tags\n"
            "- The content inside <output> must be valid JSON matching the schema below\n"
            "- Do not include any text outside the <output> tags\n\n"
            f"OUTPUT SCHEMA:\n```json\n{schema_json}\n```"
        )

        raw = await self._call_api(
            model=model,
            system_prompt=full_system,
            user_prompt=user_prompt,
            temperature=temperature,
            max_tokens=max_tokens,
            asset_type=asset_type,
        )

        # Parse and validate
        try:
            data = parse_json_from_response(raw)
            return validate_against_schema(data, output_schema)
        except (json.JSONDecodeError, ValueError) as first_err:
            logger.warning(
                "JSON parsing failed for %s, retrying with explicit instruction: %s",
                asset_type,
                first_err,
            )
            # Error recovery: retry with explicit JSON instruction
            retry_prompt = (
                f"{user_prompt}\n\n"
                "IMPORTANT: Your previous response could not be parsed as valid JSON. "
                "Return ONLY valid JSON inside <output></output> tags. No other text."
            )
            raw = await self._call_api(
                model=model,
                system_prompt=full_system,
                user_prompt=retry_prompt,
                temperature=max(temperature - 0.1, 0.0),
                max_tokens=max_tokens,
                asset_type=asset_type,
            )
            data = parse_json_from_response(raw)
            return validate_against_schema(data, output_schema)

    async def generate_text(
        self,
        model: str,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.7,
        max_tokens: int = 4096,
        asset_type: str = "unknown",
    ) -> str:
        """Plain text generation for simpler assets."""
        return await self._call_api(
            model=model,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            temperature=temperature,
            max_tokens=max_tokens,
            asset_type=asset_type,
        )

    # --- Internal ---------------------------------------------------------

    async def _call_api(
        self,
        model: str,
        system_prompt: str,
        user_prompt: str,
        temperature: float,
        max_tokens: int,
        asset_type: str,
    ) -> str:
        """Call the Anthropic API with retry logic and token tracking."""
        timeout = _TIMEOUTS.get(model, 60.0)
        last_error: Exception | None = None

        for attempt in range(_MAX_RETRIES):
            try:
                start = time.monotonic()
                response = await self._client.messages.create(
                    model=model,
                    max_tokens=max_tokens,
                    temperature=temperature,
                    system=system_prompt,
                    messages=[{"role": "user", "content": user_prompt}],
                    timeout=timeout,
                )
                elapsed = time.monotonic() - start

                # Token usage logging
                usage = response.usage
                logger.info(
                    "claude_api_call",
                    extra={
                        "model": model,
                        "asset_type": asset_type,
                        "input_tokens": usage.input_tokens,
                        "output_tokens": usage.output_tokens,
                        "elapsed_seconds": round(elapsed, 2),
                        "attempt": attempt + 1,
                    },
                )

                # Extract text content
                text_blocks = [
                    block.text for block in response.content if block.type == "text"
                ]
                return "\n".join(text_blocks)

            except anthropic.RateLimitError:
                last_error = anthropic.RateLimitError("Rate limited")  # type: ignore[assignment]
                delay = _RETRY_BASE_DELAY * (2 ** attempt)
                logger.warning(
                    "Rate limited (attempt %d/%d), retrying in %.1fs",
                    attempt + 1,
                    _MAX_RETRIES,
                    delay,
                )
                await asyncio.sleep(delay)

            except anthropic.InternalServerError as exc:
                last_error = exc
                delay = _RETRY_BASE_DELAY * (2 ** attempt)
                logger.warning(
                    "Server error (attempt %d/%d): %s — retrying in %.1fs",
                    attempt + 1,
                    _MAX_RETRIES,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)

            except anthropic.APIStatusError as exc:
                # Only retry on retryable status codes
                if exc.status_code in _RETRYABLE_STATUS_CODES:
                    last_error = exc
                    delay = _RETRY_BASE_DELAY * (2 ** attempt)
                    logger.warning(
                        "Retryable API error %d (attempt %d/%d), retrying in %.1fs",
                        exc.status_code,
                        attempt + 1,
                        _MAX_RETRIES,
                        delay,
                    )
                    await asyncio.sleep(delay)
                else:
                    raise

            except anthropic.APITimeoutError as exc:
                last_error = exc
                logger.warning(
                    "Timeout (%.0fs) on attempt %d/%d for %s",
                    timeout,
                    attempt + 1,
                    _MAX_RETRIES,
                    asset_type,
                )
                if attempt < _MAX_RETRIES - 1:
                    await asyncio.sleep(_RETRY_BASE_DELAY)

        raise RuntimeError(
            f"Claude API call failed after {_MAX_RETRIES} attempts: {last_error}"
        ) from last_error
