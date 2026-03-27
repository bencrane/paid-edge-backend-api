"""Landing page hosting + form handling (BJC-57).

Public endpoints (no auth):
- GET /lp/{slug} — serve generated HTML with RudderStack JS SDK injected
- POST /lp/{slug}/submit — handle form submission, fire RudderStack identify + track

Landing pages are customer-facing: no JWT required, CORS configured for public access.
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from typing import Any

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

from app.config import settings
from app.db.supabase import get_supabase_client
from app.shared.errors import NotFoundError

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/lp", tags=["landing_pages"])

# --- Simple per-IP rate limiting for public form submissions ---
_FORM_SUBMIT_RPM = 10  # max form submissions per IP per minute
_form_counters: dict[str, list[float]] = {}
_form_cleanup_at = 0.0


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

RUDDERSTACK_DATA_PLANE_URL = getattr(settings, "RUDDERSTACK_DATA_PLANE_URL", "")
RUDDERSTACK_WRITE_KEY = getattr(settings, "RUDDERSTACK_WRITE_KEY", "")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_asset_by_slug(slug: str) -> dict:
    """Look up a landing page / case study page by slug."""
    supabase = get_supabase_client()
    res = (
        supabase.table("generated_assets")
        .select("id, organization_id, campaign_id, content_url, input_data, template_used, slug")
        .eq("slug", slug)
        .maybe_single()
        .execute()
    )
    if not res.data:
        raise NotFoundError(detail="Landing page not found")
    return res.data


async def _fire_rudderstack_identify(
    anonymous_id: str,
    user_id: str,
    traits: dict[str, Any],
) -> None:
    """Server-side RudderStack identify() — merge anonymous → known identity."""
    if not RUDDERSTACK_DATA_PLANE_URL or not RUDDERSTACK_WRITE_KEY:
        logger.debug("RudderStack not configured — skipping identify")
        return

    payload = {
        "anonymousId": anonymous_id,
        "userId": user_id,
        "traits": traits,
    }
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{RUDDERSTACK_DATA_PLANE_URL}/v1/identify",
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Basic {RUDDERSTACK_WRITE_KEY}",
                },
                timeout=5.0,
            )
            resp.raise_for_status()
    except Exception:
        logger.exception("RudderStack identify failed")


async def _fire_rudderstack_track(
    anonymous_id: str,
    user_id: str,
    event: str,
    properties: dict[str, Any],
) -> None:
    """Server-side RudderStack track() — fire event."""
    if not RUDDERSTACK_DATA_PLANE_URL or not RUDDERSTACK_WRITE_KEY:
        logger.debug("RudderStack not configured — skipping track")
        return

    payload = {
        "anonymousId": anonymous_id,
        "userId": user_id,
        "event": event,
        "properties": properties,
    }
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{RUDDERSTACK_DATA_PLANE_URL}/v1/track",
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Basic {RUDDERSTACK_WRITE_KEY}",
                },
                timeout=5.0,
            )
            resp.raise_for_status()
    except Exception:
        logger.exception("RudderStack track failed")


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/{slug}", response_class=HTMLResponse)
async def serve_landing_page(slug: str, request: Request):
    """Serve a published landing page by its slug (public, no auth)."""
    asset = _get_asset_by_slug(slug)
    content_url = asset.get("content_url")

    if content_url:
        # Fetch rendered HTML from Supabase Storage
        async with httpx.AsyncClient() as client:
            resp = await client.get(content_url)
            resp.raise_for_status()
        return HTMLResponse(content=resp.text, status_code=200)

    # Fallback: render from input_data using Jinja2 templates
    input_data = asset.get("input_data")
    template_used = asset.get("template_used")
    if input_data and template_used:
        from pathlib import Path

        from jinja2 import Environment, FileSystemLoader

        templates_dir = Path(__file__).parent.parent / "assets" / "templates"
        env = Environment(loader=FileSystemLoader(str(templates_dir)), autoescape=True)
        template = env.get_template(f"{template_used}.html")
        html = template.render(slug=slug, **input_data)
        return HTMLResponse(content=html, status_code=200)

    raise NotFoundError(detail="Landing page content not available")


class FormSubmission(BaseModel):
    """Schema for landing page form data — all fields dynamic except known keys."""

    model_config = {"extra": "allow"}


@router.post("/{slug}/submit")
async def submit_landing_page_form(slug: str, request: Request):
    """Handle form submission from a landing page (public, no auth).

    - Stores submission in Supabase
    - Captures UTM parameters
    - Fires RudderStack identify() to merge anonymous → known identity
    - Fires RudderStack track() for form_submitted event
    """
    # Per-IP rate limiting for public form submissions
    global _form_cleanup_at
    client_ip = request.client.host if request.client else "unknown"
    now = time.monotonic()

    # Periodic cleanup
    if now - _form_cleanup_at > 300:
        cutoff = now - 60
        stale = [ip for ip, ts in _form_counters.items() if not ts or ts[-1] <= cutoff]
        for ip in stale:
            del _form_counters[ip]
        _form_cleanup_at = now

    timestamps = _form_counters.setdefault(client_ip, [])
    timestamps[:] = [t for t in timestamps if t > now - 60]
    if len(timestamps) >= _FORM_SUBMIT_RPM:
        return JSONResponse(
            status_code=429,
            content={
                "error": {
                    "type": "rate_limit_exceeded",
                    "message": "Too many form submissions. Please try again later.",
                }
            },
        )
    timestamps.append(now)

    asset = _get_asset_by_slug(slug)
    body = await request.json()

    # Extract known fields
    email = body.get("email", "")
    anonymous_id = body.get("anonymous_id", "") or body.get("anonymousId", "")

    # Extract UTM parameters from form hidden fields
    utm_params = {
        k: body.get(k, "")
        for k in ("utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content")
        if body.get(k)
    }

    # Build submission record
    submission_record = {
        "asset_id": asset["id"],
        "slug": slug,
        "form_data": body,
        "utm_params": utm_params,
        "organization_id": asset.get("organization_id"),
        "campaign_id": asset.get("campaign_id"),
        "submitted_at": datetime.now(UTC).isoformat(),
    }

    # Store in Supabase
    supabase = get_supabase_client()
    supabase.table("landing_page_submissions").insert(submission_record).execute()

    # Fire RudderStack identify — merge anonymous visitor to known identity
    if email:
        traits = {k: v for k, v in body.items() if k not in ("anonymous_id", "anonymousId")}
        traits["email"] = email
        await _fire_rudderstack_identify(
            anonymous_id=anonymous_id or email,
            user_id=email,
            traits=traits,
        )

    # Fire RudderStack track — form_submitted event
    track_properties = {
        "slug": slug,
        "template": asset.get("template_used", ""),
        "campaign_id": asset.get("campaign_id", ""),
        "organization_id": asset.get("organization_id", ""),
        **utm_params,
    }
    await _fire_rudderstack_track(
        anonymous_id=anonymous_id or email or "unknown",
        user_id=email or "",
        event="form_submitted",
        properties=track_properties,
    )

    return JSONResponse(
        content={"status": "ok", "message": "Form submitted successfully"},
        status_code=200,
    )
