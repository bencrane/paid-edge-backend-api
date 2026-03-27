"""Usage event recording for billing and analytics (CEX-40).

Fire-and-forget inserts — tracking failures never break generation.
"""

from __future__ import annotations

import logging

from pydantic import BaseModel
from supabase import Client as SupabaseClient

logger = logging.getLogger(__name__)


class UsageEvent(BaseModel):
    """Per-request usage metrics."""

    org_id: str
    user_id: str
    asset_type: str
    status: str  # "success" | "failed"
    duration_ms: int | None = None
    claude_tokens_input: int | None = None
    claude_tokens_output: int | None = None
    provider_costs: dict | None = None
    request_id: str | None = None


def record_usage_event(event: UsageEvent, supabase: SupabaseClient) -> None:
    """Insert a usage event into Supabase. Never raises."""
    try:
        supabase.table("usage_events").insert(event.model_dump()).execute()
    except Exception:
        logger.exception("Failed to record usage event: %s", event.asset_type)
