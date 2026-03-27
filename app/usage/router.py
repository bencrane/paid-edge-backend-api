"""Usage reporting endpoint (CEX-40)."""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, ConfigDict
from supabase import Client as SupabaseClient

from app.auth.models import UserProfile
from app.dependencies import get_current_user, get_supabase, get_tenant
from app.tenants.models import Organization

router = APIRouter(prefix="/usage", tags=["usage"])


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class UsageEventOut(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str
    asset_type: str
    status: str
    duration_ms: int | None = None
    claude_tokens_input: int | None = None
    claude_tokens_output: int | None = None
    provider_costs: dict | None = None
    request_id: str | None = None
    created_at: str | None = None


class UsageSummary(BaseModel):
    total_generations: int
    successful: int
    failed: int
    total_tokens_input: int
    total_tokens_output: int


class UsageResponse(BaseModel):
    summary: UsageSummary
    events: list[UsageEventOut]


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------


@router.get("", response_model=UsageResponse)
async def get_usage(
    start_date: date | None = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: date | None = Query(None, description="End date (YYYY-MM-DD)"),
    asset_type: str | None = Query(None, description="Filter by asset type"),
    user: UserProfile = Depends(get_current_user),
    tenant: Organization = Depends(get_tenant),
    supabase: SupabaseClient = Depends(get_supabase),
):
    """Return usage summary and events for the current organization."""
    query = (
        supabase.table("usage_events")
        .select("*")
        .eq("org_id", str(tenant.id))
        .order("created_at", desc=True)
    )

    if start_date:
        query = query.gte("created_at", start_date.isoformat())
    if end_date:
        query = query.lte("created_at", f"{end_date.isoformat()}T23:59:59Z")
    if asset_type:
        query = query.eq("asset_type", asset_type)

    result = query.limit(500).execute()
    events = result.data or []

    # Compute summary
    successful = sum(1 for e in events if e.get("status") == "success")
    failed = sum(1 for e in events if e.get("status") == "failed")
    total_input = sum(e.get("claude_tokens_input") or 0 for e in events)
    total_output = sum(e.get("claude_tokens_output") or 0 for e in events)

    return UsageResponse(
        summary=UsageSummary(
            total_generations=len(events),
            successful=successful,
            failed=failed,
            total_tokens_input=total_input,
            total_tokens_output=total_output,
        ),
        events=[UsageEventOut(**e) for e in events],
    )
