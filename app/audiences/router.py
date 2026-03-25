"""Audience CRUD + push API endpoints (BJC-81, BJC-135)."""

from datetime import UTC, datetime

from clickhouse_connect.driver import Client as CHClient
from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from supabase import Client as SupabaseClient

from app.audiences.export import AudienceExportService
from app.audiences.linkedin_push import LinkedInAudiencePushService
from app.audiences.models import (
    AudienceCreate,
    AudienceDetailResponse,
    AudienceExportRequest,
    AudienceListResponse,
    AudienceMember,
    AudienceMemberListResponse,
    AudienceResponse,
    AudienceUpdate,
    SignalListResponse,
)
from app.audiences.service import (
    archive_segment,
    create_segment,
    export_members_csv,
    get_segment_members,
    get_segment_or_404,
    get_signal_cards,
    list_segments,
    update_segment,
)
from app.auth.models import UserProfile
from app.dependencies import get_clickhouse, get_current_user, get_supabase, get_tenant
from app.integrations.linkedin import LinkedInAdsClient
from app.shared.errors import BadRequestError, ConflictError
from app.tenants.models import Organization

router = APIRouter(prefix="/audiences", tags=["audiences"])


# --- 1. List segments ---


@router.get("", response_model=AudienceListResponse)
async def list_audiences(
    status: str | None = Query(None, description="Filter by status"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    include_archived: bool = Query(False),
    user: UserProfile = Depends(get_current_user),
    tenant: Organization = Depends(get_tenant),
    supabase: SupabaseClient = Depends(get_supabase),
):
    """List audience segments for the current tenant."""
    data, total = list_segments(
        supabase, tenant.id,
        status=status,
        include_archived=include_archived,
        limit=limit,
        offset=offset,
    )
    audiences = [AudienceResponse(**row) for row in data]
    return AudienceListResponse(data=audiences, total=total)


# --- 2. Create segment ---


@router.post("", response_model=AudienceResponse, status_code=201)
async def create_audience(
    body: AudienceCreate,
    user: UserProfile = Depends(get_current_user),
    tenant: Organization = Depends(get_tenant),
    supabase: SupabaseClient = Depends(get_supabase),
):
    """Create a new audience segment with structured filter_config."""
    insert_data = body.model_dump(exclude_none=True)
    row = create_segment(supabase, tenant.id, insert_data)
    return AudienceResponse(**row)


# --- Signals endpoint (must be before /{segment_id} routes) ---


@router.get("/signals", response_model=SignalListResponse)
async def get_audience_signals(
    user: UserProfile = Depends(get_current_user),
    tenant: Organization = Depends(get_tenant),
    supabase: SupabaseClient = Depends(get_supabase),
):
    """Signal cards for the dashboard — active segments with counts and trends."""
    cards = get_signal_cards(supabase, tenant.id)
    return SignalListResponse(data=cards)


# --- 3. Get segment detail ---


@router.get("/{segment_id}", response_model=AudienceDetailResponse)
async def get_audience(
    segment_id: str,
    user: UserProfile = Depends(get_current_user),
    tenant: Organization = Depends(get_tenant),
    supabase: SupabaseClient = Depends(get_supabase),
):
    """Get audience segment detail with member count."""
    row = get_segment_or_404(supabase, segment_id, tenant.id)
    return AudienceDetailResponse(**row)


# --- 4. Update segment ---


@router.patch("/{segment_id}", response_model=AudienceResponse)
async def update_audience(
    segment_id: str,
    body: AudienceUpdate,
    user: UserProfile = Depends(get_current_user),
    tenant: Organization = Depends(get_tenant),
    supabase: SupabaseClient = Depends(get_supabase),
):
    """Update an existing audience segment."""
    existing = get_segment_or_404(supabase, segment_id, tenant.id)
    if existing.get("status") == "archived":
        raise ConflictError(detail="Cannot update an archived segment")

    update_data = body.model_dump(exclude_none=True)
    if not update_data:
        return AudienceResponse(**existing)

    row = update_segment(supabase, segment_id, tenant.id, update_data)
    return AudienceResponse(**row)


# --- 5. Delete (archive) segment ---


@router.delete("/{segment_id}", status_code=204)
async def delete_audience(
    segment_id: str,
    user: UserProfile = Depends(get_current_user),
    tenant: Organization = Depends(get_tenant),
    supabase: SupabaseClient = Depends(get_supabase),
):
    """Soft-delete (archive) an audience segment."""
    get_segment_or_404(supabase, segment_id, tenant.id)
    archive_segment(supabase, segment_id, tenant.id)


# --- 6. Get segment members ---


@router.get("/{segment_id}/members", response_model=AudienceMemberListResponse)
async def get_audience_members(
    segment_id: str,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    user: UserProfile = Depends(get_current_user),
    tenant: Organization = Depends(get_tenant),
    supabase: SupabaseClient = Depends(get_supabase),
    clickhouse: CHClient = Depends(get_clickhouse),
):
    """Paginated member listing from ClickHouse."""
    get_segment_or_404(supabase, segment_id, tenant.id)
    members, total = get_segment_members(
        clickhouse, segment_id, tenant.id, limit=limit, offset=offset,
    )
    return AudienceMemberListResponse(
        data=[AudienceMember(**m) for m in members],
        total=total,
    )


# --- 7. Trigger manual refresh ---


@router.post("/{segment_id}/refresh")
async def refresh_audience(
    segment_id: str,
    user: UserProfile = Depends(get_current_user),
    tenant: Organization = Depends(get_tenant),
    supabase: SupabaseClient = Depends(get_supabase),
):
    """Trigger a manual refresh of an audience segment.

    Marks the segment for immediate refresh. The actual refresh is
    executed by the Trigger.dev audience refresh task (BJC-85).
    """
    existing = get_segment_or_404(supabase, segment_id, tenant.id)
    if existing.get("status") == "archived":
        raise ConflictError(detail="Cannot refresh an archived segment")

    # Mark as pending refresh
    supabase.table("audience_segments").update(
        {"refresh_requested_at": datetime.now(UTC).isoformat()}
    ).eq("id", segment_id).eq("organization_id", tenant.id).execute()

    return {"status": "refresh_queued", "segment_id": segment_id}


# --- 8. CSV export ---


@router.post("/{segment_id}/export")
async def export_audience(
    segment_id: str,
    body: AudienceExportRequest,
    user: UserProfile = Depends(get_current_user),
    tenant: Organization = Depends(get_tenant),
    supabase: SupabaseClient = Depends(get_supabase),
    clickhouse: CHClient = Depends(get_clickhouse),
):
    """Export segment members as CSV formatted for a specific ad platform."""
    existing = get_segment_or_404(supabase, segment_id, tenant.id)
    if body.platform not in ("linkedin", "meta", "google"):
        raise BadRequestError(detail="Platform must be linkedin, meta, or google")

    csv_content = export_members_csv(
        clickhouse, segment_id, tenant.id, body.platform,
    )
    return {
        "platform": body.platform,
        "segment_id": segment_id,
        "segment_name": existing.get("name", ""),
        "csv": csv_content,
    }


# --- Push endpoints (BJC-135 — existing) ---


@router.post("/{segment_id}/push/linkedin")
async def push_audience_to_linkedin(
    segment_id: str,
    strategy: str = "auto",
    tenant: Organization = Depends(get_tenant),
    supabase: SupabaseClient = Depends(get_supabase),
    clickhouse: CHClient = Depends(get_clickhouse),
):
    """Trigger manual LinkedIn audience push."""
    async with LinkedInAdsClient(
        org_id=tenant.id, supabase=supabase
    ) as client:
        account_id = await client.get_selected_account_id()
        service = LinkedInAudiencePushService(
            linkedin_client=client,
            supabase=supabase,
            clickhouse=clickhouse,
        )
        result = await service.push_segment(
            segment_id=segment_id,
            tenant_id=tenant.id,
            account_id=account_id,
            strategy=strategy,
        )
    return result.model_dump()


@router.get("/{segment_id}/push/linkedin/status")
async def get_linkedin_push_status(
    segment_id: str,
    tenant: Organization = Depends(get_tenant),
    supabase: SupabaseClient = Depends(get_supabase),
    clickhouse: CHClient = Depends(get_clickhouse),
):
    """Get LinkedIn sync status for a PaidEdge audience segment."""
    async with LinkedInAdsClient(
        org_id=tenant.id, supabase=supabase
    ) as client:
        service = LinkedInAudiencePushService(
            linkedin_client=client,
            supabase=supabase,
            clickhouse=clickhouse,
        )
        return await service.get_sync_status(
            segment_id=segment_id,
            tenant_id=tenant.id,
        )


# --- BJC-61: Audience CSV export per ad platform format ---


@router.post("/{segment_id}/export")
async def export_audience_csv(
    segment_id: str,
    format: str = Query(
        ...,
        description="Target ad platform: linkedin, meta, or google",
    ),
    tenant: Organization = Depends(get_tenant),
    supabase: SupabaseClient = Depends(get_supabase),
    clickhouse: CHClient = Depends(get_clickhouse),
):
    """Export audience segment as platform-specific CSV for manual upload."""
    service = AudienceExportService(supabase=supabase, clickhouse=clickhouse)
    filename, csv_bytes, row_count = service.export_segment(
        segment_id=segment_id,
        tenant_id=str(tenant.id),
        platform=format,
    )

    return StreamingResponse(
        iter([csv_bytes]),
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "X-Row-Count": str(row_count),
        },
    )
