import logging
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, Query

from app.auth.models import UserProfile
from app.campaigns.models import (
    CampaignCreate,
    CampaignListResponse,
    CampaignResponse,
    CampaignUpdate,
)
from app.campaigns.platforms.linkedin import LinkedInPlatformAdapter
from app.dependencies import get_current_user, get_supabase, get_tenant
from app.integrations.linkedin import LinkedInAdsClient
from app.shared.errors import ConflictError, NotFoundError
from app.tenants.models import Organization

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/campaigns", tags=["campaigns"])


def _parse_campaign(row: dict) -> CampaignResponse:
    """Convert a DB row into a CampaignResponse, handling schedule JSONB."""
    return CampaignResponse(**row)


def _get_campaign_or_404(supabase, campaign_id: str, org_id: str) -> dict:
    """Fetch a single campaign scoped to tenant, raise 404 if missing."""
    res = (
        supabase.table("campaigns")
        .select("*")
        .eq("id", campaign_id)
        .eq("organization_id", org_id)
        .maybe_single()
        .execute()
    )
    if not res.data:
        raise NotFoundError(detail="Campaign not found")
    return res.data


# --- 1. List campaigns ---


@router.get("", response_model=CampaignListResponse)
async def list_campaigns(
    status: str | None = Query(None, description="Filter by status"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    include_archived: bool = Query(False),
    user: UserProfile = Depends(get_current_user),
    tenant: Organization = Depends(get_tenant),
    supabase=Depends(get_supabase),
):
    query = (
        supabase.table("campaigns")
        .select("*", count="exact")
        .eq("organization_id", tenant.id)
    )

    if not include_archived:
        query = query.is_("archived_at", "null")

    if status:
        query = query.eq("status", status)

    query = query.order("created_at", desc=True).range(offset, offset + limit - 1)
    res = query.execute()

    campaigns = [_parse_campaign(row) for row in res.data]
    return CampaignListResponse(data=campaigns, total=res.count or 0)


# --- 2. Create campaign ---


@router.post("", response_model=CampaignResponse, status_code=201)
async def create_campaign(
    body: CampaignCreate,
    user: UserProfile = Depends(get_current_user),
    tenant: Organization = Depends(get_tenant),
    supabase=Depends(get_supabase),
):
    insert_data = body.model_dump(exclude_none=True)
    insert_data["organization_id"] = tenant.id
    insert_data["status"] = "draft"

    # Convert schedule to dict for JSONB storage
    if "schedule" in insert_data and insert_data["schedule"] is not None:
        schedule = insert_data["schedule"]
        insert_data["schedule"] = {
            k: v.isoformat() if v is not None else None
            for k, v in schedule.items()
        }

    # Convert budget Decimal to str for JSON serialization
    if "budget" in insert_data and insert_data["budget"] is not None:
        insert_data["budget"] = float(insert_data["budget"])

    res = supabase.table("campaigns").insert(insert_data).execute()
    return _parse_campaign(res.data[0])


# --- 3. Get campaign detail ---


@router.get("/{campaign_id}", response_model=CampaignResponse)
async def get_campaign(
    campaign_id: str,
    user: UserProfile = Depends(get_current_user),
    tenant: Organization = Depends(get_tenant),
    supabase=Depends(get_supabase),
):
    row = _get_campaign_or_404(supabase, campaign_id, tenant.id)
    return _parse_campaign(row)


# --- 4. Update campaign ---


@router.patch("/{campaign_id}", response_model=CampaignResponse)
async def update_campaign(
    campaign_id: str,
    body: CampaignUpdate,
    user: UserProfile = Depends(get_current_user),
    tenant: Organization = Depends(get_tenant),
    supabase=Depends(get_supabase),
):
    existing = _get_campaign_or_404(supabase, campaign_id, tenant.id)

    if existing["status"] == "completed":
        raise ConflictError(detail="Cannot update a completed campaign")

    update_data = body.model_dump(exclude_none=True)
    if not update_data:
        return _parse_campaign(existing)

    # Convert schedule to dict for JSONB storage
    if "schedule" in update_data and update_data["schedule"] is not None:
        schedule = update_data["schedule"]
        update_data["schedule"] = {
            k: v.isoformat() if v is not None else None
            for k, v in schedule.items()
        }

    # Convert budget Decimal to float for JSON serialization
    if "budget" in update_data and update_data["budget"] is not None:
        update_data["budget"] = float(update_data["budget"])

    res = (
        supabase.table("campaigns")
        .update(update_data)
        .eq("id", campaign_id)
        .eq("organization_id", tenant.id)
        .execute()
    )
    return _parse_campaign(res.data[0])


# --- 5. Launch campaign (draft → active) ---


@router.post("/{campaign_id}/launch", response_model=CampaignResponse)
async def launch_campaign(
    campaign_id: str,
    user: UserProfile = Depends(get_current_user),
    tenant: Organization = Depends(get_tenant),
    supabase=Depends(get_supabase),
):
    existing = _get_campaign_or_404(supabase, campaign_id, tenant.id)

    if existing["status"] != "draft":
        raise ConflictError(
            detail="Can only launch a draft campaign. "
            "Allowed transition: draft → active"
        )

    # Validate: must have audience_segment_id
    if not existing.get("audience_segment_id"):
        raise ConflictError(detail="Campaign must have an audience segment before launching")

    # Validate: must have budget
    if not existing.get("budget"):
        raise ConflictError(detail="Campaign must have a budget before launching")

    # Validate: must have at least one generated asset
    assets_res = (
        supabase.table("generated_assets")
        .select("*")
        .eq("campaign_id", campaign_id)
        .execute()
    )
    if not assets_res.data:
        raise ConflictError(
            detail="Campaign must have at least one generated asset before launching"
        )

    # Update PaidEdge status to active
    res = (
        supabase.table("campaigns")
        .update({"status": "active"})
        .eq("id", campaign_id)
        .eq("organization_id", tenant.id)
        .execute()
    )

    # LinkedIn platform launch (additive — does not block PaidEdge status)
    platforms = existing.get("platforms", [])
    if "linkedin" in platforms:
        try:
            linkedin_client = LinkedInAdsClient(
                org_id=tenant.id, supabase=supabase
            )
            adapter = LinkedInPlatformAdapter(
                linkedin_client=linkedin_client,
                supabase=supabase,
            )

            # Load audience segment if present
            audience_segment = None
            if existing.get("audience_segment_id"):
                seg_res = (
                    supabase.table("audience_segments")
                    .select("*")
                    .eq("id", existing["audience_segment_id"])
                    .maybe_single()
                    .execute()
                )
                audience_segment = seg_res.data

            result = await adapter.launch_campaign(
                paidedge_campaign=existing,
                audience_segment=audience_segment,
                assets=assets_res.data,
            )

            if result.status == "error":
                logger.error(
                    "LinkedIn launch errors for campaign %s: %s",
                    campaign_id,
                    result.errors,
                )
        except Exception:
            logger.exception(
                "LinkedIn launch failed for campaign %s", campaign_id
            )

    return _parse_campaign(res.data[0])


# --- 6. Pause campaign (active → paused) ---


@router.post("/{campaign_id}/pause", response_model=CampaignResponse)
async def pause_campaign(
    campaign_id: str,
    user: UserProfile = Depends(get_current_user),
    tenant: Organization = Depends(get_tenant),
    supabase=Depends(get_supabase),
):
    existing = _get_campaign_or_404(supabase, campaign_id, tenant.id)

    if existing["status"] != "active":
        raise ConflictError(
            detail="Can only pause an active campaign. "
            "Allowed transition: active → paused"
        )

    res = (
        supabase.table("campaigns")
        .update({"status": "paused"})
        .eq("id", campaign_id)
        .eq("organization_id", tenant.id)
        .execute()
    )
    return _parse_campaign(res.data[0])


# --- 7. Resume campaign (paused → active) ---


@router.post("/{campaign_id}/resume", response_model=CampaignResponse)
async def resume_campaign(
    campaign_id: str,
    user: UserProfile = Depends(get_current_user),
    tenant: Organization = Depends(get_tenant),
    supabase=Depends(get_supabase),
):
    existing = _get_campaign_or_404(supabase, campaign_id, tenant.id)

    if existing["status"] != "paused":
        raise ConflictError(
            detail="Can only resume a paused campaign. "
            "Allowed transition: paused → active"
        )

    res = (
        supabase.table("campaigns")
        .update({"status": "active"})
        .eq("id", campaign_id)
        .eq("organization_id", tenant.id)
        .execute()
    )
    return _parse_campaign(res.data[0])


# --- 8. Complete campaign (active/paused → completed) ---


@router.post("/{campaign_id}/complete", response_model=CampaignResponse)
async def complete_campaign(
    campaign_id: str,
    user: UserProfile = Depends(get_current_user),
    tenant: Organization = Depends(get_tenant),
    supabase=Depends(get_supabase),
):
    existing = _get_campaign_or_404(supabase, campaign_id, tenant.id)

    if existing["status"] not in ("active", "paused"):
        raise ConflictError(
            detail="Can only complete an active or paused campaign. "
            "Allowed transitions: active → completed, paused → completed"
        )

    res = (
        supabase.table("campaigns")
        .update({"status": "completed"})
        .eq("id", campaign_id)
        .eq("organization_id", tenant.id)
        .execute()
    )
    return _parse_campaign(res.data[0])


# --- 9. Soft delete (archive) campaign ---


@router.delete("/{campaign_id}", status_code=204)
async def delete_campaign(
    campaign_id: str,
    user: UserProfile = Depends(get_current_user),
    tenant: Organization = Depends(get_tenant),
    supabase=Depends(get_supabase),
):
    existing = _get_campaign_or_404(supabase, campaign_id, tenant.id)

    if existing["status"] != "draft":
        raise ConflictError(
            detail="Can only archive a draft campaign. "
            "Allowed transition: draft → archived"
        )

    supabase.table("campaigns").update(
        {"archived_at": datetime.now(UTC).isoformat()}
    ).eq("id", campaign_id).eq("organization_id", tenant.id).execute()
