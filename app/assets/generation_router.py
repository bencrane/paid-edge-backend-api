"""Asset API endpoints (BJC-55).

REST layer over the generation service: generate, preview, approve,
list assets for a campaign, get asset detail, update asset content.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from supabase import Client as SupabaseClient

from app.assets.service import AssetGenerationService
from app.auth.models import UserProfile
from app.dependencies import get_claude, get_current_user, get_supabase, get_tenant
from app.integrations.claude_ai import ClaudeClient
from app.shared.errors import BadRequestError
from app.tenants.models import Organization

router = APIRouter(prefix="/assets", tags=["assets"])


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class GenerateRequest(BaseModel):
    campaign_id: str
    asset_types: list[str]
    platforms: list[str] | None = None
    angle: str | None = None
    tone: str | None = None
    cta: str | None = None
    # Per-type config
    lead_magnet_format: str | None = None
    landing_page_template: str | None = None
    document_ad_pattern: str | None = None
    video_duration: str | None = None
    email_trigger: str | None = None


class GeneratedAssetResponse(BaseModel):
    id: str
    asset_type: str
    status: str
    content_url: str | None = None
    content_preview: dict | None = None
    template_used: str | None = None
    error: str | None = None


class ReviseRequest(BaseModel):
    revision_instructions: str


class AssetUpdateRequest(BaseModel):
    """Direct content edits — update stored content without AI re-generation."""

    content_json: dict | None = None
    content_url: str | None = None
    status: str | None = None


class AssetDetailResponse(BaseModel):
    id: str
    asset_type: str
    status: str
    content_url: str | None = None
    content_json: dict | None = None
    template_used: str | None = None
    campaign_id: str | None = None
    organization_id: str | None = None
    slug: str | None = None
    error_message: str | None = None
    created_at: str | None = None
    updated_at: str | None = None


# ---------------------------------------------------------------------------
# Service factory
# ---------------------------------------------------------------------------


def _get_service(
    claude: ClaudeClient = Depends(get_claude),
    supabase: SupabaseClient = Depends(get_supabase),
) -> AssetGenerationService:
    return AssetGenerationService(claude=claude, supabase=supabase)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("/generate", response_model=list[GeneratedAssetResponse])
async def generate_assets(
    body: GenerateRequest,
    user: UserProfile = Depends(get_current_user),
    tenant: Organization = Depends(get_tenant),
    service: AssetGenerationService = Depends(_get_service),
):
    """Generate one or more assets for a campaign."""
    results = await service.generate(
        org_id=str(tenant.id),
        campaign_id=body.campaign_id,
        asset_types=body.asset_types,
        user_id=str(user.id),
        platforms=body.platforms,
        angle=body.angle,
        tone=body.tone,
        cta=body.cta,
        lead_magnet_format=body.lead_magnet_format,
        landing_page_template=body.landing_page_template,
        document_ad_pattern=body.document_ad_pattern,
        video_duration=body.video_duration,
        email_trigger=body.email_trigger,
    )
    return results


@router.get("/{asset_id}", response_model=AssetDetailResponse)
async def get_asset(
    asset_id: str,
    user: UserProfile = Depends(get_current_user),
    tenant: Organization = Depends(get_tenant),
    service: AssetGenerationService = Depends(_get_service),
):
    """Get full asset detail."""
    return service.get_asset(asset_id, str(tenant.id))


@router.patch("/{asset_id}", response_model=AssetDetailResponse)
async def update_asset(
    asset_id: str,
    body: AssetUpdateRequest,
    user: UserProfile = Depends(get_current_user),
    tenant: Organization = Depends(get_tenant),
    service: AssetGenerationService = Depends(_get_service),
):
    """Update asset content directly (edit generated content without re-generation)."""
    return service.update_asset(
        asset_id=asset_id,
        org_id=str(tenant.id),
        content_json=body.content_json,
        content_url=body.content_url,
        status=body.status,
    )


@router.post("/{asset_id}/revise", response_model=GeneratedAssetResponse)
async def revise_asset(
    asset_id: str,
    body: ReviseRequest,
    user: UserProfile = Depends(get_current_user),
    tenant: Organization = Depends(get_tenant),
    service: AssetGenerationService = Depends(_get_service),
):
    """Revise an asset with new instructions (re-generates content via AI)."""
    return await service.revise_asset(
        asset_id=asset_id,
        revision_instructions=body.revision_instructions,
        org_id=str(tenant.id),
    )


@router.post("/{asset_id}/approve", response_model=AssetDetailResponse)
async def approve_asset(
    asset_id: str,
    user: UserProfile = Depends(get_current_user),
    tenant: Organization = Depends(get_tenant),
    service: AssetGenerationService = Depends(_get_service),
):
    """Approve a draft asset (draft → approved)."""
    return service.approve_asset(asset_id, str(tenant.id))


@router.get(
    "/campaigns/{campaign_id}",
    response_model=list[AssetDetailResponse],
)
async def list_campaign_assets(
    campaign_id: str,
    status: str | None = Query(None, description="Filter by asset status"),
    asset_type: str | None = Query(None, description="Filter by asset type"),
    user: UserProfile = Depends(get_current_user),
    tenant: Organization = Depends(get_tenant),
    service: AssetGenerationService = Depends(_get_service),
):
    """List all assets for a campaign with optional filters."""
    return service.list_campaign_assets(
        campaign_id=campaign_id,
        org_id=str(tenant.id),
        status=status,
        asset_type=asset_type,
    )
