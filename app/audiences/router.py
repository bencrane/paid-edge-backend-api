"""Audience push API endpoints (BJC-135)."""

from clickhouse_connect.driver import Client as CHClient
from fastapi import APIRouter, Depends
from supabase import Client as SupabaseClient

from app.audiences.linkedin_push import LinkedInAudiencePushService
from app.dependencies import get_clickhouse, get_supabase, get_tenant
from app.integrations.linkedin import LinkedInAdsClient
from app.tenants.models import Organization

router = APIRouter(prefix="/audiences", tags=["audiences"])


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
