"""LinkedIn campaign launch orchestration — PaidEdge → LinkedIn end-to-end (BJC-133)."""

import logging
from typing import Any

import httpx
from pydantic import BaseModel

from app.audiences.linkedin_push import LinkedInAudiencePushService
from app.integrations.linkedin import (
    LinkedInAdsClient,
    LinkedInAPIError,
    extract_id_from_urn,
)
from app.integrations.linkedin_targeting import build_targeting_criteria

logger = logging.getLogger(__name__)


class LinkedInLaunchResult(BaseModel):
    platform: str = "linkedin"
    campaign_group_id: int
    campaign_id: int
    creative_ids: list[int]
    audience_segment_id: int | None = None
    tracked_link_url: str | None = None
    status: str  # 'launched', 'pending_review', 'error'
    errors: list[str] | None = None


def _detect_asset_type(asset: dict) -> str:
    """Detect media type from asset metadata or file extension."""
    asset_type = asset.get("asset_type", "").lower()
    if asset_type in ("image", "document", "video"):
        return asset_type

    url = asset.get("storage_url", asset.get("url", ""))
    lower_url = url.lower()
    if lower_url.endswith((".pdf", ".doc", ".docx", ".pptx")):
        return "document"
    if lower_url.endswith((".mp4", ".mov", ".avi", ".webm")):
        return "video"
    return "image"


async def _download_asset(url: str) -> bytes:
    """Download asset bytes from Supabase storage URL."""
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.content


class LinkedInPlatformAdapter:
    """Orchestrates PaidEdge campaign → LinkedIn campaign launch."""

    def __init__(
        self,
        linkedin_client: LinkedInAdsClient,
        supabase=None,
        clickhouse=None,
    ):
        self.client = linkedin_client
        self.supabase = supabase
        self.clickhouse = clickhouse

    async def launch_campaign(
        self,
        paidedge_campaign: dict,
        audience_segment: dict | None,
        assets: list[dict],
    ) -> LinkedInLaunchResult:
        """Full launch orchestration.

        Steps:
        1. Get tenant's selected LinkedIn ad account ID
        2. Create campaign group (or reuse existing)
        3. If audience_segment: push to LinkedIn Matched Audiences
        4. Build targeting criteria from campaign config + audience segment
        5. Create LinkedIn campaign (DRAFT status initially)
        6. For each asset: upload media → create post → create creative
        7. Set campaign status to ACTIVE
        8. Store LinkedIn entity IDs back to PaidEdge campaigns.platforms JSONB
        9. Return LinkedInLaunchResult
        """
        errors: list[str] = []
        creative_ids: list[int] = []
        audience_segment_id: int | None = None
        campaign_group_id: int | None = None
        campaign_id: int | None = None
        created_group = False

        try:
            # Step 1: Get ad account
            account_id = await self.client.get_selected_account_id()
            org_id = int(self.client.org_id)

            # Step 2: Create or reuse campaign group
            campaign_group_id, created_group = await self._get_or_create_group(
                account_id, paidedge_campaign
            )
            logger.info(
                "Campaign group %d (%s) for campaign %s",
                campaign_group_id,
                "created" if created_group else "reused",
                paidedge_campaign.get("id"),
            )

            # Step 3: Push audience if segment exists
            matched_audience_urns: list[str] = []
            if audience_segment and self.supabase and self.clickhouse:
                audience_segment_id, matched_audience_urns = (
                    await self._push_audience(
                        account_id, audience_segment
                    )
                )

            # Step 4: Build targeting criteria
            config = paidedge_campaign.get("config", {})
            targeting = build_targeting_criteria(
                locations=config.get("locations"),
                industries=config.get("industries"),
                seniorities=config.get("seniorities"),
                job_functions=config.get("job_functions"),
                company_sizes=config.get("company_sizes"),
                matched_audiences=matched_audience_urns or None,
            )

            # Step 5: Create LinkedIn campaign in DRAFT
            objective = config.get("objective", "WEBSITE_VISITS")
            campaign_type = config.get("campaign_type", "SPONSORED_UPDATES")
            cost_type = config.get("cost_type", "CPC")
            budget = paidedge_campaign.get("budget", 50)
            daily_budget = {
                "currencyCode": config.get("currency", "USD"),
                "amount": str(budget),
            }

            campaign_resp = await self.client.create_campaign(
                account_id=account_id,
                campaign_group_id=campaign_group_id,
                name=paidedge_campaign.get("name", "PaidEdge Campaign"),
                campaign_type=campaign_type,
                objective=objective,
                targeting=targeting,
                daily_budget=daily_budget,
                cost_type=cost_type,
                status="DRAFT",
            )
            campaign_id = extract_id_from_urn(
                campaign_resp.get("id", campaign_resp.get("urn", ""))
            )
            logger.info("Created LinkedIn campaign %d", campaign_id)

            # Step 6: Upload assets and create creatives
            for asset in assets:
                try:
                    creative_result = await self._upload_asset_and_create_creative(
                        account_id=account_id,
                        campaign_id=campaign_id,
                        org_id=org_id,
                        asset=asset,
                    )
                    cid = extract_id_from_urn(
                        creative_result.get(
                            "id", creative_result.get("urn", "")
                        )
                    )
                    creative_ids.append(cid)
                    logger.info("Created creative %d for asset %s", cid, asset.get("id"))
                except (LinkedInAPIError, httpx.HTTPError) as exc:
                    error_msg = f"Creative upload failed for asset {asset.get('id')}: {exc}"
                    logger.error(error_msg)
                    errors.append(error_msg)

            if not creative_ids:
                # All creatives failed — mark as error
                return LinkedInLaunchResult(
                    campaign_group_id=campaign_group_id,
                    campaign_id=campaign_id,
                    creative_ids=[],
                    audience_segment_id=audience_segment_id,
                    status="error",
                    errors=errors or ["All creative uploads failed"],
                )

            # Step 7: Activate campaign
            await self.client.update_campaign_status(
                account_id, campaign_id, "ACTIVE"
            )

            # Step 8: Store LinkedIn entity IDs back to PaidEdge
            platform_data = {
                "platform": "linkedin",
                "platform_campaign_id": campaign_id,
                "platform_campaign_group_id": campaign_group_id,
                "platform_creative_ids": creative_ids,
                "platform_account_id": account_id,
            }
            if audience_segment_id:
                platform_data["platform_audience_segment_id"] = audience_segment_id

            await self._store_platform_data(
                paidedge_campaign["id"], platform_data
            )

            status = "launched"
            if errors:
                status = "pending_review"

            return LinkedInLaunchResult(
                campaign_group_id=campaign_group_id,
                campaign_id=campaign_id,
                creative_ids=creative_ids,
                audience_segment_id=audience_segment_id,
                status=status,
                errors=errors or None,
            )

        except LinkedInAPIError as exc:
            logger.error("LinkedIn launch failed: %s", exc)
            # If campaign group was just created and campaign creation failed,
            # we don't delete it — keep for debugging
            return LinkedInLaunchResult(
                campaign_group_id=campaign_group_id or 0,
                campaign_id=campaign_id or 0,
                creative_ids=creative_ids,
                audience_segment_id=audience_segment_id,
                status="error",
                errors=[str(exc)] + errors,
            )

    async def _get_or_create_group(
        self,
        account_id: int,
        paidedge_campaign: dict,
    ) -> tuple[int, bool]:
        """Create campaign group or reuse existing one named after PaidEdge campaign."""
        campaign_name = paidedge_campaign.get("name", "PaidEdge Campaign")
        group_name = f"PaidEdge: {campaign_name}"

        # Check for existing group with same name
        existing_groups = await self.client.get_campaign_groups(account_id)
        for group in existing_groups:
            if group.name == group_name:
                return group.id, False

        # Create new group
        resp = await self.client.create_campaign_group(
            account_id=account_id,
            name=group_name,
        )
        group_id = extract_id_from_urn(resp.get("id", resp.get("urn", "")))
        return group_id, True

    async def _push_audience(
        self,
        account_id: int,
        audience_segment: dict,
    ) -> tuple[int | None, list[str]]:
        """Push audience segment to LinkedIn Matched Audiences."""
        push_service = LinkedInAudiencePushService(
            linkedin_client=self.client,
            supabase=self.supabase,
            clickhouse=self.clickhouse,
        )
        segment_id = audience_segment.get("id", "")
        tenant_id = audience_segment.get("organization_id", self.client.org_id)

        result = await push_service.push_segment(
            segment_id=segment_id,
            tenant_id=tenant_id,
            account_id=account_id,
        )

        # If there's an ad_segment_urn, use it for targeting
        matched_audience_urns: list[str] = []
        if result.ad_segment_urn:
            matched_audience_urns.append(result.ad_segment_urn)

        ad_segment_id: int | None = None
        if result.segment_id:
            try:
                ad_segment_id = int(result.segment_id)
            except (ValueError, TypeError):
                ad_segment_id = None

        return ad_segment_id, matched_audience_urns

    async def _upload_asset_and_create_creative(
        self,
        account_id: int,
        campaign_id: int,
        org_id: int,
        asset: dict,
    ) -> dict[str, Any]:
        """Upload a single asset and create the corresponding creative."""
        asset_type = _detect_asset_type(asset)
        storage_url = asset.get("storage_url", asset.get("url", ""))
        asset_bytes = await _download_asset(storage_url)

        headline = asset.get("headline", asset.get("title", ""))
        intro_text = asset.get("intro_text", asset.get("commentary", ""))

        if asset_type == "document":
            return await self.client.create_document_ad(
                account_id=account_id,
                campaign_id=campaign_id,
                org_id=org_id,
                pdf_bytes=asset_bytes,
                title=headline,
                commentary=intro_text,
            )
        elif asset_type == "video":
            video_urn = await self.client.upload_video(
                org_id, asset_bytes, len(asset_bytes)
            )
            post_urn = await self.client.create_sponsored_post(
                org_id=org_id,
                commentary=intro_text,
                media_urn=video_urn,
                media_title=headline,
            )
            return await self.client.create_creative(
                account_id=account_id,
                campaign_id=campaign_id,
                post_urn=post_urn,
            )
        else:
            # Default: image
            return await self.client.create_image_ad(
                account_id=account_id,
                campaign_id=campaign_id,
                org_id=org_id,
                image_bytes=asset_bytes,
                headline=headline,
                intro_text=intro_text,
            )

    async def _store_platform_data(
        self, campaign_id: str, platform_data: dict
    ) -> None:
        """Store LinkedIn entity IDs back to PaidEdge campaign."""
        if not self.supabase:
            return
        self.supabase.table("campaigns").update(
            {"platform_data": platform_data}
        ).eq("id", campaign_id).execute()

    async def pause_campaign(self, paidedge_campaign: dict) -> None:
        """Pause the LinkedIn campaign by reading platform IDs from campaign."""
        platform_data = paidedge_campaign.get("platform_data", {})
        account_id = platform_data.get("platform_account_id")
        campaign_id = platform_data.get("platform_campaign_id")

        if not account_id or not campaign_id:
            raise LinkedInAPIError(
                400, None, "No LinkedIn platform data found on campaign"
            )

        await self.client.update_campaign_status(
            account_id, campaign_id, "PAUSED"
        )

    async def resume_campaign(self, paidedge_campaign: dict) -> None:
        """Resume a paused LinkedIn campaign."""
        platform_data = paidedge_campaign.get("platform_data", {})
        account_id = platform_data.get("platform_account_id")
        campaign_id = platform_data.get("platform_campaign_id")

        if not account_id or not campaign_id:
            raise LinkedInAPIError(
                400, None, "No LinkedIn platform data found on campaign"
            )

        await self.client.update_campaign_status(
            account_id, campaign_id, "ACTIVE"
        )

    async def get_campaign_status(self, paidedge_campaign: dict) -> dict:
        """Get current status from LinkedIn."""
        platform_data = paidedge_campaign.get("platform_data", {})
        account_id = platform_data.get("platform_account_id")
        campaign_id = platform_data.get("platform_campaign_id")

        if not account_id or not campaign_id:
            return {"status": "not_launched", "linkedin_campaign_id": None}

        campaign = await self.client.get_campaign(account_id, campaign_id)
        return {
            "status": campaign.get("status", "UNKNOWN"),
            "linkedin_campaign_id": campaign_id,
            "serving_status": campaign.get("servingStatuses", []),
            "review_status": campaign.get("reviewStatus"),
        }
