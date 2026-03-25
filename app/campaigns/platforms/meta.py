"""Meta campaign launch orchestration — platform adapter for three-tier hierarchy (BJC-157)."""

import logging

import httpx
from pydantic import BaseModel

from app.audiences.meta_push import sync_segment_to_meta
from app.integrations.meta_campaigns import META_OBJECTIVE_MAP
from app.integrations.meta_client import MetaAdsClient, MetaAPIError
from app.integrations.meta_targeting import MetaTargetingBuilder

logger = logging.getLogger(__name__)


class MetaLaunchResult(BaseModel):
    platform: str = "meta"
    campaign_id: str = ""
    ad_set_ids: list[str] = []
    ad_ids: list[str] = []
    audience_id: str | None = None
    status: str = ""  # 'launched', 'pending_review', 'error'
    errors: list[str] | None = None


async def _download_asset(url: str) -> bytes:
    """Download asset bytes from storage URL."""
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.content


def _detect_asset_type(asset: dict) -> str:
    """Detect media type from asset metadata or file extension."""
    asset_type = asset.get("asset_type", "").lower()
    if asset_type in ("image", "video"):
        return asset_type
    url = asset.get("storage_url", asset.get("url", ""))
    lower_url = url.lower()
    if lower_url.endswith((".mp4", ".mov", ".avi", ".webm")):
        return "video"
    return "image"


class MetaPlatformAdapter:
    """Orchestrate Meta campaign lifecycle from PaidEdge campaign data."""

    def __init__(self, meta_client: MetaAdsClient, supabase=None, clickhouse=None):
        self.client = meta_client
        self.supabase = supabase
        self.clickhouse = clickhouse

    async def launch(
        self,
        paidedge_campaign: dict,
        audience_segment: dict | None,
        assets: list[dict],
    ) -> MetaLaunchResult:
        """Full launch orchestration: Campaign → Ad Set → Creatives → Ads → Activate."""
        errors: list[str] = []
        meta_campaign_id = ""
        ad_set_ids: list[str] = []
        ad_ids: list[str] = []
        audience_id: str | None = None

        try:
            config = paidedge_campaign.get("config", {})
            page_id = config.get("page_id", "")

            # Step 1: Get page_id from provider_configs if not in campaign config
            if not page_id and self.supabase:
                tenant_id = paidedge_campaign.get("organization_id", "")
                pc_res = (
                    self.supabase.table("provider_configs")
                    .select("config")
                    .eq("organization_id", tenant_id)
                    .eq("provider", "meta_ads")
                    .maybe_single()
                    .execute()
                )
                if pc_res.data:
                    page_id = pc_res.data.get("config", {}).get("page_id", "")

            # Step 2: Push audience segment to Meta if exists
            if audience_segment and self.supabase and self.clickhouse:
                try:
                    sync_result = await sync_segment_to_meta(
                        segment_id=audience_segment.get("id", ""),
                        tenant_id=paidedge_campaign.get("organization_id", ""),
                        supabase=self.supabase,
                        clickhouse=self.clickhouse,
                        meta_client=self.client,
                    )
                    audience_id = sync_result.audience_id
                except Exception as exc:
                    logger.warning("Audience push to Meta failed: %s", exc)

            # Step 3: Create Campaign (PAUSED)
            paidedge_objective = config.get("objective", "website_traffic")
            meta_objective = META_OBJECTIVE_MAP.get(
                paidedge_objective, "OUTCOME_TRAFFIC"
            )
            budget = paidedge_campaign.get("budget", 50)
            daily_budget_cents = int(float(budget) * 100)

            campaign_resp = await self.client.create_campaign(
                name=paidedge_campaign.get("name", "PaidEdge Campaign"),
                objective=meta_objective,
                special_ad_categories=config.get("special_ad_categories", []),
                daily_budget=daily_budget_cents,
                bid_strategy=config.get("bid_strategy", "LOWEST_COST_WITHOUT_CAP"),
                status="PAUSED",
            )
            meta_campaign_id = campaign_resp.get("id", "")
            logger.info("Created Meta campaign %s", meta_campaign_id)

            # Step 4: Build targeting and create Ad Set
            targeting_builder = MetaTargetingBuilder()
            targeting_builder.set_locations(
                countries=config.get("locations", config.get("countries", ["US"]))
            )
            if config.get("age_min") or config.get("age_max"):
                targeting_builder.set_demographics(
                    age_min=config.get("age_min", 18),
                    age_max=config.get("age_max", 65),
                )
            if config.get("interests"):
                targeting_builder.add_interests(config["interests"])
            if audience_id:
                targeting_builder.set_custom_audiences([audience_id])

            targeting = targeting_builder.build()

            optimization_goal = config.get("optimization_goal", "LINK_CLICKS")
            adset_resp = await self.client.create_ad_set(
                campaign_id=meta_campaign_id,
                name=f"{paidedge_campaign.get('name', 'Ad Set')} - Ad Set 1",
                targeting=targeting,
                optimization_goal=optimization_goal,
                billing_event="IMPRESSIONS",
                status="PAUSED",
            )
            adset_id = adset_resp.get("id", "")
            ad_set_ids.append(adset_id)
            logger.info("Created Meta ad set %s", adset_id)

            # Step 5: Upload media and create creatives + ads
            for asset in assets:
                try:
                    ad_id = await self._create_ad_from_asset(
                        asset=asset,
                        adset_id=adset_id,
                        page_id=page_id,
                        campaign_name=paidedge_campaign.get("name", ""),
                    )
                    ad_ids.append(ad_id)
                except (MetaAPIError, httpx.HTTPError) as exc:
                    error_msg = f"Ad creation failed for asset {asset.get('id')}: {exc}"
                    logger.error(error_msg)
                    errors.append(error_msg)

            if not ad_ids:
                # All ads failed — rollback
                await self._rollback_on_failure(
                    meta_campaign_id, ad_set_ids, ad_ids
                )
                return MetaLaunchResult(
                    campaign_id=meta_campaign_id,
                    ad_set_ids=ad_set_ids,
                    ad_ids=[],
                    audience_id=audience_id,
                    status="error",
                    errors=errors or ["All ad creations failed"],
                )

            # Step 6: Activate (campaign → ad sets → ads cascade)
            await self.client.set_campaign_status(meta_campaign_id, "ACTIVE")

            # Step 7: Store platform IDs
            await self._store_platform_ids(
                paidedge_campaign["id"],
                meta_campaign_id,
                ad_set_ids,
                ad_ids,
            )

            status = "launched" if not errors else "pending_review"
            return MetaLaunchResult(
                campaign_id=meta_campaign_id,
                ad_set_ids=ad_set_ids,
                ad_ids=ad_ids,
                audience_id=audience_id,
                status=status,
                errors=errors or None,
            )

        except MetaAPIError as exc:
            logger.error("Meta launch failed: %s", exc)
            if meta_campaign_id:
                await self._rollback_on_failure(
                    meta_campaign_id, ad_set_ids, ad_ids
                )
            return MetaLaunchResult(
                campaign_id=meta_campaign_id,
                ad_set_ids=ad_set_ids,
                ad_ids=ad_ids,
                audience_id=audience_id,
                status="error",
                errors=[str(exc)] + errors,
            )

    async def _create_ad_from_asset(
        self,
        asset: dict,
        adset_id: str,
        page_id: str,
        campaign_name: str,
    ) -> str:
        """Upload asset, create creative, create ad. Returns ad ID."""
        asset_type = _detect_asset_type(asset)
        storage_url = asset.get("storage_url", asset.get("url", ""))
        asset_bytes = await _download_asset(storage_url)

        headline = asset.get("headline", asset.get("title", campaign_name))
        message = asset.get("intro_text", asset.get("message", ""))
        link = asset.get("link", asset.get("cta_link", "https://example.com"))

        if asset_type == "video":
            # For video, we'd need to save to temp file; simplified for now
            import base64
            video_data = {"source": base64.b64encode(asset_bytes).decode("utf-8")}
            video_resp = await self.client._request(
                "POST",
                f"{self.client.ad_account_id}/advideos",
                data={**video_data, "title": headline},
            )
            video_id = video_resp.get("id", "")
            creative_resp = await self.client.create_video_ad_creative(
                name=f"{campaign_name} - Video Creative",
                page_id=page_id,
                video_id=video_id,
                message=message,
                cta_link=link,
            )
        else:
            # Image
            upload_resp = await self.client.upload_image(image_bytes=asset_bytes)
            image_hash = upload_resp.get("hash", "")
            creative_resp = await self.client.create_image_ad_creative(
                name=f"{campaign_name} - Image Creative",
                page_id=page_id,
                image_hash=image_hash,
                link=link,
                message=message,
                headline=headline,
            )

        creative_id = creative_resp.get("id", "")
        ad_resp = await self.client.create_ad(
            name=f"{campaign_name} - Ad",
            adset_id=adset_id,
            creative_id=creative_id,
            status="PAUSED",
        )
        return ad_resp.get("id", "")

    async def _rollback_on_failure(
        self,
        campaign_id: str | None,
        ad_set_ids: list[str],
        ad_ids: list[str],
    ) -> None:
        """Clean up partial creation on failure."""
        for ad_id in ad_ids:
            try:
                await self.client.delete_ad(ad_id)
            except Exception:
                logger.warning("Failed to rollback ad %s", ad_id)

        for adset_id in ad_set_ids:
            try:
                await self.client.delete_ad_set(adset_id)
            except Exception:
                logger.warning("Failed to rollback ad set %s", adset_id)

        if campaign_id:
            try:
                await self.client.set_campaign_status(campaign_id, "ARCHIVED")
            except Exception:
                logger.warning("Failed to archive campaign %s", campaign_id)

    async def _store_platform_ids(
        self,
        campaign_id: str,
        meta_campaign_id: str,
        ad_set_ids: list[str],
        ad_ids: list[str],
    ) -> None:
        """Store Meta IDs back to PaidEdge campaign."""
        if not self.supabase:
            return
        platform_data = {
            "platform": "meta",
            "platform_campaign_id": meta_campaign_id,
            "ad_set_ids": ad_set_ids,
            "ad_ids": ad_ids,
        }
        self.supabase.table("campaigns").update(
            {"platform_data": platform_data}
        ).eq("id", campaign_id).execute()

    async def pause(self, paidedge_campaign: dict) -> None:
        """Pause campaign at campaign level."""
        platform_data = paidedge_campaign.get("platform_data", {})
        campaign_id = platform_data.get("platform_campaign_id")
        if not campaign_id:
            raise MetaAPIError(400, None, "No Meta platform data found on campaign")
        await self.client.set_campaign_status(campaign_id, "PAUSED")

    async def resume(self, paidedge_campaign: dict) -> None:
        """Resume campaign."""
        platform_data = paidedge_campaign.get("platform_data", {})
        campaign_id = platform_data.get("platform_campaign_id")
        if not campaign_id:
            raise MetaAPIError(400, None, "No Meta platform data found on campaign")
        await self.client.set_campaign_status(campaign_id, "ACTIVE")

    async def complete(self, paidedge_campaign: dict) -> None:
        """Archive campaign."""
        platform_data = paidedge_campaign.get("platform_data", {})
        campaign_id = platform_data.get("platform_campaign_id")
        if campaign_id:
            await self.client.set_campaign_status(campaign_id, "ARCHIVED")

    async def get_status(self, paidedge_campaign: dict) -> dict:
        """Get delivery status from Meta."""
        platform_data = paidedge_campaign.get("platform_data", {})
        campaign_id = platform_data.get("platform_campaign_id")
        if not campaign_id:
            return {"status": "not_launched", "meta_campaign_id": None}

        campaign = await self.client.get_campaign(campaign_id)
        return {
            "status": campaign.get("effective_status", "UNKNOWN"),
            "meta_campaign_id": campaign_id,
        }
