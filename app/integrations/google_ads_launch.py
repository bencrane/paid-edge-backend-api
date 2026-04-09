"""Google Ads campaign launch orchestration — PaidEdge end-to-end flow (BJC-148)."""

import logging
import time
from dataclasses import dataclass, field

from app.integrations.google_ads import GoogleAdsService, dollars_to_micros
from app.integrations.google_ads_ads import GoogleAdsAdService, RSAValidator
from app.integrations.google_ads_audience_push import GoogleAdsAudiencePushService
from app.integrations.google_ads_campaigns import GoogleAdsCampaignService
from app.integrations.google_ads_keywords import GoogleAdsKeywordService

logger = logging.getLogger(__name__)

# Google Ads status mapping
STATUS_MAP = {
    "ENABLED": "active",
    "PAUSED": "paused",
    "REMOVED": "archived",
}

PAIDEDGE_TO_GADS_STATUS = {
    "active": "ENABLED",
    "paused": "PAUSED",
    "archived": "REMOVED",
}


@dataclass
class GoogleAdsLaunchRequest:
    """Everything needed to launch a Google Ads campaign from PaidEdge."""

    org_id: str
    campaign_id: str  # PaidEdge campaign UUID

    # Campaign settings
    campaign_name: str
    daily_budget_dollars: float
    bidding_strategy: str = "maximize_conversions"
    bidding_params: dict = field(default_factory=dict)

    # Keywords
    keywords: list[dict] = field(default_factory=list)
    negative_keywords: list[dict] | None = None

    # RSA creative
    headlines: list[str] = field(default_factory=list)
    descriptions: list[str] = field(default_factory=list)
    final_url: str = ""
    path1: str | None = None
    path2: str | None = None
    pinned_headlines: dict[int, int] | None = None

    # Audience (optional)
    audience_segment: dict | None = None
    audience_members: list[dict] | None = None

    # Targeting
    geo_target_ids: list[int] | None = None
    geo_exclusion_ids: list[int] | None = None


class LaunchValidationError(ValueError):
    """Raised when launch request fails pre-flight validation."""
    pass


class GoogleAdsLaunchOrchestrator:
    """Coordinates full campaign launch sequence."""

    def __init__(self, service: GoogleAdsService):
        self.service = service
        self.campaign_service = GoogleAdsCampaignService(service)
        self.keyword_service = GoogleAdsKeywordService(service)
        self.ad_service = GoogleAdsAdService(service)
        self.audience_push_service = GoogleAdsAudiencePushService(service)

    def validate_request(self, request: GoogleAdsLaunchRequest) -> list[str]:
        """Pre-launch validation. Returns list of errors, empty if valid."""
        errors = []

        if not request.campaign_name:
            errors.append("Campaign name is required")
        if request.daily_budget_dollars <= 0:
            errors.append("Daily budget must be greater than $0")
        if not request.final_url:
            errors.append("Final URL is required")
        if not request.keywords:
            errors.append("At least one keyword is required")

        # Validate RSA creative
        rsa_errors = RSAValidator.validate(
            headlines=request.headlines,
            descriptions=request.descriptions,
            path1=request.path1,
            path2=request.path2,
        )
        errors.extend(rsa_errors)

        return errors

    async def launch(self, request: GoogleAdsLaunchRequest) -> dict:
        """Execute the full launch sequence. Returns resource names for all created entities."""
        start_time = time.time()
        results = {"steps": [], "errors": [], "resources": {}}

        # Pre-flight validation
        validation_errors = self.validate_request(request)
        if validation_errors:
            raise LaunchValidationError(
                f"Launch validation failed: {'; '.join(validation_errors)}"
            )

        try:
            # Step 1: Create campaign budget
            budget_resource = await self.campaign_service.create_campaign_budget(
                daily_budget_dollars=request.daily_budget_dollars,
                name=f"{request.campaign_name} Budget",
            )
            results["steps"].append({"step": "budget", "status": "success"})
            results["resources"]["budget"] = budget_resource
            logger.info("Step 1/7: Budget created: %s", budget_resource)

            # Step 2: Create campaign (starts PAUSED)
            campaign_resource = await self.campaign_service.create_search_campaign(
                name=request.campaign_name,
                budget_resource=budget_resource,
                bidding_strategy=request.bidding_strategy,
                bidding_params=request.bidding_params,
            )
            results["steps"].append({"step": "campaign", "status": "success"})
            results["resources"]["campaign"] = campaign_resource
            campaign_id = campaign_resource.split("/")[-1]
            logger.info("Step 2/7: Campaign created: %s", campaign_resource)

            # Step 3: Geo targeting (optional)
            if request.geo_target_ids:
                geo_result = await self.campaign_service.add_location_targeting(
                    campaign_id, request.geo_target_ids
                )
                results["steps"].append({
                    "step": "geo_targeting", "status": "success",
                    "count": len(geo_result),
                })
                logger.info("Step 3/7: Geo targeting added: %d locations", len(geo_result))

            if request.geo_exclusion_ids:
                await self.campaign_service.add_location_exclusion(
                    campaign_id, request.geo_exclusion_ids
                )

            # Step 4: Create ad group
            ad_group_resource = await self.campaign_service.create_ad_group(
                campaign_resource=campaign_resource,
                name=f"{request.campaign_name} - Ad Group 1",
            )
            results["steps"].append({"step": "ad_group", "status": "success"})
            results["resources"]["ad_group"] = ad_group_resource
            ad_group_id = ad_group_resource.split("/")[-1]
            logger.info("Step 4/7: Ad group created: %s", ad_group_resource)

            # Step 5: Add keywords
            keyword_tuples = [
                (kw["text"], kw.get("match_type", "BROAD"))
                for kw in request.keywords
            ]
            keyword_results = await self.keyword_service.add_keywords(
                ad_group_resource=ad_group_resource,
                keywords=keyword_tuples,
            )
            results["steps"].append({
                "step": "keywords", "status": "success",
                "count": len(keyword_results),
            })
            results["resources"]["keywords"] = keyword_results
            logger.info("Step 5/7: %d keywords added", len(keyword_results))

            # Step 5b: Negative keywords (optional, campaign level)
            if request.negative_keywords:
                neg_tuples = [
                    (kw["text"], kw.get("match_type", "BROAD"))
                    for kw in request.negative_keywords
                ]
                neg_results = await self.keyword_service.add_negative_keywords_campaign(
                    campaign_id, neg_tuples
                )
                results["steps"].append({
                    "step": "negative_keywords", "status": "success",
                    "count": len(neg_results),
                })

            # Step 6: Create RSA
            rsa_resource = await self.ad_service.create_responsive_search_ad(
                ad_group_id=ad_group_id,
                headlines=request.headlines,
                descriptions=request.descriptions,
                final_url=request.final_url,
                pinned_headlines=request.pinned_headlines,
                path1=request.path1,
                path2=request.path2,
            )
            results["steps"].append({"step": "rsa", "status": "success"})
            results["resources"]["rsa"] = rsa_resource
            logger.info("Step 6/7: RSA created: %s", rsa_resource)

            # Step 7: Audience push (optional)
            if request.audience_segment and request.audience_members:
                audience_result = await self.audience_push_service.push_segment(
                    segment=request.audience_segment,
                    members=request.audience_members,
                )
                results["steps"].append({
                    "step": "audience", "status": "success",
                    "member_count": audience_result.get("member_count", 0),
                })
                results["resources"]["audience"] = audience_result
                logger.info("Step 7/7: Audience pushed: %d members",
                            audience_result.get("member_count", 0))

            # Step 8: Enable campaign (go live)
            await self.campaign_service.update_campaign_status(
                campaign_id, "ENABLED"
            )
            results["steps"].append({"step": "enable", "status": "success"})

            elapsed = time.time() - start_time
            results["status"] = "launched"
            results["elapsed_seconds"] = round(elapsed, 2)
            logger.info(
                "Campaign '%s' launched in %.2fs", request.campaign_name, elapsed
            )
            return results

        except LaunchValidationError:
            raise
        except Exception as e:
            elapsed = time.time() - start_time
            results["errors"].append(str(e))
            results["status"] = "failed"
            results["elapsed_seconds"] = round(elapsed, 2)
            logger.error(
                "Campaign launch failed after %.2fs: %s", elapsed, str(e)
            )
            raise

    async def pause_campaign(self, campaign_id: str) -> None:
        """Pause a running campaign."""
        await self.campaign_service.update_campaign_status(campaign_id, "PAUSED")

    async def resume_campaign(self, campaign_id: str) -> None:
        """Resume a paused campaign."""
        await self.campaign_service.update_campaign_status(campaign_id, "ENABLED")

    async def archive_campaign(self, campaign_id: str) -> None:
        """Archive (remove) a campaign."""
        await self.campaign_service.update_campaign_status(campaign_id, "REMOVED")

    @staticmethod
    def map_status_to_paidedge(google_status: str) -> str:
        """Map Google Ads campaign status to PaidEdge status."""
        return STATUS_MAP.get(google_status, "unknown")

    @staticmethod
    def map_status_to_google(paidedge_status: str) -> str:
        """Map PaidEdge status to Google Ads campaign status."""
        return PAIDEDGE_TO_GADS_STATUS.get(paidedge_status, "PAUSED")
