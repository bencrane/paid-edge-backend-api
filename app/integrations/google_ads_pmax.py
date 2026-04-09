"""Google Ads Performance Max campaign support — asset groups + cross-channel (BJC-158).

Performance Max (PMax) is Google's AI-driven campaign type that runs across all
Google properties. Uses Asset Groups instead of Ad Groups, audience signals
instead of hard targeting, and Google's ML decides which assets to show where.
"""

import logging

from app.integrations.google_ads import GoogleAdsService, dollars_to_micros, micros_to_dollars

logger = logging.getLogger(__name__)

# PMax asset limits
PMAX_LIMITS = {
    "headlines_min": 3,
    "headlines_max": 5,
    "long_headlines_min": 1,
    "long_headlines_max": 5,
    "descriptions_min": 2,
    "descriptions_max": 5,
    "headline_max_chars": 30,
    "long_headline_max_chars": 90,
    "description_max_chars": 90,
}

# PMax only supports conversion-based bidding
PMAX_BIDDING_STRATEGIES = {"maximize_conversions", "maximize_conversion_value"}


class PMaxValidationError(ValueError):
    """Raised when PMax campaign request fails validation."""
    pass


class GoogleAdsPMaxService:
    """Manages Performance Max campaigns and Asset Groups."""

    def __init__(self, service: GoogleAdsService):
        self.service = service
        self.customer_id = service.customer_id

    async def create_pmax_campaign(
        self,
        campaign_name: str,
        daily_budget_dollars: float,
        bidding_strategy: str = "maximize_conversions",
        target_cpa_micros: int | None = None,
        target_roas: float | None = None,
    ) -> dict:
        """Create a Performance Max campaign with budget.

        PMax campaigns must use conversion-based bidding.
        Returns dict with budget and campaign resource names.
        """
        if bidding_strategy not in PMAX_BIDDING_STRATEGIES:
            raise PMaxValidationError(
                f"PMax requires conversion-based bidding. "
                f"Valid: {PMAX_BIDDING_STRATEGIES}. Got: {bidding_strategy}"
            )

        # Create budget
        budget_operation = self.service._get_type("CampaignBudgetOperation")
        budget = budget_operation.create
        budget.name = f"{campaign_name} Budget"
        budget.amount_micros = dollars_to_micros(daily_budget_dollars)
        budget.delivery_method = (
            self.service.enums.BudgetDeliveryMethodEnum.STANDARD
        )

        budget_response = await self.service.mutate(
            "CampaignBudgetService", [budget_operation]
        )
        budget_resource = budget_response.results[0].resource_name

        # Create PMax campaign
        campaign_operation = self.service._get_type("CampaignOperation")
        campaign = campaign_operation.create
        campaign.name = campaign_name
        campaign.advertising_channel_type = (
            self.service.enums.AdvertisingChannelTypeEnum.PERFORMANCE_MAX
        )
        campaign.status = self.service.enums.CampaignStatusEnum.PAUSED
        campaign.campaign_budget = budget_resource

        if bidding_strategy == "maximize_conversion_value":
            campaign.maximize_conversion_value.target_roas = (
                target_roas if target_roas else 0
            )
        else:
            campaign.maximize_conversions.target_cpa_micros = (
                target_cpa_micros if target_cpa_micros else 0
            )

        campaign_response = await self.service.mutate(
            "CampaignService", [campaign_operation]
        )
        campaign_resource = campaign_response.results[0].resource_name

        logger.info("Created PMax campaign: %s", campaign_resource)
        return {
            "budget_resource_name": budget_resource,
            "campaign_resource_name": campaign_resource,
        }

    async def create_asset_group(
        self,
        campaign_resource_name: str,
        group_name: str,
        final_url: str,
        headlines: list[str],
        long_headlines: list[str],
        descriptions: list[str],
        business_name: str = "",
    ) -> str:
        """Create an Asset Group with text assets for a PMax campaign.

        Returns the asset group resource name.
        """
        # Validate assets
        errors = validate_pmax_assets(
            headlines=headlines,
            long_headlines=long_headlines,
            descriptions=descriptions,
        )
        if errors:
            raise PMaxValidationError(
                f"Asset validation failed: {'; '.join(errors)}"
            )

        # Create asset group
        operation = self.service._get_type("AssetGroupOperation")
        asset_group = operation.create
        asset_group.name = group_name
        asset_group.campaign = campaign_resource_name
        asset_group.final_urls.append(final_url)
        asset_group.status = self.service.enums.AssetGroupStatusEnum.ENABLED

        response = await self.service.mutate(
            "AssetGroupService", [operation]
        )
        asset_group_resource = response.results[0].resource_name

        # Link text assets
        await self._link_text_assets(
            asset_group_resource, headlines, "HEADLINE"
        )
        await self._link_text_assets(
            asset_group_resource, long_headlines, "LONG_HEADLINE"
        )
        await self._link_text_assets(
            asset_group_resource, descriptions, "DESCRIPTION"
        )
        if business_name:
            await self._link_text_assets(
                asset_group_resource, [business_name], "BUSINESS_NAME"
            )

        logger.info("Created PMax asset group: %s", asset_group_resource)
        return asset_group_resource

    async def add_audience_signal(
        self,
        asset_group_resource_name: str,
        user_list_resource_name: str | None = None,
    ) -> None:
        """Add audience signals to a PMax Asset Group.

        Audience signals are hints, not hard targeting — Google uses them
        as starting points for its ML to find similar users.
        """
        operation = self.service._get_type("AssetGroupSignalOperation")
        signal = operation.create
        signal.asset_group = asset_group_resource_name

        if user_list_resource_name:
            audience = signal.audience
            audience.user_lists.append(
                self.service._get_type("UserListInfo")
            )
            audience.user_lists[0].user_list = user_list_resource_name

        await self.service.mutate(
            "AssetGroupSignalService", [operation]
        )
        logger.info(
            "Added audience signal to asset group: %s",
            asset_group_resource_name,
        )

    async def get_pmax_campaigns(self) -> list[dict]:
        """List all Performance Max campaigns with metrics."""
        query = """
            SELECT campaign.id,
                   campaign.name,
                   campaign.status,
                   campaign.campaign_budget,
                   metrics.impressions,
                   metrics.clicks,
                   metrics.cost_micros,
                   metrics.conversions,
                   metrics.conversions_value
            FROM campaign
            WHERE campaign.advertising_channel_type = 'PERFORMANCE_MAX'
            AND campaign.status != 'REMOVED'
        """
        rows = await self.service.search_stream(query)
        return [
            {
                "id": str(row.campaign.id),
                "name": row.campaign.name,
                "status": row.campaign.status.name,
                "impressions": row.metrics.impressions,
                "clicks": row.metrics.clicks,
                "cost_dollars": micros_to_dollars(row.metrics.cost_micros),
                "conversions": row.metrics.conversions,
                "conversion_value": micros_to_dollars(
                    row.metrics.conversions_value
                ) if hasattr(row.metrics, "conversions_value") else 0,
            }
            for row in rows
        ]

    async def get_asset_group_performance(
        self, campaign_id: str
    ) -> list[dict]:
        """Get asset group performance for a PMax campaign."""
        query = f"""
            SELECT asset_group.id,
                   asset_group.name,
                   asset_group.status,
                   metrics.impressions,
                   metrics.clicks,
                   metrics.cost_micros,
                   metrics.conversions
            FROM asset_group
            WHERE campaign.id = {campaign_id}
            AND asset_group.status != 'REMOVED'
        """
        rows = await self.service.search_stream(query)
        return [
            {
                "id": str(row.asset_group.id),
                "name": row.asset_group.name,
                "status": row.asset_group.status.name,
                "impressions": row.metrics.impressions,
                "clicks": row.metrics.clicks,
                "cost_dollars": micros_to_dollars(row.metrics.cost_micros),
                "conversions": row.metrics.conversions,
            }
            for row in rows
        ]

    async def _link_text_assets(
        self,
        asset_group_resource: str,
        texts: list[str],
        field_type: str,
    ) -> None:
        """Create text assets and link them to an asset group."""
        for text in texts:
            # Create the text asset
            asset_operation = self.service._get_type("AssetOperation")
            asset = asset_operation.create
            asset.text_asset.text = text

            asset_response = await self.service.mutate(
                "AssetService", [asset_operation]
            )
            asset_resource = asset_response.results[0].resource_name

            # Link asset to asset group
            link_operation = self.service._get_type(
                "AssetGroupAssetOperation"
            )
            link = link_operation.create
            link.asset = asset_resource
            link.asset_group = asset_group_resource
            link.field_type = getattr(
                self.service.enums.AssetFieldTypeEnum, field_type
            )

            await self.service.mutate(
                "AssetGroupAssetService", [link_operation]
            )


def validate_pmax_assets(
    headlines: list[str],
    long_headlines: list[str],
    descriptions: list[str],
) -> list[str]:
    """Validate PMax asset group inputs. Returns list of errors."""
    errors = []
    limits = PMAX_LIMITS

    if len(headlines) < limits["headlines_min"]:
        errors.append(
            f"Need at least {limits['headlines_min']} headlines, got {len(headlines)}"
        )
    if len(headlines) > limits["headlines_max"]:
        errors.append(
            f"Max {limits['headlines_max']} headlines, got {len(headlines)}"
        )
    for i, h in enumerate(headlines):
        if len(h) > limits["headline_max_chars"]:
            errors.append(
                f"Headline {i+1} is {len(h)} chars (max {limits['headline_max_chars']})"
            )

    if len(long_headlines) < limits["long_headlines_min"]:
        errors.append(
            f"Need at least {limits['long_headlines_min']} long headlines, "
            f"got {len(long_headlines)}"
        )
    if len(long_headlines) > limits["long_headlines_max"]:
        errors.append(
            f"Max {limits['long_headlines_max']} long headlines, "
            f"got {len(long_headlines)}"
        )
    for i, h in enumerate(long_headlines):
        if len(h) > limits["long_headline_max_chars"]:
            errors.append(
                f"Long headline {i+1} is {len(h)} chars "
                f"(max {limits['long_headline_max_chars']})"
            )

    if len(descriptions) < limits["descriptions_min"]:
        errors.append(
            f"Need at least {limits['descriptions_min']} descriptions, "
            f"got {len(descriptions)}"
        )
    if len(descriptions) > limits["descriptions_max"]:
        errors.append(
            f"Max {limits['descriptions_max']} descriptions, "
            f"got {len(descriptions)}"
        )
    for i, d in enumerate(descriptions):
        if len(d) > limits["description_max_chars"]:
            errors.append(
                f"Description {i+1} is {len(d)} chars "
                f"(max {limits['description_max_chars']})"
            )

    return errors
