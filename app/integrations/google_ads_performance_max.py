"""Google Ads Performance Max campaign support — asset groups + cross-channel (BJC-158).

PMax campaigns run across all Google properties (Search, Display, YouTube,
Discover, Gmail, Maps). Uses Asset Groups instead of Ad Groups, and audience
signals instead of hard targeting.
"""

import asyncio
import logging
from functools import partial as functools_partial

from app.integrations.google_ads import GoogleAdsService, dollars_to_micros, micros_to_dollars

logger = logging.getLogger(__name__)


# --- Asset validation constants ---

PMAX_HEADLINE_MIN = 3
PMAX_HEADLINE_MAX = 5
PMAX_HEADLINE_MAX_CHARS = 30

PMAX_LONG_HEADLINE_MIN = 1
PMAX_LONG_HEADLINE_MAX = 5
PMAX_LONG_HEADLINE_MAX_CHARS = 90

PMAX_DESCRIPTION_MIN = 2
PMAX_DESCRIPTION_MAX = 5
PMAX_DESCRIPTION_MAX_CHARS = 90

# PMax only supports conversion-based bidding
PMAX_ALLOWED_BIDDING = {"MAXIMIZE_CONVERSIONS", "MAXIMIZE_CONVERSION_VALUE"}


class PMaxValidationError(ValueError):
    """Raised when Performance Max campaign inputs fail validation."""
    pass


class PMaxAssetValidator:
    """Validates Performance Max asset inputs before API call."""

    @classmethod
    def validate(
        cls,
        headlines: list[str],
        long_headlines: list[str],
        descriptions: list[str],
        business_name: str = "",
    ) -> list[str]:
        """Returns list of validation errors, empty if valid."""
        errors = []

        # Headlines (short)
        if len(headlines) < PMAX_HEADLINE_MIN:
            errors.append(
                f"Need at least {PMAX_HEADLINE_MIN} short headlines, got {len(headlines)}"
            )
        if len(headlines) > PMAX_HEADLINE_MAX:
            errors.append(
                f"Max {PMAX_HEADLINE_MAX} short headlines, got {len(headlines)}"
            )
        for i, h in enumerate(headlines):
            if len(h) > PMAX_HEADLINE_MAX_CHARS:
                errors.append(
                    f"Short headline {i+1} is {len(h)} chars "
                    f"(max {PMAX_HEADLINE_MAX_CHARS}): '{h}'"
                )

        # Long headlines
        if len(long_headlines) < PMAX_LONG_HEADLINE_MIN:
            errors.append(
                f"Need at least {PMAX_LONG_HEADLINE_MIN} long headline, got {len(long_headlines)}"
            )
        if len(long_headlines) > PMAX_LONG_HEADLINE_MAX:
            errors.append(
                f"Max {PMAX_LONG_HEADLINE_MAX} long headlines, got {len(long_headlines)}"
            )
        for i, lh in enumerate(long_headlines):
            if len(lh) > PMAX_LONG_HEADLINE_MAX_CHARS:
                errors.append(
                    f"Long headline {i+1} is {len(lh)} chars "
                    f"(max {PMAX_LONG_HEADLINE_MAX_CHARS}): '{lh}'"
                )

        # Descriptions
        if len(descriptions) < PMAX_DESCRIPTION_MIN:
            errors.append(
                f"Need at least {PMAX_DESCRIPTION_MIN} descriptions, got {len(descriptions)}"
            )
        if len(descriptions) > PMAX_DESCRIPTION_MAX:
            errors.append(
                f"Max {PMAX_DESCRIPTION_MAX} descriptions, got {len(descriptions)}"
            )
        for i, d in enumerate(descriptions):
            if len(d) > PMAX_DESCRIPTION_MAX_CHARS:
                errors.append(
                    f"Description {i+1} is {len(d)} chars "
                    f"(max {PMAX_DESCRIPTION_MAX_CHARS}): '{d}'"
                )

        if not business_name:
            errors.append("Business name is required for Performance Max")

        return errors


class GoogleAdsPMaxClient:
    """Manages Performance Max campaigns and Asset Groups."""

    def __init__(self, service: GoogleAdsService):
        self.service = service
        self.customer_id = service.customer_id

    async def create_pmax_campaign(
        self,
        campaign_name: str,
        daily_budget_dollars: float,
        bidding_strategy: str = "MAXIMIZE_CONVERSIONS",
        target_cpa_dollars: float | None = None,
        target_roas: float | None = None,
    ) -> dict:
        """Create a Performance Max campaign with budget.

        PMax requires conversion-based bidding only.
        Returns dict with budget and campaign resource names.
        """
        if bidding_strategy not in PMAX_ALLOWED_BIDDING:
            raise PMaxValidationError(
                f"Performance Max requires conversion-based bidding. "
                f"Got '{bidding_strategy}', allowed: {PMAX_ALLOWED_BIDDING}"
            )

        # Create budget
        budget_operation = self.service._get_type("CampaignBudgetOperation")
        budget = budget_operation.create
        budget.name = f"{campaign_name} Budget"
        budget.amount_micros = dollars_to_micros(daily_budget_dollars)
        budget.delivery_method = (
            self.service.enums.BudgetDeliveryMethodEnum.STANDARD
        )
        budget.explicitly_shared = False

        budget_response = await self.service.mutate(
            "CampaignBudgetService", [budget_operation]
        )
        budget_resource = budget_response.results[0].resource_name

        # Create campaign
        campaign_operation = self.service._get_type("CampaignOperation")
        campaign = campaign_operation.create
        campaign.name = campaign_name
        campaign.advertising_channel_type = (
            self.service.enums.AdvertisingChannelTypeEnum.PERFORMANCE_MAX
        )
        campaign.status = self.service.enums.CampaignStatusEnum.PAUSED
        campaign.campaign_budget = budget_resource

        # Apply bidding strategy
        if bidding_strategy == "MAXIMIZE_CONVERSION_VALUE":
            campaign.maximize_conversion_value.target_roas = (
                target_roas if target_roas else 0
            )
        else:
            # MAXIMIZE_CONVERSIONS
            campaign.maximize_conversions.target_cpa_micros = (
                dollars_to_micros(target_cpa_dollars)
                if target_cpa_dollars
                else 0
            )

        campaign_response = await self.service.mutate(
            "CampaignService", [campaign_operation]
        )
        campaign_resource = campaign_response.results[0].resource_name

        logger.info(
            "Created PMax campaign: %s (budget: %s)",
            campaign_resource,
            budget_resource,
        )

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
        business_name: str,
        images: list[str] | None = None,
        logos: list[str] | None = None,
    ) -> str:
        """Create an Asset Group with text assets for a PMax campaign.

        Asset Groups replace Ad Groups in PMax. They contain assets
        that Google mixes and matches across channels.
        Returns asset group resource name.
        """
        # Validate inputs
        errors = PMaxAssetValidator.validate(
            headlines, long_headlines, descriptions, business_name
        )
        if errors:
            raise PMaxValidationError(
                f"Asset validation failed: {'; '.join(errors)}"
            )

        # Create the asset group
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
        logger.info("Created asset group: %s", asset_group_resource)

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
        await self._link_text_assets(
            asset_group_resource, [business_name], "BUSINESS_NAME"
        )

        # Link image assets (optional)
        if images:
            await self._link_existing_assets(
                asset_group_resource, images, "MARKETING_IMAGE"
            )
        if logos:
            await self._link_existing_assets(
                asset_group_resource, logos, "LOGO"
            )

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
        if not user_list_resource_name:
            return

        operation = self.service._get_type("AssetGroupSignalOperation")
        signal = operation.create
        signal.asset_group = asset_group_resource_name

        # Set the audience signal with user list
        audience = signal.audience
        audience_info = audience.audiences.add()
        audience_info.user_list = user_list_resource_name

        await self.service.mutate(
            "AssetGroupSignalService", [operation]
        )
        logger.info(
            "Added audience signal %s to asset group %s",
            user_list_resource_name,
            asset_group_resource_name,
        )

    async def get_pmax_campaigns(self) -> list[dict]:
        """List all Performance Max campaigns."""
        query = """
            SELECT
                campaign.id, campaign.name, campaign.status,
                campaign.advertising_channel_type,
                campaign_budget.amount_micros,
                metrics.impressions, metrics.clicks,
                metrics.cost_micros, metrics.conversions
            FROM campaign
            WHERE campaign.advertising_channel_type = 'PERFORMANCE_MAX'
            AND campaign.status != 'REMOVED'
        """
        rows = await self.service.search_stream(query)
        campaigns = []
        for row in rows:
            campaigns.append({
                "id": str(row.campaign.id),
                "name": row.campaign.name,
                "status": row.campaign.status.name,
                "channel_type": "PERFORMANCE_MAX",
                "daily_budget_dollars": micros_to_dollars(
                    row.campaign_budget.amount_micros
                ),
                "impressions": row.metrics.impressions,
                "clicks": row.metrics.clicks,
                "cost_dollars": micros_to_dollars(row.metrics.cost_micros),
                "conversions": row.metrics.conversions,
            })
        return campaigns

    async def get_asset_groups(self, campaign_id: str) -> list[dict]:
        """List asset groups for a PMax campaign."""
        query = f"""
            SELECT
                asset_group.id, asset_group.name, asset_group.status,
                asset_group.campaign
            FROM asset_group
            WHERE asset_group.campaign = 'customers/{self.customer_id}/campaigns/{campaign_id}'
            AND asset_group.status != 'REMOVED'
        """
        rows = await self.service.search_stream(query)
        groups = []
        for row in rows:
            groups.append({
                "id": str(row.asset_group.id),
                "name": row.asset_group.name,
                "status": row.asset_group.status.name,
                "campaign": row.asset_group.campaign,
            })
        return groups

    async def get_asset_group_performance(
        self,
        campaign_id: str,
        start_date: str,
        end_date: str,
    ) -> list[dict]:
        """Get asset group-level performance metrics."""
        query = f"""
            SELECT
                asset_group.id, asset_group.name,
                metrics.impressions, metrics.clicks,
                metrics.cost_micros, metrics.conversions
            FROM asset_group
            WHERE asset_group.campaign = 'customers/{self.customer_id}/campaigns/{campaign_id}'
            AND segments.date >= '{start_date}'
            AND segments.date <= '{end_date}'
        """
        rows = await self.service.search_stream(query)
        results = []
        for row in rows:
            results.append({
                "asset_group_id": str(row.asset_group.id),
                "asset_group_name": row.asset_group.name,
                "impressions": row.metrics.impressions,
                "clicks": row.metrics.clicks,
                "cost_dollars": micros_to_dollars(row.metrics.cost_micros),
                "conversions": row.metrics.conversions,
            })
        return results

    async def update_campaign_status(
        self, campaign_id: str, new_status: str
    ) -> None:
        """Enable, pause, or remove a PMax campaign."""
        operation = self.service._get_type("CampaignOperation")
        campaign = operation.update
        campaign.resource_name = (
            f"customers/{self.customer_id}/campaigns/{campaign_id}"
        )
        campaign.status = getattr(
            self.service.enums.CampaignStatusEnum, new_status
        )

        field_mask = self.service._get_type("FieldMask")
        field_mask.paths.append("status")
        operation.update_mask.CopyFrom(field_mask)

        await self.service.mutate("CampaignService", [operation])
        logger.info("PMax campaign %s status → %s", campaign_id, new_status)

    async def _link_text_assets(
        self,
        asset_group_resource: str,
        texts: list[str],
        field_type: str,
    ) -> None:
        """Create text assets and link them to an asset group."""
        if not texts:
            return

        asset_service = self.service._get_service("AssetService")
        asset_group_asset_service = self.service._get_service(
            "AssetGroupAssetService"
        )

        for text in texts:
            # Create the text asset
            asset_op = self.service._get_type("AssetOperation")
            asset = asset_op.create
            asset.text_asset.text = text

            loop = asyncio.get_event_loop()
            asset_response = await loop.run_in_executor(
                None,
                functools_partial(
                    asset_service.mutate_assets,
                    customer_id=self.customer_id,
                    operations=[asset_op],
                ),
            )
            asset_resource = asset_response.results[0].resource_name

            # Link asset to asset group
            link_op = self.service._get_type("AssetGroupAssetOperation")
            link = link_op.create
            link.asset_group = asset_group_resource
            link.asset = asset_resource
            link.field_type = getattr(
                self.service.enums.AssetFieldTypeEnum, field_type
            )

            await loop.run_in_executor(
                None,
                functools_partial(
                    asset_group_asset_service.mutate_asset_group_assets,
                    customer_id=self.customer_id,
                    operations=[link_op],
                ),
            )

        logger.info(
            "Linked %d %s assets to asset group %s",
            len(texts),
            field_type,
            asset_group_resource,
        )

    async def _link_existing_assets(
        self,
        asset_group_resource: str,
        asset_resource_names: list[str],
        field_type: str,
    ) -> None:
        """Link existing assets (images, logos) to an asset group."""
        asset_group_asset_service = self.service._get_service(
            "AssetGroupAssetService"
        )
        loop = asyncio.get_event_loop()

        for asset_resource in asset_resource_names:
            link_op = self.service._get_type("AssetGroupAssetOperation")
            link = link_op.create
            link.asset_group = asset_group_resource
            link.asset = asset_resource
            link.field_type = getattr(
                self.service.enums.AssetFieldTypeEnum, field_type
            )

            await loop.run_in_executor(
                None,
                functools_partial(
                    asset_group_asset_service.mutate_asset_group_assets,
                    customer_id=self.customer_id,
                    operations=[link_op],
                ),
            )

        logger.info(
            "Linked %d %s assets to asset group %s",
            len(asset_resource_names),
            field_type,
            asset_group_resource,
        )
