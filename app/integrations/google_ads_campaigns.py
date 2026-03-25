"""Google Ads Search campaign CRUD + budget + bidding strategy builder (BJC-142)."""

import logging

from app.integrations.google_ads import GoogleAdsService, dollars_to_micros, micros_to_dollars

logger = logging.getLogger(__name__)

BIDDING_STRATEGIES = {
    "manual_cpc": {
        "description": "Set bids manually per keyword",
        "works_with": ["SEARCH", "DISPLAY"],
        "params": {"enhanced_cpc_enabled": bool},
    },
    "maximize_clicks": {
        "description": "Auto-bid for most clicks within budget",
        "works_with": ["SEARCH", "DISPLAY"],
        "params": {"cpc_bid_ceiling_micros": int},
    },
    "maximize_conversions": {
        "description": "Auto-bid for most conversions",
        "works_with": ["SEARCH", "DISPLAY", "PERFORMANCE_MAX", "DEMAND_GEN"],
        "params": {"target_cpa_micros": int},
    },
    "target_cpa": {
        "description": "Auto-bid targeting a cost per conversion",
        "works_with": ["SEARCH", "DISPLAY", "PERFORMANCE_MAX", "DEMAND_GEN"],
        "params": {"target_cpa_micros": int},
    },
    "target_roas": {
        "description": "Auto-bid targeting a return on ad spend",
        "works_with": ["SEARCH", "DISPLAY", "SHOPPING", "PERFORMANCE_MAX"],
        "params": {"target_roas": float},
    },
    "maximize_conversion_value": {
        "description": "Auto-bid for highest total conversion value",
        "works_with": ["SEARCH", "PERFORMANCE_MAX"],
        "params": {"target_roas": float},
    },
}


def apply_bidding_strategy(campaign, strategy: str, params: dict | None, enums):
    """Apply a bidding strategy to a campaign object."""
    params = params or {}
    if strategy == "manual_cpc":
        campaign.manual_cpc.enhanced_cpc_enabled = params.get("enhanced_cpc_enabled", False)
    elif strategy == "maximize_clicks":
        campaign.maximize_clicks.cpc_bid_ceiling_micros = params.get("cpc_bid_ceiling_micros", 0)
    elif strategy == "maximize_conversions":
        campaign.maximize_conversions.target_cpa_micros = params.get("target_cpa_micros", 0)
    elif strategy == "target_cpa":
        campaign.target_cpa.target_cpa_micros = params.get("target_cpa_micros", 0)
    elif strategy == "target_roas":
        campaign.target_roas.target_roas = params.get("target_roas", 0)
    elif strategy == "maximize_conversion_value":
        campaign.maximize_conversion_value.target_roas = params.get("target_roas", 0)
    else:
        raise ValueError(f"Unknown bidding strategy: {strategy}")


class GoogleAdsCampaignService:
    """Manages Google Ads Search campaigns, budgets, ad groups, and geo targeting."""

    def __init__(self, service: GoogleAdsService):
        self.service = service
        self.customer_id = service.customer_id

    # --- Budget ---

    async def create_campaign_budget(
        self,
        daily_budget_dollars: float,
        name: str | None = None,
        shared: bool = False,
    ) -> str:
        """Create a campaign budget. Returns resource name."""
        operation = self.service._get_type("CampaignBudgetOperation")
        budget = operation.create
        budget.name = name or f"Budget {daily_budget_dollars}"
        budget.amount_micros = dollars_to_micros(daily_budget_dollars)
        budget.delivery_method = self.service.enums.BudgetDeliveryMethodEnum.STANDARD
        budget.explicitly_shared = shared

        response = await self.service.mutate("CampaignBudgetService", [operation])
        return response.results[0].resource_name

    async def update_campaign_budget(
        self, budget_resource: str, daily_budget_dollars: float
    ) -> None:
        """Update an existing budget amount."""
        operation = self.service._get_type("CampaignBudgetOperation")
        budget = operation.update
        budget.resource_name = budget_resource
        budget.amount_micros = dollars_to_micros(daily_budget_dollars)

        field_mask = self.service.client.get_service("CampaignBudgetService")
        from google.api_core import protobuf_helpers
        operation.update_mask.paths.append("amount_micros")

        await self.service.mutate("CampaignBudgetService", [operation])

    # --- Campaign ---

    async def create_search_campaign(
        self,
        name: str,
        budget_resource: str,
        bidding_strategy: str = "maximize_conversions",
        bidding_params: dict | None = None,
        network_settings: dict | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        status: str = "PAUSED",
    ) -> str:
        """Create a Search campaign. Returns resource name."""
        operation = self.service._get_type("CampaignOperation")
        campaign = operation.create
        campaign.name = name
        campaign.campaign_budget = budget_resource
        campaign.advertising_channel_type = self.service.enums.AdvertisingChannelTypeEnum.SEARCH
        campaign.status = getattr(self.service.enums.CampaignStatusEnum, status)

        # Network settings
        net = network_settings or {}
        campaign.network_settings.target_google_search = net.get("target_google_search", True)
        campaign.network_settings.target_search_network = net.get("target_search_network", False)
        campaign.network_settings.target_content_network = False

        # Bidding strategy
        apply_bidding_strategy(campaign, bidding_strategy, bidding_params, self.service.enums)

        if start_date:
            campaign.start_date = start_date
        if end_date:
            campaign.end_date = end_date

        response = await self.service.mutate("CampaignService", [operation])
        resource_name = response.results[0].resource_name
        logger.info("Created Search campaign: %s", resource_name)
        return resource_name

    async def get_campaigns(self, statuses: list[str] | None = None) -> list[dict]:
        """List campaigns filtered by status."""
        query = """
            SELECT campaign.id, campaign.name, campaign.status,
                   campaign.advertising_channel_type, campaign.campaign_budget,
                   metrics.cost_micros, metrics.impressions, metrics.clicks
            FROM campaign
            WHERE campaign.status != 'REMOVED'
        """
        if statuses:
            status_filter = ", ".join(f"'{s}'" for s in statuses)
            query += f" AND campaign.status IN ({status_filter})"

        rows = await self.service.search_stream(query)
        campaigns = []
        for row in rows:
            campaigns.append({
                "id": str(row.campaign.id),
                "name": row.campaign.name,
                "status": row.campaign.status.name,
                "channel_type": row.campaign.advertising_channel_type.name,
                "budget_resource": row.campaign.campaign_budget,
                "cost_dollars": micros_to_dollars(row.metrics.cost_micros),
                "impressions": row.metrics.impressions,
                "clicks": row.metrics.clicks,
            })
        return campaigns

    async def get_campaign(self, campaign_id: str) -> dict:
        """Get a single campaign's full details."""
        query = f"""
            SELECT campaign.id, campaign.name, campaign.status,
                   campaign.advertising_channel_type, campaign.campaign_budget,
                   campaign.start_date, campaign.end_date,
                   metrics.cost_micros, metrics.impressions, metrics.clicks
            FROM campaign
            WHERE campaign.id = {campaign_id}
        """
        rows = await self.service.search_stream(query)
        if not rows:
            return {}
        row = rows[0]
        return {
            "id": str(row.campaign.id),
            "name": row.campaign.name,
            "status": row.campaign.status.name,
            "channel_type": row.campaign.advertising_channel_type.name,
            "budget_resource": row.campaign.campaign_budget,
            "start_date": row.campaign.start_date,
            "end_date": row.campaign.end_date,
            "cost_dollars": micros_to_dollars(row.metrics.cost_micros),
            "impressions": row.metrics.impressions,
            "clicks": row.metrics.clicks,
        }

    async def update_campaign(self, campaign_id: str, updates: dict) -> None:
        """Update campaign fields. Uses field masks automatically."""
        operation = self.service._get_type("CampaignOperation")
        campaign = operation.update
        campaign.resource_name = f"customers/{self.customer_id}/campaigns/{campaign_id}"

        for field, value in updates.items():
            setattr(campaign, field, value)
            operation.update_mask.paths.append(field)

        await self.service.mutate("CampaignService", [operation])

    async def update_campaign_status(self, campaign_id: str, new_status: str) -> None:
        """Enable, pause, or remove a campaign."""
        operation = self.service._get_type("CampaignOperation")
        campaign = operation.update
        campaign.resource_name = f"customers/{self.customer_id}/campaigns/{campaign_id}"
        campaign.status = getattr(self.service.enums.CampaignStatusEnum, new_status)
        operation.update_mask.paths.append("status")

        await self.service.mutate("CampaignService", [operation])
        logger.info("Campaign %s status changed to %s", campaign_id, new_status)

    # --- Ad Group ---

    async def create_ad_group(
        self,
        campaign_resource: str,
        name: str,
        cpc_bid_micros: int = 1_000_000,
        ad_group_type: str = "SEARCH_STANDARD",
        status: str = "ENABLED",
    ) -> str:
        """Create an ad group within a campaign. Returns resource name."""
        operation = self.service._get_type("AdGroupOperation")
        ad_group = operation.create
        ad_group.name = name
        ad_group.campaign = campaign_resource
        ad_group.type_ = getattr(self.service.enums.AdGroupTypeEnum, ad_group_type)
        ad_group.cpc_bid_micros = cpc_bid_micros
        ad_group.status = getattr(self.service.enums.AdGroupStatusEnum, status)

        response = await self.service.mutate("AdGroupService", [operation])
        resource_name = response.results[0].resource_name
        logger.info("Created ad group: %s", resource_name)
        return resource_name

    async def get_ad_groups(self, campaign_id: str) -> list[dict]:
        """List ad groups for a campaign."""
        query = f"""
            SELECT ad_group.id, ad_group.name, ad_group.status,
                   ad_group.campaign, ad_group.cpc_bid_micros
            FROM ad_group
            WHERE ad_group.campaign = 'customers/{self.customer_id}/campaigns/{campaign_id}'
            AND ad_group.status != 'REMOVED'
        """
        rows = await self.service.search_stream(query)
        return [
            {
                "id": str(row.ad_group.id),
                "name": row.ad_group.name,
                "status": row.ad_group.status.name,
                "campaign_resource": row.ad_group.campaign,
                "cpc_bid_dollars": micros_to_dollars(row.ad_group.cpc_bid_micros),
            }
            for row in rows
        ]

    async def update_ad_group_status(self, ad_group_id: str, new_status: str) -> None:
        """Enable, pause, or remove an ad group."""
        operation = self.service._get_type("AdGroupOperation")
        ad_group = operation.update
        ad_group.resource_name = f"customers/{self.customer_id}/adGroups/{ad_group_id}"
        ad_group.status = getattr(self.service.enums.AdGroupStatusEnum, new_status)
        operation.update_mask.paths.append("status")

        await self.service.mutate("AdGroupService", [operation])

    # --- Geographic targeting ---

    async def add_location_targeting(
        self, campaign_id: str, geo_target_constant_ids: list[int]
    ) -> list[str]:
        """Add geographic targeting to a campaign."""
        operations = []
        for geo_id in geo_target_constant_ids:
            operation = self.service._get_type("CampaignCriterionOperation")
            criterion = operation.create
            criterion.campaign = f"customers/{self.customer_id}/campaigns/{campaign_id}"
            criterion.geo_target_constant = f"geoTargetConstants/{geo_id}"
            operations.append(operation)

        response = await self.service.mutate("CampaignCriterionService", operations)
        return [r.resource_name for r in response.results]

    async def add_location_exclusion(
        self, campaign_id: str, geo_target_constant_ids: list[int]
    ) -> list[str]:
        """Exclude geographic locations from a campaign."""
        operations = []
        for geo_id in geo_target_constant_ids:
            operation = self.service._get_type("CampaignCriterionOperation")
            criterion = operation.create
            criterion.campaign = f"customers/{self.customer_id}/campaigns/{campaign_id}"
            criterion.geo_target_constant = f"geoTargetConstants/{geo_id}"
            criterion.negative = True
            operations.append(operation)

        response = await self.service.mutate("CampaignCriterionService", operations)
        return [r.resource_name for r in response.results]
