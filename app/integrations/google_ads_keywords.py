"""Google Ads keyword targeting + negative keywords + match types (BJC-143)."""

import logging

from app.integrations.google_ads import GoogleAdsService, micros_to_dollars

logger = logging.getLogger(__name__)

# Standard B2B negative keywords to exclude consumer/non-commercial searches
DEFAULT_B2B_NEGATIVES = [
    "free", "jobs", "salary", "career", "tutorial", "course",
    "reddit", "review", "vs", "alternative", "download", "open source",
    "internship", "volunteer", "diy", "how to", "what is",
]


class GoogleAdsKeywordService:
    """Manages keyword targeting and negative keywords for Google Ads campaigns."""

    def __init__(self, service: GoogleAdsService):
        self.service = service
        self.customer_id = service.customer_id

    # --- Keywords ---

    async def add_keywords(
        self,
        ad_group_resource: str,
        keywords: list[tuple[str, str]],  # [(text, match_type), ...]
    ) -> list[str]:
        """Add keywords to an ad group. Returns resource names."""
        operations = []
        for text, match_type in keywords:
            operation = self.service._get_type("AdGroupCriterionOperation")
            criterion = operation.create
            criterion.ad_group = ad_group_resource
            criterion.status = self.service.enums.AdGroupCriterionStatusEnum.ENABLED
            criterion.keyword.text = text
            criterion.keyword.match_type = getattr(
                self.service.enums.KeywordMatchTypeEnum, match_type
            )
            operations.append(operation)

        response = await self.service.mutate("AdGroupCriterionService", operations)
        resource_names = [r.resource_name for r in response.results]
        logger.info("Added %d keywords to %s", len(resource_names), ad_group_resource)
        return resource_names

    async def get_keywords(self, ad_group_id: str) -> list[dict]:
        """List all keywords in an ad group with performance metrics."""
        query = f"""
            SELECT ad_group_criterion.criterion_id,
                   ad_group_criterion.keyword.text,
                   ad_group_criterion.keyword.match_type,
                   ad_group_criterion.status,
                   ad_group_criterion.cpc_bid_micros,
                   metrics.impressions, metrics.clicks, metrics.cost_micros,
                   metrics.conversions, metrics.average_cpc
            FROM keyword_view
            WHERE ad_group.id = {ad_group_id}
            AND ad_group_criterion.status != 'REMOVED'
        """
        rows = await self.service.search_stream(query)
        return [
            {
                "criterion_id": str(row.ad_group_criterion.criterion_id),
                "text": row.ad_group_criterion.keyword.text,
                "match_type": row.ad_group_criterion.keyword.match_type.name,
                "status": row.ad_group_criterion.status.name,
                "cpc_bid_dollars": micros_to_dollars(row.ad_group_criterion.cpc_bid_micros),
                "impressions": row.metrics.impressions,
                "clicks": row.metrics.clicks,
                "cost_dollars": micros_to_dollars(row.metrics.cost_micros),
                "conversions": row.metrics.conversions,
            }
            for row in rows
        ]

    async def update_keyword_bid(
        self, ad_group_id: str, criterion_id: str, cpc_bid_micros: int
    ) -> None:
        """Update the CPC bid for a specific keyword."""
        operation = self.service._get_type("AdGroupCriterionOperation")
        criterion = operation.update
        criterion.resource_name = (
            f"customers/{self.customer_id}/adGroupCriteria/{ad_group_id}~{criterion_id}"
        )
        criterion.cpc_bid_micros = cpc_bid_micros
        operation.update_mask.paths.append("cpc_bid_micros")

        await self.service.mutate("AdGroupCriterionService", [operation])

    async def remove_keyword(self, ad_group_id: str, criterion_id: str) -> None:
        """Remove a keyword from an ad group (sets status to REMOVED)."""
        operation = self.service._get_type("AdGroupCriterionOperation")
        operation.remove = (
            f"customers/{self.customer_id}/adGroupCriteria/{ad_group_id}~{criterion_id}"
        )
        await self.service.mutate("AdGroupCriterionService", [operation])

    async def pause_keyword(self, ad_group_id: str, criterion_id: str) -> None:
        """Pause a keyword."""
        operation = self.service._get_type("AdGroupCriterionOperation")
        criterion = operation.update
        criterion.resource_name = (
            f"customers/{self.customer_id}/adGroupCriteria/{ad_group_id}~{criterion_id}"
        )
        criterion.status = self.service.enums.AdGroupCriterionStatusEnum.PAUSED
        operation.update_mask.paths.append("status")

        await self.service.mutate("AdGroupCriterionService", [operation])

    # --- Negative keywords ---

    async def add_negative_keywords_campaign(
        self,
        campaign_id: str,
        keywords: list[tuple[str, str]],
    ) -> list[str]:
        """Add negative keywords at the campaign level."""
        operations = []
        for text, match_type in keywords:
            operation = self.service._get_type("CampaignCriterionOperation")
            criterion = operation.create
            criterion.campaign = f"customers/{self.customer_id}/campaigns/{campaign_id}"
            criterion.negative = True
            criterion.keyword.text = text
            criterion.keyword.match_type = getattr(
                self.service.enums.KeywordMatchTypeEnum, match_type
            )
            operations.append(operation)

        response = await self.service.mutate("CampaignCriterionService", operations)
        resource_names = [r.resource_name for r in response.results]
        logger.info(
            "Added %d negative keywords to campaign %s", len(resource_names), campaign_id
        )
        return resource_names

    async def add_negative_keywords_ad_group(
        self,
        ad_group_resource: str,
        keywords: list[tuple[str, str]],
    ) -> list[str]:
        """Add negative keywords at the ad group level."""
        operations = []
        for text, match_type in keywords:
            operation = self.service._get_type("AdGroupCriterionOperation")
            criterion = operation.create
            criterion.ad_group = ad_group_resource
            criterion.negative = True
            criterion.keyword.text = text
            criterion.keyword.match_type = getattr(
                self.service.enums.KeywordMatchTypeEnum, match_type
            )
            operations.append(operation)

        response = await self.service.mutate("AdGroupCriterionService", operations)
        return [r.resource_name for r in response.results]

    async def get_negative_keywords(self, campaign_id: str) -> list[dict]:
        """List negative keywords for a campaign."""
        query = f"""
            SELECT campaign_criterion.criterion_id,
                   campaign_criterion.keyword.text,
                   campaign_criterion.keyword.match_type
            FROM campaign_criterion
            WHERE campaign_criterion.campaign = 'customers/{self.customer_id}/campaigns/{campaign_id}'
            AND campaign_criterion.negative = TRUE
            AND campaign_criterion.type = 'KEYWORD'
        """
        rows = await self.service.search_stream(query)
        return [
            {
                "criterion_id": str(row.campaign_criterion.criterion_id),
                "text": row.campaign_criterion.keyword.text,
                "match_type": row.campaign_criterion.keyword.match_type.name,
            }
            for row in rows
        ]

    async def remove_negative_keyword(self, campaign_id: str, criterion_id: str) -> None:
        """Remove a negative keyword."""
        operation = self.service._get_type("CampaignCriterionOperation")
        operation.remove = (
            f"customers/{self.customer_id}/campaignCriteria/{campaign_id}~{criterion_id}"
        )
        await self.service.mutate("CampaignCriterionService", [operation])

    # --- Search terms ---

    async def get_search_terms(
        self,
        campaign_id: str | None = None,
        ad_group_id: str | None = None,
        date_range: str = "LAST_30_DAYS",
        min_impressions: int = 1,
    ) -> list[dict]:
        """Get actual search queries that triggered ads."""
        conditions = [
            f"segments.date DURING {date_range}",
            f"metrics.impressions > {min_impressions}",
        ]
        if campaign_id:
            conditions.append(f"campaign.id = {campaign_id}")
        if ad_group_id:
            conditions.append(f"ad_group.id = {ad_group_id}")

        where_clause = " AND ".join(conditions)
        query = f"""
            SELECT campaign.name, ad_group.name,
                   search_term_view.search_term, search_term_view.status,
                   metrics.impressions, metrics.clicks, metrics.cost_micros,
                   metrics.conversions
            FROM search_term_view
            WHERE {where_clause}
            ORDER BY metrics.cost_micros DESC
        """
        rows = await self.service.search_stream(query)
        return [
            {
                "search_term": row.search_term_view.search_term,
                "status": row.search_term_view.status.name,
                "campaign_name": row.campaign.name,
                "ad_group_name": row.ad_group.name,
                "impressions": row.metrics.impressions,
                "clicks": row.metrics.clicks,
                "cost_dollars": micros_to_dollars(row.metrics.cost_micros),
                "conversions": row.metrics.conversions,
            }
            for row in rows
        ]


def build_b2b_keyword_set(
    product_category: str,
    use_cases: list[str],
    competitors: list[str] | None = None,
) -> dict:
    """Generate a suggested keyword set for B2B search campaigns.

    Returns dict with keys: exact, phrase, broad, negative
    """
    exact = [f"{product_category} software", f"{product_category} platform"]
    phrase = [product_category]
    broad = [f"{product_category} solutions for enterprise"]

    for use_case in use_cases:
        exact.append(f"{use_case} {product_category}")
        phrase.append(use_case)

    if competitors:
        for comp in competitors:
            phrase.append(f"{comp} alternative")
            phrase.append(f"{comp} vs")

    return {
        "exact": exact,
        "phrase": phrase,
        "broad": broad,
        "negative": list(DEFAULT_B2B_NEGATIVES),
    }
