"""Google Ads Responsive Search Ad (RSA) creation + ad management (BJC-144)."""

import logging

from app.integrations.google_ads import GoogleAdsService, micros_to_dollars

logger = logging.getLogger(__name__)


class RSAValidator:
    """Validates RSA inputs before API call to catch errors early."""

    MAX_HEADLINES = 15
    MIN_HEADLINES = 3
    MAX_DESCRIPTIONS = 4
    MIN_DESCRIPTIONS = 2
    HEADLINE_MAX_CHARS = 30
    DESCRIPTION_MAX_CHARS = 90
    PATH_MAX_CHARS = 15

    @classmethod
    def validate(
        cls,
        headlines: list[str],
        descriptions: list[str],
        path1: str | None = None,
        path2: str | None = None,
    ) -> list[str]:
        """Returns list of validation errors, empty if valid."""
        errors = []
        if len(headlines) < cls.MIN_HEADLINES:
            errors.append(
                f"Need at least {cls.MIN_HEADLINES} headlines, got {len(headlines)}"
            )
        if len(headlines) > cls.MAX_HEADLINES:
            errors.append(
                f"Max {cls.MAX_HEADLINES} headlines, got {len(headlines)}"
            )
        for i, h in enumerate(headlines):
            if len(h) > cls.HEADLINE_MAX_CHARS:
                errors.append(
                    f"Headline {i+1} is {len(h)} chars (max {cls.HEADLINE_MAX_CHARS}): '{h}'"
                )
        if len(descriptions) < cls.MIN_DESCRIPTIONS:
            errors.append(
                f"Need at least {cls.MIN_DESCRIPTIONS} descriptions, got {len(descriptions)}"
            )
        if len(descriptions) > cls.MAX_DESCRIPTIONS:
            errors.append(
                f"Max {cls.MAX_DESCRIPTIONS} descriptions, got {len(descriptions)}"
            )
        for i, d in enumerate(descriptions):
            if len(d) > cls.DESCRIPTION_MAX_CHARS:
                errors.append(
                    f"Description {i+1} is {len(d)} chars (max {cls.DESCRIPTION_MAX_CHARS}): '{d}'"
                )
        if path1 and len(path1) > cls.PATH_MAX_CHARS:
            errors.append(
                f"Path1 is {len(path1)} chars (max {cls.PATH_MAX_CHARS})"
            )
        if path2 and len(path2) > cls.PATH_MAX_CHARS:
            errors.append(
                f"Path2 is {len(path2)} chars (max {cls.PATH_MAX_CHARS})"
            )
        return errors


class GoogleAdsAdService:
    """Manages Responsive Search Ads and ad lifecycle."""

    def __init__(self, service: GoogleAdsService):
        self.service = service
        self.customer_id = service.customer_id

    async def create_responsive_search_ad(
        self,
        ad_group_id: str,
        headlines: list[str],
        descriptions: list[str],
        final_url: str,
        pinned_headlines: dict[int, int] | None = None,
        pinned_descriptions: dict[int, int] | None = None,
        path1: str | None = None,
        path2: str | None = None,
        status: str = "PAUSED",
    ) -> str:
        """Create a Responsive Search Ad. Returns resource name."""
        # Validate inputs
        errors = RSAValidator.validate(headlines, descriptions, path1, path2)
        if errors:
            raise ValueError(f"RSA validation failed: {'; '.join(errors)}")

        operation = self.service._get_type("AdGroupAdOperation")
        ad_group_ad = operation.create
        ad_group_ad.ad_group = (
            f"customers/{self.customer_id}/adGroups/{ad_group_id}"
        )
        ad_group_ad.status = getattr(
            self.service.enums.AdGroupAdStatusEnum, status
        )

        ad = ad_group_ad.ad
        ad.final_urls.append(final_url)

        # Add headlines
        pinned_headlines = pinned_headlines or {}
        for i, headline_text in enumerate(headlines):
            headline = self.service._get_type("AdTextAsset")
            headline.text = headline_text
            if i in pinned_headlines:
                headline.pinned_field = getattr(
                    self.service.enums.ServedAssetFieldTypeEnum,
                    f"HEADLINE_{pinned_headlines[i]}",
                )
            ad.responsive_search_ad.headlines.append(headline)

        # Add descriptions
        pinned_descriptions = pinned_descriptions or {}
        for i, desc_text in enumerate(descriptions):
            desc = self.service._get_type("AdTextAsset")
            desc.text = desc_text
            if i in pinned_descriptions:
                desc.pinned_field = getattr(
                    self.service.enums.ServedAssetFieldTypeEnum,
                    f"DESCRIPTION_{pinned_descriptions[i]}",
                )
            ad.responsive_search_ad.descriptions.append(desc)

        # Display URL paths
        if path1:
            ad.responsive_search_ad.path1 = path1
        if path2:
            ad.responsive_search_ad.path2 = path2

        response = await self.service.mutate("AdGroupAdService", [operation])
        resource_name = response.results[0].resource_name
        logger.info("Created RSA: %s", resource_name)
        return resource_name

    async def get_ads(self, ad_group_id: str) -> list[dict]:
        """List ads in an ad group with performance and policy status."""
        query = f"""
            SELECT ad_group_ad.ad.id,
                   ad_group_ad.ad.responsive_search_ad.headlines,
                   ad_group_ad.ad.responsive_search_ad.descriptions,
                   ad_group_ad.ad.final_urls,
                   ad_group_ad.status,
                   ad_group_ad.policy_summary.approval_status,
                   ad_group_ad.ad_strength,
                   metrics.impressions, metrics.clicks, metrics.cost_micros,
                   metrics.conversions
            FROM ad_group_ad
            WHERE ad_group.id = {ad_group_id}
            AND ad_group_ad.status != 'REMOVED'
        """
        rows = await self.service.search_stream(query)
        ads = []
        for row in rows:
            headlines = [
                asset.text for asset in row.ad_group_ad.ad.responsive_search_ad.headlines
            ]
            descriptions = [
                asset.text for asset in row.ad_group_ad.ad.responsive_search_ad.descriptions
            ]
            ads.append({
                "ad_id": str(row.ad_group_ad.ad.id),
                "ad_group_id": ad_group_id,
                "status": row.ad_group_ad.status.name,
                "headlines": headlines,
                "descriptions": descriptions,
                "final_urls": list(row.ad_group_ad.ad.final_urls),
                "ad_strength": row.ad_group_ad.ad_strength.name if hasattr(row.ad_group_ad, "ad_strength") else None,
                "approval_status": row.ad_group_ad.policy_summary.approval_status.name if hasattr(row.ad_group_ad.policy_summary, "approval_status") else None,
                "impressions": row.metrics.impressions,
                "clicks": row.metrics.clicks,
                "cost_dollars": micros_to_dollars(row.metrics.cost_micros),
                "conversions": row.metrics.conversions,
            })
        return ads

    async def get_ad_strength(self, ad_group_id: str, ad_id: str) -> dict:
        """Check ad strength rating."""
        query = f"""
            SELECT ad_group_ad.ad_strength
            FROM ad_group_ad
            WHERE ad_group.id = {ad_group_id}
            AND ad_group_ad.ad.id = {ad_id}
        """
        rows = await self.service.search_stream(query)
        if not rows:
            return {"ad_strength": "UNKNOWN"}
        return {"ad_strength": rows[0].ad_group_ad.ad_strength.name}

    async def update_ad_status(
        self, ad_group_id: str, ad_id: str, new_status: str
    ) -> None:
        """Enable, pause, or remove an ad."""
        operation = self.service._get_type("AdGroupAdOperation")
        ad_group_ad = operation.update
        ad_group_ad.resource_name = (
            f"customers/{self.customer_id}/adGroupAds/{ad_group_id}~{ad_id}"
        )
        ad_group_ad.status = getattr(
            self.service.enums.AdGroupAdStatusEnum, new_status
        )
        operation.update_mask.paths.append("status")

        await self.service.mutate("AdGroupAdService", [operation])
        logger.info("Ad %s in ad group %s status changed to %s", ad_id, ad_group_id, new_status)

    async def get_ad_policy_status(self, ad_group_id: str, ad_id: str) -> dict:
        """Check policy review status for an ad."""
        query = f"""
            SELECT ad_group_ad.policy_summary.approval_status,
                   ad_group_ad.policy_summary.policy_topic_entries
            FROM ad_group_ad
            WHERE ad_group.id = {ad_group_id}
            AND ad_group_ad.ad.id = {ad_id}
        """
        rows = await self.service.search_stream(query)
        if not rows:
            return {"approval_status": "UNKNOWN", "topics": []}
        row = rows[0]
        return {
            "approval_status": row.ad_group_ad.policy_summary.approval_status.name,
            "topics": [
                entry.topic for entry in row.ad_group_ad.policy_summary.policy_topic_entries
            ] if hasattr(row.ad_group_ad.policy_summary, "policy_topic_entries") else [],
        }

    async def upload_image_asset(self, image_data: bytes, asset_name: str) -> str:
        """Upload an image and return asset resource name."""
        operation = self.service._get_type("AssetOperation")
        asset = operation.create
        asset.name = asset_name
        asset.type_ = self.service.enums.AssetTypeEnum.IMAGE
        asset.image_asset.data = image_data

        response = await self.service.mutate("AssetService", [operation])
        return response.results[0].resource_name

    async def create_youtube_video_asset(
        self, youtube_video_id: str, asset_name: str
    ) -> str:
        """Create a YouTube video asset reference."""
        operation = self.service._get_type("AssetOperation")
        asset = operation.create
        asset.name = asset_name
        asset.type_ = self.service.enums.AssetTypeEnum.YOUTUBE_VIDEO
        asset.youtube_video_asset.youtube_video_id = youtube_video_id

        response = await self.service.mutate("AssetService", [operation])
        return response.results[0].resource_name
