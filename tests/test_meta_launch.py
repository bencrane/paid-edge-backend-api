"""Tests for Meta campaign launch orchestrator (BJC-157)."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.campaigns.platforms.meta import (
    MetaLaunchResult,
    MetaPlatformAdapter,
    _detect_asset_type,
)
from app.integrations.meta_client import MetaAPIError


class TestDetectAssetType:
    def test_explicit_image(self):
        assert _detect_asset_type({"asset_type": "image"}) == "image"

    def test_explicit_video(self):
        assert _detect_asset_type({"asset_type": "video"}) == "video"

    def test_infer_video_from_url(self):
        assert _detect_asset_type({"url": "https://cdn.io/vid.mp4"}) == "video"

    def test_infer_image_from_url(self):
        assert _detect_asset_type({"url": "https://cdn.io/img.jpg"}) == "image"

    def test_default_is_image(self):
        assert _detect_asset_type({}) == "image"


class TestMetaPlatformAdapter:
    def _make_adapter(self):
        mock_client = AsyncMock()
        mock_client.ad_account_id = "act_123"
        mock_client.create_campaign.return_value = {"id": "campaign_meta_1"}
        mock_client.create_ad_set.return_value = {"id": "adset_meta_1"}
        mock_client.upload_image.return_value = {"hash": "img_hash_1"}
        mock_client.create_image_ad_creative.return_value = {"id": "creative_1"}
        mock_client.create_ad.return_value = {"id": "ad_meta_1"}
        mock_client.set_campaign_status.return_value = {"success": True}
        mock_client.get_campaign.return_value = {
            "effective_status": "ACTIVE", "status": "ACTIVE"
        }

        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.maybe_single.return_value.execute.return_value = MagicMock(
            data={"config": {"page_id": "page_456"}}
        )
        mock_supabase.table.return_value.update.return_value.eq.return_value.execute.return_value = MagicMock()

        return MetaPlatformAdapter(mock_client, mock_supabase)

    @pytest.mark.asyncio
    async def test_full_launch(self):
        adapter = self._make_adapter()

        campaign = {
            "id": "pe-campaign-1",
            "name": "Test Campaign",
            "organization_id": "org-1",
            "budget": 50,
            "config": {"objective": "lead_generation", "locations": ["US"]},
        }
        assets = [
            {
                "id": "asset-1",
                "asset_type": "image",
                "storage_url": "https://cdn.io/img.jpg",
                "headline": "Great Product",
                "intro_text": "Check it out",
            }
        ]

        with patch("app.campaigns.platforms.meta._download_asset", new_callable=AsyncMock, return_value=b"fake_image"):
            result = await adapter.launch(campaign, None, assets)

        assert result.status == "launched"
        assert result.campaign_id == "campaign_meta_1"
        assert len(result.ad_set_ids) == 1
        assert len(result.ad_ids) == 1

    @pytest.mark.asyncio
    async def test_launch_with_all_ads_failing(self):
        adapter = self._make_adapter()
        adapter.client.create_image_ad_creative.side_effect = MetaAPIError(
            100, None, "Creative rejected"
        )

        campaign = {
            "id": "pe-1", "name": "Test", "organization_id": "org-1",
            "budget": 50, "config": {},
        }
        assets = [{"id": "a1", "asset_type": "image", "storage_url": "https://cdn.io/img.jpg"}]

        with patch("app.campaigns.platforms.meta._download_asset", new_callable=AsyncMock, return_value=b"img"):
            result = await adapter.launch(campaign, None, assets)

        assert result.status == "error"
        assert len(result.errors) > 0

    @pytest.mark.asyncio
    async def test_pause_campaign(self):
        adapter = self._make_adapter()
        campaign = {"platform_data": {"platform_campaign_id": "campaign_1"}}
        await adapter.pause(campaign)
        adapter.client.set_campaign_status.assert_called_with("campaign_1", "PAUSED")

    @pytest.mark.asyncio
    async def test_resume_campaign(self):
        adapter = self._make_adapter()
        campaign = {"platform_data": {"platform_campaign_id": "campaign_1"}}
        await adapter.resume(campaign)
        adapter.client.set_campaign_status.assert_called_with("campaign_1", "ACTIVE")

    @pytest.mark.asyncio
    async def test_pause_without_platform_data(self):
        adapter = self._make_adapter()
        with pytest.raises(MetaAPIError):
            await adapter.pause({"platform_data": {}})

    @pytest.mark.asyncio
    async def test_get_status(self):
        adapter = self._make_adapter()
        campaign = {"platform_data": {"platform_campaign_id": "campaign_1"}}
        status = await adapter.get_status(campaign)
        assert status["status"] == "ACTIVE"

    @pytest.mark.asyncio
    async def test_get_status_not_launched(self):
        adapter = self._make_adapter()
        status = await adapter.get_status({"platform_data": {}})
        assert status["status"] == "not_launched"


class TestMetaLaunchResult:
    def test_default_values(self):
        r = MetaLaunchResult(status="launched")
        assert r.platform == "meta"
        assert r.ad_set_ids == []
        assert r.ad_ids == []
