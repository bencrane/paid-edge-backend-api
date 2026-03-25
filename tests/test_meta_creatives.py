"""Tests for Meta media upload, ad creative, and ad CRUD (BJC-155)."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.integrations.meta_creatives import (
    IMAGE_SPECS,
    META_CTA_TYPES,
    VIDEO_SPECS,
    MetaCreativesMixin,
)
from app.integrations.meta_media import MetaMediaMixin


class FakeClient(MetaCreativesMixin, MetaMediaMixin):
    def __init__(self):
        self.ad_account_id = "act_123"
        self._request = AsyncMock()
        self._paginate = AsyncMock()


class TestImageUpload:
    @pytest.mark.asyncio
    async def test_upload_image_from_bytes(self):
        client = FakeClient()
        client._request.return_value = {
            "images": {"bytes": {"hash": "abc123", "url": "https://img.fb.com/123"}}
        }
        result = await client.upload_image(image_bytes=b"fake_image_data")
        assert result["hash"] == "abc123"
        assert result["url"] == "https://img.fb.com/123"


class TestVideoUpload:
    @pytest.mark.asyncio
    async def test_upload_small_video(self):
        client = FakeClient()
        client._request.return_value = {"id": "video_456"}

        with patch("builtins.open", MagicMock()), \
             patch("os.path.getsize", return_value=100_000):
            result = await client.upload_video("/tmp/test.mp4", "Test Video")

        assert result["video_id"] == "video_456"

    @pytest.mark.asyncio
    async def test_wait_for_video_ready(self):
        client = FakeClient()
        client._request.side_effect = [
            {"status": {"video_status": "processing"}},
            {"status": {"video_status": "ready"}},
        ]

        with patch("app.integrations.meta_media.asyncio.sleep", new_callable=AsyncMock):
            result = await client.wait_for_video_ready("video_123", poll_interval=0)

        assert result["status"]["video_status"] == "ready"


class TestImageAdCreative:
    @pytest.mark.asyncio
    async def test_create_image_creative(self):
        client = FakeClient()
        client._request.return_value = {"id": "creative_123"}

        result = await client.create_image_ad_creative(
            name="Test Creative",
            page_id="page_456",
            image_hash="abc123",
            link="https://example.com",
            message="Check this out",
            headline="Great Product",
        )
        assert result["id"] == "creative_123"
        call_data = client._request.call_args[1]["data"]
        assert "object_story_spec" in call_data


class TestVideoAdCreative:
    @pytest.mark.asyncio
    async def test_create_video_creative(self):
        client = FakeClient()
        client._request.return_value = {"id": "creative_video_123"}

        result = await client.create_video_ad_creative(
            name="Video Creative",
            page_id="page_456",
            video_id="video_789",
            message="Watch this",
            cta_type="SIGN_UP",
            cta_link="https://example.com",
        )
        assert result["id"] == "creative_video_123"


class TestCarouselCreative:
    @pytest.mark.asyncio
    async def test_create_carousel(self):
        client = FakeClient()
        client._request.return_value = {"id": "creative_carousel"}

        cards = [
            {"link": "https://a.com", "image_hash": "h1", "name": "Card 1"},
            {"link": "https://b.com", "image_hash": "h2", "name": "Card 2"},
        ]
        result = await client.create_carousel_creative(
            name="Carousel",
            page_id="page_456",
            message="See our products",
            cards=cards,
            link="https://example.com",
        )
        assert result["id"] == "creative_carousel"


class TestLeadAdCreative:
    @pytest.mark.asyncio
    async def test_create_lead_ad(self):
        client = FakeClient()
        client._request.return_value = {"id": "creative_lead"}

        result = await client.create_lead_ad_creative(
            name="Lead Creative",
            page_id="page_456",
            message="Sign up now",
            form_id="form_789",
            cta_type="SIGN_UP",
        )
        assert result["id"] == "creative_lead"


class TestAdCRUD:
    @pytest.mark.asyncio
    async def test_create_ad(self):
        client = FakeClient()
        client._request.return_value = {"id": "ad_123"}

        result = await client.create_ad(
            name="Test Ad",
            adset_id="adset_456",
            creative_id="creative_789",
        )
        assert result["id"] == "ad_123"

    @pytest.mark.asyncio
    async def test_get_ad(self):
        client = FakeClient()
        client._request.return_value = {
            "id": "ad_123", "status": "PAUSED", "effective_status": "PAUSED"
        }
        result = await client.get_ad("ad_123")
        assert result["status"] == "PAUSED"

    @pytest.mark.asyncio
    async def test_update_ad(self):
        client = FakeClient()
        client._request.return_value = {"success": True}
        await client.update_ad("ad_123", status="ACTIVE")
        assert client._request.call_count == 1

    @pytest.mark.asyncio
    async def test_list_ads(self):
        client = FakeClient()
        client._paginate.return_value = [{"id": "ad_1"}, {"id": "ad_2"}]
        result = await client.list_ads(adset_id="adset_123")
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_delete_ad(self):
        client = FakeClient()
        client._request.return_value = {"success": True}
        await client.delete_ad("ad_123")
        assert client._request.call_args[0][0] == "DELETE"


class TestConstants:
    def test_cta_types(self):
        assert "LEARN_MORE" in META_CTA_TYPES
        assert "SIGN_UP" in META_CTA_TYPES
        assert len(META_CTA_TYPES) > 10

    def test_image_specs(self):
        assert IMAGE_SPECS["recommended"] == (1080, 1080)
        assert IMAGE_SPECS["max_file_size_mb"] == 30

    def test_video_specs(self):
        assert VIDEO_SPECS["max_file_size_gb"] == 4
