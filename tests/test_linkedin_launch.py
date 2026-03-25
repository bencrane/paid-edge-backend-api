"""Tests for LinkedIn campaign launch orchestration — BJC-133."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.campaigns.platforms.linkedin import (
    LinkedInLaunchResult,
    LinkedInPlatformAdapter,
    _detect_asset_type,
)
from app.integrations.linkedin import LinkedInAdsClient, LinkedInAPIError
from app.integrations.linkedin_models import LinkedInAudienceSyncResult

# --- Helpers ---

_ACCOUNT_ID = 507404993
_CAMPAIGN_GROUP_ID = 100200
_CAMPAIGN_ID = 300400
_CREATIVE_ID = 500600
_ORG_ID = "12345"


def _make_campaign(
    name="Test Campaign",
    campaign_id="camp-001",
    platforms=None,
    audience_segment_id="seg-001",
    budget=100,
    config=None,
    platform_data=None,
):
    """Build a mock PaidEdge campaign dict."""
    return {
        "id": campaign_id,
        "name": name,
        "organization_id": _ORG_ID,
        "platforms": platforms or ["linkedin"],
        "audience_segment_id": audience_segment_id,
        "budget": budget,
        "status": "draft",
        "config": config or {
            "objective": "WEBSITE_VISITS",
            "campaign_type": "SPONSORED_UPDATES",
            "cost_type": "CPC",
            "currency": "USD",
            "locations": ["urn:li:geo:103644278"],
            "industries": ["urn:li:industry:4"],
        },
        "platform_data": platform_data,
    }


def _make_assets(count=2, asset_type="image"):
    """Build mock asset list."""
    assets = []
    for i in range(count):
        assets.append({
            "id": f"asset-{i}",
            "asset_type": asset_type,
            "storage_url": f"https://storage.example.com/asset-{i}.png",
            "headline": f"Headline {i}",
            "intro_text": f"Intro text {i}",
            "title": f"Title {i}",
        })
    return assets


def _mock_supabase():
    """Create a Supabase mock for storing platform data."""
    mock = MagicMock()
    mock_chain = MagicMock()
    mock_chain.eq.return_value = mock_chain
    mock_chain.maybe_single.return_value = mock_chain
    mock_chain.select.return_value = mock_chain
    mock_chain.update.return_value = mock_chain

    # Default: provider_configs returns selected account
    config_data = {"config": {"selected_ad_account_id": str(_ACCOUNT_ID)}}
    mock_chain.execute.return_value = SimpleNamespace(data=config_data)
    mock.table.return_value = mock_chain
    return mock, mock_chain


def _setup_client_mocks(mock_client: AsyncMock):
    """Configure common return values for LinkedIn client mocks."""
    mock_client.org_id = _ORG_ID

    # get_selected_account_id
    mock_client.get_selected_account_id = AsyncMock(return_value=_ACCOUNT_ID)

    # Campaign groups — empty list (no existing groups)
    mock_client.get_campaign_groups = AsyncMock(return_value=[])

    # Create campaign group
    mock_client.create_campaign_group = AsyncMock(
        return_value={"id": f"urn:li:sponsoredCampaignGroup:{_CAMPAIGN_GROUP_ID}"}
    )

    # Create campaign
    mock_client.create_campaign = AsyncMock(
        return_value={"id": f"urn:li:sponsoredCampaign:{_CAMPAIGN_ID}"}
    )

    # Create image ad
    mock_client.create_image_ad = AsyncMock(
        return_value={"id": f"urn:li:adCreative:{_CREATIVE_ID}"}
    )

    # Create document ad
    mock_client.create_document_ad = AsyncMock(
        return_value={"id": f"urn:li:adCreative:{_CREATIVE_ID + 1}"}
    )

    # Upload video
    mock_client.upload_video = AsyncMock(return_value="urn:li:video:123")

    # Create sponsored post
    mock_client.create_sponsored_post = AsyncMock(
        return_value="urn:li:ugcPost:456"
    )

    # Create creative
    mock_client.create_creative = AsyncMock(
        return_value={"id": f"urn:li:adCreative:{_CREATIVE_ID + 2}"}
    )

    # Update campaign status
    mock_client.update_campaign_status = AsyncMock(return_value=None)

    # Get campaign (for status check)
    mock_client.get_campaign = AsyncMock(
        return_value={
            "status": "ACTIVE",
            "servingStatuses": ["ACCOUNT_SERVING"],
            "reviewStatus": "APPROVED",
        }
    )

    return mock_client


# --- Asset type detection ---


class TestDetectAssetType:
    def test_explicit_image_type(self):
        assert _detect_asset_type({"asset_type": "image"}) == "image"

    def test_explicit_document_type(self):
        assert _detect_asset_type({"asset_type": "document"}) == "document"

    def test_explicit_video_type(self):
        assert _detect_asset_type({"asset_type": "video"}) == "video"

    def test_pdf_extension_detected(self):
        asset = {"storage_url": "https://storage.example.com/whitepaper.pdf"}
        assert _detect_asset_type(asset) == "document"

    def test_mp4_extension_detected(self):
        asset = {"storage_url": "https://storage.example.com/demo.mp4"}
        assert _detect_asset_type(asset) == "video"

    def test_unknown_defaults_to_image(self):
        asset = {"storage_url": "https://storage.example.com/file.unknown"}
        assert _detect_asset_type(asset) == "image"


# --- Full launch flow ---


class TestFullLaunchFlow:
    @pytest.mark.asyncio
    @patch("app.campaigns.platforms.linkedin._download_asset", new_callable=AsyncMock)
    async def test_launch_all_10_steps(self, mock_download):
        """Full launch: account → group → targeting → campaign → creatives → activate → store."""
        mock_download.return_value = b"fake-image-bytes"

        mock_client = _setup_client_mocks(AsyncMock(spec=LinkedInAdsClient))
        mock_supabase, mock_chain = _mock_supabase()

        adapter = LinkedInPlatformAdapter(
            linkedin_client=mock_client,
            supabase=mock_supabase,
        )

        campaign = _make_campaign()
        assets = _make_assets(2)

        result = await adapter.launch_campaign(
            paidedge_campaign=campaign,
            audience_segment=None,
            assets=assets,
        )

        assert isinstance(result, LinkedInLaunchResult)
        assert result.platform == "linkedin"
        assert result.status == "launched"
        assert result.campaign_group_id == _CAMPAIGN_GROUP_ID
        assert result.campaign_id == _CAMPAIGN_ID
        assert len(result.creative_ids) == 2
        assert result.errors is None

        # Verify step order
        mock_client.get_selected_account_id.assert_awaited_once()
        mock_client.get_campaign_groups.assert_awaited_once()
        mock_client.create_campaign_group.assert_awaited_once()
        mock_client.create_campaign.assert_awaited_once()
        assert mock_client.create_image_ad.await_count == 2
        mock_client.update_campaign_status.assert_awaited_once_with(
            _ACCOUNT_ID, _CAMPAIGN_ID, "ACTIVE"
        )

        # Verify platform data stored
        mock_supabase.table.assert_any_call("campaigns")

    @pytest.mark.asyncio
    @patch("app.campaigns.platforms.linkedin._download_asset", new_callable=AsyncMock)
    async def test_launch_stores_platform_data(self, mock_download):
        """Platform data should be stored back to PaidEdge campaign."""
        mock_download.return_value = b"fake-image-bytes"

        mock_client = _setup_client_mocks(AsyncMock(spec=LinkedInAdsClient))
        mock_supabase, mock_chain = _mock_supabase()

        adapter = LinkedInPlatformAdapter(
            linkedin_client=mock_client,
            supabase=mock_supabase,
        )

        campaign = _make_campaign()
        assets = _make_assets(1)

        result = await adapter.launch_campaign(
            paidedge_campaign=campaign,
            audience_segment=None,
            assets=assets,
        )

        assert result.status == "launched"

        # Verify update was called with platform_data
        update_calls = [
            c for c in mock_chain.update.call_args_list
            if c.args and isinstance(c.args[0], dict) and "platform_data" in c.args[0]
        ]
        assert len(update_calls) == 1
        platform_data = update_calls[0].args[0]["platform_data"]
        assert platform_data["platform"] == "linkedin"
        assert platform_data["platform_campaign_id"] == _CAMPAIGN_ID
        assert platform_data["platform_campaign_group_id"] == _CAMPAIGN_GROUP_ID
        assert platform_data["platform_account_id"] == _ACCOUNT_ID


# --- Pause and resume ---


class TestPauseAndResume:
    @pytest.mark.asyncio
    async def test_pause_via_stored_platform_ids(self):
        """Pause should use stored platform_campaign_id and platform_account_id."""
        mock_client = _setup_client_mocks(AsyncMock(spec=LinkedInAdsClient))

        adapter = LinkedInPlatformAdapter(linkedin_client=mock_client)

        campaign = _make_campaign(
            platform_data={
                "platform_account_id": _ACCOUNT_ID,
                "platform_campaign_id": _CAMPAIGN_ID,
            }
        )

        await adapter.pause_campaign(campaign)

        mock_client.update_campaign_status.assert_awaited_once_with(
            _ACCOUNT_ID, _CAMPAIGN_ID, "PAUSED"
        )

    @pytest.mark.asyncio
    async def test_resume_via_stored_platform_ids(self):
        """Resume should use stored platform IDs to set ACTIVE."""
        mock_client = _setup_client_mocks(AsyncMock(spec=LinkedInAdsClient))

        adapter = LinkedInPlatformAdapter(linkedin_client=mock_client)

        campaign = _make_campaign(
            platform_data={
                "platform_account_id": _ACCOUNT_ID,
                "platform_campaign_id": _CAMPAIGN_ID,
            }
        )

        await adapter.resume_campaign(campaign)

        mock_client.update_campaign_status.assert_awaited_once_with(
            _ACCOUNT_ID, _CAMPAIGN_ID, "ACTIVE"
        )

    @pytest.mark.asyncio
    async def test_pause_without_platform_data_raises(self):
        """Pause with no platform_data should raise LinkedInAPIError."""
        mock_client = _setup_client_mocks(AsyncMock(spec=LinkedInAdsClient))
        adapter = LinkedInPlatformAdapter(linkedin_client=mock_client)

        campaign = _make_campaign(platform_data={})

        with pytest.raises(LinkedInAPIError, match="No LinkedIn platform data"):
            await adapter.pause_campaign(campaign)

    @pytest.mark.asyncio
    async def test_resume_without_platform_data_raises(self):
        """Resume with no platform_data should raise LinkedInAPIError."""
        mock_client = _setup_client_mocks(AsyncMock(spec=LinkedInAdsClient))
        adapter = LinkedInPlatformAdapter(linkedin_client=mock_client)

        campaign = _make_campaign(platform_data={})

        with pytest.raises(LinkedInAPIError, match="No LinkedIn platform data"):
            await adapter.resume_campaign(campaign)


# --- Campaign status ---


class TestCampaignStatus:
    @pytest.mark.asyncio
    async def test_get_status_from_linkedin(self):
        """Should return status, serving_status, review_status from LinkedIn."""
        mock_client = _setup_client_mocks(AsyncMock(spec=LinkedInAdsClient))
        adapter = LinkedInPlatformAdapter(linkedin_client=mock_client)

        campaign = _make_campaign(
            platform_data={
                "platform_account_id": _ACCOUNT_ID,
                "platform_campaign_id": _CAMPAIGN_ID,
            }
        )

        status = await adapter.get_campaign_status(campaign)

        assert status["status"] == "ACTIVE"
        assert status["linkedin_campaign_id"] == _CAMPAIGN_ID
        assert status["review_status"] == "APPROVED"
        mock_client.get_campaign.assert_awaited_once_with(
            _ACCOUNT_ID, _CAMPAIGN_ID
        )

    @pytest.mark.asyncio
    async def test_get_status_no_platform_data(self):
        """Should return not_launched when no platform data."""
        mock_client = _setup_client_mocks(AsyncMock(spec=LinkedInAdsClient))
        adapter = LinkedInPlatformAdapter(linkedin_client=mock_client)

        campaign = _make_campaign(platform_data={})

        status = await adapter.get_campaign_status(campaign)

        assert status["status"] == "not_launched"
        assert status["linkedin_campaign_id"] is None


# --- Error handling ---


class TestErrorHandling:
    @pytest.mark.asyncio
    @patch("app.campaigns.platforms.linkedin._download_asset", new_callable=AsyncMock)
    async def test_partial_creative_failure(self, mock_download):
        """If some creatives fail, others should still be created."""
        mock_download.return_value = b"fake-image-bytes"

        mock_client = _setup_client_mocks(AsyncMock(spec=LinkedInAdsClient))

        # First creative succeeds, second fails
        mock_client.create_image_ad = AsyncMock(
            side_effect=[
                {"id": f"urn:li:adCreative:{_CREATIVE_ID}"},
                LinkedInAPIError(400, None, "Headline too long"),
            ]
        )

        adapter = LinkedInPlatformAdapter(
            linkedin_client=mock_client,
            supabase=MagicMock(),
        )

        campaign = _make_campaign()
        assets = _make_assets(2)

        result = await adapter.launch_campaign(
            paidedge_campaign=campaign,
            audience_segment=None,
            assets=assets,
        )

        # Campaign should still launch with partial creatives
        assert result.status == "pending_review"
        assert len(result.creative_ids) == 1
        assert result.creative_ids[0] == _CREATIVE_ID
        assert result.errors is not None
        assert len(result.errors) == 1
        assert "Headline too long" in result.errors[0]

        # Campaign should still be activated
        mock_client.update_campaign_status.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("app.campaigns.platforms.linkedin._download_asset", new_callable=AsyncMock)
    async def test_all_creatives_fail_returns_error(self, mock_download):
        """If all creatives fail, status should be error."""
        mock_download.return_value = b"fake-image-bytes"

        mock_client = _setup_client_mocks(AsyncMock(spec=LinkedInAdsClient))
        mock_client.create_image_ad = AsyncMock(
            side_effect=LinkedInAPIError(400, None, "Bad creative")
        )

        adapter = LinkedInPlatformAdapter(
            linkedin_client=mock_client,
            supabase=MagicMock(),
        )

        campaign = _make_campaign()
        assets = _make_assets(2)

        result = await adapter.launch_campaign(
            paidedge_campaign=campaign,
            audience_segment=None,
            assets=assets,
        )

        assert result.status == "error"
        assert len(result.creative_ids) == 0
        assert result.errors is not None
        # Campaign should NOT be activated
        mock_client.update_campaign_status.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_campaign_creation_failure(self):
        """If campaign creation fails, should return error with group ID."""
        mock_client = _setup_client_mocks(AsyncMock(spec=LinkedInAdsClient))
        mock_client.create_campaign = AsyncMock(
            side_effect=LinkedInAPIError(400, None, "Invalid targeting")
        )

        adapter = LinkedInPlatformAdapter(
            linkedin_client=mock_client,
            supabase=MagicMock(),
        )

        campaign = _make_campaign()
        assets = _make_assets(1)

        result = await adapter.launch_campaign(
            paidedge_campaign=campaign,
            audience_segment=None,
            assets=assets,
        )

        assert result.status == "error"
        assert result.campaign_group_id == _CAMPAIGN_GROUP_ID
        assert result.campaign_id == 0
        assert "Invalid targeting" in result.errors[0]


# --- Campaign group reuse ---


class TestCampaignGroupReuse:
    @pytest.mark.asyncio
    @patch("app.campaigns.platforms.linkedin._download_asset", new_callable=AsyncMock)
    async def test_reuses_existing_group(self, mock_download):
        """Should reuse campaign group if one with matching name exists."""
        mock_download.return_value = b"fake-image-bytes"

        mock_client = _setup_client_mocks(AsyncMock(spec=LinkedInAdsClient))

        # Return existing group with matching name
        existing_group = MagicMock()
        existing_group.name = "PaidEdge: Test Campaign"
        existing_group.id = 999888
        mock_client.get_campaign_groups = AsyncMock(
            return_value=[existing_group]
        )

        adapter = LinkedInPlatformAdapter(
            linkedin_client=mock_client,
            supabase=MagicMock(),
        )

        campaign = _make_campaign()
        assets = _make_assets(1)

        result = await adapter.launch_campaign(
            paidedge_campaign=campaign,
            audience_segment=None,
            assets=assets,
        )

        assert result.campaign_group_id == 999888
        mock_client.create_campaign_group.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("app.campaigns.platforms.linkedin._download_asset", new_callable=AsyncMock)
    async def test_creates_new_group_when_no_match(self, mock_download):
        """Should create new group when no existing group matches."""
        mock_download.return_value = b"fake-image-bytes"

        mock_client = _setup_client_mocks(AsyncMock(spec=LinkedInAdsClient))

        # Return existing group with different name
        other_group = MagicMock()
        other_group.name = "PaidEdge: Other Campaign"
        other_group.id = 111222
        mock_client.get_campaign_groups = AsyncMock(
            return_value=[other_group]
        )

        adapter = LinkedInPlatformAdapter(
            linkedin_client=mock_client,
            supabase=MagicMock(),
        )

        campaign = _make_campaign()
        assets = _make_assets(1)

        result = await adapter.launch_campaign(
            paidedge_campaign=campaign,
            audience_segment=None,
            assets=assets,
        )

        assert result.campaign_group_id == _CAMPAIGN_GROUP_ID
        mock_client.create_campaign_group.assert_awaited_once()


# --- Audience push integration ---


class TestAudiencePush:
    @pytest.mark.asyncio
    @patch("app.campaigns.platforms.linkedin._download_asset", new_callable=AsyncMock)
    @patch(
        "app.campaigns.platforms.linkedin.LinkedInAudiencePushService",
    )
    async def test_push_audience_on_launch(self, mock_push_cls, mock_download):
        """Should push audience to LinkedIn when audience_segment is provided."""
        mock_download.return_value = b"fake-image-bytes"

        mock_push_instance = AsyncMock()
        mock_push_instance.push_segment = AsyncMock(
            return_value=LinkedInAudienceSyncResult(
                segment_id="dmp-123",
                segment_type="USER",
                total_uploaded=500,
                batches_completed=1,
                status="building",
                ad_segment_urn="urn:li:adSegment:789",
            )
        )
        mock_push_cls.return_value = mock_push_instance

        mock_client = _setup_client_mocks(AsyncMock(spec=LinkedInAdsClient))
        mock_supabase = MagicMock()
        mock_ch = MagicMock()

        adapter = LinkedInPlatformAdapter(
            linkedin_client=mock_client,
            supabase=mock_supabase,
            clickhouse=mock_ch,
        )

        audience = {"id": "seg-001", "organization_id": _ORG_ID}
        campaign = _make_campaign()
        assets = _make_assets(1)

        result = await adapter.launch_campaign(
            paidedge_campaign=campaign,
            audience_segment=audience,
            assets=assets,
        )

        assert result.status == "launched"
        mock_push_instance.push_segment.assert_awaited_once()

        # Targeting should include matched audiences
        create_campaign_call = mock_client.create_campaign.call_args
        targeting = create_campaign_call.kwargs.get(
            "targeting", create_campaign_call[1].get("targeting")
        )
        assert targeting is not None


# --- Document and video assets ---


class TestAssetTypes:
    @pytest.mark.asyncio
    @patch("app.campaigns.platforms.linkedin._download_asset", new_callable=AsyncMock)
    async def test_document_asset_uses_create_document_ad(self, mock_download):
        """Document assets should go through create_document_ad pipeline."""
        mock_download.return_value = b"fake-pdf-bytes"

        mock_client = _setup_client_mocks(AsyncMock(spec=LinkedInAdsClient))
        adapter = LinkedInPlatformAdapter(
            linkedin_client=mock_client,
            supabase=MagicMock(),
        )

        campaign = _make_campaign()
        assets = _make_assets(1, asset_type="document")

        result = await adapter.launch_campaign(
            paidedge_campaign=campaign,
            audience_segment=None,
            assets=assets,
        )

        assert result.status == "launched"
        mock_client.create_document_ad.assert_awaited_once()
        mock_client.create_image_ad.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("app.campaigns.platforms.linkedin._download_asset", new_callable=AsyncMock)
    async def test_video_asset_pipeline(self, mock_download):
        """Video assets should use upload_video → create_sponsored_post → create_creative."""
        mock_download.return_value = b"fake-video-bytes"

        mock_client = _setup_client_mocks(AsyncMock(spec=LinkedInAdsClient))
        adapter = LinkedInPlatformAdapter(
            linkedin_client=mock_client,
            supabase=MagicMock(),
        )

        campaign = _make_campaign()
        assets = _make_assets(1, asset_type="video")

        result = await adapter.launch_campaign(
            paidedge_campaign=campaign,
            audience_segment=None,
            assets=assets,
        )

        assert result.status == "launched"
        mock_client.upload_video.assert_awaited_once()
        mock_client.create_sponsored_post.assert_awaited_once()
        mock_client.create_creative.assert_awaited_once()
        mock_client.create_image_ad.assert_not_awaited()


# --- LinkedInLaunchResult model ---


class TestLinkedInLaunchResult:
    def test_default_platform(self):
        result = LinkedInLaunchResult(
            campaign_group_id=1,
            campaign_id=2,
            creative_ids=[3],
            status="launched",
        )
        assert result.platform == "linkedin"

    def test_optional_fields(self):
        result = LinkedInLaunchResult(
            campaign_group_id=1,
            campaign_id=2,
            creative_ids=[3, 4],
            audience_segment_id=5,
            tracked_link_url="https://dub.co/abc",
            status="pending_review",
            errors=["warning"],
        )
        assert result.audience_segment_id == 5
        assert result.tracked_link_url == "https://dub.co/abc"
        assert result.errors == ["warning"]
