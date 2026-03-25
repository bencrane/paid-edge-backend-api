"""Tests for landing page hosting + form handling (BJC-57).

Covers: serving landing pages, form submissions, RudderStack calls,
UTM parameter capture, and edge cases.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.landing_pages.router import (
    _fire_rudderstack_identify,
    _fire_rudderstack_track,
    _get_asset_by_slug,
)
from app.shared.errors import NotFoundError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_supabase_with_asset(asset_data: dict | None) -> MagicMock:
    mock = MagicMock()
    chain = MagicMock()
    chain.select.return_value = chain
    chain.eq.return_value = chain
    chain.maybe_single.return_value = chain
    chain.insert.return_value = chain
    res = MagicMock()
    res.data = asset_data
    chain.execute.return_value = res
    mock.table.return_value = chain
    return mock


# ---------------------------------------------------------------------------
# _get_asset_by_slug
# ---------------------------------------------------------------------------


class TestGetAssetBySlug:
    @patch("app.landing_pages.router.get_supabase_client")
    def test_returns_asset(self, mock_get_sb):
        asset = {
            "id": "a1",
            "content_url": "https://storage.example.com/lp.html",
            "input_data": {"headline": "Test"},
            "template_used": "lead_magnet_download",
            "slug": "abc123",
            "organization_id": "org-1",
            "campaign_id": "camp-1",
        }
        mock_get_sb.return_value = _mock_supabase_with_asset(asset)
        result = _get_asset_by_slug("abc123")
        assert result["id"] == "a1"
        assert result["slug"] == "abc123"

    @patch("app.landing_pages.router.get_supabase_client")
    def test_not_found_raises(self, mock_get_sb):
        mock_get_sb.return_value = _mock_supabase_with_asset(None)
        with pytest.raises(NotFoundError, match="Landing page not found"):
            _get_asset_by_slug("nonexistent")


# ---------------------------------------------------------------------------
# RudderStack identify
# ---------------------------------------------------------------------------


class TestRudderStackIdentify:
    @pytest.mark.asyncio
    @patch("app.landing_pages.router.RUDDERSTACK_DATA_PLANE_URL", "https://rs.example.com")
    @patch("app.landing_pages.router.RUDDERSTACK_WRITE_KEY", "test-key")
    @patch("httpx.AsyncClient.post", new_callable=AsyncMock)
    async def test_identify_sends_correct_payload(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        await _fire_rudderstack_identify(
            anonymous_id="anon-123",
            user_id="user@example.com",
            traits={"email": "user@example.com", "first_name": "Test"},
        )
        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args
        payload = call_kwargs.kwargs["json"] if "json" in call_kwargs.kwargs else call_kwargs[1].get("json")
        assert payload["anonymousId"] == "anon-123"
        assert payload["userId"] == "user@example.com"
        assert payload["traits"]["email"] == "user@example.com"

    @pytest.mark.asyncio
    @patch("app.landing_pages.router.RUDDERSTACK_DATA_PLANE_URL", "")
    @patch("app.landing_pages.router.RUDDERSTACK_WRITE_KEY", "")
    async def test_identify_skips_when_not_configured(self):
        """No error when RudderStack is not configured."""
        await _fire_rudderstack_identify(
            anonymous_id="anon",
            user_id="user@example.com",
            traits={},
        )
        # Should complete without error

    @pytest.mark.asyncio
    @patch("app.landing_pages.router.RUDDERSTACK_DATA_PLANE_URL", "https://rs.example.com")
    @patch("app.landing_pages.router.RUDDERSTACK_WRITE_KEY", "test-key")
    @patch("httpx.AsyncClient.post", new_callable=AsyncMock)
    async def test_identify_handles_api_error_gracefully(self, mock_post):
        mock_post.side_effect = Exception("Network error")
        # Should not raise — error is logged and swallowed
        await _fire_rudderstack_identify(
            anonymous_id="anon",
            user_id="user@example.com",
            traits={},
        )


# ---------------------------------------------------------------------------
# RudderStack track
# ---------------------------------------------------------------------------


class TestRudderStackTrack:
    @pytest.mark.asyncio
    @patch("app.landing_pages.router.RUDDERSTACK_DATA_PLANE_URL", "https://rs.example.com")
    @patch("app.landing_pages.router.RUDDERSTACK_WRITE_KEY", "test-key")
    @patch("httpx.AsyncClient.post", new_callable=AsyncMock)
    async def test_track_sends_correct_event(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        await _fire_rudderstack_track(
            anonymous_id="anon-123",
            user_id="user@example.com",
            event="form_submitted",
            properties={"slug": "abc123", "template": "lead_magnet_download"},
        )
        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args
        payload = call_kwargs.kwargs["json"] if "json" in call_kwargs.kwargs else call_kwargs[1].get("json")
        assert payload["event"] == "form_submitted"
        assert payload["properties"]["slug"] == "abc123"

    @pytest.mark.asyncio
    @patch("app.landing_pages.router.RUDDERSTACK_DATA_PLANE_URL", "")
    @patch("app.landing_pages.router.RUDDERSTACK_WRITE_KEY", "")
    async def test_track_skips_when_not_configured(self):
        await _fire_rudderstack_track(
            anonymous_id="anon",
            user_id="",
            event="form_submitted",
            properties={},
        )


# ---------------------------------------------------------------------------
# Form submission integration
# ---------------------------------------------------------------------------


class TestFormSubmission:
    """Test the full form submission flow (unit-level, mocked supabase + rudderstack)."""

    @pytest.mark.asyncio
    @patch("app.landing_pages.router.get_supabase_client")
    @patch("app.landing_pages.router._fire_rudderstack_identify", new_callable=AsyncMock)
    @patch("app.landing_pages.router._fire_rudderstack_track", new_callable=AsyncMock)
    async def test_submission_stores_and_fires_events(
        self, mock_track, mock_identify, mock_get_sb
    ):
        from starlette.testclient import TestClient
        from app.main import app

        # Mock supabase
        sb_mock = _mock_supabase_with_asset({
            "id": "a1",
            "content_url": "https://storage.example.com/lp.html",
            "input_data": {},
            "template_used": "lead_magnet_download",
            "slug": "abc123",
            "organization_id": "org-1",
            "campaign_id": "camp-1",
        })
        mock_get_sb.return_value = sb_mock

        client = TestClient(app)
        resp = client.post(
            "/lp/abc123/submit",
            json={
                "email": "test@example.com",
                "first_name": "Test",
                "utm_source": "linkedin",
                "utm_medium": "paid",
                "utm_campaign": "q1-2026",
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"

        # Verify RudderStack identify was called
        mock_identify.assert_called_once()
        call_args = mock_identify.call_args
        assert call_args.kwargs["user_id"] == "test@example.com"

        # Verify RudderStack track was called
        mock_track.assert_called_once()
        track_args = mock_track.call_args
        assert track_args.kwargs["event"] == "form_submitted"
        assert track_args.kwargs["properties"]["slug"] == "abc123"
        assert track_args.kwargs["properties"]["utm_source"] == "linkedin"

    @pytest.mark.asyncio
    @patch("app.landing_pages.router.get_supabase_client")
    @patch("app.landing_pages.router._fire_rudderstack_identify", new_callable=AsyncMock)
    @patch("app.landing_pages.router._fire_rudderstack_track", new_callable=AsyncMock)
    async def test_submission_without_email_skips_identify(
        self, mock_track, mock_identify, mock_get_sb
    ):
        from starlette.testclient import TestClient
        from app.main import app

        sb_mock = _mock_supabase_with_asset({
            "id": "a1",
            "content_url": "https://storage.example.com/lp.html",
            "input_data": {},
            "template_used": "lead_magnet_download",
            "slug": "abc123",
            "organization_id": "org-1",
            "campaign_id": "camp-1",
        })
        mock_get_sb.return_value = sb_mock

        client = TestClient(app)
        resp = client.post(
            "/lp/abc123/submit",
            json={"company_name": "TestCo"},
        )
        assert resp.status_code == 200

        # No email — identify should NOT be called
        mock_identify.assert_not_called()
        # Track should still fire
        mock_track.assert_called_once()
