"""Tests for Asset API endpoints (BJC-55).

Covers: generate, get detail, update, approve, list campaign assets,
revise, and edge cases (not found, invalid status, filters).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import BaseModel

from app.assets.service import AssetGenerationService
from app.shared.errors import BadRequestError, NotFoundError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_supabase(data: Any = None) -> MagicMock:
    """Supabase mock with chained query builder pattern."""
    mock = MagicMock()

    def _make_chain(d=None):
        chain = MagicMock()
        chain.select.return_value = chain
        chain.insert.return_value = chain
        chain.update.return_value = chain
        chain.eq.return_value = chain
        chain.maybe_single.return_value = chain
        chain.order.return_value = chain
        chain.range.return_value = chain
        res = MagicMock()
        res.data = d
        res.count = len(d) if d else 0
        chain.execute.return_value = res
        return chain

    mock.table.return_value = _make_chain(data or [])
    return mock


def _mock_supabase_with_asset(asset_data: dict | None) -> MagicMock:
    """Supabase mock returning specific asset data on select."""
    mock = MagicMock()

    def _table(name: str):
        chain = MagicMock()
        chain.select.return_value = chain
        chain.insert.return_value = chain
        chain.update.return_value = chain
        chain.eq.return_value = chain
        chain.maybe_single.return_value = chain
        chain.order.return_value = chain

        if name == "generated_assets":
            res = MagicMock()
            res.data = asset_data
            chain.execute.return_value = res
        else:
            res = MagicMock()
            res.data = []
            chain.execute.return_value = res
        return chain

    mock.table.side_effect = _table
    return mock


def _mock_claude() -> MagicMock:
    mock = MagicMock()
    mock.generate_structured = AsyncMock(return_value=MagicMock())
    return mock


def _service(supabase=None, claude=None) -> AssetGenerationService:
    return AssetGenerationService(
        claude=claude or _mock_claude(),
        supabase=supabase or _mock_supabase(),
    )


# ---------------------------------------------------------------------------
# GET /assets/:id — get_asset
# ---------------------------------------------------------------------------


class TestGetAsset:
    def test_returns_asset_detail(self):
        asset = {
            "id": "a1",
            "asset_type": "ad_copy",
            "status": "draft",
            "organization_id": "org-1",
            "content_url": None,
            "content_json": {"headline": "Test"},
            "template_used": None,
            "campaign_id": "camp-1",
        }
        svc = _service(supabase=_mock_supabase_with_asset(asset))
        result = svc.get_asset("a1", "org-1")
        assert result["id"] == "a1"
        assert result["asset_type"] == "ad_copy"
        assert result["status"] == "draft"

    def test_not_found_raises_404(self):
        svc = _service(supabase=_mock_supabase_with_asset(None))
        with pytest.raises(NotFoundError, match="Asset not found"):
            svc.get_asset("missing", "org-1")


# ---------------------------------------------------------------------------
# PATCH /assets/:id — update_asset (direct content edit)
# ---------------------------------------------------------------------------


class TestUpdateAsset:
    def test_update_content_json(self):
        asset = {
            "id": "a1",
            "asset_type": "ad_copy",
            "status": "draft",
            "organization_id": "org-1",
            "content_json": {"old": "data"},
        }
        svc = _service(supabase=_mock_supabase_with_asset(asset))
        result = svc.update_asset(
            "a1", "org-1", content_json={"new": "data"}
        )
        assert result["content_json"] == {"new": "data"}

    def test_update_status(self):
        asset = {
            "id": "a1",
            "asset_type": "ad_copy",
            "status": "draft",
            "organization_id": "org-1",
        }
        svc = _service(supabase=_mock_supabase_with_asset(asset))
        result = svc.update_asset("a1", "org-1", status="approved")
        assert result["status"] == "approved"

    def test_invalid_status_raises(self):
        asset = {
            "id": "a1",
            "asset_type": "ad_copy",
            "status": "draft",
            "organization_id": "org-1",
        }
        svc = _service(supabase=_mock_supabase_with_asset(asset))
        with pytest.raises(BadRequestError, match="Invalid status"):
            svc.update_asset("a1", "org-1", status="bogus")

    def test_no_op_update_returns_existing(self):
        asset = {
            "id": "a1",
            "asset_type": "ad_copy",
            "status": "draft",
            "organization_id": "org-1",
        }
        svc = _service(supabase=_mock_supabase_with_asset(asset))
        result = svc.update_asset("a1", "org-1")
        assert result["id"] == "a1"
        assert result["status"] == "draft"

    def test_update_not_found_raises(self):
        svc = _service(supabase=_mock_supabase_with_asset(None))
        with pytest.raises(NotFoundError):
            svc.update_asset("missing", "org-1", status="draft")


# ---------------------------------------------------------------------------
# POST /assets/:id/approve
# ---------------------------------------------------------------------------


class TestApproveAsset:
    def test_approve_draft_succeeds(self):
        asset = {
            "id": "a1",
            "asset_type": "ad_copy",
            "status": "draft",
            "organization_id": "org-1",
        }
        svc = _service(supabase=_mock_supabase_with_asset(asset))
        result = svc.approve_asset("a1", "org-1")
        assert result["status"] == "approved"

    def test_approve_non_draft_raises(self):
        asset = {
            "id": "a1",
            "asset_type": "ad_copy",
            "status": "approved",
            "organization_id": "org-1",
        }
        svc = _service(supabase=_mock_supabase_with_asset(asset))
        with pytest.raises(BadRequestError, match="Can only approve draft"):
            svc.approve_asset("a1", "org-1")


# ---------------------------------------------------------------------------
# GET /assets/campaigns/:campaign_id — list_campaign_assets
# ---------------------------------------------------------------------------


class TestListCampaignAssets:
    def test_returns_list(self):
        assets = [
            {"id": "a1", "asset_type": "ad_copy", "status": "draft"},
            {"id": "a2", "asset_type": "email_copy", "status": "approved"},
        ]
        svc = _service(supabase=_mock_supabase(assets))
        result = svc.list_campaign_assets("camp-1", "org-1")
        assert len(result) == 2

    def test_empty_campaign_returns_empty_list(self):
        svc = _service(supabase=_mock_supabase([]))
        result = svc.list_campaign_assets("camp-empty", "org-1")
        assert result == []

    def test_filter_params_accepted(self):
        """Filters are passed through — verify no error is raised."""
        svc = _service(supabase=_mock_supabase([]))
        result = svc.list_campaign_assets(
            "camp-1", "org-1", status="draft", asset_type="ad_copy"
        )
        assert result == []


# ---------------------------------------------------------------------------
# POST /assets/generate
# ---------------------------------------------------------------------------


class TestGenerateAssets:
    @pytest.mark.asyncio
    @patch("app.assets.service.build_asset_context", new_callable=AsyncMock)
    @patch("app.assets.service.generate_ad_copy", new_callable=AsyncMock)
    async def test_generate_returns_results(self, mock_gen, mock_ctx):
        from app.assets.context import AssetContext

        mock_ctx.return_value = AssetContext(
            organization_id="org-1",
            company_name="TestCo",
            brand_voice="Pro",
            value_proposition="We X",
            target_persona="CISOs",
            angle="Security",
            objective="lead_gen",
            platforms=["linkedin"],
            industry="SaaS",
        )

        class FakeOut(BaseModel):
            headline: str = "Test"

        mock_gen.return_value = {"linkedin": FakeOut()}

        svc = _service(supabase=_mock_supabase())
        results = await svc.generate(
            org_id="org-1",
            campaign_id="camp-1",
            asset_types=["ad_copy"],
            platforms=["linkedin"],
        )
        assert len(results) == 1
        assert results[0]["asset_type"] == "ad_copy"
        assert results[0]["status"] == "draft"

    @pytest.mark.asyncio
    async def test_invalid_type_raises(self):
        svc = _service()
        with pytest.raises(BadRequestError, match="Unknown asset type"):
            await svc.generate(
                org_id="org-1",
                campaign_id="camp-1",
                asset_types=["nonexistent"],
            )


# ---------------------------------------------------------------------------
# POST /assets/:id/revise
# ---------------------------------------------------------------------------


class TestReviseAsset:
    @pytest.mark.asyncio
    @patch("app.assets.service.build_asset_context", new_callable=AsyncMock)
    @patch("app.assets.service.generate_email_sequence", new_callable=AsyncMock)
    async def test_revise_flow(self, mock_gen, mock_ctx):
        from app.assets.context import AssetContext
        from app.assets.prompts.schemas import EmailSequenceOutput, NurtureEmail

        mock_ctx.return_value = AssetContext(
            organization_id="org-1",
            company_name="TestCo",
            brand_voice="Pro",
            value_proposition="We X",
            target_persona="CISOs",
            angle="Security",
            objective="lead_gen",
            platforms=["linkedin"],
            industry="SaaS",
        )
        mock_gen.return_value = EmailSequenceOutput(
            sequence_name="Rev",
            trigger="lead_magnet_download",
            emails=[
                NurtureEmail(
                    subject_line="S",
                    preview_text="P",
                    body_html="<p>B</p>",
                    send_delay_days=0,
                    purpose="value_delivery",
                ),
                NurtureEmail(
                    subject_line="S2",
                    preview_text="P2",
                    body_html="<p>B2</p>",
                    send_delay_days=2,
                    purpose="education",
                ),
                NurtureEmail(
                    subject_line="S3",
                    preview_text="P3",
                    body_html="<p>B3</p>",
                    send_delay_days=5,
                    purpose="social_proof",
                ),
            ],
        )
        asset = {
            "id": "a1",
            "asset_type": "email_copy",
            "status": "draft",
            "organization_id": "org-1",
            "campaign_id": "camp-1",
        }
        svc = _service(supabase=_mock_supabase_with_asset(asset))
        result = await svc.revise_asset("a1", "Make it shorter", "org-1")
        assert result["id"] == "a1"
        assert result["status"] == "draft"


# ---------------------------------------------------------------------------
# Request model validation
# ---------------------------------------------------------------------------


class TestRequestModels:
    def test_generate_request(self):
        from app.assets.generation_router import GenerateRequest

        req = GenerateRequest(
            campaign_id="c1",
            asset_types=["ad_copy"],
            platforms=["linkedin"],
        )
        assert req.campaign_id == "c1"

    def test_update_request(self):
        from app.assets.generation_router import AssetUpdateRequest

        req = AssetUpdateRequest(content_json={"h": "new"}, status="draft")
        assert req.content_json == {"h": "new"}
        assert req.status == "draft"

    def test_update_request_all_optional(self):
        from app.assets.generation_router import AssetUpdateRequest

        req = AssetUpdateRequest()
        assert req.content_json is None
        assert req.content_url is None
        assert req.status is None
