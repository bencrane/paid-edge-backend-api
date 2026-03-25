"""Tests for BJC-167: Brand context ingestion."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from app.assets.context import (
    AssetContext,
    build_asset_context,
    format_brand_context_block,
    format_persona_block,
    format_social_proof_block,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_full_context() -> AssetContext:
    return AssetContext(
        organization_id="org-1",
        campaign_id="camp-1",
        company_name="Acme Security",
        brand_voice="Authoritative but approachable",
        brand_guidelines={"tone": "professional", "dos": ["Be specific"], "donts": ["No jargon"]},
        value_proposition="AI-powered compliance automation",
        icp_definition={
            "job_titles": ["CISO", "VP Engineering"],
            "company_size": "100-500",
            "industry": "Healthcare SaaS",
            "pain_points": ["Audit fatigue", "Manual compliance"],
            "goals": ["SOC 2 in 90 days"],
        },
        target_persona="Job titles: CISO, VP Engineering\nCompany size: 100-500\nIndustry: Healthcare SaaS",
        case_studies=[
            {
                "customer_name": "MedTech Corp",
                "customer_industry": "Healthcare",
                "problem": "Failed SOC 2 audit twice",
                "solution": "Automated compliance with Acme",
                "results": {"pipeline_generated": "$2.1M", "roi": "3.2x"},
                "quote": {
                    "text": "Acme changed everything",
                    "author": "Jane Smith",
                    "title": "VP Engineering",
                },
            }
        ],
        testimonials=[
            {
                "quote": "Best tool we've used",
                "author": "John Doe",
                "title": "CTO",
                "company": "StartupCo",
            }
        ],
        customer_logos=["https://example.com/logo1.png"],
        competitor_differentiators=["Only platform with automated evidence collection"],
        angle="SOC 2 compliance for healthtech",
        objective="lead_generation",
        platforms=["linkedin", "meta"],
        industry="Healthcare SaaS",
    )


def _make_minimal_context() -> AssetContext:
    return AssetContext(organization_id="org-2", company_name="Unknown Co")


# ---------------------------------------------------------------------------
# Format functions tests
# ---------------------------------------------------------------------------


class TestFormatBrandContextBlock:
    def test_full_context(self):
        ctx = _make_full_context()
        block = format_brand_context_block(ctx)
        assert "Acme Security" in block
        assert "AI-powered compliance automation" in block
        assert "Authoritative but approachable" in block
        assert "DIFFERENTIATORS" in block

    def test_minimal_context(self):
        ctx = _make_minimal_context()
        block = format_brand_context_block(ctx)
        assert "Unknown Co" in block

    def test_empty_context(self):
        ctx = AssetContext(organization_id="org-3")
        block = format_brand_context_block(ctx)
        assert block == ""


class TestFormatPersonaBlock:
    def test_full_persona(self):
        ctx = _make_full_context()
        block = format_persona_block(ctx)
        assert "CISO" in block
        assert "Healthcare SaaS" in block

    def test_empty_persona(self):
        ctx = _make_minimal_context()
        block = format_persona_block(ctx)
        assert block == ""


class TestFormatSocialProofBlock:
    def test_full_proof(self):
        ctx = _make_full_context()
        block = format_social_proof_block(ctx)
        assert "MedTech Corp" in block
        assert "Acme changed everything" in block
        assert "Best tool we've used" in block
        assert "1 logos available" in block

    def test_empty_proof(self):
        ctx = _make_minimal_context()
        block = format_social_proof_block(ctx)
        assert block == ""


# ---------------------------------------------------------------------------
# build_asset_context tests (mocked Supabase)
# ---------------------------------------------------------------------------


def _mock_supabase_with_data(tenant_rows: list[dict], campaign: dict | None = None):
    """Create a mock Supabase client returning specified data."""
    sb = MagicMock()

    # tenant_context query chain
    tc_query = MagicMock()
    tc_query.select.return_value = tc_query
    tc_query.eq.return_value = tc_query
    tc_exec = MagicMock()
    tc_exec.data = tenant_rows
    tc_query.execute.return_value = tc_exec

    # campaigns query chain
    camp_query = MagicMock()
    camp_query.select.return_value = camp_query
    camp_query.eq.return_value = camp_query
    camp_query.maybe_single.return_value = camp_query
    camp_exec = MagicMock()
    camp_exec.data = campaign
    camp_query.execute.return_value = camp_exec

    # organizations query chain (fallback for company name)
    org_query = MagicMock()
    org_query.select.return_value = org_query
    org_query.eq.return_value = org_query
    org_query.maybe_single.return_value = org_query
    org_exec = MagicMock()
    org_exec.data = {"name": "Org Name"}
    org_query.execute.return_value = org_exec

    def table_router(table_name: str):
        if table_name == "tenant_context":
            return tc_query
        if table_name == "campaigns":
            return camp_query
        if table_name == "organizations":
            return org_query
        return MagicMock()

    sb.table.side_effect = table_router
    return sb


class TestBuildAssetContext:
    @pytest.mark.asyncio
    async def test_loads_brand_guidelines(self):
        rows = [
            {
                "context_type": "brand_guidelines",
                "context_data": {
                    "company_name": "TestCo",
                    "voice": "Friendly",
                },
            }
        ]
        sb = _mock_supabase_with_data(rows)
        ctx = await build_asset_context("org-1", None, sb)
        assert ctx.company_name == "TestCo"
        assert ctx.brand_voice == "Friendly"

    @pytest.mark.asyncio
    async def test_loads_icp(self):
        rows = [
            {
                "context_type": "icp_definition",
                "context_data": {
                    "job_titles": ["CTO", "VP Eng"],
                    "industry": "SaaS",
                    "pain_points": ["Slow deploys"],
                },
            }
        ]
        sb = _mock_supabase_with_data(rows)
        ctx = await build_asset_context("org-1", None, sb)
        assert ctx.icp_definition is not None
        assert "CTO" in ctx.target_persona

    @pytest.mark.asyncio
    async def test_loads_case_studies(self):
        rows = [
            {
                "context_type": "case_study",
                "context_data": {
                    "customer_name": "BigCorp",
                    "results": {"roi": "5x"},
                },
            }
        ]
        sb = _mock_supabase_with_data(rows)
        ctx = await build_asset_context("org-1", None, sb)
        assert len(ctx.case_studies) == 1
        assert ctx.case_studies[0]["customer_name"] == "BigCorp"

    @pytest.mark.asyncio
    async def test_loads_campaign_data(self):
        rows = [
            {
                "context_type": "brand_guidelines",
                "context_data": {"company_name": "CampCo", "voice": "Bold"},
            }
        ]
        campaign = {
            "angle": "DevOps automation",
            "objective": "demos",
            "platforms": ["linkedin"],
        }
        sb = _mock_supabase_with_data(rows, campaign=campaign)
        ctx = await build_asset_context("org-1", "camp-1", sb)
        assert ctx.angle == "DevOps automation"
        assert ctx.objective == "demos"
        assert ctx.platforms == ["linkedin"]

    @pytest.mark.asyncio
    async def test_missing_context_doesnt_fail(self):
        sb = _mock_supabase_with_data([])
        ctx = await build_asset_context("org-1", None, sb)
        assert ctx.organization_id == "org-1"
        assert ctx.company_name == "Org Name"  # Falls back to org table

    @pytest.mark.asyncio
    async def test_missing_context_logs_warnings(self, caplog):
        sb = _mock_supabase_with_data([])
        import logging
        with caplog.at_level(logging.WARNING):
            await build_asset_context("org-1", None, sb)
        assert "Missing brand_guidelines" in caplog.text
        assert "Missing icp_definition" in caplog.text
