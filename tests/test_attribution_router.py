"""Tests for Attribution API endpoints (PEX-67)."""

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

from app.attribution.models import (
    CostPerClosedWonResponse,
    CostPerOpportunityResponse,
    FunnelResponse,
    LookalikeProfileResponse,
    PipelineInfluencedResponse,
)
from app.attribution.router import _default_date_range, _load_sql

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TENANT_ID = "org-test-123"


def _mock_named_results(rows: list[dict]):
    """Return a mock CH result whose named_results() yields dicts."""
    mock_result = MagicMock()
    mock_result.named_results.return_value = rows
    return mock_result


# ---------------------------------------------------------------------------
# _load_sql
# ---------------------------------------------------------------------------


class TestLoadSql:
    def test_loads_funnel_stages(self):
        sql = _load_sql("funnel_stages")
        assert "campaign_metrics" in sql or "campaign_id" in sql

    def test_loads_all_query_files(self):
        for name in [
            "funnel_stages",
            "cost_per_opportunity",
            "cost_per_closed_won",
            "pipeline_influenced",
            "lookalike_profile",
        ]:
            sql = _load_sql(name)
            assert len(sql) > 0


# ---------------------------------------------------------------------------
# _default_date_range
# ---------------------------------------------------------------------------


class TestDefaultDateRange:
    def test_returns_30_day_range(self):
        start, end = _default_date_range()
        delta = (end - start).days
        assert delta == 30

    def test_end_is_today(self):
        _, end = _default_date_range()
        assert end == date.today()


# ---------------------------------------------------------------------------
# Funnel endpoint
# ---------------------------------------------------------------------------


class TestGetFunnel:
    def test_funnel_empty_result(self):
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_named_results([])

        # Call the endpoint function directly with mocks
        import asyncio

        from app.attribution.router import get_funnel

        tenant = MagicMock()
        tenant.id = TENANT_ID

        resp = asyncio.get_event_loop().run_until_complete(
            get_funnel(
                start_date=date(2026, 1, 1),
                end_date=date(2026, 3, 31),
                tenant=tenant,
                ch=mock_ch,
            )
        )

        assert isinstance(resp, FunnelResponse)
        assert resp.campaigns == []
        assert resp.totals.total_spend == 0
        assert resp.totals.total_leads == 0

    def test_funnel_with_data(self):
        rows = [
            {
                "campaign_id": "c-1",
                "platform": "linkedin",
                "total_spend": Decimal("1000.00"),
                "lead_count": 50,
                "opportunity_count": 10,
                "closed_won_count": 3,
                "lead_to_opportunity_rate": 0.2,
                "opportunity_to_won_rate": 0.3,
            },
            {
                "campaign_id": "c-2",
                "platform": "meta",
                "total_spend": Decimal("500.00"),
                "lead_count": 30,
                "opportunity_count": 5,
                "closed_won_count": 1,
                "lead_to_opportunity_rate": 0.167,
                "opportunity_to_won_rate": 0.2,
            },
        ]
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_named_results(rows)

        import asyncio

        from app.attribution.router import get_funnel

        tenant = MagicMock()
        tenant.id = TENANT_ID

        resp = asyncio.get_event_loop().run_until_complete(
            get_funnel(
                start_date=date(2026, 1, 1),
                end_date=date(2026, 3, 31),
                tenant=tenant,
                ch=mock_ch,
            )
        )

        assert len(resp.campaigns) == 2
        assert resp.totals.total_spend == 1500.0
        assert resp.totals.total_leads == 80
        assert resp.totals.total_opportunities == 15
        assert resp.totals.total_closed_won == 4

    def test_funnel_passes_correct_params(self):
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_named_results([])

        import asyncio

        from app.attribution.router import get_funnel

        tenant = MagicMock()
        tenant.id = TENANT_ID

        asyncio.get_event_loop().run_until_complete(
            get_funnel(
                start_date=date(2026, 2, 1),
                end_date=date(2026, 2, 28),
                tenant=tenant,
                ch=mock_ch,
            )
        )

        call_args = mock_ch.query.call_args
        params = call_args[1]["parameters"]
        assert params["tid"] == TENANT_ID
        assert params["start"] == date(2026, 2, 1)
        assert params["end"] == date(2026, 2, 28)


# ---------------------------------------------------------------------------
# Cost per opportunity endpoint
# ---------------------------------------------------------------------------


class TestGetCostPerOpportunity:
    def test_cost_per_opportunity_with_data(self):
        rows = [
            {
                "campaign_id": "c-1",
                "platform": "linkedin",
                "total_spend": Decimal("2000.00"),
                "opportunity_count": 10,
                "cost_per_opportunity": Decimal("200.00"),
            },
        ]
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_named_results(rows)

        import asyncio

        from app.attribution.router import get_cost_per_opportunity

        tenant = MagicMock()
        tenant.id = TENANT_ID

        resp = asyncio.get_event_loop().run_until_complete(
            get_cost_per_opportunity(
                start_date=date(2026, 1, 1),
                end_date=date(2026, 3, 31),
                tenant=tenant,
                ch=mock_ch,
            )
        )

        assert isinstance(resp, CostPerOpportunityResponse)
        assert len(resp.campaigns) == 1
        assert resp.campaigns[0].cost_per_opportunity == 200.0


# ---------------------------------------------------------------------------
# Cost per closed-won endpoint
# ---------------------------------------------------------------------------


class TestGetCostPerClosedWon:
    def test_cost_per_closed_won_with_data(self):
        rows = [
            {
                "campaign_id": "c-1",
                "platform": "linkedin",
                "total_spend": Decimal("5000.00"),
                "closed_won_count": 2,
                "cost_per_closed_won": Decimal("2500.00"),
            },
        ]
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_named_results(rows)

        import asyncio

        from app.attribution.router import get_cost_per_closed_won

        tenant = MagicMock()
        tenant.id = TENANT_ID

        resp = asyncio.get_event_loop().run_until_complete(
            get_cost_per_closed_won(
                start_date=date(2026, 1, 1),
                end_date=date(2026, 3, 31),
                tenant=tenant,
                ch=mock_ch,
            )
        )

        assert isinstance(resp, CostPerClosedWonResponse)
        assert resp.campaigns[0].cost_per_closed_won == 2500.0


# ---------------------------------------------------------------------------
# Pipeline influenced endpoint
# ---------------------------------------------------------------------------


class TestGetPipelineInfluenced:
    def test_pipeline_influenced_totals(self):
        rows = [
            {
                "campaign_id": "c-1",
                "opportunity_count": 5,
                "pipeline_value": Decimal("100000.00"),
                "closed_won_value": Decimal("40000.00"),
                "closed_won_count": 2,
            },
            {
                "campaign_id": "c-2",
                "opportunity_count": 3,
                "pipeline_value": Decimal("60000.00"),
                "closed_won_value": Decimal("20000.00"),
                "closed_won_count": 1,
            },
        ]
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_named_results(rows)

        import asyncio

        from app.attribution.router import get_pipeline_influenced

        tenant = MagicMock()
        tenant.id = TENANT_ID

        resp = asyncio.get_event_loop().run_until_complete(
            get_pipeline_influenced(
                start_date=date(2026, 1, 1),
                end_date=date(2026, 3, 31),
                tenant=tenant,
                ch=mock_ch,
            )
        )

        assert isinstance(resp, PipelineInfluencedResponse)
        assert resp.total_pipeline_value == 160000.0
        assert resp.total_closed_won_value == 60000.0
        assert len(resp.campaigns) == 2


# ---------------------------------------------------------------------------
# Lookalike profile endpoint
# ---------------------------------------------------------------------------


class TestGetLookalikeProfile:
    def test_lookalike_profile_with_data(self):
        rows = [
            {
                "company_domain": "acme.com",
                "company_name": "Acme Corp",
                "deal_count": 3,
                "total_revenue": Decimal("150000.00"),
                "avg_deal_size": Decimal("50000.00"),
            },
            {
                "company_domain": "globex.com",
                "company_name": "Globex Inc",
                "deal_count": 1,
                "total_revenue": Decimal("75000.00"),
                "avg_deal_size": Decimal("75000.00"),
            },
        ]
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_named_results(rows)

        import asyncio

        from app.attribution.router import get_lookalike_profile

        tenant = MagicMock()
        tenant.id = TENANT_ID

        resp = asyncio.get_event_loop().run_until_complete(
            get_lookalike_profile(
                start_date=date(2026, 1, 1),
                end_date=date(2026, 3, 31),
                tenant=tenant,
                ch=mock_ch,
            )
        )

        assert isinstance(resp, LookalikeProfileResponse)
        assert resp.total_companies == 2
        assert resp.total_revenue == 225000.0
        assert resp.companies[0].company_domain == "acme.com"

    def test_lookalike_empty(self):
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_named_results([])

        import asyncio

        from app.attribution.router import get_lookalike_profile

        tenant = MagicMock()
        tenant.id = TENANT_ID

        resp = asyncio.get_event_loop().run_until_complete(
            get_lookalike_profile(
                start_date=date(2026, 1, 1),
                end_date=date(2026, 3, 31),
                tenant=tenant,
                ch=mock_ch,
            )
        )

        assert resp.total_companies == 0
        assert resp.total_revenue == 0
        assert resp.companies == []
