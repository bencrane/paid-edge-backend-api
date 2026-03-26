"""Tests for Analytics API endpoints (PEX-68)."""

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

from app.analytics.models import (
    CampaignPerformanceResponse,
    OverviewResponse,
    PlatformComparisonResponse,
    TimeSeriesResponse,
)
from app.analytics.router import (
    GRANULARITY_FUNCTIONS,
    _compute_trend,
    _default_date_range,
    _load_sql,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TENANT_ID = "org-test-456"


def _mock_named_results(rows: list[dict]):
    """Return a mock CH result whose named_results() yields dicts."""
    mock_result = MagicMock()
    mock_result.named_results.return_value = rows
    return mock_result


def _make_tenant():
    tenant = MagicMock()
    tenant.id = TENANT_ID
    return tenant


# ---------------------------------------------------------------------------
# _load_sql
# ---------------------------------------------------------------------------


class TestLoadSql:
    def test_loads_all_analytics_queries(self):
        for name in [
            "overview",
            "overview_trends",
            "campaign_performance",
            "platform_comparison",
            "timeseries",
        ]:
            sql = _load_sql(name)
            assert len(sql) > 0
            assert "tenant_id" in sql


# ---------------------------------------------------------------------------
# _compute_trend
# ---------------------------------------------------------------------------


class TestComputeTrend:
    def test_positive_change(self):
        trend = _compute_trend(150.0, 100.0)
        assert trend.value == 150.0
        assert trend.previous_value == 100.0
        assert trend.change_pct == 50.0

    def test_negative_change(self):
        trend = _compute_trend(80.0, 100.0)
        assert trend.change_pct == -20.0

    def test_zero_previous_returns_none(self):
        trend = _compute_trend(100.0, 0.0)
        assert trend.change_pct is None

    def test_no_change(self):
        trend = _compute_trend(100.0, 100.0)
        assert trend.change_pct == 0.0


# ---------------------------------------------------------------------------
# _default_date_range
# ---------------------------------------------------------------------------


class TestDefaultDateRange:
    def test_returns_30_day_range(self):
        start, end = _default_date_range()
        assert (end - start).days == 30

    def test_end_is_today(self):
        _, end = _default_date_range()
        assert end == date.today()


# ---------------------------------------------------------------------------
# Overview endpoint
# ---------------------------------------------------------------------------


class TestGetOverview:
    def test_overview_with_data(self):
        current_row = {
            "total_spend": Decimal("5000.00"),
            "total_conversions": 100,
            "total_leads": 200,
            "total_clicks": 1500,
            "total_impressions": 50000,
            "avg_cac": Decimal("50.00"),
            "avg_cpc": Decimal("3.33"),
            "avg_ctr": 3.0,
        }
        prev_row = {
            "total_spend": Decimal("4000.00"),
            "total_conversions": 80,
            "total_leads": 150,
            "total_clicks": 1200,
            "total_impressions": 40000,
            "avg_cac": Decimal("50.00"),
        }

        mock_ch = MagicMock()
        mock_ch.query.side_effect = [
            _mock_named_results([current_row]),
            _mock_named_results([prev_row]),
        ]

        import asyncio

        from app.analytics.router import get_overview

        resp = asyncio.get_event_loop().run_until_complete(
            get_overview(
                start_date=date(2026, 3, 1),
                end_date=date(2026, 3, 31),
                tenant=_make_tenant(),
                ch=mock_ch,
            )
        )

        assert isinstance(resp, OverviewResponse)
        assert resp.total_spend.value == 5000.0
        assert resp.total_spend.previous_value == 4000.0
        assert resp.total_spend.change_pct == 25.0
        assert resp.total_conversions.value == 100.0

    def test_overview_empty_data(self):
        mock_ch = MagicMock()
        mock_ch.query.side_effect = [
            _mock_named_results([]),
            _mock_named_results([]),
        ]

        import asyncio

        from app.analytics.router import get_overview

        resp = asyncio.get_event_loop().run_until_complete(
            get_overview(
                start_date=date(2026, 3, 1),
                end_date=date(2026, 3, 31),
                tenant=_make_tenant(),
                ch=mock_ch,
            )
        )

        assert resp.total_spend.value == 0
        assert resp.total_spend.change_pct is None

    def test_overview_calls_two_queries(self):
        mock_ch = MagicMock()
        mock_ch.query.side_effect = [
            _mock_named_results([{
                "total_spend": 0, "total_conversions": 0,
                "total_leads": 0, "total_clicks": 0,
                "total_impressions": 0, "avg_cac": 0,
                "avg_cpc": 0, "avg_ctr": 0,
            }]),
            _mock_named_results([{
                "total_spend": 0, "total_conversions": 0,
                "total_leads": 0, "total_clicks": 0,
                "total_impressions": 0, "avg_cac": 0,
            }]),
        ]

        import asyncio

        from app.analytics.router import get_overview

        asyncio.get_event_loop().run_until_complete(
            get_overview(
                start_date=date(2026, 3, 1),
                end_date=date(2026, 3, 31),
                tenant=_make_tenant(),
                ch=mock_ch,
            )
        )

        assert mock_ch.query.call_count == 2


# ---------------------------------------------------------------------------
# Campaign performance endpoint
# ---------------------------------------------------------------------------


class TestGetCampaignPerformance:
    def test_campaign_performance_with_data(self):
        rows = [
            {
                "campaign_id": "c-1",
                "platform": "linkedin",
                "total_spend": Decimal("2000.00"),
                "total_impressions": 20000,
                "total_clicks": 400,
                "total_conversions": 20,
                "total_leads": 40,
                "ctr": 2.0,
                "cpc": Decimal("5.00"),
                "cpm": Decimal("100.00"),
                "cost_per_conversion": Decimal("100.00"),
            },
        ]
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_named_results(rows)

        import asyncio

        from app.analytics.router import get_campaign_performance

        resp = asyncio.get_event_loop().run_until_complete(
            get_campaign_performance(
                start_date=date(2026, 1, 1),
                end_date=date(2026, 3, 31),
                platform=None,
                campaign_id=None,
                sort_by="total_spend",
                sort_order="desc",
                tenant=_make_tenant(),
                ch=mock_ch,
            )
        )

        assert isinstance(resp, CampaignPerformanceResponse)
        assert resp.total == 1
        assert resp.campaigns[0].campaign_id == "c-1"
        assert resp.campaigns[0].total_spend == 2000.0

    def test_campaign_performance_sort_by_ctr(self):
        rows = [
            {
                "campaign_id": "c-1", "platform": "linkedin",
                "total_spend": Decimal("2000"), "total_impressions": 20000,
                "total_clicks": 400, "total_conversions": 20,
                "total_leads": 40, "ctr": 2.0, "cpc": Decimal("5"),
                "cpm": Decimal("100"), "cost_per_conversion": Decimal("100"),
            },
            {
                "campaign_id": "c-2", "platform": "meta",
                "total_spend": Decimal("1000"), "total_impressions": 10000,
                "total_clicks": 500, "total_conversions": 10,
                "total_leads": 20, "ctr": 5.0, "cpc": Decimal("2"),
                "cpm": Decimal("100"), "cost_per_conversion": Decimal("100"),
            },
        ]
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_named_results(rows)

        import asyncio

        from app.analytics.router import get_campaign_performance

        resp = asyncio.get_event_loop().run_until_complete(
            get_campaign_performance(
                start_date=date(2026, 1, 1),
                end_date=date(2026, 3, 31),
                platform=None,
                campaign_id=None,
                sort_by="ctr",
                sort_order="desc",
                tenant=_make_tenant(),
                ch=mock_ch,
            )
        )

        # c-2 has higher CTR (5.0) so should come first
        assert resp.campaigns[0].campaign_id == "c-2"

    def test_platform_filter_appended(self):
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_named_results([])

        import asyncio

        from app.analytics.router import get_campaign_performance

        asyncio.get_event_loop().run_until_complete(
            get_campaign_performance(
                start_date=date(2026, 1, 1),
                end_date=date(2026, 3, 31),
                platform="linkedin",
                campaign_id=None,
                sort_by="total_spend",
                sort_order="desc",
                tenant=_make_tenant(),
                ch=mock_ch,
            )
        )

        call_args = mock_ch.query.call_args
        params = call_args[1]["parameters"]
        assert params["platform"] == "linkedin"


# ---------------------------------------------------------------------------
# Platform comparison endpoint
# ---------------------------------------------------------------------------


class TestGetPlatformComparison:
    def test_platform_comparison_with_data(self):
        rows = [
            {
                "platform": "linkedin",
                "campaign_count": 5,
                "total_spend": Decimal("10000.00"),
                "total_impressions": 100000,
                "total_clicks": 2000,
                "total_conversions": 50,
                "total_leads": 100,
                "ctr": 2.0,
                "cpc": Decimal("5.00"),
                "cpm": Decimal("100.00"),
                "cost_per_conversion": Decimal("200.00"),
            },
            {
                "platform": "meta",
                "campaign_count": 3,
                "total_spend": Decimal("5000.00"),
                "total_impressions": 80000,
                "total_clicks": 3000,
                "total_conversions": 30,
                "total_leads": 60,
                "ctr": 3.75,
                "cpc": Decimal("1.67"),
                "cpm": Decimal("62.50"),
                "cost_per_conversion": Decimal("166.67"),
            },
        ]
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_named_results(rows)

        import asyncio

        from app.analytics.router import get_platform_comparison

        resp = asyncio.get_event_loop().run_until_complete(
            get_platform_comparison(
                start_date=date(2026, 1, 1),
                end_date=date(2026, 3, 31),
                tenant=_make_tenant(),
                ch=mock_ch,
            )
        )

        assert isinstance(resp, PlatformComparisonResponse)
        assert len(resp.platforms) == 2
        assert resp.platforms[0].platform == "linkedin"
        assert resp.platforms[1].platform == "meta"


# ---------------------------------------------------------------------------
# Time series endpoint
# ---------------------------------------------------------------------------


class TestGetTimeseries:
    def test_timeseries_daily(self):
        rows = [
            {
                "period": date(2026, 3, 1),
                "spend": Decimal("100.00"),
                "impressions": 1000,
                "clicks": 50,
                "conversions": 5,
                "leads": 10,
                "ctr": 5.0,
                "cpc": Decimal("2.00"),
            },
            {
                "period": date(2026, 3, 2),
                "spend": Decimal("120.00"),
                "impressions": 1200,
                "clicks": 60,
                "conversions": 6,
                "leads": 12,
                "ctr": 5.0,
                "cpc": Decimal("2.00"),
            },
        ]
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_named_results(rows)

        import asyncio

        from app.analytics.router import get_timeseries

        resp = asyncio.get_event_loop().run_until_complete(
            get_timeseries(
                start_date=date(2026, 3, 1),
                end_date=date(2026, 3, 31),
                granularity="daily",
                tenant=_make_tenant(),
                ch=mock_ch,
            )
        )

        assert isinstance(resp, TimeSeriesResponse)
        assert resp.granularity == "daily"
        assert len(resp.data) == 2
        assert resp.data[0].period == date(2026, 3, 1)

    def test_timeseries_granularity_mapping(self):
        """Verify that the SQL uses the correct bucket function."""
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_named_results([])

        import asyncio

        from app.analytics.router import get_timeseries

        for granularity, expected_fn in GRANULARITY_FUNCTIONS.items():
            mock_ch.reset_mock()
            asyncio.get_event_loop().run_until_complete(
                get_timeseries(
                    start_date=date(2026, 3, 1),
                    end_date=date(2026, 3, 31),
                    granularity=granularity,
                    tenant=_make_tenant(),
                    ch=mock_ch,
                )
            )
            sql_used = mock_ch.query.call_args[0][0]
            assert expected_fn in sql_used, (
                f"Expected {expected_fn} in SQL for granularity={granularity}"
            )

    def test_timeseries_passes_correct_params(self):
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_named_results([])

        import asyncio

        from app.analytics.router import get_timeseries

        asyncio.get_event_loop().run_until_complete(
            get_timeseries(
                start_date=date(2026, 3, 1),
                end_date=date(2026, 3, 31),
                granularity="weekly",
                tenant=_make_tenant(),
                ch=mock_ch,
            )
        )

        params = mock_ch.query.call_args[1]["parameters"]
        assert params["tid"] == TENANT_ID
        assert params["start"] == date(2026, 3, 1)
        assert params["end"] == date(2026, 3, 31)
