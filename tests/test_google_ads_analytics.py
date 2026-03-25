"""Tests for Google Ads GAQL analytics client + metrics mapping (BJC-150)."""

from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.integrations.google_ads import GoogleAdsService
from app.integrations.google_ads_analytics import (
    CLICKHOUSE_MAPPING,
    MICROS_FIELDS,
    GoogleAdsAnalyticsClient,
    map_metrics_to_clickhouse,
    write_metrics_to_clickhouse,
)


@pytest.fixture
def mock_service():
    service = MagicMock(spec=GoogleAdsService)
    service.customer_id = "1234567890"
    service.search_stream = AsyncMock(return_value=[])
    return service


@pytest.fixture
def analytics(mock_service):
    return GoogleAdsAnalyticsClient(mock_service)


# --- Query building ---


class TestBuildQuery:
    def test_basic_campaign_query(self, analytics):
        query = analytics._build_query(
            resource="campaign",
            fields=["campaign.id", "campaign.name", "metrics.clicks"],
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 7),
        )
        assert "SELECT campaign.id, campaign.name, metrics.clicks" in query
        assert "FROM campaign" in query
        assert "segments.date >= '2026-01-01'" in query
        assert "segments.date <= '2026-01-07'" in query

    def test_query_with_conditions(self, analytics):
        query = analytics._build_query(
            resource="campaign",
            fields=["campaign.id"],
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 7),
            conditions=["campaign.status != 'REMOVED'"],
        )
        assert "campaign.status != 'REMOVED'" in query

    def test_query_with_campaign_filter(self, analytics):
        conditions = analytics._campaign_filter(["111", "222"])
        assert len(conditions) == 1
        assert "campaign.id IN (111, 222)" in conditions[0]

    def test_campaign_filter_none(self, analytics):
        assert analytics._campaign_filter(None) == []

    def test_campaign_filter_empty(self, analytics):
        assert analytics._campaign_filter([]) == []


# --- Fetching metrics ---


class TestFetchCampaignMetrics:
    @pytest.mark.asyncio
    async def test_fetch_campaign_metrics_empty(self, analytics, mock_service):
        mock_service.search_stream.return_value = []
        result = await analytics.fetch_campaign_metrics(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 7),
        )
        assert result == []
        mock_service.search_stream.assert_called_once()

    @pytest.mark.asyncio
    async def test_fetch_campaign_metrics_with_data(self, analytics, mock_service):
        # Mock row as a dict (flatten_row handles both proto and dict)
        mock_row = {
            "campaign.id": "123",
            "campaign.name": "Test Campaign",
            "metrics.impressions": 1000,
            "metrics.clicks": 50,
            "metrics.cost_micros": 5000000,
            "segments.date": "2026-01-01",
        }
        mock_service.search_stream.return_value = [mock_row]

        with patch.object(
            GoogleAdsAnalyticsClient, "_flatten_row", return_value=mock_row
        ):
            result = await analytics.fetch_campaign_metrics(
                start_date=date(2026, 1, 1),
                end_date=date(2026, 1, 7),
            )

        assert len(result) == 1
        assert result[0]["campaign.id"] == "123"
        assert result[0]["metrics.impressions"] == 1000

    @pytest.mark.asyncio
    async def test_fetch_campaign_metrics_with_campaign_filter(
        self, analytics, mock_service
    ):
        mock_service.search_stream.return_value = []
        await analytics.fetch_campaign_metrics(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 7),
            campaign_ids=["111", "222"],
        )
        call_args = mock_service.search_stream.call_args
        query = call_args[0][0]
        assert "campaign.id IN (111, 222)" in query


class TestFetchAdGroupMetrics:
    @pytest.mark.asyncio
    async def test_fetch_ad_group_metrics(self, analytics, mock_service):
        mock_service.search_stream.return_value = []
        result = await analytics.fetch_ad_group_metrics(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 7),
        )
        assert result == []
        call_args = mock_service.search_stream.call_args
        query = call_args[0][0]
        assert "FROM ad_group" in query


class TestFetchKeywordMetrics:
    @pytest.mark.asyncio
    async def test_fetch_keyword_metrics(self, analytics, mock_service):
        mock_service.search_stream.return_value = []
        result = await analytics.fetch_keyword_metrics(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 7),
        )
        assert result == []
        call_args = mock_service.search_stream.call_args
        query = call_args[0][0]
        assert "FROM keyword_view" in query
        assert "ad_group_criterion.status != 'REMOVED'" in query


# --- Row flattening ---


class TestFlattenRow:
    def test_flatten_dict_already_flat(self):
        d = {"a": 1, "b": 2}
        result = GoogleAdsAnalyticsClient._flatten_dict(d)
        assert result == {"a": 1, "b": 2}

    def test_flatten_dict_nested(self):
        d = {"campaign": {"id": 123, "name": "Test"}, "metrics": {"clicks": 50}}
        result = GoogleAdsAnalyticsClient._flatten_dict(d)
        assert result == {
            "campaign.id": 123,
            "campaign.name": "Test",
            "metrics.clicks": 50,
        }

    def test_flatten_dict_deeply_nested(self):
        d = {"a": {"b": {"c": 1}}}
        result = GoogleAdsAnalyticsClient._flatten_dict(d)
        assert result == {"a.b.c": 1}

    def test_flatten_row_with_dict(self):
        row = {"campaign.id": "123", "metrics.clicks": 50}
        result = GoogleAdsAnalyticsClient._flatten_row(row)
        assert result == row

    def test_flatten_row_with_proto_like_object(self):
        """Test with object that has _pb attribute."""
        mock_pb = MagicMock()
        mock_row = MagicMock()
        mock_row._pb = mock_pb

        with patch(
            "app.integrations.google_ads_analytics.MessageToDict",
            return_value={"campaign": {"id": "123"}, "metrics": {"clicks": 50}},
        ):
            result = GoogleAdsAnalyticsClient._flatten_row(mock_row)

        assert result["campaign.id"] == "123"
        assert result["metrics.clicks"] == 50


# --- ClickHouse mapping ---


class TestMapMetricsToClickhouse:
    def test_basic_mapping(self):
        raw = [
            {
                "campaign.id": "123",
                "campaign.name": "Test Campaign",
                "segments.date": "2026-01-01",
                "metrics.impressions": 1000,
                "metrics.clicks": 50,
                "metrics.cost_micros": 5000000,
                "metrics.conversions": 3.0,
                "metrics.conversions_value": 1500000000,
                "metrics.ctr": 0.05,
                "metrics.average_cpc": 100000,
                "metrics.average_cpm": 5000000,
                "metrics.cost_per_conversion": 1666667,
            }
        ]
        result = map_metrics_to_clickhouse("org-1", raw)
        assert len(result) == 1
        row = result[0]
        assert row["tenant_id"] == "org-1"
        assert row["provider"] == "google_ads"
        assert row["provider_campaign_id"] == "123"
        assert row["campaign_name"] == "Test Campaign"
        assert row["metric_date"] == "2026-01-01"
        assert row["impressions"] == 1000
        assert row["clicks"] == 50
        assert row["spend"] == 5.0  # 5000000 micros = $5.00
        assert row["conversions"] == 3.0
        assert row["ctr"] == 0.05

    def test_micros_conversion(self):
        raw = [
            {
                "campaign.id": "1",
                "campaign.name": "C",
                "segments.date": "2026-01-01",
                "metrics.impressions": 0,
                "metrics.clicks": 0,
                "metrics.cost_micros": 12340000,
                "metrics.conversions": 0,
                "metrics.conversions_value": 0,
                "metrics.ctr": 0,
                "metrics.average_cpc": 2500000,
                "metrics.average_cpm": 15000000,
                "metrics.cost_per_conversion": 0,
            }
        ]
        result = map_metrics_to_clickhouse("org-1", raw)
        row = result[0]
        assert row["spend"] == 12.34
        assert row["cpc"] == 2.5
        assert row["cpm"] == 15.0

    def test_empty_metrics(self):
        result = map_metrics_to_clickhouse("org-1", [])
        assert result == []

    def test_missing_fields_default_to_zero(self):
        raw = [{"campaign.id": "1"}]
        result = map_metrics_to_clickhouse("org-1", raw)
        row = result[0]
        assert row["impressions"] == 0
        assert row["clicks"] == 0
        assert row["spend"] == 0.0

    def test_synced_at_present(self):
        raw = [
            {
                "campaign.id": "1",
                "campaign.name": "C",
                "segments.date": "2026-01-01",
                "metrics.impressions": 0,
                "metrics.clicks": 0,
                "metrics.cost_micros": 0,
                "metrics.conversions": 0,
                "metrics.conversions_value": 0,
                "metrics.ctr": 0,
                "metrics.average_cpc": 0,
                "metrics.average_cpm": 0,
                "metrics.cost_per_conversion": 0,
            }
        ]
        result = map_metrics_to_clickhouse("org-1", raw)
        assert "synced_at" in result[0]


# --- ClickHouse write ---


class TestWriteMetricsToClickhouse:
    @pytest.mark.asyncio
    async def test_write_empty(self):
        mock_ch = MagicMock()
        result = await write_metrics_to_clickhouse(mock_ch, [])
        assert result == 0
        mock_ch.command.assert_not_called()

    @pytest.mark.asyncio
    async def test_write_rows(self):
        mock_ch = MagicMock()
        rows = [
            {
                "tenant_id": "org-1",
                "provider": "google_ads",
                "provider_campaign_id": "123",
                "metric_date": "2026-01-01",
                "impressions": 1000,
                "clicks": 50,
                "spend": 5.0,
                "synced_at": "2026-01-02T00:00:00",
            }
        ]
        result = await write_metrics_to_clickhouse(mock_ch, rows)
        assert result == 1
        mock_ch.command.assert_called_once()
        call_sql = mock_ch.command.call_args[0][0]
        assert "INSERT INTO paid_edge.campaign_metrics" in call_sql
        assert "'org-1'" in call_sql

    @pytest.mark.asyncio
    async def test_write_multiple_rows(self):
        mock_ch = MagicMock()
        rows = [
            {"tenant_id": "o1", "provider": "google_ads", "impressions": 100},
            {"tenant_id": "o1", "provider": "google_ads", "impressions": 200},
        ]
        result = await write_metrics_to_clickhouse(mock_ch, rows)
        assert result == 2


# --- Constants ---


class TestConstants:
    def test_clickhouse_mapping_has_required_fields(self):
        required = {"provider_campaign_id", "campaign_name", "metric_date", "impressions", "clicks", "spend"}
        mapped_values = set(CLICKHOUSE_MAPPING.values())
        assert required.issubset(mapped_values)

    def test_micros_fields_are_subset_of_mapping(self):
        mapping_keys = set(CLICKHOUSE_MAPPING.keys())
        assert MICROS_FIELDS.issubset(mapping_keys)

    def test_campaign_metrics_fields_contain_required(self):
        fields = GoogleAdsAnalyticsClient.CAMPAIGN_METRICS_FIELDS
        assert "campaign.id" in fields
        assert "campaign.name" in fields
        assert "metrics.impressions" in fields
        assert "metrics.clicks" in fields
        assert "metrics.cost_micros" in fields
        assert "segments.date" in fields
