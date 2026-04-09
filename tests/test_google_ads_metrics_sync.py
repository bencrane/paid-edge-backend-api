"""Tests for Google Ads metrics sync Trigger.dev task (BJC-152)."""

from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from trigger.google_ads_metrics_sync import (
    FIRST_SYNC_BACKFILL_DAYS,
    LOOKBACK_DAYS,
    MAX_CONSECUTIVE_FAILURES,
    _DAILY_QUOTA_LIMIT,
    _QUOTA_WARNING_THRESHOLD,
    get_google_ads_connected_tenants,
    get_sync_date_range,
    google_ads_metrics_sync_task,
    sync_tenant_metrics,
)


# --- Date range ---


class TestGetSyncDateRange:
    def test_normal_sync_3_day_lookback(self):
        start, end = get_sync_date_range(is_first_sync=False)
        assert (end - start).days == LOOKBACK_DAYS

    def test_first_sync_30_day_backfill(self):
        start, end = get_sync_date_range(is_first_sync=True)
        assert (end - start).days == FIRST_SYNC_BACKFILL_DAYS

    def test_end_date_is_today(self):
        _, end = get_sync_date_range()
        assert end == date.today()


# --- Tenant discovery ---


class TestGetConnectedTenants:
    @pytest.mark.asyncio
    async def test_finds_active_tenants(self):
        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.execute.return_value = MagicMock(
            data=[
                {
                    "organization_id": "org-1",
                    "config": {
                        "refresh_token": "valid-token",
                        "selected_customer_id": "123",
                    },
                    "is_active": True,
                },
                {
                    "organization_id": "org-2",
                    "config": {
                        "refresh_token": "valid-token",
                        "selected_customer_id": "456",
                    },
                    "is_active": True,
                },
            ]
        )
        tenants = await get_google_ads_connected_tenants(mock_supabase)
        assert len(tenants) == 2

    @pytest.mark.asyncio
    async def test_excludes_tenants_without_customer_id(self):
        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.execute.return_value = MagicMock(
            data=[
                {
                    "organization_id": "org-1",
                    "config": {
                        "refresh_token": "valid-token",
                        # No selected_customer_id
                    },
                    "is_active": True,
                },
            ]
        )
        tenants = await get_google_ads_connected_tenants(mock_supabase)
        assert len(tenants) == 0

    @pytest.mark.asyncio
    async def test_excludes_tenants_without_refresh_token(self):
        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.execute.return_value = MagicMock(
            data=[
                {
                    "organization_id": "org-1",
                    "config": {
                        "selected_customer_id": "123",
                        # No refresh_token
                    },
                    "is_active": True,
                },
            ]
        )
        tenants = await get_google_ads_connected_tenants(mock_supabase)
        assert len(tenants) == 0

    @pytest.mark.asyncio
    async def test_empty_results(self):
        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.execute.return_value = MagicMock(
            data=[]
        )
        tenants = await get_google_ads_connected_tenants(mock_supabase)
        assert len(tenants) == 0


# --- Sync tenant metrics ---


class TestSyncTenantMetrics:
    @pytest.fixture
    def tenant_config(self):
        return {
            "organization_id": "org-1",
            "config": {
                "refresh_token": "valid-token",
                "selected_customer_id": "123456",
                "last_synced_at": "2026-01-01T00:00:00",
            },
        }

    @pytest.fixture
    def first_sync_tenant_config(self):
        return {
            "organization_id": "org-1",
            "config": {
                "refresh_token": "valid-token",
                "selected_customer_id": "123456",
            },
        }

    @pytest.mark.asyncio
    async def test_successful_sync(self, tenant_config):
        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.maybe_single.return_value.execute.return_value = MagicMock(
            data={"config": tenant_config["config"]}
        )
        mock_clickhouse = MagicMock()
        daily_ops = {"count": 0}

        mock_metrics = [
            {
                "campaign.id": "111",
                "campaign.name": "Test",
                "segments.date": "2026-01-01",
                "metrics.impressions": 100,
                "metrics.clicks": 10,
                "metrics.cost_micros": 1000000,
                "metrics.conversions": 1.0,
                "metrics.conversions_value": 0,
                "metrics.ctr": 0.1,
                "metrics.average_cpc": 100000,
                "metrics.average_cpm": 10000000,
                "metrics.cost_per_conversion": 1000000,
            }
        ]

        with patch(
            "trigger.google_ads_metrics_sync.GoogleAdsClientFactory"
        ) as MockFactory, patch(
            "trigger.google_ads_metrics_sync.GoogleAdsService"
        ), patch(
            "trigger.google_ads_metrics_sync.GoogleAdsAnalyticsClient"
        ) as MockAnalytics, patch(
            "trigger.google_ads_metrics_sync.write_metrics_to_clickhouse",
            new_callable=AsyncMock,
            return_value=1,
        ), patch(
            "trigger.google_ads_metrics_sync._update_sync_timestamp",
            new_callable=AsyncMock,
        ), patch(
            "trigger.google_ads_metrics_sync._reset_failure_count",
            new_callable=AsyncMock,
        ):
            mock_factory = MockFactory.return_value
            mock_factory.get_client = AsyncMock()

            mock_analytics = MockAnalytics.return_value
            mock_analytics.fetch_campaign_metrics = AsyncMock(
                return_value=mock_metrics
            )

            result = await sync_tenant_metrics(
                tenant_config, mock_supabase, mock_clickhouse, daily_ops
            )

        assert result["status"] == "success"
        assert result["tenant_id"] == "org-1"
        assert result["rows_inserted"] == 1
        assert result["campaigns_synced"] == 1
        assert result["is_first_sync"] is False
        assert daily_ops["count"] == 1

    @pytest.mark.asyncio
    async def test_first_sync_uses_30_day_backfill(self, first_sync_tenant_config):
        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.maybe_single.return_value.execute.return_value = MagicMock(
            data={"config": first_sync_tenant_config["config"]}
        )
        mock_clickhouse = MagicMock()
        daily_ops = {"count": 0}

        with patch(
            "trigger.google_ads_metrics_sync.GoogleAdsClientFactory"
        ) as MockFactory, patch(
            "trigger.google_ads_metrics_sync.GoogleAdsService"
        ), patch(
            "trigger.google_ads_metrics_sync.GoogleAdsAnalyticsClient"
        ) as MockAnalytics, patch(
            "trigger.google_ads_metrics_sync.write_metrics_to_clickhouse",
            new_callable=AsyncMock,
            return_value=0,
        ), patch(
            "trigger.google_ads_metrics_sync._update_sync_timestamp",
            new_callable=AsyncMock,
        ), patch(
            "trigger.google_ads_metrics_sync._reset_failure_count",
            new_callable=AsyncMock,
        ):
            mock_factory = MockFactory.return_value
            mock_factory.get_client = AsyncMock()

            mock_analytics = MockAnalytics.return_value
            mock_analytics.fetch_campaign_metrics = AsyncMock(return_value=[])

            result = await sync_tenant_metrics(
                first_sync_tenant_config,
                mock_supabase,
                mock_clickhouse,
                daily_ops,
            )

        assert result["is_first_sync"] is True
        assert result["status"] == "skipped_no_data"

    @pytest.mark.asyncio
    async def test_deferred_on_quota_exhaustion(self, tenant_config):
        mock_supabase = MagicMock()
        mock_clickhouse = MagicMock()
        daily_ops = {"count": _DAILY_QUOTA_LIMIT}

        result = await sync_tenant_metrics(
            tenant_config, mock_supabase, mock_clickhouse, daily_ops
        )

        assert result["status"] == "deferred_quota"
        assert result["rows_inserted"] == 0

    @pytest.mark.asyncio
    async def test_error_handling_increments_failure(self, tenant_config):
        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.maybe_single.return_value.execute.return_value = MagicMock(
            data={"config": tenant_config["config"]}
        )
        mock_clickhouse = MagicMock()
        daily_ops = {"count": 0}

        with patch(
            "trigger.google_ads_metrics_sync.GoogleAdsClientFactory"
        ) as MockFactory, patch(
            "trigger.google_ads_metrics_sync._increment_failure_count",
            new_callable=AsyncMock,
        ) as mock_increment:
            mock_factory = MockFactory.return_value
            mock_factory.get_client = AsyncMock(
                side_effect=Exception("Auth failed")
            )

            result = await sync_tenant_metrics(
                tenant_config, mock_supabase, mock_clickhouse, daily_ops
            )

        assert result["status"] == "error"
        assert "Auth failed" in result["error"]
        mock_increment.assert_called_once_with(mock_supabase, "org-1")


# --- Full task ---


class TestFullSyncTask:
    @pytest.mark.asyncio
    async def test_sync_task_runs_for_all_tenants(self):
        mock_tenants = [
            {
                "organization_id": "org-1",
                "config": {
                    "refresh_token": "t1",
                    "selected_customer_id": "111",
                    "last_synced_at": "2026-01-01",
                },
            },
            {
                "organization_id": "org-2",
                "config": {
                    "refresh_token": "t2",
                    "selected_customer_id": "222",
                    "last_synced_at": "2026-01-01",
                },
            },
        ]

        with patch(
            "trigger.google_ads_metrics_sync.get_supabase_client"
        ) as mock_sb, patch(
            "trigger.google_ads_metrics_sync.get_clickhouse_client"
        ) as mock_ch, patch(
            "trigger.google_ads_metrics_sync.get_google_ads_connected_tenants",
            new_callable=AsyncMock,
            return_value=mock_tenants,
        ), patch(
            "trigger.google_ads_metrics_sync.sync_tenant_metrics",
            new_callable=AsyncMock,
            side_effect=[
                {"tenant_id": "org-1", "status": "success", "rows_inserted": 5},
                {"tenant_id": "org-2", "status": "success", "rows_inserted": 3},
            ],
        ):
            results = await google_ads_metrics_sync_task()

        assert len(results) == 2
        assert results[0]["status"] == "success"
        assert results[1]["status"] == "success"

    @pytest.mark.asyncio
    async def test_sync_task_continues_on_failure(self):
        mock_tenants = [
            {
                "organization_id": "org-1",
                "config": {
                    "refresh_token": "t1",
                    "selected_customer_id": "111",
                    "last_synced_at": "2026-01-01",
                },
            },
            {
                "organization_id": "org-2",
                "config": {
                    "refresh_token": "t2",
                    "selected_customer_id": "222",
                    "last_synced_at": "2026-01-01",
                },
            },
        ]

        with patch(
            "trigger.google_ads_metrics_sync.get_supabase_client"
        ), patch(
            "trigger.google_ads_metrics_sync.get_clickhouse_client"
        ), patch(
            "trigger.google_ads_metrics_sync.get_google_ads_connected_tenants",
            new_callable=AsyncMock,
            return_value=mock_tenants,
        ), patch(
            "trigger.google_ads_metrics_sync.sync_tenant_metrics",
            new_callable=AsyncMock,
            side_effect=[
                {"tenant_id": "org-1", "status": "error", "rows_inserted": 0, "error": "fail"},
                {"tenant_id": "org-2", "status": "success", "rows_inserted": 5},
            ],
        ):
            results = await google_ads_metrics_sync_task()

        assert len(results) == 2
        assert results[0]["status"] == "error"
        assert results[1]["status"] == "success"

    @pytest.mark.asyncio
    async def test_sync_task_no_tenants(self):
        with patch(
            "trigger.google_ads_metrics_sync.get_supabase_client"
        ), patch(
            "trigger.google_ads_metrics_sync.get_clickhouse_client"
        ), patch(
            "trigger.google_ads_metrics_sync.get_google_ads_connected_tenants",
            new_callable=AsyncMock,
            return_value=[],
        ):
            results = await google_ads_metrics_sync_task()

        assert results == []


# --- Constants ---


class TestConstants:
    def test_quota_limit(self):
        assert _DAILY_QUOTA_LIMIT == 15_000

    def test_warning_threshold(self):
        assert _QUOTA_WARNING_THRESHOLD == 0.80

    def test_lookback_days(self):
        assert LOOKBACK_DAYS == 3

    def test_first_sync_backfill(self):
        assert FIRST_SYNC_BACKFILL_DAYS == 30

    def test_max_consecutive_failures(self):
        assert MAX_CONSECUTIVE_FAILURES == 3
