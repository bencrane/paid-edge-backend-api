"""Tests for Meta metrics sync Trigger.dev task (BJC-163)."""

from datetime import date, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from trigger.meta_metrics_sync import (
    get_meta_connected_tenants,
    get_sync_date_range,
    meta_metrics_sync_task,
    sync_tenant_metrics,
)


class TestGetSyncDateRange:
    def test_three_day_lookback(self):
        start, end = get_sync_date_range()
        assert end == date.today()
        assert start == date.today() - timedelta(days=3)


class TestGetMetaConnectedTenants:
    @pytest.mark.asyncio
    async def test_finds_tenants(self):
        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.execute.return_value = MagicMock(
            data=[
                {"organization_id": "org-1", "config": {"selected_ad_account_id": "act_1"}},
                {"organization_id": "org-2", "config": {"selected_ad_account_id": "act_2"}},
            ]
        )
        tenants = await get_meta_connected_tenants(mock_supabase)
        assert len(tenants) == 2

    @pytest.mark.asyncio
    async def test_no_tenants(self):
        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.execute.return_value = MagicMock(data=[])
        tenants = await get_meta_connected_tenants(mock_supabase)
        assert len(tenants) == 0


class TestSyncTenantMetrics:
    @pytest.mark.asyncio
    async def test_successful_sync(self):
        mock_supabase = MagicMock()
        mock_clickhouse = MagicMock()

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.get_campaign_insights = AsyncMock(return_value=[
            {
                "campaign_id": "mc1", "date_start": "2026-03-25",
                "impressions": "1000", "clicks": "50", "spend": "10.00",
                "ctr": "5.0", "cpc": "0.20", "cpm": "10.00",
                "actions": [{"action_type": "lead", "value": "5"}],
            }
        ])

        with patch("trigger.meta_metrics_sync.MetaAdsClient") as MockClient, \
             patch("trigger.meta_metrics_sync.build_meta_campaign_id_map", new_callable=AsyncMock, return_value={"mc1": "pe-1"}), \
             patch("trigger.meta_metrics_sync.insert_meta_metrics", new_callable=AsyncMock, return_value=1):
            MockClient.for_tenant = AsyncMock(return_value=mock_client)

            result = await sync_tenant_metrics(
                {"organization_id": "org-1", "config": {}},
                mock_supabase,
                mock_clickhouse,
            )

        assert result["status"] == "success"
        assert result["rows_inserted"] == 1

    @pytest.mark.asyncio
    async def test_skip_no_campaigns(self):
        mock_supabase = MagicMock()
        mock_clickhouse = MagicMock()

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("trigger.meta_metrics_sync.MetaAdsClient") as MockClient, \
             patch("trigger.meta_metrics_sync.build_meta_campaign_id_map", new_callable=AsyncMock, return_value={}):
            MockClient.for_tenant = AsyncMock(return_value=mock_client)

            result = await sync_tenant_metrics(
                {"organization_id": "org-1", "config": {}},
                mock_supabase,
                mock_clickhouse,
            )

        assert result["status"] == "skipped_no_campaigns"

    @pytest.mark.asyncio
    async def test_error_isolation(self):
        """Errors for one tenant should not crash the task."""
        mock_supabase = MagicMock()
        mock_clickhouse = MagicMock()

        with patch("trigger.meta_metrics_sync.MetaAdsClient") as MockClient:
            MockClient.for_tenant = AsyncMock(side_effect=Exception("Token expired"))

            result = await sync_tenant_metrics(
                {"organization_id": "org-1", "config": {}},
                mock_supabase,
                mock_clickhouse,
            )

        assert result["status"] == "error"


class TestMetaMetricsSyncTask:
    @pytest.mark.asyncio
    async def test_full_task(self):
        mock_supabase = MagicMock()
        mock_clickhouse = MagicMock()

        with patch("trigger.meta_metrics_sync.get_meta_connected_tenants", new_callable=AsyncMock, return_value=[
            {"organization_id": "org-1", "config": {}},
        ]), patch("trigger.meta_metrics_sync.sync_tenant_metrics", new_callable=AsyncMock, return_value={
            "tenant_id": "org-1", "status": "success", "rows_inserted": 5,
        }):
            results = await meta_metrics_sync_task(mock_supabase, mock_clickhouse)

        assert len(results) == 1
        assert results[0]["status"] == "success"
