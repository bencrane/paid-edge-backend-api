"""Tests for LinkedIn metrics sync Trigger.dev task (BJC-137)."""

from datetime import date, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from trigger.linkedin_metrics_sync import (
    get_linkedin_connected_tenants,
    get_sync_date_range,
    linkedin_metrics_sync_task,
    sync_tenant_metrics,
)

# --- Date range ---


class TestGetSyncDateRange:
    def test_returns_3_day_lookback(self):
        """Should return (today-3, today) for retroactive adjustments."""
        start, end = get_sync_date_range()
        today = date.today()
        assert end == today
        assert start == today - timedelta(days=3)

    def test_end_is_exclusive(self):
        """End date should be today (exclusive in LinkedIn API)."""
        _, end = get_sync_date_range()
        assert end == date.today()


# --- Tenant discovery ---


class TestGetLinkedInConnectedTenants:
    @pytest.mark.asyncio
    async def test_returns_connected_tenants(self):
        """Should query provider_configs for linkedin_ads tenants."""
        mock_sb = MagicMock()
        mock_result = MagicMock()
        mock_result.data = [
            {
                "organization_id": "org-1",
                "config": {
                    "access_token": "tok",
                    "selected_ad_account_id": 507404993,
                },
            },
            {
                "organization_id": "org-2",
                "config": {"access_token": "tok2"},
            },
        ]
        (
            mock_sb.table.return_value
            .select.return_value
            .eq.return_value
            .execute.return_value
        ) = mock_result

        result = await get_linkedin_connected_tenants(mock_sb)

        assert len(result) == 2
        assert result[0]["organization_id"] == "org-1"
        mock_sb.table.assert_called_with("provider_configs")

    @pytest.mark.asyncio
    async def test_returns_empty_when_none(self):
        """Should return empty list when no tenants connected."""
        mock_sb = MagicMock()
        mock_result = MagicMock()
        mock_result.data = []
        (
            mock_sb.table.return_value
            .select.return_value
            .eq.return_value
            .execute.return_value
        ) = mock_result

        result = await get_linkedin_connected_tenants(mock_sb)

        assert result == []


# --- Single tenant sync ---


class TestSyncTenantMetrics:
    @pytest.mark.asyncio
    async def test_successful_sync(self):
        """Should pull metrics and insert into ClickHouse."""
        mock_sb = MagicMock()
        mock_ch = MagicMock()
        tenant_config = {
            "organization_id": "org-1",
            "config": {"selected_ad_account_id": 507404993},
        }

        raw_elements = [
            {
                "pivotValue": "urn:li:sponsoredCampaign:111",
                "impressions": 1000,
                "clicks": 50,
                "costInLocalCurrency": 75,
                "dateRange": {
                    "start": {"year": 2026, "month": 3, "day": 22},
                    "end": {"year": 2026, "month": 3, "day": 23},
                },
            }
        ]

        with (
            patch(
                "trigger.linkedin_metrics_sync.LinkedInAdsClient"
            ) as mock_client_cls,
            patch(
                "trigger.linkedin_metrics_sync.build_campaign_id_map",
                new_callable=AsyncMock,
                return_value={111: "uuid-1"},
            ),
            patch(
                "trigger.linkedin_metrics_sync"
                ".insert_linkedin_metrics",
                new_callable=AsyncMock,
                return_value=1,
            ),
        ):
            mock_client = AsyncMock()
            mock_client.get_selected_account_id.return_value = (
                507404993
            )
            mock_client.get_campaign_analytics.return_value = (
                raw_elements
            )
            mock_client_cls.return_value.__aenter__ = AsyncMock(
                return_value=mock_client
            )
            mock_client_cls.return_value.__aexit__ = AsyncMock(
                return_value=False
            )

            result = await sync_tenant_metrics(
                tenant_config=tenant_config,
                start_date=date(2026, 3, 22),
                end_date=date(2026, 3, 25),
                supabase=mock_sb,
                clickhouse=mock_ch,
            )

        assert result["status"] == "success"
        assert result["tenant_id"] == "org-1"
        assert result["ad_account_id"] == 507404993
        assert result["campaigns_synced"] == 1
        assert result["rows_inserted"] == 1

    @pytest.mark.asyncio
    async def test_skips_when_no_campaigns_mapped(self):
        """Should skip gracefully when no campaign mappings exist."""
        mock_sb = MagicMock()
        mock_ch = MagicMock()
        tenant_config = {
            "organization_id": "org-1",
            "config": {},
        }

        with (
            patch(
                "trigger.linkedin_metrics_sync.LinkedInAdsClient"
            ) as mock_client_cls,
            patch(
                "trigger.linkedin_metrics_sync.build_campaign_id_map",
                new_callable=AsyncMock,
                return_value={},  # No mappings
            ),
        ):
            mock_client = AsyncMock()
            mock_client.get_selected_account_id.return_value = 1
            mock_client_cls.return_value.__aenter__ = AsyncMock(
                return_value=mock_client
            )
            mock_client_cls.return_value.__aexit__ = AsyncMock(
                return_value=False
            )

            result = await sync_tenant_metrics(
                tenant_config=tenant_config,
                start_date=date(2026, 3, 22),
                end_date=date(2026, 3, 25),
                supabase=mock_sb,
                clickhouse=mock_ch,
            )

        assert result["status"] == "skipped_no_campaigns"
        assert result["campaigns_synced"] == 0
        assert result["rows_inserted"] == 0

    @pytest.mark.asyncio
    async def test_skips_when_no_analytics_data(self):
        """Should handle empty analytics response gracefully."""
        mock_sb = MagicMock()
        mock_ch = MagicMock()
        tenant_config = {
            "organization_id": "org-1",
            "config": {},
        }

        with (
            patch(
                "trigger.linkedin_metrics_sync.LinkedInAdsClient"
            ) as mock_client_cls,
            patch(
                "trigger.linkedin_metrics_sync.build_campaign_id_map",
                new_callable=AsyncMock,
                return_value={111: "uuid-1"},
            ),
        ):
            mock_client = AsyncMock()
            mock_client.get_selected_account_id.return_value = 1
            mock_client.get_campaign_analytics.return_value = []
            mock_client_cls.return_value.__aenter__ = AsyncMock(
                return_value=mock_client
            )
            mock_client_cls.return_value.__aexit__ = AsyncMock(
                return_value=False
            )

            result = await sync_tenant_metrics(
                tenant_config=tenant_config,
                start_date=date(2026, 3, 22),
                end_date=date(2026, 3, 25),
                supabase=mock_sb,
                clickhouse=mock_ch,
            )

        assert result["status"] == "skipped_no_data"


# --- Full sync task ---


class TestLinkedInMetricsSyncTask:
    @pytest.mark.asyncio
    async def test_full_sync_flow(self):
        """Should discover tenants, sync each, return results."""
        mock_sb = MagicMock()
        mock_ch = MagicMock()

        tenants = [
            {
                "organization_id": "org-1",
                "config": {"selected_ad_account_id": 1},
            },
            {
                "organization_id": "org-2",
                "config": {"selected_ad_account_id": 2},
            },
        ]

        with (
            patch(
                "trigger.linkedin_metrics_sync.get_supabase_client",
                return_value=mock_sb,
            ),
            patch(
                "trigger.linkedin_metrics_sync.get_clickhouse_client",
                return_value=mock_ch,
            ),
            patch(
                "trigger.linkedin_metrics_sync"
                ".get_linkedin_connected_tenants",
                new_callable=AsyncMock,
                return_value=tenants,
            ),
            patch(
                "trigger.linkedin_metrics_sync.sync_tenant_metrics",
                new_callable=AsyncMock,
                return_value={
                    "task": "linkedin_metrics_sync",
                    "tenant_id": "org-x",
                    "status": "success",
                    "rows_inserted": 5,
                },
            ) as mock_sync,
        ):
            results = await linkedin_metrics_sync_task()

        assert len(results) == 2
        assert mock_sync.call_count == 2
        assert all(r["status"] == "success" for r in results)

    @pytest.mark.asyncio
    async def test_per_tenant_error_isolation(self):
        """One tenant failure should not stop others."""
        mock_sb = MagicMock()
        mock_ch = MagicMock()

        tenants = [
            {"organization_id": "org-fail", "config": {}},
            {"organization_id": "org-ok", "config": {}},
        ]

        call_count = 0

        async def sync_side_effect(**kwargs):
            nonlocal call_count
            call_count += 1
            if kwargs["tenant_config"]["organization_id"] == "org-fail":
                raise RuntimeError("Token expired")
            return {
                "task": "linkedin_metrics_sync",
                "tenant_id": "org-ok",
                "status": "success",
                "rows_inserted": 3,
            }

        with (
            patch(
                "trigger.linkedin_metrics_sync.get_supabase_client",
                return_value=mock_sb,
            ),
            patch(
                "trigger.linkedin_metrics_sync.get_clickhouse_client",
                return_value=mock_ch,
            ),
            patch(
                "trigger.linkedin_metrics_sync"
                ".get_linkedin_connected_tenants",
                new_callable=AsyncMock,
                return_value=tenants,
            ),
            patch(
                "trigger.linkedin_metrics_sync.sync_tenant_metrics",
                side_effect=sync_side_effect,
            ),
        ):
            results = await linkedin_metrics_sync_task()

        assert len(results) == 2
        # First tenant errored
        assert results[0]["status"] == "error"
        assert results[0]["tenant_id"] == "org-fail"
        # Second tenant succeeded
        assert results[1]["status"] == "success"

    @pytest.mark.asyncio
    async def test_no_tenants(self):
        """Should handle no connected tenants gracefully."""
        with (
            patch(
                "trigger.linkedin_metrics_sync.get_supabase_client",
                return_value=MagicMock(),
            ),
            patch(
                "trigger.linkedin_metrics_sync.get_clickhouse_client",
                return_value=MagicMock(),
            ),
            patch(
                "trigger.linkedin_metrics_sync"
                ".get_linkedin_connected_tenants",
                new_callable=AsyncMock,
                return_value=[],
            ),
        ):
            results = await linkedin_metrics_sync_task()

        assert results == []
