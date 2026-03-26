"""Tests for unified ad platform metrics sync Trigger.dev task (PEX-74)."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from trigger.ad_platform_sync import (
    ad_platform_sync_task,
    get_tenants_with_ad_platforms,
    sync_tenant_all_platforms,
)


# --- Tenant discovery ---


class TestGetTenantsWithAdPlatforms:
    @pytest.mark.asyncio
    async def test_groups_platforms_by_tenant(self):
        """Should group multiple platform connections per tenant."""
        mock_sb = MagicMock()
        mock_result = MagicMock()
        mock_result.data = [
            {"organization_id": "org-1", "provider": "linkedin_ads", "config": {"token": "li"}},
            {"organization_id": "org-1", "provider": "meta_ads", "config": {"token": "meta"}},
            {"organization_id": "org-2", "provider": "linkedin_ads", "config": {"token": "li2"}},
        ]
        (
            mock_sb.table.return_value
            .select.return_value
            .in_.return_value
            .execute.return_value
        ) = mock_result

        result = await get_tenants_with_ad_platforms(mock_sb)

        assert len(result) == 2
        org1 = next(t for t in result if t["organization_id"] == "org-1")
        assert len(org1["platforms"]) == 2
        org2 = next(t for t in result if t["organization_id"] == "org-2")
        assert len(org2["platforms"]) == 1

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_connections(self):
        """Should return empty list when no tenants have ad platforms."""
        mock_sb = MagicMock()
        mock_result = MagicMock()
        mock_result.data = []
        (
            mock_sb.table.return_value
            .select.return_value
            .in_.return_value
            .execute.return_value
        ) = mock_result

        result = await get_tenants_with_ad_platforms(mock_sb)

        assert result == []

    @pytest.mark.asyncio
    async def test_queries_correct_providers(self):
        """Should filter by supported ad platform providers."""
        mock_sb = MagicMock()
        mock_result = MagicMock()
        mock_result.data = []
        (
            mock_sb.table.return_value
            .select.return_value
            .in_.return_value
            .execute.return_value
        ) = mock_result

        await get_tenants_with_ad_platforms(mock_sb)

        mock_sb.table.assert_called_with("provider_configs")
        in_call = mock_sb.table.return_value.select.return_value.in_
        in_call.assert_called_once()
        args = in_call.call_args
        assert "provider" in args[0]
        providers = args[0][1]
        assert "linkedin_ads" in providers
        assert "meta_ads" in providers


# --- Per-tenant multi-platform sync ---


class TestSyncTenantAllPlatforms:
    @pytest.mark.asyncio
    async def test_syncs_all_connected_platforms(self):
        """Should call sync for each connected platform."""
        tenant = {
            "organization_id": "org-1",
            "platforms": [
                {"provider": "linkedin_ads", "config": {"token": "li"}},
                {"provider": "meta_ads", "config": {"token": "meta"}},
            ],
        }

        li_result = {
            "task": "linkedin_metrics_sync",
            "tenant_id": "org-1",
            "status": "success",
            "rows_inserted": 10,
        }
        meta_result = {
            "task": "meta_metrics_sync",
            "tenant_id": "org-1",
            "status": "success",
            "rows_inserted": 5,
        }

        with (
            patch(
                "trigger.ad_platform_sync._sync_linkedin_for_tenant",
                new_callable=AsyncMock,
                return_value=li_result,
            ) as mock_li,
            patch(
                "trigger.ad_platform_sync._sync_meta_for_tenant",
                new_callable=AsyncMock,
                return_value=meta_result,
            ) as mock_meta,
        ):
            results = await sync_tenant_all_platforms(
                tenant=tenant,
                supabase=MagicMock(),
                clickhouse=MagicMock(),
            )

        assert len(results) == 2
        assert mock_li.call_count == 1
        assert mock_meta.call_count == 1
        assert results[0]["platform"] == "linkedin"
        assert results[1]["platform"] == "meta"
        assert sum(r["rows_inserted"] for r in results) == 15

    @pytest.mark.asyncio
    async def test_per_platform_error_isolation(self):
        """One platform failing should not stop others."""
        tenant = {
            "organization_id": "org-1",
            "platforms": [
                {"provider": "linkedin_ads", "config": {}},
                {"provider": "meta_ads", "config": {}},
            ],
        }

        with (
            patch(
                "trigger.ad_platform_sync._sync_linkedin_for_tenant",
                new_callable=AsyncMock,
                side_effect=RuntimeError("LinkedIn token expired"),
            ),
            patch(
                "trigger.ad_platform_sync._sync_meta_for_tenant",
                new_callable=AsyncMock,
                return_value={
                    "task": "meta_metrics_sync",
                    "tenant_id": "org-1",
                    "status": "success",
                    "rows_inserted": 5,
                },
            ),
        ):
            results = await sync_tenant_all_platforms(
                tenant=tenant,
                supabase=MagicMock(),
                clickhouse=MagicMock(),
            )

        assert len(results) == 2
        li_result = next(r for r in results if r["platform"] == "linkedin")
        meta_result = next(r for r in results if r["platform"] == "meta")
        assert li_result["status"] == "error"
        assert meta_result["status"] == "success"

    @pytest.mark.asyncio
    async def test_single_platform_tenant(self):
        """Should work for tenants with only one platform."""
        tenant = {
            "organization_id": "org-1",
            "platforms": [
                {"provider": "meta_ads", "config": {}},
            ],
        }

        with patch(
            "trigger.ad_platform_sync._sync_meta_for_tenant",
            new_callable=AsyncMock,
            return_value={
                "task": "meta_metrics_sync",
                "tenant_id": "org-1",
                "status": "success",
                "rows_inserted": 3,
            },
        ):
            results = await sync_tenant_all_platforms(
                tenant=tenant,
                supabase=MagicMock(),
                clickhouse=MagicMock(),
            )

        assert len(results) == 1
        assert results[0]["platform"] == "meta"


# --- Rate limit retry ---


class TestRateLimitRetry:
    @pytest.mark.asyncio
    async def test_linkedin_retries_on_rate_limit(self):
        """Should retry LinkedIn sync on rate limit errors."""
        from trigger.ad_platform_sync import _sync_linkedin_for_tenant

        call_count = 0

        async def mock_sync(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("429 Too Many Requests")
            return {
                "task": "linkedin_metrics_sync",
                "tenant_id": "org-1",
                "status": "success",
                "rows_inserted": 5,
            }

        with (
            patch(
                "trigger.ad_platform_sync.asyncio.sleep",
                new_callable=AsyncMock,
            ),
            patch(
                "trigger.linkedin_metrics_sync.sync_tenant_metrics",
                side_effect=mock_sync,
            ),
            patch(
                "trigger.linkedin_metrics_sync.get_sync_date_range",
                return_value=("2026-03-22", "2026-03-25"),
            ),
        ):
            result = await _sync_linkedin_for_tenant(
                tenant_config={"organization_id": "org-1", "config": {}},
                supabase=MagicMock(),
                clickhouse=MagicMock(),
            )

        assert result["status"] == "success"
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_meta_retries_on_rate_limit(self):
        """Should retry Meta sync on rate limit errors."""
        from trigger.ad_platform_sync import _sync_meta_for_tenant

        call_count = 0

        async def mock_sync(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("rate limit exceeded")
            return {
                "task": "meta_metrics_sync",
                "tenant_id": "org-1",
                "status": "success",
                "rows_inserted": 3,
            }

        with (
            patch(
                "trigger.ad_platform_sync.asyncio.sleep",
                new_callable=AsyncMock,
            ),
            patch(
                "trigger.meta_metrics_sync.sync_tenant_metrics",
                side_effect=mock_sync,
            ),
        ):
            result = await _sync_meta_for_tenant(
                tenant_config={"organization_id": "org-1", "config": {}},
                supabase=MagicMock(),
                clickhouse=MagicMock(),
            )

        assert result["status"] == "success"
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_no_retry_on_non_rate_limit_error(self):
        """Should not retry on non-rate-limit errors."""
        from trigger.ad_platform_sync import _sync_linkedin_for_tenant

        with (
            patch(
                "trigger.linkedin_metrics_sync.sync_tenant_metrics",
                new_callable=AsyncMock,
                side_effect=ValueError("Bad config"),
            ),
            patch(
                "trigger.linkedin_metrics_sync.get_sync_date_range",
                return_value=("2026-03-22", "2026-03-25"),
            ),
        ):
            with pytest.raises(ValueError, match="Bad config"):
                await _sync_linkedin_for_tenant(
                    tenant_config={"organization_id": "org-1", "config": {}},
                    supabase=MagicMock(),
                    clickhouse=MagicMock(),
                )


# --- Full sync task ---


class TestAdPlatformSyncTask:
    @pytest.mark.asyncio
    async def test_full_sync_flow(self):
        """Should discover tenants, sync all platforms, return results."""
        tenants = [
            {
                "organization_id": "org-1",
                "platforms": [
                    {"provider": "linkedin_ads", "config": {}},
                    {"provider": "meta_ads", "config": {}},
                ],
            },
            {
                "organization_id": "org-2",
                "platforms": [
                    {"provider": "linkedin_ads", "config": {}},
                ],
            },
        ]

        with (
            patch(
                "trigger.ad_platform_sync.get_supabase_client",
                return_value=MagicMock(),
            ),
            patch(
                "trigger.ad_platform_sync.get_clickhouse_client",
                return_value=MagicMock(),
            ),
            patch(
                "trigger.ad_platform_sync.get_tenants_with_ad_platforms",
                new_callable=AsyncMock,
                return_value=tenants,
            ),
            patch(
                "trigger.ad_platform_sync.sync_tenant_all_platforms",
                new_callable=AsyncMock,
                side_effect=[
                    [
                        {"platform": "linkedin", "status": "success", "rows_inserted": 10},
                        {"platform": "meta", "status": "success", "rows_inserted": 5},
                    ],
                    [
                        {"platform": "linkedin", "status": "success", "rows_inserted": 8},
                    ],
                ],
            ),
        ):
            results = await ad_platform_sync_task()

        assert len(results) == 3
        assert sum(r["rows_inserted"] for r in results) == 23

    @pytest.mark.asyncio
    async def test_per_tenant_error_isolation(self):
        """One tenant failing should not stop other tenants."""
        tenants = [
            {
                "organization_id": "org-fail",
                "platforms": [{"provider": "linkedin_ads", "config": {}}],
            },
            {
                "organization_id": "org-ok",
                "platforms": [{"provider": "meta_ads", "config": {}}],
            },
        ]

        call_count = 0

        async def mock_sync(**kwargs):
            nonlocal call_count
            call_count += 1
            if kwargs["tenant"]["organization_id"] == "org-fail":
                raise RuntimeError("Catastrophic failure")
            return [
                {"platform": "meta", "status": "success", "rows_inserted": 5},
            ]

        with (
            patch(
                "trigger.ad_platform_sync.get_supabase_client",
                return_value=MagicMock(),
            ),
            patch(
                "trigger.ad_platform_sync.get_clickhouse_client",
                return_value=MagicMock(),
            ),
            patch(
                "trigger.ad_platform_sync.get_tenants_with_ad_platforms",
                new_callable=AsyncMock,
                return_value=tenants,
            ),
            patch(
                "trigger.ad_platform_sync.sync_tenant_all_platforms",
                side_effect=mock_sync,
            ),
        ):
            results = await ad_platform_sync_task()

        assert len(results) == 2
        error_result = next(r for r in results if r.get("tenant_id") == "org-fail")
        assert error_result["status"] == "error"
        ok_result = next(r for r in results if r.get("platform") == "meta")
        assert ok_result["status"] == "success"

    @pytest.mark.asyncio
    async def test_no_tenants(self):
        """Should handle no connected tenants gracefully."""
        with (
            patch(
                "trigger.ad_platform_sync.get_supabase_client",
                return_value=MagicMock(),
            ),
            patch(
                "trigger.ad_platform_sync.get_clickhouse_client",
                return_value=MagicMock(),
            ),
            patch(
                "trigger.ad_platform_sync.get_tenants_with_ad_platforms",
                new_callable=AsyncMock,
                return_value=[],
            ),
        ):
            results = await ad_platform_sync_task()

        assert results == []
