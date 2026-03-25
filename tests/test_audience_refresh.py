"""Tests for audience refresh Trigger.dev tasks (BJC-85)."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from trigger.audience_refresh import (
    audience_daily_refresh_task,
    audience_hourly_delta_task,
    get_active_tenants,
    get_tenant_segments,
    refresh_segment,
    refresh_tenant_segments,
)


# --- Fixtures ---


SAMPLE_SEGMENT = {
    "id": "seg-1",
    "name": "VP Engineering ICP",
    "filter_config": {"signal_type": "new_in_role", "seniority": "VP"},
    "priority": "normal",
    "member_count": 0,
    "last_refreshed_at": None,
}

SAMPLE_SEGMENT_HIGH = {
    **SAMPLE_SEGMENT,
    "id": "seg-2",
    "name": "High Priority Segment",
    "priority": "high",
}


def _mock_supabase(data=None):
    """Build a mock Supabase client with chained query builder."""
    mock = MagicMock()
    result = MagicMock()
    result.data = data if data is not None else []

    chain = MagicMock()
    chain.select.return_value = chain
    chain.eq.return_value = chain
    chain.is_.return_value = chain
    chain.order.return_value = chain
    chain.update.return_value = chain
    chain.execute.return_value = result
    mock.table.return_value = chain
    return mock


# --- get_active_tenants ---


class TestGetActiveTenants:
    @pytest.mark.asyncio
    async def test_returns_unique_org_ids(self):
        """Should deduplicate organization_ids."""
        mock_sb = _mock_supabase(data=[
            {"organization_id": "org-1"},
            {"organization_id": "org-1"},
            {"organization_id": "org-2"},
        ])
        result = await get_active_tenants(mock_sb)
        assert result == ["org-1", "org-2"]

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_segments(self):
        """Should return empty list when no active segments."""
        mock_sb = _mock_supabase(data=[])
        result = await get_active_tenants(mock_sb)
        assert result == []


# --- get_tenant_segments ---


class TestGetTenantSegments:
    @pytest.mark.asyncio
    async def test_returns_segments(self):
        """Should return active segments for tenant."""
        mock_sb = _mock_supabase(data=[SAMPLE_SEGMENT])
        result = await get_tenant_segments(mock_sb, "org-1")
        assert len(result) == 1
        assert result[0]["id"] == "seg-1"

    @pytest.mark.asyncio
    async def test_filters_by_priority(self):
        """Should pass priority filter when specified."""
        mock_sb = _mock_supabase(data=[SAMPLE_SEGMENT_HIGH])
        result = await get_tenant_segments(mock_sb, "org-1", priority="high")
        assert len(result) == 1
        mock_sb.table.assert_called_with("audience_segments")

    @pytest.mark.asyncio
    async def test_returns_empty(self):
        """Should return empty list when no segments."""
        mock_sb = _mock_supabase(data=[])
        result = await get_tenant_segments(mock_sb, "org-1")
        assert result == []


# --- refresh_segment ---


class TestRefreshSegment:
    @pytest.mark.asyncio
    async def test_signal_based_refresh(self):
        """Should fetch signals and write to ClickHouse."""
        mock_sb = _mock_supabase()
        mock_ch = MagicMock()
        mock_dex = AsyncMock()

        # Mock signal response
        mock_signal = MagicMock()
        mock_signal.entity_id = "eid-1"
        mock_signal.entity_type = "person"
        mock_signal.details = {
            "full_name": "Jane Doe",
            "work_email": "jane@acme.com",
            "title": "VP Engineering",
            "company_name": "Acme",
            "linkedin_url": "https://li.com/jane",
        }
        mock_dex.get_signals.return_value = [mock_signal]

        result = await refresh_segment(
            segment=SAMPLE_SEGMENT,
            org_id="org-1",
            dex_client=mock_dex,
            supabase=mock_sb,
            clickhouse=mock_ch,
        )

        assert result["status"] == "success"
        assert result["members_written"] == 1
        assert result["segment_id"] == "seg-1"
        mock_ch.insert.assert_called_once()
        mock_dex.get_signals.assert_called_once()

    @pytest.mark.asyncio
    async def test_entity_based_refresh(self):
        """Should fetch entities and write to ClickHouse when no signal_type."""
        mock_sb = _mock_supabase()
        mock_ch = MagicMock()
        mock_dex = AsyncMock()

        segment_no_signal = {
            **SAMPLE_SEGMENT,
            "filter_config": {"entity_type": "company", "industry": "Software"},
        }

        # Mock search response
        mock_result = MagicMock()
        mock_result.items = [
            {"entity_id": "eid-1", "canonical_name": "Acme", "linkedin_url": "https://li.com/acme"},
        ]
        mock_dex.search_entities.return_value = mock_result

        result = await refresh_segment(
            segment=segment_no_signal,
            org_id="org-1",
            dex_client=mock_dex,
            supabase=mock_sb,
            clickhouse=mock_ch,
        )

        assert result["status"] == "success"
        assert result["members_written"] == 1
        mock_dex.search_entities.assert_called_once()

    @pytest.mark.asyncio
    async def test_empty_results(self):
        """Should handle no matching signals/entities."""
        mock_sb = _mock_supabase()
        mock_ch = MagicMock()
        mock_dex = AsyncMock()
        mock_dex.get_signals.return_value = []

        result = await refresh_segment(
            segment=SAMPLE_SEGMENT,
            org_id="org-1",
            dex_client=mock_dex,
            supabase=mock_sb,
            clickhouse=mock_ch,
        )

        assert result["status"] == "success"
        assert result["members_written"] == 0
        mock_ch.insert.assert_not_called()

    @pytest.mark.asyncio
    async def test_updates_supabase_metadata(self):
        """Should update member_count and last_refreshed_at in Supabase."""
        mock_sb = _mock_supabase()
        mock_ch = MagicMock()
        mock_dex = AsyncMock()
        mock_dex.get_signals.return_value = []

        await refresh_segment(
            segment=SAMPLE_SEGMENT,
            org_id="org-1",
            dex_client=mock_dex,
            supabase=mock_sb,
            clickhouse=mock_ch,
        )

        mock_sb.table.assert_called_with("audience_segments")
        chain = mock_sb.table.return_value
        chain.update.assert_called_once()
        update_data = chain.update.call_args[0][0]
        assert "member_count" in update_data
        assert "last_refreshed_at" in update_data


# --- refresh_tenant_segments ---


class TestRefreshTenantSegments:
    @pytest.mark.asyncio
    async def test_refreshes_all_segments(self):
        """Should refresh each segment and return results."""
        mock_sb = _mock_supabase(data=[SAMPLE_SEGMENT, SAMPLE_SEGMENT_HIGH])
        mock_ch = MagicMock()
        mock_dex = AsyncMock()
        mock_dex.get_signals.return_value = []

        results = await refresh_tenant_segments(
            org_id="org-1",
            supabase=mock_sb,
            clickhouse=mock_ch,
            dex_client=mock_dex,
        )

        assert len(results) == 2
        assert all(r["status"] == "success" for r in results)

    @pytest.mark.asyncio
    async def test_skips_when_no_segments(self):
        """Should return skipped status when no segments."""
        mock_sb = _mock_supabase(data=[])
        mock_ch = MagicMock()
        mock_dex = AsyncMock()

        results = await refresh_tenant_segments(
            org_id="org-1",
            supabase=mock_sb,
            clickhouse=mock_ch,
            dex_client=mock_dex,
        )

        assert len(results) == 1
        assert results[0]["status"] == "skipped_no_segments"

    @pytest.mark.asyncio
    async def test_per_segment_error_isolation(self):
        """One segment failure should not stop others."""
        segments = [
            {**SAMPLE_SEGMENT, "id": "seg-fail"},
            {**SAMPLE_SEGMENT, "id": "seg-ok"},
        ]
        mock_sb = _mock_supabase(data=segments)
        mock_ch = MagicMock()
        mock_dex = AsyncMock()

        call_count = 0

        async def get_signals_side_effect(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("data-engine-x unreachable")
            return []

        mock_dex.get_signals.side_effect = get_signals_side_effect

        results = await refresh_tenant_segments(
            org_id="org-1",
            supabase=mock_sb,
            clickhouse=mock_ch,
            dex_client=mock_dex,
        )

        assert len(results) == 2
        assert results[0]["status"] == "error"
        assert results[0]["segment_id"] == "seg-fail"
        assert results[1]["status"] == "success"


# --- audience_daily_refresh_task ---


class TestAudienceDailyRefreshTask:
    @pytest.mark.asyncio
    async def test_full_daily_flow(self):
        """Should discover tenants, refresh each, return results."""
        with (
            patch(
                "trigger.audience_refresh.get_supabase_client",
                return_value=MagicMock(),
            ),
            patch(
                "trigger.audience_refresh.get_clickhouse_client",
                return_value=MagicMock(),
            ),
            patch(
                "trigger.audience_refresh.get_active_tenants",
                new_callable=AsyncMock,
                return_value=["org-1", "org-2"],
            ),
            patch(
                "trigger.audience_refresh.refresh_tenant_segments",
                new_callable=AsyncMock,
                return_value=[{
                    "task": "audience_refresh",
                    "tenant_id": "org-x",
                    "status": "success",
                    "members_written": 10,
                }],
            ) as mock_refresh,
            patch(
                "trigger.audience_refresh.DataEngineXClient",
            ) as mock_dex_cls,
        ):
            mock_dex = AsyncMock()
            mock_dex_cls.return_value.__aenter__ = AsyncMock(return_value=mock_dex)
            mock_dex_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            results = await audience_daily_refresh_task()

        assert len(results) == 2
        assert mock_refresh.call_count == 2

    @pytest.mark.asyncio
    async def test_per_tenant_error_isolation(self):
        """One tenant failure should not stop others."""
        call_count = 0

        async def refresh_side_effect(**kwargs):
            nonlocal call_count
            call_count += 1
            if kwargs["org_id"] == "org-fail":
                raise RuntimeError("Token expired")
            return [{
                "task": "audience_refresh",
                "tenant_id": "org-ok",
                "status": "success",
                "members_written": 5,
            }]

        with (
            patch(
                "trigger.audience_refresh.get_supabase_client",
                return_value=MagicMock(),
            ),
            patch(
                "trigger.audience_refresh.get_clickhouse_client",
                return_value=MagicMock(),
            ),
            patch(
                "trigger.audience_refresh.get_active_tenants",
                new_callable=AsyncMock,
                return_value=["org-fail", "org-ok"],
            ),
            patch(
                "trigger.audience_refresh.refresh_tenant_segments",
                side_effect=refresh_side_effect,
            ),
            patch(
                "trigger.audience_refresh.DataEngineXClient",
            ) as mock_dex_cls,
        ):
            mock_dex = AsyncMock()
            mock_dex_cls.return_value.__aenter__ = AsyncMock(return_value=mock_dex)
            mock_dex_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            results = await audience_daily_refresh_task()

        assert len(results) == 2
        assert results[0]["status"] == "error"
        assert results[0]["tenant_id"] == "org-fail"
        assert results[1]["status"] == "success"

    @pytest.mark.asyncio
    async def test_no_tenants(self):
        """Should handle no active tenants gracefully."""
        with (
            patch(
                "trigger.audience_refresh.get_supabase_client",
                return_value=MagicMock(),
            ),
            patch(
                "trigger.audience_refresh.get_clickhouse_client",
                return_value=MagicMock(),
            ),
            patch(
                "trigger.audience_refresh.get_active_tenants",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "trigger.audience_refresh.DataEngineXClient",
            ) as mock_dex_cls,
        ):
            mock_dex = AsyncMock()
            mock_dex_cls.return_value.__aenter__ = AsyncMock(return_value=mock_dex)
            mock_dex_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            results = await audience_daily_refresh_task()

        assert results == []


# --- audience_hourly_delta_task ---


class TestAudienceHourlyDeltaTask:
    @pytest.mark.asyncio
    async def test_filters_high_priority(self):
        """Should only refresh high-priority segments."""
        with (
            patch(
                "trigger.audience_refresh.get_supabase_client",
                return_value=MagicMock(),
            ),
            patch(
                "trigger.audience_refresh.get_clickhouse_client",
                return_value=MagicMock(),
            ),
            patch(
                "trigger.audience_refresh.get_active_tenants",
                new_callable=AsyncMock,
                return_value=["org-1"],
            ),
            patch(
                "trigger.audience_refresh.refresh_tenant_segments",
                new_callable=AsyncMock,
                return_value=[{
                    "task": "audience_refresh",
                    "tenant_id": "org-1",
                    "status": "success",
                    "members_written": 3,
                }],
            ) as mock_refresh,
            patch(
                "trigger.audience_refresh.DataEngineXClient",
            ) as mock_dex_cls,
        ):
            mock_dex = AsyncMock()
            mock_dex_cls.return_value.__aenter__ = AsyncMock(return_value=mock_dex)
            mock_dex_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            results = await audience_hourly_delta_task()

        assert len(results) == 1
        # Verify priority="high" was passed
        call_kwargs = mock_refresh.call_args.kwargs
        assert call_kwargs["priority"] == "high"
        assert call_kwargs["since"] is not None
