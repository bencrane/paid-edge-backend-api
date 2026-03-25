"""Tests for Meta Insights + reporting client + metrics mapping (BJC-162)."""

from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.integrations.meta_metrics import (
    build_meta_campaign_id_map,
    insert_meta_metrics,
    map_meta_insights_to_campaign_metrics,
    parse_actions,
)


class TestParseActions:
    def test_basic_actions(self):
        actions = [
            {"action_type": "link_click", "value": "150"},
            {"action_type": "lead", "value": "23"},
            {"action_type": "landing_page_view", "value": "120"},
        ]
        result = parse_actions(actions)
        assert result["link_clicks"] == 150
        assert result["leads"] == 23
        assert result["landing_page_views"] == 120

    def test_offsite_conversions(self):
        actions = [
            {"action_type": "offsite_conversion.fb_pixel_lead", "value": "18"},
            {"action_type": "offsite_conversion.fb_pixel_purchase", "value": "5"},
        ]
        result = parse_actions(actions)
        assert result["conversions"] == 23

    def test_empty_actions(self):
        assert parse_actions(None) == {}
        assert parse_actions([]) == {}


class TestMapMetaInsights:
    def test_basic_mapping(self):
        raw_rows = [
            {
                "campaign_id": "meta_c1",
                "date_start": "2026-03-25",
                "impressions": "10000",
                "clicks": "150",
                "spend": "45.50",
                "ctr": "1.5",
                "cpc": "0.30",
                "cpm": "4.55",
                "actions": [
                    {"action_type": "lead", "value": "10"},
                    {"action_type": "link_click", "value": "150"},
                ],
            }
        ]
        campaign_id_map = {"meta_c1": "pe-uuid-1"}
        result = map_meta_insights_to_campaign_metrics(
            raw_rows, "tenant-1", campaign_id_map
        )
        assert len(result) == 1
        m = result[0]
        assert m["tenant_id"] == "tenant-1"
        assert m["campaign_id"] == "pe-uuid-1"
        assert m["platform"] == "meta"
        assert m["spend"] == Decimal("45.50")
        assert m["impressions"] == 10000
        assert m["clicks"] == 150
        assert m["leads"] == 10

    def test_unmapped_campaigns_skipped(self):
        raw_rows = [{"campaign_id": "unknown_campaign", "date_start": "2026-03-25"}]
        result = map_meta_insights_to_campaign_metrics(
            raw_rows, "t1", {}
        )
        assert len(result) == 0


class TestBuildCampaignIdMap:
    @pytest.mark.asyncio
    async def test_build_map(self):
        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.eq.return_value.execute.return_value = MagicMock(
            data=[
                {"id": "pe-1", "platform_data": {"platform": "meta", "platform_campaign_id": "meta_1"}},
                {"id": "pe-2", "platform_data": {"platform": "linkedin", "platform_campaign_id": "li_1"}},
                {"id": "pe-3", "platform_data": {"platform": "meta", "platform_campaign_id": "meta_3"}},
            ]
        )
        result = await build_meta_campaign_id_map(mock_supabase, "tenant-1")
        assert result["meta_1"] == "pe-1"
        assert result["meta_3"] == "pe-3"
        assert "li_1" not in result

    @pytest.mark.asyncio
    async def test_empty_campaigns(self):
        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.eq.return_value.execute.return_value = MagicMock(data=[])
        result = await build_meta_campaign_id_map(mock_supabase, "t1")
        assert result == {}


class TestInsertMetaMetrics:
    @pytest.mark.asyncio
    async def test_insert(self):
        mock_ch = MagicMock()
        metrics = [
            {
                "tenant_id": "t1", "campaign_id": "c1", "platform": "meta",
                "platform_campaign_id": "mc1", "platform_ad_group_id": "",
                "platform_ad_id": "", "date": "2026-03-25",
                "spend": Decimal("10"), "impressions": 1000, "clicks": 50,
                "conversions": 5, "leads": 3, "ctr": 5.0, "cpc": Decimal("0.2"),
                "cpm": Decimal("10"), "roas": Decimal("0"),
            }
        ]
        count = await insert_meta_metrics(mock_ch, metrics)
        assert count == 1
        mock_ch.insert.assert_called_once()

    @pytest.mark.asyncio
    async def test_insert_empty(self):
        mock_ch = MagicMock()
        count = await insert_meta_metrics(mock_ch, [])
        assert count == 0
