"""Tests for usage tracking and reporting (CEX-40)."""

from __future__ import annotations

from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel

from app.shared.usage import UsageEvent, record_usage_event
from app.usage.router import UsageResponse, UsageSummary


# ---------------------------------------------------------------------------
# record_usage_event
# ---------------------------------------------------------------------------


class TestRecordUsageEvent:
    def test_inserts_event_into_supabase(self):
        supabase = MagicMock()
        event = UsageEvent(
            org_id="org-1",
            user_id="user-1",
            asset_type="ad_copy",
            status="success",
            duration_ms=1200,
            claude_tokens_input=500,
            claude_tokens_output=300,
        )

        record_usage_event(event, supabase)

        supabase.table.assert_called_once_with("usage_events")
        insert_call = supabase.table().insert
        insert_call.assert_called_once()
        inserted = insert_call.call_args[0][0]
        assert inserted["org_id"] == "org-1"
        assert inserted["asset_type"] == "ad_copy"
        assert inserted["status"] == "success"
        assert inserted["duration_ms"] == 1200

    def test_swallows_exceptions(self):
        """Tracking failures must never break the caller."""
        supabase = MagicMock()
        supabase.table.side_effect = Exception("DB connection lost")

        event = UsageEvent(
            org_id="org-1",
            user_id="user-1",
            asset_type="lead_magnet",
            status="success",
        )

        # Should not raise
        record_usage_event(event, supabase)

    def test_includes_optional_fields(self):
        supabase = MagicMock()
        event = UsageEvent(
            org_id="org-1",
            user_id="user-1",
            asset_type="video_script",
            status="failed",
            duration_ms=5000,
            claude_tokens_input=1000,
            claude_tokens_output=0,
            provider_costs={"model": "claude-sonnet-4-20250514", "input_cost": 0.003},
            request_id="req_abc123",
        )

        record_usage_event(event, supabase)

        inserted = supabase.table().insert.call_args[0][0]
        assert inserted["provider_costs"] == {"model": "claude-sonnet-4-20250514", "input_cost": 0.003}
        assert inserted["request_id"] == "req_abc123"

    def test_defaults_optional_fields_to_none(self):
        event = UsageEvent(
            org_id="org-1",
            user_id="user-1",
            asset_type="ad_copy",
            status="success",
        )
        assert event.duration_ms is None
        assert event.claude_tokens_input is None
        assert event.claude_tokens_output is None
        assert event.provider_costs is None
        assert event.request_id is None


# ---------------------------------------------------------------------------
# UsageResponse model
# ---------------------------------------------------------------------------


class TestUsageResponseModel:
    def test_summary_computes_correctly(self):
        summary = UsageSummary(
            total_generations=10,
            successful=8,
            failed=2,
            total_tokens_input=5000,
            total_tokens_output=3000,
        )
        assert summary.total_generations == 10
        assert summary.successful == 8
        assert summary.failed == 2

    def test_response_structure(self):
        resp = UsageResponse(
            summary=UsageSummary(
                total_generations=1,
                successful=1,
                failed=0,
                total_tokens_input=100,
                total_tokens_output=50,
            ),
            events=[],
        )
        data = resp.model_dump()
        assert "summary" in data
        assert "events" in data


# ---------------------------------------------------------------------------
# Integration: usage recording in generation service
# ---------------------------------------------------------------------------


class TestUsageIntegration:
    def test_usage_event_model_serialization(self):
        """UsageEvent.model_dump() produces a flat dict suitable for Supabase insert."""
        event = UsageEvent(
            org_id="org-1",
            user_id="user-1",
            asset_type="landing_page",
            status="success",
            duration_ms=2500,
        )
        d = event.model_dump()
        assert isinstance(d, dict)
        assert d["org_id"] == "org-1"
        assert d["duration_ms"] == 2500
        # provider_costs should be None, not missing
        assert "provider_costs" in d

    def test_failed_event_tracked(self):
        """Failed generations should also create usage events."""
        supabase = MagicMock()
        event = UsageEvent(
            org_id="org-1",
            user_id="user-1",
            asset_type="document_ad",
            status="failed",
            duration_ms=100,
        )

        record_usage_event(event, supabase)

        inserted = supabase.table().insert.call_args[0][0]
        assert inserted["status"] == "failed"
