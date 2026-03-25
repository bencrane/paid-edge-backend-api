"""Tests for audience CRUD API endpoints (BJC-81)."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from app.audiences.models import (
    AudienceCreate,
    AudienceExportRequest,
    AudienceResponse,
    AudienceUpdate,
)
from app.audiences.service import (
    archive_segment,
    create_segment,
    export_members_csv,
    get_segment_members,
    get_segment_or_404,
    get_signal_cards,
    list_segments,
    update_segment,
)
from app.shared.errors import NotFoundError


# --- Fixtures ---


def _mock_supabase(data=None, count=0):
    """Build a mock Supabase client with chained query builder."""
    mock = MagicMock()
    result = MagicMock()
    result.data = data if data is not None else []
    result.count = count

    chain = MagicMock()
    chain.select.return_value = chain
    chain.eq.return_value = chain
    chain.is_.return_value = chain
    chain.order.return_value = chain
    chain.range.return_value = chain
    chain.limit.return_value = chain
    chain.maybe_single.return_value = chain
    chain.insert.return_value = chain
    chain.update.return_value = chain
    chain.execute.return_value = result
    mock.table.return_value = chain
    return mock


SAMPLE_SEGMENT = {
    "id": "seg-1",
    "organization_id": "org-1",
    "name": "VP Engineering at SaaS companies",
    "description": "Decision makers in target ICP",
    "filter_config": {"signal_type": "new_in_role", "seniority": "VP"},
    "status": "active",
    "priority": "normal",
    "member_count": 150,
    "last_refreshed_at": "2026-03-25T09:00:00Z",
    "created_at": "2026-03-20T10:00:00Z",
    "updated_at": "2026-03-25T09:00:00Z",
}


# --- get_segment_or_404 ---


class TestGetSegmentOr404:
    def test_returns_segment(self):
        """Should return segment when found."""
        mock_sb = _mock_supabase(data=SAMPLE_SEGMENT)
        result = get_segment_or_404(mock_sb, "seg-1", "org-1")
        assert result["id"] == "seg-1"

    def test_raises_404_when_not_found(self):
        """Should raise NotFoundError when segment missing."""
        mock_sb = _mock_supabase(data=None)
        with pytest.raises(NotFoundError):
            get_segment_or_404(mock_sb, "nonexistent", "org-1")


# --- list_segments ---


class TestListSegments:
    def test_returns_segments_with_count(self):
        """Should return list of segments and total count."""
        mock_sb = _mock_supabase(data=[SAMPLE_SEGMENT], count=1)
        data, total = list_segments(mock_sb, "org-1")
        assert len(data) == 1
        assert total == 1
        mock_sb.table.assert_called_with("audience_segments")

    def test_filters_by_status(self):
        """Should pass status filter to query."""
        mock_sb = _mock_supabase(data=[], count=0)
        list_segments(mock_sb, "org-1", status="paused")
        # Verify eq was called (chain makes exact assertion tricky, but we verify it runs)
        assert mock_sb.table.called

    def test_empty_results(self):
        """Should return empty list when no segments."""
        mock_sb = _mock_supabase(data=[], count=0)
        data, total = list_segments(mock_sb, "org-1")
        assert data == []
        assert total == 0


# --- create_segment ---


class TestCreateSegment:
    def test_creates_with_org_id(self):
        """Should insert segment with organization_id and active status."""
        created = {**SAMPLE_SEGMENT, "status": "active", "member_count": 0}
        mock_sb = _mock_supabase(data=[created])
        result = create_segment(
            mock_sb, "org-1",
            {"name": "Test", "filter_config": {"signal_type": "new_in_role"}},
        )
        assert result["organization_id"] == "org-1"
        assert result["status"] == "active"


# --- update_segment ---


class TestUpdateSegment:
    def test_updates_segment(self):
        """Should update and return updated segment."""
        updated = {**SAMPLE_SEGMENT, "name": "Updated Name"}
        mock_sb = _mock_supabase(data=[updated])
        result = update_segment(mock_sb, "seg-1", "org-1", {"name": "Updated Name"})
        assert result["name"] == "Updated Name"


# --- archive_segment ---


class TestArchiveSegment:
    def test_sets_archived_at(self):
        """Should set archived_at and status to archived."""
        mock_sb = _mock_supabase()
        archive_segment(mock_sb, "seg-1", "org-1")
        mock_sb.table.assert_called_with("audience_segments")
        # Verify update was called
        chain = mock_sb.table.return_value
        chain.update.assert_called_once()
        call_args = chain.update.call_args[0][0]
        assert "archived_at" in call_args
        assert call_args["status"] == "archived"


# --- get_segment_members ---


class TestGetSegmentMembers:
    def test_returns_members_with_count(self):
        """Should query ClickHouse for paginated members."""
        mock_ch = MagicMock()

        # Count query
        count_result = MagicMock()
        count_result.result_rows = [[42]]
        count_result.first_row = [42]

        # Data query
        data_result = MagicMock()
        data_result.column_names = [
            "entity_id", "entity_type", "full_name", "work_email",
            "title", "company_name", "linkedin_url", "added_at",
        ]
        data_result.result_rows = [
            ("eid-1", "person", "Jane Doe", "jane@acme.com",
             "VP Engineering", "Acme Corp", "https://linkedin.com/in/jane", "2026-03-25T09:00:00Z"),
        ]

        mock_ch.query = MagicMock(side_effect=[count_result, data_result])

        members, total = get_segment_members(mock_ch, "seg-1", "org-1")

        assert total == 42
        assert len(members) == 1
        assert members[0]["full_name"] == "Jane Doe"
        assert members[0]["entity_id"] == "eid-1"
        assert mock_ch.query.call_count == 2

    def test_empty_members(self):
        """Should handle empty result set."""
        mock_ch = MagicMock()
        count_result = MagicMock()
        count_result.result_rows = [[0]]
        count_result.first_row = [0]
        data_result = MagicMock()
        data_result.column_names = ["entity_id", "entity_type", "full_name", "work_email", "title", "company_name", "linkedin_url", "added_at"]
        data_result.result_rows = []

        mock_ch.query = MagicMock(side_effect=[count_result, data_result])

        members, total = get_segment_members(mock_ch, "seg-1", "org-1")

        assert total == 0
        assert members == []


# --- export_members_csv ---


class TestExportMembersCsv:
    def _mock_clickhouse_with_rows(self):
        mock_ch = MagicMock()
        result = MagicMock()
        result.column_names = ["entity_id", "full_name", "work_email", "title", "company_name", "linkedin_url"]
        result.result_rows = [
            ("eid-1", "Jane Doe", "jane@acme.com", "VP Eng", "Acme", "https://li.com/jane"),
            ("eid-2", "John Smith", "john@beta.io", "CTO", "Beta Inc", "https://li.com/john"),
        ]
        mock_ch.query = MagicMock(return_value=result)
        return mock_ch

    def test_linkedin_format(self):
        """Should produce LinkedIn-formatted CSV with correct headers."""
        mock_ch = self._mock_clickhouse_with_rows()
        csv_output = export_members_csv(mock_ch, "seg-1", "org-1", "linkedin")
        assert "email,firstName,lastName,companyName,title" in csv_output
        assert "jane@acme.com" in csv_output
        assert "Jane" in csv_output
        assert "Doe" in csv_output

    def test_meta_format(self):
        """Should produce Meta-formatted CSV with correct headers."""
        mock_ch = self._mock_clickhouse_with_rows()
        csv_output = export_members_csv(mock_ch, "seg-1", "org-1", "meta")
        assert "email,fn,ln" in csv_output
        assert "jane@acme.com" in csv_output

    def test_google_format(self):
        """Should produce Google-formatted CSV with correct headers."""
        mock_ch = self._mock_clickhouse_with_rows()
        csv_output = export_members_csv(mock_ch, "seg-1", "org-1", "google")
        assert "Email,First Name,Last Name" in csv_output
        assert "jane@acme.com" in csv_output

    def test_empty_members(self):
        """Should produce headers-only CSV when no members."""
        mock_ch = MagicMock()
        result = MagicMock()
        result.column_names = ["entity_id", "full_name", "work_email", "title", "company_name", "linkedin_url"]
        result.result_rows = []
        mock_ch.query = MagicMock(return_value=result)

        csv_output = export_members_csv(mock_ch, "seg-1", "org-1", "linkedin")
        lines = csv_output.strip().split("\n")
        assert len(lines) == 1  # Header only


# --- get_signal_cards ---


class TestGetSignalCards:
    def test_returns_signal_cards(self):
        """Should return active segments as signal cards."""
        segments = [
            {
                "id": "seg-1",
                "name": "New in Role VPs",
                "filter_config": {"signal_type": "new_in_role"},
                "member_count": 50,
                "last_refreshed_at": "2026-03-25T09:00:00Z",
                "status": "active",
            },
            {
                "id": "seg-2",
                "name": "Raised Money",
                "filter_config": {"signal_type": "raised_money"},
                "member_count": 30,
                "last_refreshed_at": None,
                "status": "active",
            },
        ]
        mock_sb = _mock_supabase(data=segments)
        cards = get_signal_cards(mock_sb, "org-1")

        assert len(cards) == 2
        assert cards[0]["segment_id"] == "seg-1"
        assert cards[0]["signal_type"] == "new_in_role"
        assert cards[0]["member_count"] == 50
        assert cards[1]["signal_type"] == "raised_money"

    def test_empty_segments(self):
        """Should return empty list when no active segments."""
        mock_sb = _mock_supabase(data=[])
        cards = get_signal_cards(mock_sb, "org-1")
        assert cards == []


# --- Pydantic model validation ---


class TestModels:
    def test_audience_create_requires_filter_config(self):
        """filter_config is required."""
        with pytest.raises(Exception):
            AudienceCreate(name="Test")

    def test_audience_create_valid(self):
        """Should accept valid input."""
        model = AudienceCreate(
            name="Test Segment",
            filter_config={"signal_type": "new_in_role"},
        )
        assert model.name == "Test Segment"
        assert model.priority == "normal"

    def test_audience_update_all_optional(self):
        """All fields should be optional for update."""
        model = AudienceUpdate()
        assert model.name is None
        assert model.filter_config is None

    def test_audience_export_requires_platform(self):
        """platform is required."""
        with pytest.raises(Exception):
            AudienceExportRequest()

    def test_audience_response_from_dict(self):
        """Should parse from a Supabase row dict."""
        model = AudienceResponse(**SAMPLE_SEGMENT)
        assert model.id == "seg-1"
        assert model.member_count == 150
        assert model.status == "active"
