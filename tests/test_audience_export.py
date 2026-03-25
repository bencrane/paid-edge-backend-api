"""Tests for audience CSV export per ad platform format (BJC-61).

Covers: LinkedIn/Meta/Google CSV formats, SHA-256 hashing for Meta,
empty segment handling, export history tracking, and invalid platform error.
"""

from __future__ import annotations

import csv
import hashlib
import io
from unittest.mock import MagicMock

import pytest

from app.audiences.export import (
    PLATFORM_COLUMNS,
    VALID_PLATFORMS,
    AudienceExportService,
    _sha256,
)
from app.shared.errors import BadRequestError, NotFoundError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SAMPLE_MEMBERS = [
    {
        "entity_id": "e1",
        "first_name": "Alice",
        "last_name": "Smith",
        "email": "alice@example.com",
        "phone": "+15551234567",
        "company_name": "Acme Corp",
        "job_title": "VP Engineering",
        "country": "US",
        "zip_code": "94105",
    },
    {
        "entity_id": "e2",
        "first_name": "Bob",
        "last_name": "Jones",
        "email": "bob@example.com",
        "phone": "+15559876543",
        "company_name": "TechCo",
        "job_title": "CISO",
        "country": "US",
        "zip_code": "10001",
    },
]


def _mock_supabase(segment_data: dict | None = None) -> MagicMock:
    mock = MagicMock()

    def _table(name: str):
        chain = MagicMock()
        chain.select.return_value = chain
        chain.insert.return_value = chain
        chain.eq.return_value = chain
        chain.maybe_single.return_value = chain

        if name == "audience_segments" and segment_data is not None:
            res = MagicMock()
            res.data = segment_data
            chain.execute.return_value = res
        elif name == "audience_export_history":
            res = MagicMock()
            res.data = [{}]
            chain.execute.return_value = res
        else:
            res = MagicMock()
            res.data = segment_data
            chain.execute.return_value = res
        return chain

    mock.table.side_effect = _table
    return mock


def _mock_clickhouse(members: list[dict] | None = None) -> MagicMock:
    mock = MagicMock()
    if members:
        columns = [
            "entity_id", "first_name", "last_name", "email",
            "phone", "company_name", "job_title", "country", "zip_code",
        ]
        rows = [
            [m.get(c, "") for c in columns]
            for m in members
        ]
        result = MagicMock()
        result.column_names = columns
        result.result_rows = rows
        mock.query.return_value = result
    else:
        result = MagicMock()
        result.column_names = []
        result.result_rows = []
        mock.query.return_value = result
    return mock


def _parse_csv(csv_bytes: bytes) -> tuple[list[str], list[list[str]]]:
    """Parse CSV bytes into (headers, rows)."""
    reader = csv.reader(io.StringIO(csv_bytes.decode("utf-8")))
    rows = list(reader)
    return rows[0], rows[1:]


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


class TestConstants:
    def test_valid_platforms(self):
        assert VALID_PLATFORMS == {"linkedin", "meta", "google"}

    def test_linkedin_columns(self):
        assert PLATFORM_COLUMNS["linkedin"] == [
            "first_name", "last_name", "email", "company_name", "job_title",
        ]

    def test_meta_columns(self):
        assert PLATFORM_COLUMNS["meta"] == [
            "sha256_email", "sha256_phone", "first_name", "last_name",
        ]

    def test_google_columns(self):
        assert PLATFORM_COLUMNS["google"] == [
            "email", "phone", "first_name", "last_name", "country", "zip",
        ]


# ---------------------------------------------------------------------------
# SHA-256 hashing
# ---------------------------------------------------------------------------


class TestSHA256:
    def test_hashes_correctly(self):
        expected = hashlib.sha256(b"alice@example.com").hexdigest()
        assert _sha256("alice@example.com") == expected

    def test_lowercases_before_hashing(self):
        assert _sha256("Alice@Example.COM") == _sha256("alice@example.com")

    def test_strips_whitespace(self):
        assert _sha256("  alice@example.com  ") == _sha256("alice@example.com")

    def test_empty_returns_empty(self):
        assert _sha256("") == ""
        assert _sha256("  ") == ""

    def test_none_like_returns_empty(self):
        assert _sha256("") == ""


# ---------------------------------------------------------------------------
# LinkedIn export
# ---------------------------------------------------------------------------


class TestLinkedInExport:
    def test_correct_format(self):
        segment = {"id": "seg-1", "name": "Q1 Prospects", "organization_id": "org-1"}
        service = AudienceExportService(
            supabase=_mock_supabase(segment),
            clickhouse=_mock_clickhouse(SAMPLE_MEMBERS),
        )
        filename, csv_bytes, count = service.export_segment("seg-1", "org-1", "linkedin")

        headers, rows = _parse_csv(csv_bytes)
        assert headers == ["first_name", "last_name", "email", "company_name", "job_title"]
        assert count == 2
        assert rows[0] == ["Alice", "Smith", "alice@example.com", "Acme Corp", "VP Engineering"]
        assert rows[1] == ["Bob", "Jones", "bob@example.com", "TechCo", "CISO"]
        assert "q1_prospects_linkedin_" in filename
        assert filename.endswith(".csv")


# ---------------------------------------------------------------------------
# Meta export (SHA-256 hashing)
# ---------------------------------------------------------------------------


class TestMetaExport:
    def test_hashes_email_and_phone(self):
        segment = {"id": "seg-1", "name": "Meta Segment", "organization_id": "org-1"}
        service = AudienceExportService(
            supabase=_mock_supabase(segment),
            clickhouse=_mock_clickhouse(SAMPLE_MEMBERS),
        )
        filename, csv_bytes, count = service.export_segment("seg-1", "org-1", "meta")

        headers, rows = _parse_csv(csv_bytes)
        assert headers == ["sha256_email", "sha256_phone", "first_name", "last_name"]
        assert count == 2

        # Verify hashing
        expected_email_hash = hashlib.sha256(b"alice@example.com").hexdigest()
        expected_phone_hash = hashlib.sha256(b"+15551234567").hexdigest()
        assert rows[0][0] == expected_email_hash
        assert rows[0][1] == expected_phone_hash
        assert rows[0][2] == "Alice"
        assert rows[0][3] == "Smith"


# ---------------------------------------------------------------------------
# Google export
# ---------------------------------------------------------------------------


class TestGoogleExport:
    def test_includes_country_and_zip(self):
        segment = {"id": "seg-1", "name": "Google Segment", "organization_id": "org-1"}
        service = AudienceExportService(
            supabase=_mock_supabase(segment),
            clickhouse=_mock_clickhouse(SAMPLE_MEMBERS),
        )
        filename, csv_bytes, count = service.export_segment("seg-1", "org-1", "google")

        headers, rows = _parse_csv(csv_bytes)
        assert headers == ["email", "phone", "first_name", "last_name", "country", "zip"]
        assert count == 2
        assert rows[0] == [
            "alice@example.com", "+15551234567", "Alice", "Smith", "US", "94105",
        ]


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


class TestExportErrors:
    def test_invalid_platform_raises(self):
        segment = {"id": "seg-1", "name": "Test", "organization_id": "org-1"}
        service = AudienceExportService(
            supabase=_mock_supabase(segment),
            clickhouse=_mock_clickhouse([]),
        )
        with pytest.raises(BadRequestError, match="Unsupported platform"):
            service.export_segment("seg-1", "org-1", "tiktok")

    def test_segment_not_found_raises(self):
        service = AudienceExportService(
            supabase=_mock_supabase(None),
            clickhouse=_mock_clickhouse([]),
        )
        with pytest.raises(NotFoundError, match="Audience segment not found"):
            service.export_segment("missing", "org-1", "linkedin")

    def test_empty_segment_raises(self):
        segment = {"id": "seg-1", "name": "Empty", "organization_id": "org-1"}
        service = AudienceExportService(
            supabase=_mock_supabase(segment),
            clickhouse=_mock_clickhouse([]),
        )
        with pytest.raises(BadRequestError, match="no members"):
            service.export_segment("seg-1", "org-1", "linkedin")


# ---------------------------------------------------------------------------
# Export history
# ---------------------------------------------------------------------------


class TestExportHistory:
    def test_records_export(self):
        segment = {"id": "seg-1", "name": "Tracked", "organization_id": "org-1"}
        sb = _mock_supabase(segment)
        service = AudienceExportService(
            supabase=sb,
            clickhouse=_mock_clickhouse(SAMPLE_MEMBERS),
        )
        service.export_segment("seg-1", "org-1", "linkedin")

        # Verify insert was called on audience_export_history
        calls = sb.table.call_args_list
        table_names = [c.args[0] for c in calls]
        assert "audience_export_history" in table_names
