"""Audience CSV export per ad platform format (BJC-61).

V1 audience push is CSV export formatted for manual upload to ad platforms.
PaidEdge's job is formatting — Prospeo/email resolution lives in data-engine-x.

Platform-specific CSV formats:
- LinkedIn: first_name, last_name, email, company_name, job_title
- Meta: sha256_email, sha256_phone, first_name, last_name  (Meta requires pre-hashing)
- Google: email, phone, first_name, last_name, country, zip
"""

from __future__ import annotations

import csv
import hashlib
import io
import logging
from datetime import UTC, datetime
from typing import Any

from clickhouse_connect.driver import Client as CHClient
from supabase import Client as SupabaseClient

from app.shared.errors import BadRequestError, NotFoundError

logger = logging.getLogger(__name__)

# Platform column definitions
PLATFORM_COLUMNS: dict[str, list[str]] = {
    "linkedin": ["first_name", "last_name", "email", "company_name", "job_title"],
    "meta": ["sha256_email", "sha256_phone", "first_name", "last_name"],
    "google": ["email", "phone", "first_name", "last_name", "country", "zip"],
}

VALID_PLATFORMS = set(PLATFORM_COLUMNS.keys())


def _sha256(value: str) -> str:
    """SHA-256 hash a value (lowercase, stripped). Returns empty string if empty."""
    if not value or not value.strip():
        return ""
    return hashlib.sha256(value.strip().lower().encode("utf-8")).hexdigest()


class AudienceExportService:
    """Formats segment member data into platform-specific CSV files."""

    def __init__(
        self,
        supabase: SupabaseClient,
        clickhouse: CHClient,
    ):
        self.supabase = supabase
        self.clickhouse = clickhouse

    def export_segment(
        self,
        segment_id: str,
        tenant_id: str,
        platform: str,
    ) -> tuple[str, bytes, int]:
        """Export an audience segment as a platform-specific CSV.

        Returns:
            Tuple of (filename, csv_bytes, row_count)
        """
        if platform not in VALID_PLATFORMS:
            raise BadRequestError(
                detail=f"Unsupported platform '{platform}'. "
                f"Valid: {sorted(VALID_PLATFORMS)}"
            )

        # Verify segment exists and belongs to tenant
        segment = self._get_segment_or_404(segment_id, tenant_id)

        # Pull segment members
        members = self._fetch_segment_members(segment_id, tenant_id)

        if not members:
            raise BadRequestError(
                detail="Segment has no members. "
                "Add contacts to this audience segment before exporting."
            )

        # Format for platform
        columns = PLATFORM_COLUMNS[platform]
        rows = [self._format_member(member, platform) for member in members]

        # Generate CSV in memory
        csv_bytes = self._generate_csv(columns, rows)

        # Build filename
        segment_name = segment.get("name", segment_id).replace(" ", "_").lower()
        filename = f"{segment_name}_{platform}_{datetime.now(UTC).strftime('%Y%m%d')}.csv"

        # Record export history
        self._record_export(
            segment_id=segment_id,
            tenant_id=tenant_id,
            platform=platform,
            row_count=len(rows),
            filename=filename,
        )

        return filename, csv_bytes, len(rows)

    # ------------------------------------------------------------------
    # Data access
    # ------------------------------------------------------------------

    def _get_segment_or_404(self, segment_id: str, tenant_id: str) -> dict:
        """Fetch segment metadata from Supabase."""
        res = (
            self.supabase.table("audience_segments")
            .select("*")
            .eq("id", segment_id)
            .eq("organization_id", tenant_id)
            .maybe_single()
            .execute()
        )
        if not res.data:
            raise NotFoundError(detail="Audience segment not found")
        return res.data

    def _fetch_segment_members(
        self, segment_id: str, tenant_id: str
    ) -> list[dict[str, Any]]:
        """Pull segment members from ClickHouse joined with entity data."""
        query = """
            SELECT
                m.entity_id,
                m.first_name,
                m.last_name,
                m.email,
                m.phone,
                m.company_name,
                m.job_title,
                m.country,
                m.zip_code
            FROM audience_segment_members m
            WHERE m.segment_id = {segment_id:String}
              AND m.tenant_id = {tenant_id:String}
            ORDER BY m.email ASC
        """
        try:
            result = self.clickhouse.query(
                query,
                parameters={"segment_id": segment_id, "tenant_id": tenant_id},
            )
            columns = result.column_names
            return [dict(zip(columns, row)) for row in result.result_rows]
        except Exception:
            logger.exception("ClickHouse query failed for segment %s", segment_id)
            return []

    # ------------------------------------------------------------------
    # Formatting
    # ------------------------------------------------------------------

    def _format_member(self, member: dict, platform: str) -> list[str]:
        """Format a single member row for the target platform."""
        if platform == "linkedin":
            return [
                member.get("first_name", ""),
                member.get("last_name", ""),
                member.get("email", ""),
                member.get("company_name", ""),
                member.get("job_title", ""),
            ]

        if platform == "meta":
            # Meta requires SHA-256 hashed email and phone
            return [
                _sha256(member.get("email", "")),
                _sha256(member.get("phone", "")),
                member.get("first_name", ""),
                member.get("last_name", ""),
            ]

        if platform == "google":
            return [
                member.get("email", ""),
                member.get("phone", ""),
                member.get("first_name", ""),
                member.get("last_name", ""),
                member.get("country", ""),
                member.get("zip_code", ""),
            ]

        raise BadRequestError(detail=f"No formatter for platform '{platform}'")

    # ------------------------------------------------------------------
    # CSV generation
    # ------------------------------------------------------------------

    @staticmethod
    def _generate_csv(columns: list[str], rows: list[list[str]]) -> bytes:
        """Generate a CSV file in memory."""
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(columns)
        writer.writerows(rows)
        return output.getvalue().encode("utf-8")

    # ------------------------------------------------------------------
    # Export history
    # ------------------------------------------------------------------

    def _record_export(
        self,
        segment_id: str,
        tenant_id: str,
        platform: str,
        row_count: int,
        filename: str,
    ) -> None:
        """Record export to Supabase for audit trail."""
        try:
            self.supabase.table("audience_export_history").insert({
                "segment_id": segment_id,
                "organization_id": tenant_id,
                "platform": platform,
                "row_count": row_count,
                "filename": filename,
                "exported_at": datetime.now(UTC).isoformat(),
            }).execute()
        except Exception:
            logger.exception("Failed to record export history")
