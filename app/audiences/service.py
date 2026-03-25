"""Audience segment service layer (BJC-81).

Supabase for segment metadata, ClickHouse for member data.
All operations are tenant-scoped via organization_id.
"""

import csv
import io
import logging
from datetime import UTC, datetime

from clickhouse_connect.driver import Client as CHClient
from supabase import Client as SupabaseClient

from app.shared.errors import NotFoundError

logger = logging.getLogger(__name__)


def get_segment_or_404(supabase: SupabaseClient, segment_id: str, org_id: str) -> dict:
    """Fetch a single audience segment scoped to tenant, raise 404 if missing."""
    res = (
        supabase.table("audience_segments")
        .select("*")
        .eq("id", segment_id)
        .eq("organization_id", org_id)
        .maybe_single()
        .execute()
    )
    if not res.data:
        raise NotFoundError(detail="Audience segment not found")
    return res.data


def list_segments(
    supabase: SupabaseClient,
    org_id: str,
    *,
    status: str | None = None,
    include_archived: bool = False,
    limit: int = 20,
    offset: int = 0,
) -> tuple[list[dict], int]:
    """List audience segments for a tenant."""
    query = (
        supabase.table("audience_segments")
        .select("*", count="exact")
        .eq("organization_id", org_id)
    )
    if not include_archived:
        query = query.is_("archived_at", "null")
    if status:
        query = query.eq("status", status)
    query = query.order("created_at", desc=True).range(offset, offset + limit - 1)
    res = query.execute()
    return res.data, res.count or 0


def create_segment(
    supabase: SupabaseClient,
    org_id: str,
    data: dict,
) -> dict:
    """Create a new audience segment."""
    data["organization_id"] = org_id
    data["status"] = "active"
    data["member_count"] = 0
    res = supabase.table("audience_segments").insert(data).execute()
    return res.data[0]


def update_segment(
    supabase: SupabaseClient,
    segment_id: str,
    org_id: str,
    data: dict,
) -> dict:
    """Update an existing audience segment."""
    res = (
        supabase.table("audience_segments")
        .update(data)
        .eq("id", segment_id)
        .eq("organization_id", org_id)
        .execute()
    )
    return res.data[0]


def archive_segment(
    supabase: SupabaseClient,
    segment_id: str,
    org_id: str,
) -> None:
    """Soft-delete (archive) an audience segment."""
    supabase.table("audience_segments").update(
        {"archived_at": datetime.now(UTC).isoformat(), "status": "archived"}
    ).eq("id", segment_id).eq("organization_id", org_id).execute()


def get_segment_members(
    clickhouse: CHClient,
    segment_id: str,
    tenant_id: str,
    *,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[dict], int]:
    """Paginated member listing from ClickHouse."""
    count_result = clickhouse.query(
        "SELECT count() as cnt FROM audience_segment_members "
        "WHERE tenant_id = {tenant_id:String} AND segment_id = {segment_id:String}",
        parameters={"tenant_id": tenant_id, "segment_id": segment_id},
    )
    total = count_result.first_row[0] if count_result.result_rows else 0

    result = clickhouse.query(
        "SELECT entity_id, entity_type, full_name, work_email, title, "
        "company_name, linkedin_url, added_at "
        "FROM audience_segment_members "
        "WHERE tenant_id = {tenant_id:String} AND segment_id = {segment_id:String} "
        "ORDER BY added_at DESC "
        "LIMIT {limit:UInt32} OFFSET {offset:UInt32}",
        parameters={
            "tenant_id": tenant_id,
            "segment_id": segment_id,
            "limit": limit,
            "offset": offset,
        },
    )
    columns = result.column_names
    members = [dict(zip(columns, row)) for row in result.result_rows]
    return members, total


def export_members_csv(
    clickhouse: CHClient,
    segment_id: str,
    tenant_id: str,
    platform: str,
) -> str:
    """Export segment members as CSV formatted for a specific ad platform."""
    result = clickhouse.query(
        "SELECT entity_id, full_name, work_email, title, company_name, linkedin_url "
        "FROM audience_segment_members "
        "WHERE tenant_id = {tenant_id:String} AND segment_id = {segment_id:String} "
        "ORDER BY added_at DESC",
        parameters={"tenant_id": tenant_id, "segment_id": segment_id},
    )
    columns = result.column_names
    rows = [dict(zip(columns, row)) for row in result.result_rows]

    output = io.StringIO()

    if platform == "linkedin":
        writer = csv.DictWriter(output, fieldnames=["email", "firstName", "lastName", "companyName", "title"])
        writer.writeheader()
        for row in rows:
            name_parts = (row.get("full_name") or "").split(" ", 1)
            writer.writerow({
                "email": row.get("work_email", ""),
                "firstName": name_parts[0] if name_parts else "",
                "lastName": name_parts[1] if len(name_parts) > 1 else "",
                "companyName": row.get("company_name", ""),
                "title": row.get("title", ""),
            })
    elif platform == "meta":
        writer = csv.DictWriter(output, fieldnames=["email", "fn", "ln"])
        writer.writeheader()
        for row in rows:
            name_parts = (row.get("full_name") or "").split(" ", 1)
            writer.writerow({
                "email": row.get("work_email", ""),
                "fn": name_parts[0] if name_parts else "",
                "ln": name_parts[1] if len(name_parts) > 1 else "",
            })
    elif platform == "google":
        writer = csv.DictWriter(output, fieldnames=["Email", "First Name", "Last Name"])
        writer.writeheader()
        for row in rows:
            name_parts = (row.get("full_name") or "").split(" ", 1)
            writer.writerow({
                "Email": row.get("work_email", ""),
                "First Name": name_parts[0] if name_parts else "",
                "Last Name": name_parts[1] if len(name_parts) > 1 else "",
            })
    else:
        # Generic CSV
        writer = csv.DictWriter(output, fieldnames=["entity_id", "full_name", "work_email", "title", "company_name", "linkedin_url"])
        writer.writeheader()
        writer.writerows(rows)

    return output.getvalue()


def get_signal_cards(
    supabase: SupabaseClient,
    org_id: str,
) -> list[dict]:
    """Return active segments with counts and trends for signal dashboard."""
    res = (
        supabase.table("audience_segments")
        .select("id, name, filter_config, member_count, last_refreshed_at, status")
        .eq("organization_id", org_id)
        .eq("status", "active")
        .is_("archived_at", "null")
        .order("member_count", desc=True)
        .execute()
    )
    cards = []
    for seg in res.data:
        filter_config = seg.get("filter_config") or {}
        signal_type = filter_config.get("signal_type", "custom")
        cards.append({
            "segment_id": seg["id"],
            "segment_name": seg["name"],
            "signal_type": signal_type,
            "member_count": seg.get("member_count", 0),
            "trend": "stable",
            "last_refreshed_at": seg.get("last_refreshed_at"),
        })
    return cards
