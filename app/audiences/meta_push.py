"""Meta audience push — segment sync bridge (BJC-161)."""

import logging
from datetime import UTC, datetime

from pydantic import BaseModel

from app.integrations.meta_audiences import hash_for_meta, prepare_audience_data

logger = logging.getLogger(__name__)


class MetaAudienceSyncResult(BaseModel):
    audience_id: str = ""
    audience_name: str = ""
    members_sent: int = 0
    members_matched: int = 0
    members_invalid: int = 0
    status: str = "success"
    errors: list[str] = []


async def sync_segment_to_meta(
    segment_id: str,
    tenant_id: str,
    supabase,
    clickhouse,
    meta_client,
) -> MetaAudienceSyncResult:
    """Sync a PaidEdge audience segment to a Meta Custom Audience.

    Steps:
    1. Load segment from Supabase
    2. Check if Meta audience already exists
    3. Load current segment members from ClickHouse
    4. Determine sync strategy (replace for first sync, diff for subsequent)
    5. Hash member data
    6. Upload in batches
    7. Update segment metadata
    """
    # Step 1: Load segment
    seg_res = (
        supabase.table("audience_segments")
        .select("*")
        .eq("id", segment_id)
        .eq("organization_id", tenant_id)
        .maybe_single()
        .execute()
    )
    if not seg_res.data:
        return MetaAudienceSyncResult(status="error", errors=["Segment not found"])

    segment = seg_res.data
    filter_config = segment.get("filter_config", {})
    meta_push = filter_config.get("meta_push", {})
    meta_audience_id = meta_push.get("audience_id")

    # Step 2: Create audience if needed
    if not meta_audience_id:
        audience_name = f"PaidEdge - {segment.get('name', segment_id)}"
        resp = await meta_client.create_custom_audience(name=audience_name)
        meta_audience_id = resp.get("id", "")
        meta_push["audience_id"] = meta_audience_id
        meta_push["audience_name"] = audience_name

    # Step 3: Load members from ClickHouse
    query = (
        f"SELECT entity_id, email, full_name "
        f"FROM paid_edge.audience_segment_members "
        f"WHERE tenant_id = '{tenant_id}' AND segment_id = '{segment_id}'"
    )
    members_result = clickhouse.query(query)
    members = []
    for row in members_result.result_rows:
        members.append({
            "entity_id": row[0],
            "email": row[1],
            "full_name": row[2] if len(row) > 2 else "",
        })

    if not members:
        return MetaAudienceSyncResult(
            audience_id=meta_audience_id,
            status="skipped",
            errors=["No members in segment"],
        )

    # Step 4: Determine strategy and extract data
    schema = ["EMAIL", "FN", "LN", "EXTERN_ID"]
    processed_members = []
    for m in members:
        full_name = m.get("full_name", "")
        parts = full_name.split(" ", 1) if full_name else ["", ""]
        first_name = parts[0] if parts else ""
        last_name = parts[1] if len(parts) > 1 else ""
        processed_members.append({
            "email": m.get("email", ""),
            "first_name": first_name,
            "last_name": last_name,
            "entity_id": m.get("entity_id", ""),
        })

    hashed_data = prepare_audience_data(processed_members, schema)
    previous_member_ids = set(meta_push.get("previous_member_ids", []))
    current_member_ids = {m.get("entity_id", "") for m in members}

    # Step 5: Upload
    is_first_sync = not previous_member_ids
    if is_first_sync or len(members) < 100_000:
        # Replace for first sync or small audiences
        result = await meta_client.replace_users(
            meta_audience_id, schema, hashed_data
        )
    else:
        # Diff-based sync
        to_add = current_member_ids - previous_member_ids
        to_remove = previous_member_ids - current_member_ids

        result = {"total_received": 0, "total_invalid": 0}

        if to_add:
            add_data = [
                row for row, m in zip(hashed_data, processed_members)
                if m.get("entity_id", "") in to_add
            ]
            if add_data:
                add_result = await meta_client.upload_users(
                    meta_audience_id, schema, add_data
                )
                result["total_received"] += add_result.get("num_received", len(add_data))

        if to_remove:
            remove_data = [
                [hash_for_meta(eid, "EXTERN_ID")]
                for eid in to_remove
            ]
            if remove_data:
                await meta_client.remove_users(
                    meta_audience_id, ["EXTERN_ID"], remove_data
                )

    # Step 6: Update metadata
    meta_push["last_synced_at"] = datetime.now(UTC).isoformat()
    meta_push["last_sync_members_sent"] = len(members)
    meta_push["last_sync_matched"] = result.get("total_received", len(members))
    meta_push["previous_member_ids"] = list(current_member_ids)

    filter_config["meta_push"] = meta_push
    supabase.table("audience_segments").update(
        {"filter_config": filter_config}
    ).eq("id", segment_id).execute()

    return MetaAudienceSyncResult(
        audience_id=meta_audience_id,
        audience_name=meta_push.get("audience_name", ""),
        members_sent=len(members),
        members_matched=result.get("total_received", 0),
        members_invalid=result.get("total_invalid", 0),
    )


async def extract_and_hash_members(
    members: list[dict],
) -> tuple[list[str], list[list[str]]]:
    """Extract member data and hash for Meta upload."""
    schema = ["EMAIL", "FN", "LN", "EXTERN_ID"]
    processed = []
    for m in members:
        full_name = m.get("full_name", "")
        parts = full_name.split(" ", 1) if full_name else ["", ""]
        processed.append({
            "email": m.get("email", ""),
            "first_name": parts[0],
            "last_name": parts[1] if len(parts) > 1 else "",
            "entity_id": m.get("entity_id", ""),
        })
    return schema, prepare_audience_data(processed, schema)


async def compute_member_diff(
    current_members: set[str],
    previous_members: set[str],
) -> tuple[set[str], set[str]]:
    """Returns: (to_add, to_remove)"""
    return current_members - previous_members, previous_members - current_members


async def on_segment_refresh_complete(
    segment_id: str,
    tenant_id: str,
    supabase,
    clickhouse,
    meta_client,
) -> None:
    """Called after audience_refresh task completes for a segment.

    If segment has meta_push configured → trigger sync.
    """
    seg_res = (
        supabase.table("audience_segments")
        .select("filter_config")
        .eq("id", segment_id)
        .eq("organization_id", tenant_id)
        .maybe_single()
        .execute()
    )
    if not seg_res.data:
        return

    filter_config = seg_res.data.get("filter_config", {})
    if "meta_push" in filter_config:
        await sync_segment_to_meta(
            segment_id, tenant_id, supabase, clickhouse, meta_client
        )
