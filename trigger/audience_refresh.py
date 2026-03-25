"""Audience segment refresh Trigger.dev tasks (BJC-85).

Scheduled: daily (0 6 * * *) for full refresh, hourly (0 * * * *) for high-priority delta.

Refreshes audience segments by pulling updated signal/enrichment data from
data-engine-x. PaidEdge does not call enrichment providers directly — all
data flows through data-engine-x's API.

Fan-out: all tenants → per tenant → per segment.
"""

import logging
import time
from datetime import UTC, datetime, timedelta

from app.db.clickhouse import get_clickhouse_client
from app.db.supabase import get_supabase_client
from app.integrations.data_engine_x import DataEngineXClient

logger = logging.getLogger(__name__)


async def get_active_tenants(supabase) -> list[str]:
    """Find all distinct organization_ids with active audience segments."""
    res = (
        supabase.table("audience_segments")
        .select("organization_id")
        .eq("status", "active")
        .is_("archived_at", "null")
        .execute()
    )
    # Deduplicate org IDs
    seen: set[str] = set()
    orgs: list[str] = []
    for row in res.data or []:
        org_id = row["organization_id"]
        if org_id not in seen:
            seen.add(org_id)
            orgs.append(org_id)
    return orgs


async def get_tenant_segments(
    supabase,
    org_id: str,
    *,
    priority: str | None = None,
) -> list[dict]:
    """Get active segments for a tenant, optionally filtered by priority."""
    query = (
        supabase.table("audience_segments")
        .select("id, name, filter_config, priority, member_count, last_refreshed_at")
        .eq("organization_id", org_id)
        .eq("status", "active")
        .is_("archived_at", "null")
    )
    if priority:
        query = query.eq("priority", priority)
    res = query.execute()
    return res.data or []


async def refresh_segment(
    segment: dict,
    org_id: str,
    dex_client: DataEngineXClient,
    supabase,
    clickhouse,
    *,
    since: datetime | None = None,
) -> dict:
    """Refresh a single audience segment by querying data-engine-x.

    1. Read segment filter_config to determine what signals/entities to fetch
    2. Call data-engine-x search_entities / get_signals
    3. Write updated members to ClickHouse audience_segment_members
    4. Update member_count + last_refreshed_at on Supabase segment row

    Returns structured result dict.
    """
    segment_id = segment["id"]
    segment_name = segment["name"]
    filter_config = segment.get("filter_config") or {}
    start_ms = time.monotonic_ns() // 1_000_000

    # Determine what to fetch based on filter_config
    signal_type = filter_config.get("signal_type")
    entity_type = filter_config.get("entity_type", "company")
    filters = {k: v for k, v in filter_config.items() if k not in ("signal_type", "entity_type")}

    members: list[dict] = []

    if signal_type:
        # Fetch signal-based members
        signals = await dex_client.get_signals(
            signal_type=signal_type,
            since=since,
        )
        for sig in signals:
            members.append({
                "tenant_id": org_id,
                "segment_id": segment_id,
                "entity_id": sig.entity_id,
                "entity_type": sig.entity_type,
                "full_name": sig.details.get("full_name"),
                "work_email": sig.details.get("work_email"),
                "title": sig.details.get("title"),
                "company_name": sig.details.get("company_name"),
                "linkedin_url": sig.details.get("linkedin_url"),
                "added_at": datetime.now(UTC).isoformat(),
            })
    else:
        # Fetch entity-based members via search
        result = await dex_client.search_entities(
            entity_type=entity_type,
            filters=filters,
            per_page=100,
        )
        for item in result.items:
            members.append({
                "tenant_id": org_id,
                "segment_id": segment_id,
                "entity_id": item.get("entity_id", ""),
                "entity_type": entity_type,
                "full_name": item.get("full_name") or item.get("canonical_name"),
                "work_email": item.get("work_email"),
                "title": item.get("title"),
                "company_name": item.get("company_name") or item.get("canonical_name"),
                "linkedin_url": item.get("linkedin_url"),
                "added_at": datetime.now(UTC).isoformat(),
            })

    # Write to ClickHouse
    rows_inserted = 0
    if members:
        columns = [
            "tenant_id", "segment_id", "entity_id", "entity_type",
            "full_name", "work_email", "title", "company_name",
            "linkedin_url", "added_at",
        ]
        data = [[m.get(c, "") or "" for c in columns] for m in members]
        clickhouse.insert(
            "audience_segment_members",
            data,
            column_names=columns,
        )
        rows_inserted = len(data)

    # Update Supabase segment metadata
    now_iso = datetime.now(UTC).isoformat()
    supabase.table("audience_segments").update({
        "member_count": rows_inserted,
        "last_refreshed_at": now_iso,
    }).eq("id", segment_id).eq("organization_id", org_id).execute()

    duration_ms = time.monotonic_ns() // 1_000_000 - start_ms
    return {
        "task": "audience_refresh",
        "tenant_id": org_id,
        "segment_id": segment_id,
        "segment_name": segment_name,
        "members_written": rows_inserted,
        "duration_ms": duration_ms,
        "status": "success",
    }


async def refresh_tenant_segments(
    org_id: str,
    supabase,
    clickhouse,
    dex_client: DataEngineXClient,
    *,
    priority: str | None = None,
    since: datetime | None = None,
) -> list[dict]:
    """Refresh all active segments for a single tenant.

    Per-segment error isolation: one segment failure doesn't stop others.
    """
    segments = await get_tenant_segments(supabase, org_id, priority=priority)
    if not segments:
        return [{
            "task": "audience_refresh",
            "tenant_id": org_id,
            "status": "skipped_no_segments",
        }]

    results = []
    for segment in segments:
        try:
            result = await refresh_segment(
                segment=segment,
                org_id=org_id,
                dex_client=dex_client,
                supabase=supabase,
                clickhouse=clickhouse,
                since=since,
            )
            results.append(result)
            logger.info(
                "Segment refresh complete: %s/%s — %d members",
                org_id, segment["id"], result["members_written"],
            )
        except Exception:
            logger.exception(
                "Segment refresh failed for %s/%s",
                org_id, segment["id"],
            )
            results.append({
                "task": "audience_refresh",
                "tenant_id": org_id,
                "segment_id": segment["id"],
                "segment_name": segment.get("name"),
                "status": "error",
            })

    return results


async def audience_daily_refresh_task():
    """Daily full refresh of all active audience segments across all tenants.

    Scheduled: 0 6 * * * (daily at 6 AM UTC)

    Fan-out: all tenants → per tenant → per segment.
    Per-tenant isolation: one tenant failure doesn't stop others.
    """
    supabase = get_supabase_client()
    clickhouse = get_clickhouse_client()

    tenants = await get_active_tenants(supabase)
    logger.info(
        "Audience daily refresh starting for %d tenants", len(tenants),
    )

    all_results = []
    async with DataEngineXClient() as dex_client:
        for org_id in tenants:
            try:
                results = await refresh_tenant_segments(
                    org_id=org_id,
                    supabase=supabase,
                    clickhouse=clickhouse,
                    dex_client=dex_client,
                )
                all_results.extend(results)
            except Exception:
                logger.exception(
                    "Audience refresh failed for tenant %s", org_id,
                )
                all_results.append({
                    "task": "audience_daily_refresh",
                    "tenant_id": org_id,
                    "status": "error",
                })

    total_members = sum(r.get("members_written", 0) for r in all_results)
    logger.info(
        "Audience daily refresh finished: %d tenants, %d total members written",
        len(tenants), total_members,
    )
    return all_results


async def audience_hourly_delta_task():
    """Hourly delta refresh for high-priority audience segments.

    Scheduled: 0 * * * * (every hour)

    Only refreshes segments with priority='high'.
    Only checks for new signals since last refresh to minimize API calls.
    """
    supabase = get_supabase_client()
    clickhouse = get_clickhouse_client()
    since = datetime.now(UTC) - timedelta(hours=1)

    tenants = await get_active_tenants(supabase)
    logger.info(
        "Audience hourly delta starting for %d tenants (since %s)",
        len(tenants), since.isoformat(),
    )

    all_results = []
    async with DataEngineXClient() as dex_client:
        for org_id in tenants:
            try:
                results = await refresh_tenant_segments(
                    org_id=org_id,
                    supabase=supabase,
                    clickhouse=clickhouse,
                    dex_client=dex_client,
                    priority="high",
                    since=since,
                )
                all_results.extend(results)
            except Exception:
                logger.exception(
                    "Audience hourly delta failed for tenant %s", org_id,
                )
                all_results.append({
                    "task": "audience_hourly_delta",
                    "tenant_id": org_id,
                    "status": "error",
                })

    total_members = sum(r.get("members_written", 0) for r in all_results)
    logger.info(
        "Audience hourly delta finished: %d tenants, %d total members written",
        len(tenants), total_members,
    )
    return all_results
