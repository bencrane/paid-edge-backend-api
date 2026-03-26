"""Attribution matching Trigger.dev task (PEX-70).

Scheduled: daily 7am UTC (0 7 * * *)

For each tenant: match CRM contacts → behavioral events → opportunities.
Updates source_campaign_id on crm_opportunities and calculates cost metrics.

Pattern: mirrors trigger/hubspot_crm_sync.py.
"""

import logging
import time

from app.attribution.service import run_attribution_for_tenant
from app.db.clickhouse import get_clickhouse_client
from app.db.supabase import get_supabase_client

logger = logging.getLogger(__name__)


async def get_all_tenants(supabase) -> list[str]:
    """Get all active tenant (organization) IDs.

    Attribution matching should run for every tenant that has
    CRM data in ClickHouse, regardless of which CRM provider.
    """
    res = (
        supabase.table("organizations")
        .select("id")
        .execute()
    )
    return [row["id"] for row in (res.data or [])]


async def attribution_match_task():
    """Daily attribution matching — 7am UTC.

    For each tenant:
    1. Match CRM contacts to behavioral events via email + UTM params
    2. Resolve UTM campaign values to campaign_ids
    3. Update source_campaign_id on crm_opportunities
    4. Calculate cost-per-opportunity and pipeline influenced

    Per-tenant error isolation: one tenant failure doesn't stop others.
    """
    supabase = get_supabase_client()
    clickhouse = get_clickhouse_client()

    tenants = await get_all_tenants(supabase)
    logger.info(
        "Attribution matching starting for %d tenants", len(tenants),
    )

    results = []
    for tenant_id in tenants:
        start_ms = time.monotonic_ns() // 1_000_000
        try:
            result = run_attribution_for_tenant(
                tenant_id=tenant_id,
                clickhouse=clickhouse,
            )
            duration_ms = time.monotonic_ns() // 1_000_000 - start_ms
            result["task"] = "attribution_match"
            result["duration_ms"] = duration_ms
            results.append(result)
            logger.info(
                "Attribution match complete for tenant=%s: "
                "%d matches, %d opps updated",
                tenant_id,
                result.get("matches_found", 0),
                result.get("opportunities_updated", 0),
            )
        except Exception:
            logger.exception(
                "Attribution matching failed for tenant %s", tenant_id,
            )
            results.append({
                "task": "attribution_match",
                "tenant_id": tenant_id,
                "status": "error",
            })

    total_updated = sum(r.get("opportunities_updated", 0) for r in results)
    total_matches = sum(r.get("matches_found", 0) for r in results)
    logger.info(
        "Attribution matching finished: %d tenants, %d matches, %d opps updated",
        len(results), total_matches, total_updated,
    )
    return results
