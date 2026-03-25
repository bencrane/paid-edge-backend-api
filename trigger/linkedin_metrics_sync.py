"""LinkedIn metrics sync Trigger.dev task — scheduled per-tenant pull (BJC-137).

Scheduled: every 6 hours (0 */6 * * *)
Pulls LinkedIn campaign metrics for all tenants with active connections.
"""

import logging
import time
from datetime import date, timedelta

from app.db.clickhouse import get_clickhouse_client
from app.db.supabase import get_supabase_client
from app.integrations.linkedin import LinkedInAdsClient
from app.integrations.linkedin_metrics import (
    build_campaign_id_map,
    insert_linkedin_metrics,
    map_linkedin_analytics_to_campaign_metrics,
)

logger = logging.getLogger(__name__)


def get_sync_date_range() -> tuple[date, date]:
    """Calculate the date range for this sync run.

    Pull last 3 days to catch LinkedIn's retroactive metric adjustments.
    LinkedIn adjusts metrics for up to 72 hours after delivery.
    Returns: (start_date, end_date) where end_date is exclusive (today).
    """
    today = date.today()
    return (today - timedelta(days=3), today)


async def get_linkedin_connected_tenants(supabase) -> list[dict]:
    """Find all tenants with active LinkedIn Ads connections.

    Query: provider_configs WHERE provider='linkedin_ads'
    Returns: [{organization_id, config}, ...]
    """
    res = (
        supabase.table("provider_configs")
        .select("organization_id, config")
        .eq("provider", "linkedin_ads")
        .execute()
    )
    return res.data or []


async def sync_tenant_metrics(
    tenant_config: dict,
    start_date: date,
    end_date: date,
    supabase,
    clickhouse,
) -> dict:
    """Sync LinkedIn metrics for a single tenant.

    Returns structured log dict with sync results.
    """
    org_id = tenant_config["organization_id"]
    start_ms = time.monotonic_ns() // 1_000_000

    async with LinkedInAdsClient(
        org_id=org_id, supabase=supabase
    ) as client:
        # Get selected ad account
        account_id = await client.get_selected_account_id()

        # Build campaign ID map
        campaign_id_map = await build_campaign_id_map(
            supabase, org_id
        )
        if not campaign_id_map:
            return {
                "task": "linkedin_metrics_sync",
                "tenant_id": org_id,
                "ad_account_id": account_id,
                "campaigns_synced": 0,
                "rows_inserted": 0,
                "date_range": f"{start_date} to {end_date}",
                "duration_ms": (
                    time.monotonic_ns() // 1_000_000 - start_ms
                ),
                "status": "skipped_no_campaigns",
            }

        # Pull metrics
        raw_elements = await client.get_campaign_analytics(
            account_id=account_id,
            start_date=start_date,
            end_date=end_date,
        )

        if not raw_elements:
            return {
                "task": "linkedin_metrics_sync",
                "tenant_id": org_id,
                "ad_account_id": account_id,
                "campaigns_synced": 0,
                "rows_inserted": 0,
                "date_range": f"{start_date} to {end_date}",
                "duration_ms": (
                    time.monotonic_ns() // 1_000_000 - start_ms
                ),
                "status": "skipped_no_data",
            }

        # Map to ClickHouse schema
        metrics = map_linkedin_analytics_to_campaign_metrics(
            raw_elements, org_id, campaign_id_map
        )

        # Insert into ClickHouse
        rows_inserted = await insert_linkedin_metrics(
            clickhouse, metrics
        )

        # Count unique campaigns
        campaigns_synced = len(
            {m["platform_campaign_id"] for m in metrics}
        )

        duration_ms = time.monotonic_ns() // 1_000_000 - start_ms
        return {
            "task": "linkedin_metrics_sync",
            "tenant_id": org_id,
            "ad_account_id": account_id,
            "campaigns_synced": campaigns_synced,
            "rows_inserted": rows_inserted,
            "date_range": f"{start_date} to {end_date}",
            "duration_ms": duration_ms,
            "status": "success",
        }


async def linkedin_metrics_sync_task():
    """Pull LinkedIn campaign metrics for all tenants with active connections.

    Scheduled: every 6 hours (0 */6 * * *)

    For each tenant:
    1. Check provider_configs for active linkedin_ads connection
    2. Verify token is valid (refresh if needed via get_valid_linkedin_token)
    3. Get selected_ad_account_id
    4. Build campaign_id_map (LinkedIn → PaidEdge)
    5. Pull metrics for last 3 days (covers retroactive adjustments)
    6. Map to ClickHouse schema
    7. Insert into paid_edge.campaign_metrics
    8. Log: tenant_id, campaigns_synced, rows_inserted, duration

    Per-tenant isolation: one tenant failure doesn't stop others.
    """
    supabase = get_supabase_client()
    clickhouse = get_clickhouse_client()
    start_date, end_date = get_sync_date_range()

    tenants = await get_linkedin_connected_tenants(supabase)
    logger.info(
        "LinkedIn metrics sync starting for %d tenants "
        "(date range: %s to %s)",
        len(tenants),
        start_date,
        end_date,
    )

    results = []
    for tenant_config in tenants:
        org_id = tenant_config["organization_id"]
        try:
            result = await sync_tenant_metrics(
                tenant_config=tenant_config,
                start_date=start_date,
                end_date=end_date,
                supabase=supabase,
                clickhouse=clickhouse,
            )
            results.append(result)
            logger.info(
                "LinkedIn sync complete for tenant %s: %s",
                org_id,
                result,
            )
        except Exception:
            logger.exception(
                "LinkedIn metrics sync failed for tenant %s",
                org_id,
            )
            results.append(
                {
                    "task": "linkedin_metrics_sync",
                    "tenant_id": org_id,
                    "status": "error",
                }
            )

    total_rows = sum(
        r.get("rows_inserted", 0) for r in results
    )
    logger.info(
        "LinkedIn metrics sync finished: %d tenants, %d total rows",
        len(results),
        total_rows,
    )
    return results
