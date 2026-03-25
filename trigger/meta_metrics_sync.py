"""Meta metrics sync Trigger.dev task — scheduled per-tenant pull (BJC-163)."""

import logging
import time
from datetime import date, timedelta

logger = logging.getLogger(__name__)


def get_sync_date_range() -> tuple[date, date]:
    """Pull last 3 days to catch retroactive adjustments and iOS delayed reporting."""
    today = date.today()
    return (today - timedelta(days=3), today)


async def get_meta_connected_tenants(supabase) -> list[dict]:
    """Find all tenants with active Meta Ads connections."""
    res = (
        supabase.table("provider_configs")
        .select("*")
        .eq("provider", "meta_ads")
        .eq("is_active", True)
        .execute()
    )
    return res.data or []


async def sync_tenant_metrics(
    tenant_config: dict,
    supabase,
    clickhouse,
) -> dict:
    """Sync metrics for a single tenant."""
    from app.integrations.meta_client import MetaAdsClient
    from app.integrations.meta_metrics import (
        build_meta_campaign_id_map,
        insert_meta_metrics,
        map_meta_insights_to_campaign_metrics,
    )

    org_id = tenant_config["organization_id"]
    start_date, end_date = get_sync_date_range()
    start_time = time.time()

    try:
        async with await MetaAdsClient.for_tenant(org_id, supabase) as client:
            # Build campaign ID map
            campaign_id_map = await build_meta_campaign_id_map(supabase, org_id)
            if not campaign_id_map:
                return {
                    "tenant_id": org_id,
                    "status": "skipped_no_campaigns",
                    "rows_inserted": 0,
                }

            # Determine if large account (> 50 active campaigns → async)
            use_async = len(campaign_id_map) > 50

            fields = [
                "campaign_id", "adset_id", "ad_id", "impressions", "clicks",
                "spend", "actions", "cpc", "cpm", "ctr", "date_start",
            ]

            if use_async:
                raw_data = await client.get_insights_async(
                    level="campaign",
                    start_date=start_date,
                    end_date=end_date,
                    fields=fields,
                )
            else:
                raw_data = await client.get_campaign_insights(
                    start_date=start_date,
                    end_date=end_date,
                    fields=fields,
                    level="campaign",
                )

            if not raw_data:
                return {
                    "tenant_id": org_id,
                    "status": "skipped_no_data",
                    "rows_inserted": 0,
                }

            # Map to ClickHouse schema
            metrics = map_meta_insights_to_campaign_metrics(
                raw_data, org_id, campaign_id_map
            )

            # Insert
            rows_inserted = await insert_meta_metrics(clickhouse, metrics)

            duration_ms = int((time.time() - start_time) * 1000)
            return {
                "task": "meta_metrics_sync",
                "tenant_id": org_id,
                "ad_account_id": tenant_config.get("config", {}).get("selected_ad_account_id"),
                "campaigns_synced": len(campaign_id_map),
                "rows_inserted": rows_inserted,
                "date_range": f"{start_date} to {end_date}",
                "duration_ms": duration_ms,
                "used_async_report": use_async,
                "status": "success",
            }

    except Exception as exc:
        logger.exception("Meta metrics sync failed for tenant %s: %s", org_id, exc)
        return {
            "tenant_id": org_id,
            "status": "error",
            "error": str(exc),
            "rows_inserted": 0,
        }


async def meta_metrics_sync_task(supabase, clickhouse) -> list[dict]:
    """Pull Meta campaign metrics for all tenants. Scheduled every 6 hours."""
    tenants = await get_meta_connected_tenants(supabase)
    results = []

    for tenant_config in tenants:
        result = await sync_tenant_metrics(tenant_config, supabase, clickhouse)
        results.append(result)
        logger.info(
            "Meta metrics sync for tenant %s: %s (%d rows)",
            result.get("tenant_id"),
            result.get("status"),
            result.get("rows_inserted", 0),
        )

    total_rows = sum(r.get("rows_inserted", 0) for r in results)
    logger.info(
        "Meta metrics sync complete: %d tenants, %d total rows",
        len(results),
        total_rows,
    )
    return results
