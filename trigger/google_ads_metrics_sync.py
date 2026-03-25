"""Google Ads metrics sync Trigger.dev task — scheduled per-tenant pull (BJC-152).

Scheduled: every 4 hours (0 */4 * * *)
Pulls Google Ads campaign metrics for all tenants with active connections.
"""

import logging
import time
from datetime import date, timedelta

from app.db.clickhouse import get_clickhouse_client
from app.db.supabase import get_supabase_client
from app.integrations.google_ads import GoogleAdsClientFactory, GoogleAdsService
from app.integrations.google_ads_analytics import (
    GoogleAdsAnalyticsClient,
    map_metrics_to_clickhouse,
    write_metrics_to_clickhouse,
)

logger = logging.getLogger(__name__)

# Basic Access: 15,000 ops/day. Track daily usage.
_DAILY_QUOTA_LIMIT = 15_000
_QUOTA_WARNING_THRESHOLD = 0.80  # 80% = 12,000 ops

# Default lookback window
LOOKBACK_DAYS = 3
FIRST_SYNC_BACKFILL_DAYS = 30

# Consecutive failures before marking needs_reauth
MAX_CONSECUTIVE_FAILURES = 3


def get_sync_date_range(is_first_sync: bool = False) -> tuple[date, date]:
    """Calculate the date range for this sync run.

    Google Ads data can be revised for up to 72 hours.
    Pull last 3 days (or 30 days on first sync for backfill).
    """
    today = date.today()
    lookback = FIRST_SYNC_BACKFILL_DAYS if is_first_sync else LOOKBACK_DAYS
    return (today - timedelta(days=lookback), today)


async def get_google_ads_connected_tenants(supabase) -> list[dict]:
    """Find all tenants with active Google Ads connections."""
    res = (
        supabase.table("provider_configs")
        .select("organization_id, config, is_active")
        .eq("provider", "google_ads")
        .eq("is_active", True)
        .execute()
    )
    tenants = []
    for row in (res.data or []):
        config = row.get("config", {})
        # Only include tenants with a refresh token and selected customer ID
        if config.get("refresh_token") and config.get("selected_customer_id"):
            tenants.append(row)
    return tenants


async def sync_tenant_metrics(
    tenant_config: dict,
    supabase,
    clickhouse,
    daily_ops_counter: dict,
) -> dict:
    """Sync Google Ads metrics for a single tenant.

    Returns structured result dict with sync status and metrics.
    """
    org_id = tenant_config["organization_id"]
    config = tenant_config.get("config", {})
    customer_id = config.get("selected_customer_id", "")
    start_time = time.time()

    # Check daily quota
    if daily_ops_counter.get("count", 0) >= _DAILY_QUOTA_LIMIT:
        logger.warning(
            "Daily API quota limit (%d) reached. Deferring org %s to next cycle.",
            _DAILY_QUOTA_LIMIT,
            org_id,
        )
        return {
            "task": "google_ads_metrics_sync",
            "tenant_id": org_id,
            "status": "deferred_quota",
            "rows_inserted": 0,
        }

    try:
        # Determine if first sync
        last_synced = config.get("last_synced_at")
        is_first_sync = last_synced is None
        start_date, end_date = get_sync_date_range(is_first_sync)

        # Get Google Ads client
        factory = GoogleAdsClientFactory()
        client = await factory.get_client(org_id, supabase)
        service = GoogleAdsService(client, customer_id)
        analytics = GoogleAdsAnalyticsClient(service)

        # Fetch campaign metrics
        raw_metrics = await analytics.fetch_campaign_metrics(
            start_date=start_date,
            end_date=end_date,
        )
        daily_ops_counter["count"] = daily_ops_counter.get("count", 0) + 1

        if not raw_metrics:
            duration_ms = int((time.time() - start_time) * 1000)
            return {
                "task": "google_ads_metrics_sync",
                "tenant_id": org_id,
                "customer_id": customer_id,
                "campaigns_synced": 0,
                "rows_inserted": 0,
                "date_range": f"{start_date} to {end_date}",
                "duration_ms": duration_ms,
                "is_first_sync": is_first_sync,
                "status": "skipped_no_data",
            }

        # Map to ClickHouse schema
        ch_rows = map_metrics_to_clickhouse(org_id, raw_metrics)

        # Write to ClickHouse
        rows_inserted = await write_metrics_to_clickhouse(clickhouse, ch_rows)

        # Count unique campaigns
        campaigns_synced = len(
            {r.get("provider_campaign_id", "") for r in ch_rows}
        )

        # Update last_synced_at
        await _update_sync_timestamp(supabase, org_id)

        # Reset consecutive failure counter on success
        await _reset_failure_count(supabase, org_id)

        duration_ms = int((time.time() - start_time) * 1000)
        return {
            "task": "google_ads_metrics_sync",
            "tenant_id": org_id,
            "customer_id": customer_id,
            "campaigns_synced": campaigns_synced,
            "rows_inserted": rows_inserted,
            "date_range": f"{start_date} to {end_date}",
            "duration_ms": duration_ms,
            "is_first_sync": is_first_sync,
            "status": "success",
        }

    except Exception as exc:
        duration_ms = int((time.time() - start_time) * 1000)
        logger.exception(
            "Google Ads metrics sync failed for tenant %s: %s", org_id, exc
        )

        # Track consecutive failures
        await _increment_failure_count(supabase, org_id)

        return {
            "task": "google_ads_metrics_sync",
            "tenant_id": org_id,
            "status": "error",
            "error": str(exc),
            "duration_ms": duration_ms,
            "rows_inserted": 0,
        }


async def google_ads_metrics_sync_task() -> list[dict]:
    """Pull Google Ads campaign metrics for all tenants with active connections.

    Scheduled: every 4 hours (0 */4 * * *)

    For each tenant:
    1. Check provider_configs for active google_ads connection
    2. Determine sync range (3 days, or 30 days on first sync)
    3. Pull campaign metrics via GAQL
    4. Map to ClickHouse schema (micros → dollars)
    5. Insert into paid_edge.campaign_metrics
    6. Log results per-tenant

    Per-tenant isolation: one failure doesn't stop others.
    Rate limit awareness: track daily ops, defer on quota exhaustion.
    """
    supabase = get_supabase_client()
    clickhouse = get_clickhouse_client()

    tenants = await get_google_ads_connected_tenants(supabase)
    logger.info(
        "Google Ads metrics sync starting for %d tenants",
        len(tenants),
    )

    daily_ops_counter = {"count": 0}
    results = []

    for tenant_config in tenants:
        org_id = tenant_config["organization_id"]

        # Check quota warning threshold
        if daily_ops_counter["count"] >= int(
            _DAILY_QUOTA_LIMIT * _QUOTA_WARNING_THRESHOLD
        ):
            logger.warning(
                "Google Ads API daily usage at %d/%d (%.0f%%). "
                "Consider applying for Standard Access.",
                daily_ops_counter["count"],
                _DAILY_QUOTA_LIMIT,
                (daily_ops_counter["count"] / _DAILY_QUOTA_LIMIT) * 100,
            )

        result = await sync_tenant_metrics(
            tenant_config=tenant_config,
            supabase=supabase,
            clickhouse=clickhouse,
            daily_ops_counter=daily_ops_counter,
        )
        results.append(result)
        logger.info(
            "Google Ads sync for tenant %s: %s (%d rows)",
            org_id,
            result.get("status"),
            result.get("rows_inserted", 0),
        )

    total_rows = sum(r.get("rows_inserted", 0) for r in results)
    total_success = sum(1 for r in results if r.get("status") == "success")
    total_failed = sum(1 for r in results if r.get("status") == "error")

    logger.info(
        "Google Ads metrics sync complete: %d tenants "
        "(%d success, %d failed), %d total rows, %d API ops used",
        len(results),
        total_success,
        total_failed,
        total_rows,
        daily_ops_counter["count"],
    )
    return results


async def _update_sync_timestamp(supabase, org_id: str) -> None:
    """Update the last_synced_at timestamp for a tenant."""
    from datetime import datetime, timezone

    try:
        res = (
            supabase.table("provider_configs")
            .select("config")
            .eq("organization_id", org_id)
            .eq("provider", "google_ads")
            .maybe_single()
            .execute()
        )
        if res.data:
            config = res.data.get("config", {})
            config["last_synced_at"] = datetime.now(timezone.utc).isoformat()
            config["consecutive_failures"] = 0
            supabase.table("provider_configs").update(
                {"config": config}
            ).eq("organization_id", org_id).eq(
                "provider", "google_ads"
            ).execute()
    except Exception as exc:
        logger.warning("Failed to update sync timestamp for org %s: %s", org_id, exc)


async def _reset_failure_count(supabase, org_id: str) -> None:
    """Reset consecutive failure counter on success."""
    try:
        res = (
            supabase.table("provider_configs")
            .select("config")
            .eq("organization_id", org_id)
            .eq("provider", "google_ads")
            .maybe_single()
            .execute()
        )
        if res.data:
            config = res.data.get("config", {})
            if config.get("consecutive_failures", 0) > 0:
                config["consecutive_failures"] = 0
                config["needs_reauth"] = False
                supabase.table("provider_configs").update(
                    {"config": config}
                ).eq("organization_id", org_id).eq(
                    "provider", "google_ads"
                ).execute()
    except Exception as exc:
        logger.warning("Failed to reset failure count for org %s: %s", org_id, exc)


async def _increment_failure_count(supabase, org_id: str) -> None:
    """Increment consecutive failure counter. Mark needs_reauth after 3."""
    try:
        res = (
            supabase.table("provider_configs")
            .select("config")
            .eq("organization_id", org_id)
            .eq("provider", "google_ads")
            .maybe_single()
            .execute()
        )
        if res.data:
            config = res.data.get("config", {})
            failures = config.get("consecutive_failures", 0) + 1
            config["consecutive_failures"] = failures
            if failures >= MAX_CONSECUTIVE_FAILURES:
                config["needs_reauth"] = True
                logger.warning(
                    "Org %s: %d consecutive Google Ads sync failures. "
                    "Marked needs_reauth=True.",
                    org_id,
                    failures,
                )
            supabase.table("provider_configs").update(
                {"config": config}
            ).eq("organization_id", org_id).eq(
                "provider", "google_ads"
            ).execute()
    except Exception as exc:
        logger.warning("Failed to increment failure count for org %s: %s", org_id, exc)
