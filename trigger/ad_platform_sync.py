"""Unified ad platform metrics sync Trigger.dev task (PEX-74).

Scheduled: every 6 hours (0 */6 * * *)

For each tenant with connected ad platforms (LinkedIn, Meta), pull campaign
metrics via the existing platform clients and write to ClickHouse
paid_engine_x_api.campaign_metrics.

Delegates to per-platform sync tasks for actual data pulling and mapping.
Google Ads support will be added when the integration client is implemented.
"""

import asyncio
import logging
import time

from app.db.clickhouse import get_clickhouse_client
from app.db.supabase import get_supabase_client

logger = logging.getLogger(__name__)

# Platforms to sync — each entry maps to a provider_configs.provider value
# and the corresponding per-tenant sync function.
PLATFORM_REGISTRY = {
    "linkedin_ads": "linkedin",
    "meta_ads": "meta",
    # "google_ads": "google",  # TODO: add when Google Ads client is ready
}

# Rate limit backoff settings per platform
RATE_LIMIT_BACKOFF = {
    "linkedin": {"initial_wait": 2.0, "max_retries": 3, "backoff_factor": 2.0},
    "meta": {"initial_wait": 1.0, "max_retries": 3, "backoff_factor": 2.0},
}


async def get_tenants_with_ad_platforms(supabase) -> list[dict]:
    """Get all tenants with at least one active ad platform connection.

    Queries provider_configs for all supported ad platform providers
    and groups them by tenant.

    Returns: [{organization_id, platforms: [{provider, config}, ...]}, ...]
    """
    providers = list(PLATFORM_REGISTRY.keys())
    res = (
        supabase.table("provider_configs")
        .select("organization_id, provider, config")
        .in_("provider", providers)
        .execute()
    )
    rows = res.data or []

    # Group by organization_id
    tenant_map: dict[str, list[dict]] = {}
    for row in rows:
        org_id = row["organization_id"]
        if org_id not in tenant_map:
            tenant_map[org_id] = []
        tenant_map[org_id].append({
            "provider": row["provider"],
            "config": row.get("config", {}),
        })

    return [
        {"organization_id": org_id, "platforms": platforms}
        for org_id, platforms in tenant_map.items()
    ]


async def _sync_linkedin_for_tenant(
    tenant_config: dict,
    supabase,
    clickhouse,
) -> dict:
    """Sync LinkedIn metrics for a single tenant with rate-limit retry."""
    from trigger.linkedin_metrics_sync import (
        get_sync_date_range,
        sync_tenant_metrics,
    )

    start_date, end_date = get_sync_date_range()
    backoff = RATE_LIMIT_BACKOFF["linkedin"]
    last_exc = None

    for attempt in range(backoff["max_retries"] + 1):
        try:
            return await sync_tenant_metrics(
                tenant_config={
                    "organization_id": tenant_config["organization_id"],
                    "config": tenant_config.get("config", {}),
                },
                start_date=start_date,
                end_date=end_date,
                supabase=supabase,
                clickhouse=clickhouse,
            )
        except Exception as exc:
            last_exc = exc
            err_str = str(exc).lower()
            is_rate_limit = "429" in err_str or "rate" in err_str or "throttl" in err_str
            if is_rate_limit and attempt < backoff["max_retries"]:
                wait = backoff["initial_wait"] * (backoff["backoff_factor"] ** attempt)
                logger.warning(
                    "LinkedIn rate limit for tenant %s (attempt %d/%d), "
                    "retrying in %.1fs",
                    tenant_config["organization_id"],
                    attempt + 1,
                    backoff["max_retries"],
                    wait,
                )
                await asyncio.sleep(wait)
            else:
                raise

    # Should not reach here, but safety net
    raise last_exc  # type: ignore[misc]


async def _sync_meta_for_tenant(
    tenant_config: dict,
    supabase,
    clickhouse,
) -> dict:
    """Sync Meta metrics for a single tenant with rate-limit retry."""
    from trigger.meta_metrics_sync import sync_tenant_metrics

    backoff = RATE_LIMIT_BACKOFF["meta"]
    last_exc = None

    for attempt in range(backoff["max_retries"] + 1):
        try:
            return await sync_tenant_metrics(
                tenant_config={
                    "organization_id": tenant_config["organization_id"],
                    "config": tenant_config.get("config", {}),
                },
                supabase=supabase,
                clickhouse=clickhouse,
            )
        except Exception as exc:
            last_exc = exc
            err_str = str(exc).lower()
            is_rate_limit = "429" in err_str or "rate" in err_str or "throttl" in err_str
            if is_rate_limit and attempt < backoff["max_retries"]:
                wait = backoff["initial_wait"] * (backoff["backoff_factor"] ** attempt)
                logger.warning(
                    "Meta rate limit for tenant %s (attempt %d/%d), "
                    "retrying in %.1fs",
                    tenant_config["organization_id"],
                    attempt + 1,
                    backoff["max_retries"],
                    wait,
                )
                await asyncio.sleep(wait)
            else:
                raise

    raise last_exc  # type: ignore[misc]


def _get_platform_sync_fn(provider: str):
    """Resolve provider name to sync function (late binding for testability)."""
    fns = {
        "linkedin_ads": _sync_linkedin_for_tenant,
        "meta_ads": _sync_meta_for_tenant,
    }
    return fns.get(provider)


async def sync_tenant_all_platforms(
    tenant: dict,
    supabase,
    clickhouse,
) -> list[dict]:
    """Sync all connected platforms for a single tenant.

    Returns list of per-platform result dicts.
    Per-platform error isolation: one platform failing doesn't stop others.
    """
    org_id = tenant["organization_id"]
    results = []

    for platform_config in tenant["platforms"]:
        provider = platform_config["provider"]
        platform_name = PLATFORM_REGISTRY.get(provider, provider)
        sync_fn = _get_platform_sync_fn(provider)

        if not sync_fn:
            logger.warning(
                "No sync function for provider %s (tenant %s), skipping",
                provider,
                org_id,
            )
            continue

        start_ms = time.monotonic_ns() // 1_000_000
        try:
            result = await sync_fn(
                tenant_config={
                    "organization_id": org_id,
                    "config": platform_config.get("config", {}),
                },
                supabase=supabase,
                clickhouse=clickhouse,
            )
            result["platform"] = platform_name
            results.append(result)
            logger.info(
                "Platform sync complete: tenant=%s platform=%s status=%s rows=%d",
                org_id,
                platform_name,
                result.get("status"),
                result.get("rows_inserted", 0),
            )
        except Exception:
            logger.exception(
                "Platform sync failed: tenant=%s platform=%s",
                org_id,
                platform_name,
            )
            results.append({
                "task": "ad_platform_sync",
                "tenant_id": org_id,
                "platform": platform_name,
                "status": "error",
                "rows_inserted": 0,
                "duration_ms": time.monotonic_ns() // 1_000_000 - start_ms,
            })

    return results


async def ad_platform_sync_task():
    """Unified ad platform metrics sync — every 6 hours (0 */6 * * *).

    For each tenant with connected ad platforms:
    1. Discover all active platform connections (LinkedIn, Meta)
    2. For each platform, delegate to per-platform sync with rate-limit retry
    3. Per-tenant AND per-platform error isolation

    Returns list of per-tenant, per-platform result dicts.
    """
    supabase = get_supabase_client()
    clickhouse = get_clickhouse_client()

    tenants = await get_tenants_with_ad_platforms(supabase)
    logger.info(
        "Ad platform metrics sync starting for %d tenants",
        len(tenants),
    )

    all_results = []
    for tenant in tenants:
        org_id = tenant["organization_id"]
        platform_names = [
            PLATFORM_REGISTRY.get(p["provider"], p["provider"])
            for p in tenant["platforms"]
        ]
        logger.info(
            "Syncing tenant %s: platforms=%s",
            org_id,
            platform_names,
        )

        try:
            tenant_results = await sync_tenant_all_platforms(
                tenant=tenant,
                supabase=supabase,
                clickhouse=clickhouse,
            )
            all_results.extend(tenant_results)
        except Exception:
            logger.exception(
                "Ad platform sync failed entirely for tenant %s",
                org_id,
            )
            all_results.append({
                "task": "ad_platform_sync",
                "tenant_id": org_id,
                "status": "error",
                "rows_inserted": 0,
            })

    total_rows = sum(r.get("rows_inserted", 0) for r in all_results)
    success_count = sum(1 for r in all_results if r.get("status") == "success")
    error_count = sum(1 for r in all_results if r.get("status") == "error")
    logger.info(
        "Ad platform metrics sync finished: %d tenants, %d platform syncs "
        "(%d success, %d error), %d total rows",
        len(tenants),
        len(all_results),
        success_count,
        error_count,
        total_rows,
    )
    return all_results
