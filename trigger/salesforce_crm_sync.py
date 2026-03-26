"""Salesforce CRM sync Trigger.dev task (BJC-192).

Scheduled: 0 */6 * * * (every 6 hours)

Syncs Salesforce CRM data (contacts + opportunities + pipeline stages) to
Supabase (primary operational store) and ClickHouse (analytical layer).
Fan-out: all Salesforce-connected tenants -> per-tenant sync with error isolation.

Pattern: mirrors trigger/hubspot_crm_sync.py.
"""

import logging
import time
from datetime import UTC, datetime

from app.db.clickhouse import get_clickhouse_client
from app.db.supabase import get_supabase_client
from app.integrations.salesforce_engine_x import SalesforceEngineClient
from app.integrations.salesforce_syncer import SalesforceSyncer
from app.services.crm_clickhouse import insert_crm_contacts as ch_insert_contacts
from app.services.crm_clickhouse import insert_crm_opportunities as ch_insert_opportunities
from app.services.crm_supabase import upsert_crm_contacts as sb_upsert_contacts
from app.services.crm_supabase import upsert_crm_opportunities as sb_upsert_opportunities

logger = logging.getLogger(__name__)


async def get_salesforce_connected_tenants(supabase) -> list[dict]:
    """Find all orgs with an active Salesforce CRM connection.

    Reads provider_configs where provider='salesforce_crm' and status='connected'.
    Returns list of {org_id, salesforce_client_id, last_salesforce_sync}.
    """
    res = (
        supabase.table("provider_configs")
        .select("organization_id, config")
        .eq("provider", "salesforce_crm")
        .execute()
    )

    tenants = []
    for row in res.data or []:
        config = row.get("config") or {}
        if config.get("status") != "connected":
            continue
        client_id = config.get("salesforce_client_id")
        if not client_id:
            continue
        tenants.append({
            "org_id": row["organization_id"],
            "salesforce_client_id": client_id,
            "last_salesforce_sync": config.get("last_salesforce_sync"),
        })

    return tenants


async def sync_tenant_salesforce(
    tenant: dict,
    syncer: SalesforceSyncer,
    supabase,
    clickhouse,
) -> dict:
    """Sync a single tenant's Salesforce CRM data.

    Steps:
    1. Check connection health
    2. Pull contacts (incremental if last_salesforce_sync exists)
    3. Pull opportunities with contact associations
    4. Write to Supabase (primary operational store)
    5. Write to ClickHouse (analytical layer)
    6. Update last_salesforce_sync in provider_configs
    """
    org_id = tenant["org_id"]
    client_id = tenant["salesforce_client_id"]
    last_sync = tenant.get("last_salesforce_sync")
    start_ms = time.monotonic_ns() // 1_000_000

    # 1. Check connection
    is_connected = await syncer.check_connection(client_id)
    if not is_connected:
        logger.warning(
            "Salesforce connection not active for tenant=%s client=%s — skipping",
            org_id, client_id,
        )
        return {
            "task": "salesforce_crm_sync",
            "tenant_id": org_id,
            "status": "skipped_disconnected",
        }

    # 2. Pull contacts
    contacts = await syncer.pull_contacts(client_id, since=last_sync)

    # 3. Pull opportunities with contact associations
    opportunities = await syncer.pull_opportunities(client_id, since=last_sync)

    # 4a. Write to Supabase (primary operational store)
    sb_contacts = sb_upsert_contacts(contacts, org_id, "salesforce", supabase=supabase)
    sb_opps = sb_upsert_opportunities(opportunities, org_id, "salesforce", supabase=supabase)

    # 4b. Write to ClickHouse (analytical layer)
    contacts_written = ch_insert_contacts(
        org_id, "salesforce", contacts, clickhouse=clickhouse,
    )
    opps_written = ch_insert_opportunities(
        org_id, "salesforce", opportunities, clickhouse=clickhouse,
    )

    # 5. Update last_sync_date
    now_iso = datetime.now(UTC).isoformat()
    supabase.rpc("update_provider_config_field", {
        "p_org_id": org_id,
        "p_provider": "salesforce_crm",
        "p_field": "last_salesforce_sync",
        "p_value": now_iso,
    }).execute()

    duration_ms = time.monotonic_ns() // 1_000_000 - start_ms
    return {
        "task": "salesforce_crm_sync",
        "tenant_id": org_id,
        "contacts_synced": contacts_written,
        "opportunities_synced": opps_written,
        "duration_ms": duration_ms,
        "status": "success",
    }


async def salesforce_crm_sync_task():
    """Scheduled Salesforce CRM sync — every 6 hours.

    Fan-out: all connected tenants -> per-tenant sync.
    Per-tenant error isolation: one failure doesn't stop others.
    """
    supabase = get_supabase_client()
    clickhouse = get_clickhouse_client()

    tenants = await get_salesforce_connected_tenants(supabase)
    logger.info(
        "Salesforce CRM sync starting for %d tenants", len(tenants),
    )

    all_results = []
    async with SalesforceEngineClient() as sfdc_client:
        syncer = SalesforceSyncer(engine_client=sfdc_client)

        for tenant in tenants:
            try:
                result = await sync_tenant_salesforce(
                    tenant=tenant,
                    syncer=syncer,
                    supabase=supabase,
                    clickhouse=clickhouse,
                )
                all_results.append(result)
                logger.info(
                    "Salesforce sync complete for tenant=%s — %d contacts, %d opps",
                    tenant["org_id"],
                    result.get("contacts_synced", 0),
                    result.get("opportunities_synced", 0),
                )
            except Exception:
                logger.exception(
                    "Salesforce CRM sync failed for tenant %s", tenant["org_id"],
                )
                all_results.append({
                    "task": "salesforce_crm_sync",
                    "tenant_id": tenant["org_id"],
                    "status": "error",
                })

    total_contacts = sum(r.get("contacts_synced", 0) for r in all_results)
    total_opps = sum(r.get("opportunities_synced", 0) for r in all_results)
    logger.info(
        "Salesforce CRM sync finished: %d tenants, %d contacts, %d opps",
        len(tenants), total_contacts, total_opps,
    )
    return all_results
