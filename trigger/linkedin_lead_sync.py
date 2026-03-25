"""LinkedIn Lead Gen Forms sync task — scheduled per-tenant pull (BJC-139).

Scheduled: every 1 hour (0 * * * *)
Polls LinkedIn Lead Gen Forms for new submissions per tenant.
"""

import logging
import time
from datetime import UTC, datetime

from app.db.supabase import get_supabase_client
from app.integrations.linkedin import LinkedInAdsClient
from app.integrations.linkedin_leads import LinkedInLeadProcessor

logger = logging.getLogger(__name__)


async def get_linkedin_lead_tenants(supabase) -> list[dict]:
    """Find all tenants with active LinkedIn Ads connections.

    Returns: [{organization_id, config}, ...]
    """
    res = (
        supabase.table("provider_configs")
        .select("organization_id, config")
        .eq("provider", "linkedin_ads")
        .execute()
    )
    return res.data or []


async def get_active_form_ids(config: dict) -> list[int]:
    """Extract active lead gen form IDs from tenant config.

    Forms are stored in config.lead_gen_forms as a list of form IDs.
    """
    return config.get("lead_gen_forms", [])


async def sync_tenant_leads(
    tenant_config: dict,
    supabase,
) -> dict:
    """Sync lead submissions for a single tenant.

    Returns structured log dict with sync results.
    """
    org_id = tenant_config["organization_id"]
    config = tenant_config.get("config", {})
    start_ms = time.monotonic_ns() // 1_000_000

    form_ids = await get_active_form_ids(config)
    if not form_ids:
        return {
            "task": "linkedin_lead_sync",
            "tenant_id": org_id,
            "forms_synced": 0,
            "leads_processed": 0,
            "duration_ms": (
                time.monotonic_ns() // 1_000_000 - start_ms
            ),
            "status": "skipped_no_forms",
        }

    async with LinkedInAdsClient(
        org_id=org_id, supabase=supabase
    ) as client:
        account_id = await client.get_selected_account_id()
        processor = LinkedInLeadProcessor(
            client=client, supabase=supabase
        )

        total_leads = 0
        forms_synced = 0
        errors: list[str] = []

        for form_id in form_ids:
            # Get last synced timestamp for incremental polling
            lead_sync = config.get("lead_sync", {})
            form_sync = lead_sync.get(str(form_id), {})
            last_synced_ms = form_sync.get("last_synced_at")

            since = None
            if last_synced_ms:
                since = datetime.fromtimestamp(
                    last_synced_ms / 1000, tz=UTC
                )

            try:
                result = await processor.sync_leads(
                    tenant_id=org_id,
                    account_id=account_id,
                    form_id=form_id,
                    since=since,
                )
                total_leads += result["leads_processed"]
                forms_synced += 1
                errors.extend(result["errors"])
            except Exception as e:
                errors.append(
                    f"Form {form_id}: {e}"
                )

        duration_ms = (
            time.monotonic_ns() // 1_000_000 - start_ms
        )
        return {
            "task": "linkedin_lead_sync",
            "tenant_id": org_id,
            "forms_synced": forms_synced,
            "leads_processed": total_leads,
            "errors": errors,
            "duration_ms": duration_ms,
            "status": "success" if not errors else "partial_error",
        }


async def linkedin_lead_sync_task():
    """Poll LinkedIn Lead Gen Forms for new submissions.

    Scheduled: every 1 hour (0 * * * *)

    Per-tenant: find all active lead gen forms, pull new submissions.
    Per-tenant isolation: one tenant failure doesn't stop others.
    """
    supabase = get_supabase_client()
    tenants = await get_linkedin_lead_tenants(supabase)

    logger.info(
        "LinkedIn lead sync starting for %d tenants",
        len(tenants),
    )

    results = []
    for tenant_config in tenants:
        org_id = tenant_config["organization_id"]
        try:
            result = await sync_tenant_leads(
                tenant_config=tenant_config,
                supabase=supabase,
            )
            results.append(result)
            logger.info(
                "LinkedIn lead sync complete for tenant %s: %s",
                org_id,
                result,
            )
        except Exception:
            logger.exception(
                "LinkedIn lead sync failed for tenant %s",
                org_id,
            )
            results.append(
                {
                    "task": "linkedin_lead_sync",
                    "tenant_id": org_id,
                    "status": "error",
                }
            )

    total_leads = sum(
        r.get("leads_processed", 0) for r in results
    )
    logger.info(
        "LinkedIn lead sync finished: %d tenants, %d total leads",
        len(results),
        total_leads,
    )
    return results
