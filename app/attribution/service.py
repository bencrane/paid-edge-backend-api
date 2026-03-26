"""Attribution matching service (PEX-70).

Matches CRM contacts → behavioral_events via email + UTM params,
updates source_campaign_id on crm_opportunities, and calculates
cost-per-opportunity and pipeline influenced metrics.

Attribution chain:
  Ad click → PaidEdge landing page (UTM tagged) → form fill (identify)
  → CRM lead/contact → opportunity → closed-won
"""

import logging

from clickhouse_connect.driver import Client as CHClient

from app.db.clickhouse import get_clickhouse_client

logger = logging.getLogger(__name__)


def match_contacts_to_events(
    tenant_id: str,
    clickhouse: CHClient | None = None,
) -> list[dict]:
    """Match CRM contacts to behavioral_events via email + UTM params.

    For each CRM contact with a non-empty email, find the earliest
    behavioral_event that shares the same email (user_id) and has
    UTM campaign params. This links the contact to the ad campaign
    that drove their first visit.

    Returns list of {contact_email, utm_source, utm_medium, utm_campaign,
    utm_content, click_id, event_timestamp}.
    """
    ch = clickhouse or get_clickhouse_client()

    sql = """
    SELECT
        c.email AS contact_email,
        c.contact_id,
        be.utm_source,
        be.utm_medium,
        be.utm_campaign,
        be.utm_content,
        be.click_id,
        min(be.timestamp) AS first_touch_at
    FROM crm_contacts AS c
    INNER JOIN behavioral_events AS be
        ON be.tenant_id = c.tenant_id
        AND be.user_id = c.email
        AND be.utm_campaign != ''
    WHERE c.tenant_id = %(tid)s
      AND c.email != ''
    GROUP BY c.email, c.contact_id, be.utm_source, be.utm_medium,
             be.utm_campaign, be.utm_content, be.click_id
    ORDER BY first_touch_at ASC
    """

    result = ch.query(sql, parameters={"tid": tenant_id})
    matches = [dict(row) for row in result.named_results()]
    logger.info(
        "Found %d contact→event matches for tenant=%s",
        len(matches), tenant_id,
    )
    return matches


def resolve_campaign_ids(
    tenant_id: str,
    utm_campaigns: list[str],
    clickhouse: CHClient | None = None,
) -> dict[str, str]:
    """Map UTM campaign strings to campaign_ids from campaign_metrics.

    The utm_campaign value on behavioral events should correspond to
    the campaign tracked via campaign_metrics. We look up the mapping
    by matching utm_campaign to the platform_campaign_id or by finding
    campaign_metrics rows where the campaign's tracked link was used.

    Returns: {utm_campaign_value: campaign_id}
    """
    if not utm_campaigns:
        return {}

    ch = clickhouse or get_clickhouse_client()

    # Match utm_campaign values to campaign_ids in campaign_metrics.
    # utm_campaign typically contains the PaidEdge campaign_id or
    # the platform_campaign_id.
    sql = """
    SELECT DISTINCT
        campaign_id,
        platform_campaign_id
    FROM campaign_metrics
    WHERE tenant_id = %(tid)s
    """
    result = ch.query(sql, parameters={"tid": tenant_id})

    # Build lookup from platform_campaign_id → campaign_id
    # and campaign_id (as string) → campaign_id
    lookup = {}
    for row in result.named_results():
        cid = str(row["campaign_id"])
        pid = row["platform_campaign_id"]
        lookup[cid] = cid
        if pid:
            lookup[pid] = cid

    # Map utm_campaigns to campaign_ids
    mapping = {}
    for utm_val in utm_campaigns:
        if utm_val in lookup:
            mapping[utm_val] = lookup[utm_val]

    logger.info(
        "Resolved %d/%d utm_campaign values to campaign_ids for tenant=%s",
        len(mapping), len(utm_campaigns), tenant_id,
    )
    return mapping


def update_opportunity_attribution(
    tenant_id: str,
    contact_matches: list[dict],
    campaign_mapping: dict[str, str],
    clickhouse: CHClient | None = None,
) -> int:
    """Update source_campaign_id on crm_opportunities for matched contacts.

    For each matched contact, find their associated opportunities and
    set source_campaign_id + UTM fields. Uses ClickHouse INSERT to
    upsert via ReplacingMergeTree.

    Returns count of opportunities updated.
    """
    ch = clickhouse or get_clickhouse_client()

    if not contact_matches or not campaign_mapping:
        return 0

    # Build a map of contact_email → attribution data
    email_attribution = {}
    for match in contact_matches:
        email = match["contact_email"]
        utm_campaign = match["utm_campaign"]
        campaign_id = campaign_mapping.get(utm_campaign)
        if campaign_id and email not in email_attribution:
            # First-touch attribution: use the earliest match
            email_attribution[email] = {
                "campaign_id": campaign_id,
                "utm_source": match["utm_source"],
                "utm_medium": match["utm_medium"],
                "utm_campaign": match["utm_campaign"],
                "utm_content": match["utm_content"],
                "click_id": match["click_id"],
            }

    if not email_attribution:
        return 0

    # Find opportunities for these contacts that don't yet have attribution
    emails = list(email_attribution.keys())
    sql = """
    SELECT
        opportunity_id,
        contact_email,
        opportunity_name,
        company_domain,
        company_name,
        amount,
        stage,
        is_won,
        is_lost,
        close_date,
        created_at,
        crm_source
    FROM crm_opportunities
    WHERE tenant_id = %(tid)s
      AND contact_email IN %(emails)s
      AND (source_campaign_id IS NULL
           OR source_campaign_id = toUUID('00000000-0000-0000-0000-000000000000'))
    """
    result = ch.query(sql, parameters={"tid": tenant_id, "emails": emails})
    opportunities = list(result.named_results())

    if not opportunities:
        logger.info("No unattributed opportunities found for tenant=%s", tenant_id)
        return 0

    # Insert updated opportunities with source_campaign_id set.
    # ReplacingMergeTree will merge these with existing rows by
    # (tenant_id, opportunity_id), keeping the one with latest ingested_at.
    rows = []
    for opp in opportunities:
        email = opp["contact_email"]
        attr = email_attribution.get(email)
        if not attr:
            continue
        rows.append([
            tenant_id,
            opp["opportunity_id"],
            opp.get("opportunity_name", ""),
            email,
            opp.get("contact_name", ""),
            opp.get("company_domain", ""),
            opp.get("company_name", ""),
            opp.get("amount", 0),
            opp.get("stage", ""),
            opp.get("is_won", 0),
            opp.get("is_lost", 0),
            opp.get("close_date"),
            opp.get("created_at"),
            attr["campaign_id"],
            attr["utm_source"],
            attr["utm_medium"],
            attr["utm_campaign"],
            attr["utm_content"],
            attr["click_id"],
            opp.get("crm_source", ""),
        ])

    if not rows:
        return 0

    columns = [
        "tenant_id",
        "opportunity_id",
        "opportunity_name",
        "contact_email",
        "contact_name",
        "company_domain",
        "company_name",
        "amount",
        "stage",
        "is_won",
        "is_lost",
        "close_date",
        "created_at",
        "source_campaign_id",
        "source_utm_source",
        "source_utm_medium",
        "source_utm_campaign",
        "source_utm_content",
        "source_click_id",
        "crm_source",
    ]

    ch.insert("crm_opportunities", rows, column_names=columns)
    logger.info(
        "Updated %d opportunities with attribution for tenant=%s",
        len(rows), tenant_id,
    )
    return len(rows)


def calculate_attribution_metrics(
    tenant_id: str,
    clickhouse: CHClient | None = None,
) -> dict:
    """Calculate cost-per-opportunity and pipeline influenced for tenant.

    Returns summary dict with key attribution metrics.
    """
    ch = clickhouse or get_clickhouse_client()

    sql = """
    SELECT
        count(DISTINCT opp.opportunity_id) AS attributed_opportunities,
        sum(opp.amount) AS pipeline_influenced,
        sumIf(opp.amount, opp.is_won = 1) AS closed_won_value,
        countDistinct(
            CASE WHEN opp.is_won = 1 THEN opp.opportunity_id ELSE NULL END
        ) AS closed_won_count
    FROM crm_opportunities AS opp
    WHERE opp.tenant_id = %(tid)s
      AND opp.source_campaign_id IS NOT NULL
      AND opp.source_campaign_id != toUUID('00000000-0000-0000-0000-000000000000')
    """
    result = ch.query(sql, parameters={"tid": tenant_id})
    row = list(result.named_results())
    if not row:
        return {
            "attributed_opportunities": 0,
            "pipeline_influenced": 0,
            "closed_won_value": 0,
            "closed_won_count": 0,
        }

    metrics = row[0]

    # Calculate total spend for cost metrics
    spend_sql = """
    SELECT sum(spend) AS total_spend
    FROM campaign_metrics
    WHERE tenant_id = %(tid)s
    """
    spend_result = ch.query(spend_sql, parameters={"tid": tenant_id})
    spend_row = list(spend_result.named_results())
    total_spend = float(spend_row[0]["total_spend"]) if spend_row else 0

    attributed_opps = int(metrics["attributed_opportunities"])
    closed_won = int(metrics["closed_won_count"])

    return {
        "attributed_opportunities": attributed_opps,
        "pipeline_influenced": float(metrics["pipeline_influenced"]),
        "closed_won_value": float(metrics["closed_won_value"]),
        "closed_won_count": closed_won,
        "total_spend": total_spend,
        "cost_per_opportunity": total_spend / attributed_opps if attributed_opps > 0 else 0,
        "cost_per_closed_won": total_spend / closed_won if closed_won > 0 else 0,
    }


def run_attribution_for_tenant(
    tenant_id: str,
    clickhouse: CHClient | None = None,
) -> dict:
    """Run the full attribution matching pipeline for a single tenant.

    Steps:
    1. Match CRM contacts → behavioral_events via email + UTM
    2. Resolve UTM campaign values → campaign_ids
    3. Update source_campaign_id on matched opportunities
    4. Calculate cost metrics

    Returns summary dict.
    """
    ch = clickhouse or get_clickhouse_client()

    # Step 1: Match contacts to events
    contact_matches = match_contacts_to_events(tenant_id, clickhouse=ch)

    if not contact_matches:
        logger.info("No contact→event matches for tenant=%s, skipping", tenant_id)
        return {
            "tenant_id": tenant_id,
            "matches_found": 0,
            "opportunities_updated": 0,
            "status": "skipped_no_matches",
        }

    # Step 2: Resolve UTM campaign values to campaign_ids
    utm_campaigns = list({m["utm_campaign"] for m in contact_matches})
    campaign_mapping = resolve_campaign_ids(tenant_id, utm_campaigns, clickhouse=ch)

    # Step 3: Update opportunities
    opps_updated = update_opportunity_attribution(
        tenant_id, contact_matches, campaign_mapping, clickhouse=ch,
    )

    # Step 4: Calculate metrics
    metrics = calculate_attribution_metrics(tenant_id, clickhouse=ch)

    return {
        "tenant_id": tenant_id,
        "matches_found": len(contact_matches),
        "campaigns_resolved": len(campaign_mapping),
        "opportunities_updated": opps_updated,
        "metrics": metrics,
        "status": "success",
    }
