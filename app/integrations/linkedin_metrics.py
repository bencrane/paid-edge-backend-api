"""LinkedIn analytics → ClickHouse campaign_metrics mapping layer (BJC-136)."""

import logging
from datetime import date

from app.integrations.linkedin import extract_id_from_urn, from_linkedin_date

logger = logging.getLogger(__name__)


def to_linkedin_date(d: date) -> dict:
    """Convert Python date to LinkedIn date format."""
    return {"year": d.year, "month": d.month, "day": d.day}


def _safe_div(numerator: float, denominator: float) -> float:
    """Safe division, returns 0.0 on zero denominator."""
    if denominator == 0:
        return 0.0
    return numerator / denominator


def map_linkedin_analytics_to_campaign_metrics(
    raw_elements: list[dict],
    tenant_id: str,
    campaign_id_map: dict[int, str],
) -> list[dict]:
    """Map LinkedIn adAnalytics elements to ClickHouse campaign_metrics rows.

    campaign_id_map: LinkedIn campaign_id (int) → PaidEdge campaign UUID (str).
    Returns list of dicts ready for ClickHouse insert.
    """
    rows = []
    for el in raw_elements:
        pivot_value = el.get("pivotValue", "")
        try:
            platform_campaign_id = extract_id_from_urn(pivot_value)
        except (ValueError, IndexError):
            logger.warning(
                "Cannot extract campaign ID from pivotValue: %s",
                pivot_value,
            )
            continue

        paidedge_campaign_id = campaign_id_map.get(platform_campaign_id)
        if not paidedge_campaign_id:
            logger.debug(
                "No PaidEdge mapping for LinkedIn campaign %d, skipping",
                platform_campaign_id,
            )
            continue

        # Extract date from dateRange.start
        date_range = el.get("dateRange", {})
        start_date_obj = date_range.get("start", {})
        try:
            row_date = from_linkedin_date(start_date_obj)
        except (KeyError, TypeError):
            logger.warning(
                "Invalid dateRange in analytics element: %s",
                date_range,
            )
            continue

        spend = float(el.get("costInLocalCurrency", 0) or 0)
        impressions = int(el.get("impressions", 0) or 0)
        clicks = int(el.get("clicks", 0) or 0)
        conversions = int(
            el.get("externalWebsiteConversions", 0) or 0
        )
        leads = int(
            el.get("leadGenerationMailContactInfoShares", 0) or 0
        ) + int(el.get("oneClickLeads", 0) or 0)

        ctr = _safe_div(clicks, impressions)
        cpc = _safe_div(spend, clicks)
        cpm = _safe_div(spend, impressions) * 1000

        rows.append(
            {
                "tenant_id": tenant_id,
                "campaign_id": paidedge_campaign_id,
                "platform": "linkedin",
                "platform_campaign_id": str(platform_campaign_id),
                "platform_ad_group_id": "",
                "platform_ad_id": "",
                "date": row_date,
                "spend": round(spend, 2),
                "impressions": impressions,
                "clicks": clicks,
                "conversions": conversions,
                "leads": leads,
                "ctr": round(ctr, 6),
                "cpc": round(cpc, 2),
                "cpm": round(cpm, 2),
                "roas": 0.0,
            }
        )
    return rows


async def insert_linkedin_metrics(
    clickhouse,
    metrics: list[dict],
) -> int:
    """Batch insert mapped metrics into paid_edge.campaign_metrics.

    Uses ReplacingMergeTree dedup on
    (tenant_id, campaign_id, platform, platform_campaign_id, date).
    Returns number of rows inserted.
    """
    if not metrics:
        return 0

    columns = [
        "tenant_id",
        "campaign_id",
        "platform",
        "platform_campaign_id",
        "platform_ad_group_id",
        "platform_ad_id",
        "date",
        "spend",
        "impressions",
        "clicks",
        "conversions",
        "leads",
        "ctr",
        "cpc",
        "cpm",
        "roas",
    ]

    data = [
        [row[col] for col in columns] for row in metrics
    ]

    clickhouse.insert(
        "paid_edge.campaign_metrics",
        data,
        column_names=columns,
    )
    return len(metrics)


async def build_campaign_id_map(
    supabase,
    tenant_id: str,
) -> dict[int, str]:
    """Build mapping from LinkedIn campaign ID → PaidEdge campaign UUID.

    Reads from campaigns table where platforms JSONB contains linkedin entries.
    Returns: {507404993: 'uuid-of-paidedge-campaign', ...}
    """
    res = (
        supabase.table("campaigns")
        .select("id, platforms")
        .eq("organization_id", tenant_id)
        .execute()
    )

    campaign_map: dict[int, str] = {}
    for row in res.data or []:
        platforms = row.get("platforms") or {}
        linkedin_config = platforms.get("linkedin") or {}
        li_campaign_id = linkedin_config.get("campaign_id")
        if li_campaign_id:
            campaign_map[int(li_campaign_id)] = row["id"]
    return campaign_map
