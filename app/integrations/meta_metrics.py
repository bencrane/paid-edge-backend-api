"""Meta Insights & reporting client + metrics mapping to ClickHouse (BJC-162)."""

import asyncio
import logging
from datetime import date
from decimal import Decimal

logger = logging.getLogger(__name__)

# --- Attribution windows ---

META_ATTRIBUTION_WINDOWS = {
    "default": ["7d_click", "1d_view"],
    "short": ["1d_click", "1d_view"],
    "long": ["7d_click", "1d_view"],
}

# --- Actions array parser ---


def parse_actions(actions: list[dict] | None) -> dict:
    """Parse Meta's actions array into flat metrics.

    Input: [{"action_type": "link_click", "value": "150"}, ...]
    Output: {"link_clicks": 150, "leads": 23, ...}
    """
    if not actions:
        return {}

    result = {}
    type_map = {
        "link_click": "link_clicks",
        "lead": "leads",
        "landing_page_view": "landing_page_views",
        "page_engagement": "page_engagements",
        "post_engagement": "post_engagements",
        "video_view": "video_views",
    }

    for action in actions:
        action_type = action.get("action_type", "")
        value = int(action.get("value", 0))

        if action_type in type_map:
            result[type_map[action_type]] = value

        # Offsite conversion types → aggregate as "conversions"
        if action_type.startswith("offsite_conversion"):
            result["conversions"] = result.get("conversions", 0) + value

    return result


# --- Metrics mapper ---


def map_meta_insights_to_campaign_metrics(
    raw_rows: list[dict],
    tenant_id: str,
    campaign_id_map: dict[str, str],
) -> list[dict]:
    """Map Meta Insights rows to ClickHouse campaign_metrics schema."""
    metrics = []
    for row in raw_rows:
        # Determine campaign ID
        meta_campaign_id = row.get("campaign_id", "")
        paidedge_campaign_id = campaign_id_map.get(meta_campaign_id)
        if not paidedge_campaign_id:
            continue

        # Parse actions
        actions = parse_actions(row.get("actions"))

        metric = {
            "tenant_id": tenant_id,
            "campaign_id": paidedge_campaign_id,
            "platform": "meta",
            "platform_campaign_id": meta_campaign_id,
            "platform_ad_group_id": row.get("adset_id", ""),
            "platform_ad_id": row.get("ad_id", ""),
            "date": row.get("date_start", ""),
            "spend": Decimal(row.get("spend", "0")),
            "impressions": int(row.get("impressions", 0)),
            "clicks": int(row.get("clicks", 0)),
            "conversions": actions.get("conversions", 0) + actions.get("leads", 0),
            "leads": actions.get("leads", 0),
            "ctr": float(row.get("ctr", 0)),
            "cpc": Decimal(row.get("cpc", "0") or "0"),
            "cpm": Decimal(row.get("cpm", "0") or "0"),
            "roas": Decimal("0"),
        }
        metrics.append(metric)

    return metrics


# --- Campaign ID mapping ---


async def build_meta_campaign_id_map(
    supabase,
    tenant_id: str,
) -> dict[str, str]:
    """Build mapping from Meta campaign ID → PaidEdge campaign UUID."""
    res = (
        supabase.table("campaigns")
        .select("id,platform_data")
        .eq("organization_id", tenant_id)
        .execute()
    )
    mapping = {}
    for campaign in res.data or []:
        pd = campaign.get("platform_data", {})
        if pd and pd.get("platform") == "meta":
            meta_id = pd.get("platform_campaign_id", "")
            if meta_id:
                mapping[meta_id] = campaign["id"]
    return mapping


# --- ClickHouse insert ---


async def insert_meta_metrics(
    clickhouse,
    metrics: list[dict],
) -> int:
    """Batch insert mapped metrics into paid_edge.campaign_metrics."""
    if not metrics:
        return 0

    columns = [
        "tenant_id", "campaign_id", "platform", "platform_campaign_id",
        "platform_ad_group_id", "platform_ad_id", "date", "spend",
        "impressions", "clicks", "conversions", "leads", "ctr", "cpc",
        "cpm", "roas",
    ]
    data = [
        [m[col] for col in columns]
        for m in metrics
    ]
    clickhouse.insert(
        "paid_edge.campaign_metrics",
        data,
        column_names=columns,
    )
    return len(data)


# --- Insights client methods (mixin for MetaAdsClient) ---


class MetaInsightsMixin:
    """Insights query methods for MetaAdsClient."""

    async def get_campaign_insights(
        self,
        campaign_id: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        date_preset: str | None = None,
        time_increment: int | str = 1,
        level: str = "campaign",
        fields: list[str] | None = None,
        breakdowns: list[str] | None = None,
        action_breakdowns: list[str] | None = None,
        filtering: list[dict] | None = None,
        sort: str | None = None,
    ) -> list[dict]:
        """GET /{CAMPAIGN_ID}/insights or GET /act_{AD_ACCOUNT_ID}/insights"""
        import json

        path = (
            f"{campaign_id}/insights" if campaign_id
            else f"{self.ad_account_id}/insights"
        )

        default_fields = [
            "impressions", "reach", "clicks", "spend", "actions",
            "cost_per_action_type", "cpc", "cpm", "ctr", "frequency",
            "inline_link_clicks", "inline_link_click_ctr",
            "cost_per_inline_link_click", "conversions", "conversion_values",
        ]

        params = {
            "fields": ",".join(fields or default_fields),
            "time_increment": str(time_increment),
            "level": level,
        }

        if start_date and end_date:
            params["time_range"] = json.dumps({
                "since": start_date.isoformat(),
                "until": end_date.isoformat(),
            })
        elif date_preset:
            params["date_preset"] = date_preset

        if breakdowns:
            params["breakdowns"] = ",".join(breakdowns)
        if action_breakdowns:
            params["action_breakdowns"] = ",".join(action_breakdowns)
        if filtering:
            params["filtering"] = json.dumps(filtering)
        if sort:
            params["sort"] = sort

        return await self._paginate(path, params=params)

    async def get_insights_async(
        self,
        level: str,
        start_date: date,
        end_date: date,
        fields: list[str],
        breakdowns: list[str] | None = None,
    ) -> list[dict]:
        """Async report job for large queries."""
        import json

        params = {
            "fields": ",".join(fields),
            "level": level,
            "time_range": json.dumps({
                "since": start_date.isoformat(),
                "until": end_date.isoformat(),
            }),
        }
        if breakdowns:
            params["breakdowns"] = ",".join(breakdowns)

        # Step 1: Create async report
        resp = await self._request(
            "POST", f"{self.ad_account_id}/insights", data=params
        )
        report_run_id = resp.get("report_run_id", "")
        if not report_run_id:
            return []

        # Step 2: Poll until complete
        max_polls = 60
        for _ in range(max_polls):
            status_resp = await self._request(
                "GET", report_run_id,
                params={"fields": "async_status,async_percent_completion"},
            )
            status = status_resp.get("async_status", "")
            if status == "Job Completed":
                break
            if status == "Job Failed":
                logger.error("Async report %s failed", report_run_id)
                return []
            await asyncio.sleep(5)

        # Step 3: Fetch results
        return await self._paginate(f"{report_run_id}/insights")

    async def get_ad_set_insights(
        self,
        adset_id: str,
        start_date: date,
        end_date: date,
        time_increment: int = 1,
    ) -> list[dict]:
        """Ad set level insights."""
        return await self.get_campaign_insights(
            campaign_id=adset_id,
            start_date=start_date,
            end_date=end_date,
            time_increment=time_increment,
            level="adset",
        )

    async def get_ad_insights(
        self,
        ad_id: str,
        start_date: date,
        end_date: date,
        time_increment: int = 1,
    ) -> list[dict]:
        """Ad level insights."""
        return await self.get_campaign_insights(
            campaign_id=ad_id,
            start_date=start_date,
            end_date=end_date,
            time_increment=time_increment,
            level="ad",
        )
