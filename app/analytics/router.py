"""Analytics API endpoints (PEX-68).

Overview KPIs, campaign performance table, cross-platform comparison,
and time series. Named SQL queries in app/analytics/queries/*.sql.
"""

import logging
from datetime import date, timedelta
from pathlib import Path

from clickhouse_connect.driver import Client as CHClient
from fastapi import APIRouter, Depends, Query

from app.analytics.models import (
    CampaignPerformance,
    CampaignPerformanceResponse,
    KPITrend,
    OverviewResponse,
    PlatformBreakdown,
    PlatformComparisonResponse,
    TimeSeriesPoint,
    TimeSeriesResponse,
)
from app.dependencies import get_clickhouse, get_tenant
from app.tenants.models import Organization

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/analytics", tags=["analytics"])

_QUERIES_DIR = Path(__file__).parent / "queries"

GRANULARITY_FUNCTIONS = {
    "daily": "toDate",
    "weekly": "toStartOfWeek",
    "monthly": "toStartOfMonth",
}


def _load_sql(name: str) -> str:
    """Load a named SQL query from the queries directory."""
    return (_QUERIES_DIR / f"{name}.sql").read_text()


def _default_date_range() -> tuple[date, date]:
    """Default to last 30 days."""
    end = date.today()
    start = end - timedelta(days=30)
    return start, end


def _compute_trend(current: float, previous: float) -> KPITrend:
    """Build a KPITrend with % change."""
    if previous > 0:
        change_pct = round(((current - previous) / previous) * 100, 2)
    else:
        change_pct = None
    return KPITrend(value=current, previous_value=previous, change_pct=change_pct)


# --- 1. Overview KPIs ---


@router.get("/overview", response_model=OverviewResponse)
async def get_overview(
    start_date: date | None = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: date | None = Query(None, description="End date (YYYY-MM-DD)"),
    tenant: Organization = Depends(get_tenant),
    ch: CHClient = Depends(get_clickhouse),
):
    default_start, default_end = _default_date_range()
    start = start_date or default_start
    end = end_date or default_end

    # Current period
    sql = _load_sql("overview")
    result = ch.query(sql, parameters={"tid": tenant.id, "start": start, "end": end})
    current = list(result.named_results())
    cur = current[0] if current else {}

    # Previous period (same length, ending day before start)
    period_days = (end - start).days
    prev_start = start - timedelta(days=period_days)
    sql_prev = _load_sql("overview_trends")
    result_prev = ch.query(
        sql_prev,
        parameters={"tid": tenant.id, "prev_start": prev_start, "start": start},
    )
    prev_rows = list(result_prev.named_results())
    prev = prev_rows[0] if prev_rows else {}

    return OverviewResponse(
        total_spend=_compute_trend(
            float(cur.get("total_spend", 0)),
            float(prev.get("total_spend", 0)),
        ),
        avg_cac=_compute_trend(
            float(cur.get("avg_cac", 0)),
            float(prev.get("avg_cac", 0)),
        ),
        total_conversions=_compute_trend(
            float(cur.get("total_conversions", 0)),
            float(prev.get("total_conversions", 0)),
        ),
        total_leads=_compute_trend(
            float(cur.get("total_leads", 0)),
            float(prev.get("total_leads", 0)),
        ),
    )


# --- 2. Campaign performance table ---


@router.get("/campaigns", response_model=CampaignPerformanceResponse)
async def get_campaign_performance(
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    platform: str | None = Query(None, description="Filter by platform"),
    campaign_id: str | None = Query(None, description="Filter by campaign ID"),
    sort_by: str = Query("total_spend", description="Column to sort by"),
    sort_order: str = Query("desc", description="asc or desc"),
    tenant: Organization = Depends(get_tenant),
    ch: CHClient = Depends(get_clickhouse),
):
    default_start, default_end = _default_date_range()
    start = start_date or default_start
    end = end_date or default_end

    sql = _load_sql("campaign_performance")

    # Append optional filters
    params = {"tid": tenant.id, "start": start, "end": end}
    if platform:
        sql = sql.replace(
            "GROUP BY campaign_id, platform",
            "  AND platform = %(platform)s\nGROUP BY campaign_id, platform",
        )
        params["platform"] = platform
    if campaign_id:
        sql = sql.replace(
            "GROUP BY campaign_id, platform",
            "  AND campaign_id = %(campaign_id)s\nGROUP BY campaign_id, platform",
        )
        params["campaign_id"] = campaign_id

    result = ch.query(sql, parameters=params)

    campaigns = [
        CampaignPerformance(
            campaign_id=str(row["campaign_id"]),
            platform=row["platform"],
            total_spend=float(row["total_spend"]),
            total_impressions=int(row["total_impressions"]),
            total_clicks=int(row["total_clicks"]),
            total_conversions=int(row["total_conversions"]),
            total_leads=int(row["total_leads"]),
            ctr=float(row["ctr"]),
            cpc=float(row["cpc"]),
            cpm=float(row["cpm"]),
            cost_per_conversion=float(row["cost_per_conversion"]),
        )
        for row in result.named_results()
    ]

    # Sort in Python for flexibility (ClickHouse sorts by spend by default)
    reverse = sort_order.lower() != "asc"
    if campaigns and hasattr(campaigns[0], sort_by):
        campaigns.sort(key=lambda c: getattr(c, sort_by), reverse=reverse)

    return CampaignPerformanceResponse(campaigns=campaigns, total=len(campaigns))


# --- 3. Cross-platform comparison ---


@router.get("/platforms", response_model=PlatformComparisonResponse)
async def get_platform_comparison(
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    tenant: Organization = Depends(get_tenant),
    ch: CHClient = Depends(get_clickhouse),
):
    default_start, default_end = _default_date_range()
    start = start_date or default_start
    end = end_date or default_end

    sql = _load_sql("platform_comparison")
    result = ch.query(sql, parameters={"tid": tenant.id, "start": start, "end": end})

    platforms = [
        PlatformBreakdown(
            platform=row["platform"],
            campaign_count=int(row["campaign_count"]),
            total_spend=float(row["total_spend"]),
            total_impressions=int(row["total_impressions"]),
            total_clicks=int(row["total_clicks"]),
            total_conversions=int(row["total_conversions"]),
            total_leads=int(row["total_leads"]),
            ctr=float(row["ctr"]),
            cpc=float(row["cpc"]),
            cpm=float(row["cpm"]),
            cost_per_conversion=float(row["cost_per_conversion"]),
        )
        for row in result.named_results()
    ]

    return PlatformComparisonResponse(platforms=platforms)


# --- 4. Time series ---


@router.get("/timeseries", response_model=TimeSeriesResponse)
async def get_timeseries(
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    granularity: str = Query("daily", description="daily, weekly, or monthly"),
    tenant: Organization = Depends(get_tenant),
    ch: CHClient = Depends(get_clickhouse),
):
    default_start, default_end = _default_date_range()
    start = start_date or default_start
    end = end_date or default_end

    bucket_fn = GRANULARITY_FUNCTIONS.get(granularity, "toDate")
    sql_template = _load_sql("timeseries")
    # Replace the {bucket:Identifier} placeholder with the actual function name.
    # clickhouse_connect parameterized queries don't support Identifier params,
    # so we do a safe string replacement from our allowlist.
    sql = sql_template.replace("{bucket:Identifier}", bucket_fn)

    result = ch.query(sql, parameters={"tid": tenant.id, "start": start, "end": end})

    data = [
        TimeSeriesPoint(
            period=row["period"],
            spend=float(row["spend"]),
            impressions=int(row["impressions"]),
            clicks=int(row["clicks"]),
            conversions=int(row["conversions"]),
            leads=int(row["leads"]),
            ctr=float(row["ctr"]),
            cpc=float(row["cpc"]),
        )
        for row in result.named_results()
    ]

    return TimeSeriesResponse(granularity=granularity, data=data)
