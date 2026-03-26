"""Attribution API endpoints (PEX-67).

Funnel view, cost-per-opportunity, cost-per-closed-won, pipeline influenced,
and lookalike profile. All query ClickHouse paid_engine_x_api database.
"""

import logging
from datetime import date, timedelta
from pathlib import Path

from clickhouse_connect.driver import Client as CHClient
from fastapi import APIRouter, Depends, Query

from app.attribution.models import (
    CostPerClosedWon,
    CostPerClosedWonResponse,
    CostPerOpportunity,
    CostPerOpportunityResponse,
    FunnelResponse,
    FunnelStage,
    FunnelTotals,
    LookalikeCompany,
    LookalikeProfileResponse,
    PipelineCampaign,
    PipelineInfluencedResponse,
)
from app.dependencies import get_clickhouse, get_tenant
from app.tenants.models import Organization

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/attribution", tags=["attribution"])

_QUERIES_DIR = Path(__file__).parent / "queries"


def _load_sql(name: str) -> str:
    """Load a named SQL query from the queries directory."""
    return (_QUERIES_DIR / f"{name}.sql").read_text()


def _default_date_range() -> tuple[date, date]:
    """Default to last 30 days."""
    end = date.today()
    start = end - timedelta(days=30)
    return start, end


# --- 1. Funnel view ---


@router.get("/funnel", response_model=FunnelResponse)
async def get_funnel(
    start_date: date | None = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: date | None = Query(None, description="End date (YYYY-MM-DD)"),
    tenant: Organization = Depends(get_tenant),
    ch: CHClient = Depends(get_clickhouse),
):
    default_start, default_end = _default_date_range()
    start = start_date or default_start
    end = end_date or default_end

    sql = _load_sql("funnel_stages")
    result = ch.query(sql, parameters={"tid": tenant.id, "start": start, "end": end})

    campaigns = []
    for row in result.named_results():
        campaigns.append(FunnelStage(
            campaign_id=str(row["campaign_id"]),
            platform=row["platform"],
            total_spend=float(row["total_spend"]),
            lead_count=int(row["lead_count"]),
            opportunity_count=int(row["opportunity_count"]),
            closed_won_count=int(row["closed_won_count"]),
            lead_to_opportunity_rate=float(row["lead_to_opportunity_rate"]),
            opportunity_to_won_rate=float(row["opportunity_to_won_rate"]),
        ))

    total_spend = sum(c.total_spend for c in campaigns)
    total_leads = sum(c.lead_count for c in campaigns)
    total_opps = sum(c.opportunity_count for c in campaigns)
    total_won = sum(c.closed_won_count for c in campaigns)

    totals = FunnelTotals(
        total_spend=total_spend,
        total_leads=total_leads,
        total_opportunities=total_opps,
        total_closed_won=total_won,
        overall_lead_to_opp_rate=total_opps / total_leads if total_leads > 0 else 0,
        overall_opp_to_won_rate=total_won / total_opps if total_opps > 0 else 0,
    )

    return FunnelResponse(campaigns=campaigns, totals=totals)


# --- 2. Cost per opportunity ---


@router.get("/cost-per-opportunity", response_model=CostPerOpportunityResponse)
async def get_cost_per_opportunity(
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    tenant: Organization = Depends(get_tenant),
    ch: CHClient = Depends(get_clickhouse),
):
    default_start, default_end = _default_date_range()
    start = start_date or default_start
    end = end_date or default_end

    sql = _load_sql("cost_per_opportunity")
    result = ch.query(sql, parameters={"tid": tenant.id, "start": start, "end": end})

    campaigns = [
        CostPerOpportunity(
            campaign_id=str(row["campaign_id"]),
            platform=row["platform"],
            total_spend=float(row["total_spend"]),
            opportunity_count=int(row["opportunity_count"]),
            cost_per_opportunity=float(row["cost_per_opportunity"]),
        )
        for row in result.named_results()
    ]
    return CostPerOpportunityResponse(campaigns=campaigns)


# --- 3. Cost per closed-won ---


@router.get("/cost-per-closed-won", response_model=CostPerClosedWonResponse)
async def get_cost_per_closed_won(
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    tenant: Organization = Depends(get_tenant),
    ch: CHClient = Depends(get_clickhouse),
):
    default_start, default_end = _default_date_range()
    start = start_date or default_start
    end = end_date or default_end

    sql = _load_sql("cost_per_closed_won")
    result = ch.query(sql, parameters={"tid": tenant.id, "start": start, "end": end})

    campaigns = [
        CostPerClosedWon(
            campaign_id=str(row["campaign_id"]),
            platform=row["platform"],
            total_spend=float(row["total_spend"]),
            closed_won_count=int(row["closed_won_count"]),
            cost_per_closed_won=float(row["cost_per_closed_won"]),
        )
        for row in result.named_results()
    ]
    return CostPerClosedWonResponse(campaigns=campaigns)


# --- 4. Pipeline influenced ---


@router.get("/pipeline-influenced", response_model=PipelineInfluencedResponse)
async def get_pipeline_influenced(
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    tenant: Organization = Depends(get_tenant),
    ch: CHClient = Depends(get_clickhouse),
):
    default_start, default_end = _default_date_range()
    start = start_date or default_start
    end = end_date or default_end

    sql = _load_sql("pipeline_influenced")
    result = ch.query(sql, parameters={"tid": tenant.id, "start": start, "end": end})

    campaigns = []
    for row in result.named_results():
        campaigns.append(PipelineCampaign(
            campaign_id=str(row["campaign_id"]),
            opportunity_count=int(row["opportunity_count"]),
            pipeline_value=float(row["pipeline_value"]),
            closed_won_value=float(row["closed_won_value"]),
            closed_won_count=int(row["closed_won_count"]),
        ))

    total_pipeline = sum(c.pipeline_value for c in campaigns)
    total_closed_won = sum(c.closed_won_value for c in campaigns)

    return PipelineInfluencedResponse(
        campaigns=campaigns,
        total_pipeline_value=total_pipeline,
        total_closed_won_value=total_closed_won,
    )


# --- 5. Lookalike profile ---


@router.get("/lookalike-profile", response_model=LookalikeProfileResponse)
async def get_lookalike_profile(
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    tenant: Organization = Depends(get_tenant),
    ch: CHClient = Depends(get_clickhouse),
):
    default_start, default_end = _default_date_range()
    start = start_date or default_start
    end = end_date or default_end

    sql = _load_sql("lookalike_profile")
    result = ch.query(sql, parameters={"tid": tenant.id, "start": start, "end": end})

    companies = [
        LookalikeCompany(
            company_domain=row["company_domain"],
            company_name=row["company_name"],
            deal_count=int(row["deal_count"]),
            total_revenue=float(row["total_revenue"]),
            avg_deal_size=float(row["avg_deal_size"]),
        )
        for row in result.named_results()
    ]

    total_revenue = sum(c.total_revenue for c in companies)

    return LookalikeProfileResponse(
        companies=companies,
        total_companies=len(companies),
        total_revenue=total_revenue,
    )
