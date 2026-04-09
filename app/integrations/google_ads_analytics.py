"""Google Ads GAQL analytics client + metrics mapping to ClickHouse (BJC-150).

Builds GAQL queries, fetches campaign/ad-group/keyword metrics via search_stream,
flattens protobuf results, and maps them to the ClickHouse campaign_metrics schema.
"""

import logging
import time
from datetime import date

from google.protobuf.json_format import MessageToDict

from app.integrations.google_ads import GoogleAdsService, micros_to_dollars

logger = logging.getLogger(__name__)


# --- ClickHouse field mapping ---

CLICKHOUSE_MAPPING = {
    "campaign.id": "provider_campaign_id",
    "campaign.name": "campaign_name",
    "segments.date": "metric_date",
    "metrics.impressions": "impressions",
    "metrics.clicks": "clicks",
    "metrics.cost_micros": "spend",
    "metrics.conversions": "conversions",
    "metrics.conversions_value": "conversion_value",
    "metrics.ctr": "ctr",
    "metrics.average_cpc": "cpc",
    "metrics.average_cpm": "cpm",
    "metrics.cost_per_conversion": "cost_per_conversion",
}

# Fields that are in micros and need conversion to dollars
MICROS_FIELDS = {
    "metrics.cost_micros",
    "metrics.average_cpc",
    "metrics.average_cpm",
    "metrics.cost_per_conversion",
    "metrics.conversions_value",
}


class GoogleAdsAnalyticsClient:
    """Constructs GAQL queries and fetches campaign performance metrics."""

    CAMPAIGN_METRICS_FIELDS = [
        "campaign.id",
        "campaign.name",
        "campaign.status",
        "campaign.advertising_channel_type",
        "metrics.impressions",
        "metrics.clicks",
        "metrics.cost_micros",
        "metrics.conversions",
        "metrics.conversions_value",
        "metrics.all_conversions",
        "metrics.ctr",
        "metrics.average_cpc",
        "metrics.average_cpm",
        "metrics.cost_per_conversion",
        "metrics.video_views",
        "metrics.interactions",
        "segments.date",
    ]

    AD_GROUP_METRICS_FIELDS = [
        "ad_group.id",
        "ad_group.name",
        "ad_group.status",
        "ad_group.campaign",
        "metrics.impressions",
        "metrics.clicks",
        "metrics.cost_micros",
        "metrics.conversions",
        "metrics.ctr",
        "metrics.average_cpc",
        "segments.date",
    ]

    KEYWORD_METRICS_FIELDS = [
        "ad_group_criterion.keyword.text",
        "ad_group_criterion.keyword.match_type",
        "ad_group_criterion.quality_info.quality_score",
        "metrics.impressions",
        "metrics.clicks",
        "metrics.cost_micros",
        "metrics.conversions",
        "metrics.ctr",
        "metrics.average_cpc",
        "segments.date",
    ]

    def __init__(self, service: GoogleAdsService):
        self.service = service

    async def fetch_campaign_metrics(
        self,
        start_date: date,
        end_date: date,
        campaign_ids: list[str] | None = None,
    ) -> list[dict]:
        """Fetch campaign-level metrics for a date range."""
        query = self._build_query(
            resource="campaign",
            fields=self.CAMPAIGN_METRICS_FIELDS,
            start_date=start_date,
            end_date=end_date,
            conditions=self._campaign_filter(campaign_ids),
        )
        start_time = time.time()
        results = await self._execute_query(query)
        elapsed_ms = int((time.time() - start_time) * 1000)
        logger.info(
            "Fetched %d campaign metric rows (%dms, %s to %s)",
            len(results),
            elapsed_ms,
            start_date,
            end_date,
        )
        return results

    async def fetch_ad_group_metrics(
        self,
        start_date: date,
        end_date: date,
        campaign_ids: list[str] | None = None,
    ) -> list[dict]:
        """Fetch ad group-level metrics."""
        query = self._build_query(
            resource="ad_group",
            fields=self.AD_GROUP_METRICS_FIELDS,
            start_date=start_date,
            end_date=end_date,
            conditions=self._campaign_filter(campaign_ids),
        )
        return await self._execute_query(query)

    async def fetch_keyword_metrics(
        self,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """Fetch keyword-level metrics (unique to Google Ads)."""
        query = self._build_query(
            resource="keyword_view",
            fields=self.KEYWORD_METRICS_FIELDS,
            start_date=start_date,
            end_date=end_date,
            conditions=["ad_group_criterion.status != 'REMOVED'"],
        )
        return await self._execute_query(query)

    def _build_query(
        self,
        resource: str,
        fields: list[str],
        start_date: date,
        end_date: date,
        conditions: list[str] | None = None,
    ) -> str:
        """Construct a GAQL query string."""
        select = ", ".join(fields)
        where_clauses = [
            f"segments.date >= '{start_date.isoformat()}'",
            f"segments.date <= '{end_date.isoformat()}'",
        ]
        if conditions:
            where_clauses.extend(conditions)
        where = " AND ".join(where_clauses)
        query = f"SELECT {select} FROM {resource} WHERE {where}"
        logger.debug("Built GAQL query: %s", query)
        return query

    async def _execute_query(self, query: str) -> list[dict]:
        """Execute GAQL query via search_stream and return flattened results."""
        rows = await self.service.search_stream(query)
        results = []
        for row in rows:
            results.append(self._flatten_row(row))
        return results

    @staticmethod
    def _flatten_row(row) -> dict:
        """Convert a protobuf row to a flat dict with dotted keys."""
        try:
            raw = MessageToDict(row._pb, preserving_proto_field_name=True)
        except (AttributeError, TypeError):
            # If the row is already a dict (e.g. in tests), return it
            if isinstance(row, dict):
                return row
            return {}
        return GoogleAdsAnalyticsClient._flatten_dict(raw)

    @staticmethod
    def _flatten_dict(d: dict, prefix: str = "") -> dict:
        """Recursively flatten nested dict with dotted keys."""
        items = {}
        for k, v in d.items():
            key = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict):
                items.update(
                    GoogleAdsAnalyticsClient._flatten_dict(v, key)
                )
            else:
                items[key] = v
        return items

    @staticmethod
    def _campaign_filter(campaign_ids: list[str] | None) -> list[str]:
        """Build campaign ID filter conditions."""
        if not campaign_ids:
            return []
        ids = ", ".join(str(cid) for cid in campaign_ids)
        return [f"campaign.id IN ({ids})"]


def map_metrics_to_clickhouse(
    org_id: str,
    raw_metrics: list[dict],
) -> list[dict]:
    """Map GAQL results to paid_edge.campaign_metrics ClickHouse schema.

    Converts micros to dollars for all monetary fields.
    """
    from datetime import datetime, timezone

    rows = []
    synced_at = datetime.now(timezone.utc).isoformat()

    for m in raw_metrics:
        row = {
            "tenant_id": org_id,
            "provider": "google_ads",
            "synced_at": synced_at,
        }

        for gaql_field, ch_column in CLICKHOUSE_MAPPING.items():
            value = m.get(gaql_field, 0)
            # Convert micros → dollars for monetary fields
            if gaql_field in MICROS_FIELDS and isinstance(value, (int, float)):
                value = micros_to_dollars(int(value))
            row[ch_column] = value

        # Ensure provider_campaign_id is a string
        row["provider_campaign_id"] = str(row.get("provider_campaign_id", ""))

        rows.append(row)

    return rows


async def write_metrics_to_clickhouse(
    clickhouse,
    rows: list[dict],
) -> int:
    """Insert metric rows into ClickHouse.

    Uses ReplacingMergeTree semantics — upsert by
    (tenant_id, provider, provider_campaign_id, metric_date).
    Returns number of rows written.
    """
    if not rows:
        return 0

    columns = list(rows[0].keys())
    col_list = ", ".join(columns)
    values_list = []
    for row in rows:
        vals = []
        for col in columns:
            v = row[col]
            if isinstance(v, str):
                vals.append(f"'{v}'")
            elif v is None:
                vals.append("0")
            else:
                vals.append(str(v))
        values_list.append(f"({', '.join(vals)})")

    values_sql = ", ".join(values_list)
    query = (
        f"INSERT INTO paid_edge.campaign_metrics ({col_list}) VALUES {values_sql}"
    )

    clickhouse.command(query)
    logger.info("Wrote %d metric rows to ClickHouse", len(rows))
    return len(rows)
