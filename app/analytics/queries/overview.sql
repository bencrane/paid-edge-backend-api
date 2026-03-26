-- Analytics overview: KPI summary.
-- Returns total spend, avg CAC (cost-per-conversion), total conversions, pipeline influenced.
SELECT
    sum(spend) AS total_spend,
    sum(conversions) AS total_conversions,
    sum(leads) AS total_leads,
    sum(clicks) AS total_clicks,
    sum(impressions) AS total_impressions,
    if(sum(conversions) > 0, sum(spend) / sum(conversions), 0) AS avg_cac,
    if(sum(clicks) > 0, sum(spend) / sum(clicks), 0) AS avg_cpc,
    if(sum(impressions) > 0, (sum(clicks) / sum(impressions)) * 100, 0) AS avg_ctr
FROM campaign_metrics
WHERE tenant_id = %(tid)s
  AND date >= %(start)s
  AND date <= %(end)s
