-- Campaign performance table: all campaigns with aggregated metrics.
SELECT
    campaign_id,
    platform,
    sum(spend) AS total_spend,
    sum(impressions) AS total_impressions,
    sum(clicks) AS total_clicks,
    sum(conversions) AS total_conversions,
    sum(leads) AS total_leads,
    if(sum(impressions) > 0, (sum(clicks) / sum(impressions)) * 100, 0) AS ctr,
    if(sum(clicks) > 0, sum(spend) / sum(clicks), 0) AS cpc,
    if(sum(impressions) > 0, (sum(spend) / sum(impressions)) * 1000, 0) AS cpm,
    if(sum(conversions) > 0, sum(spend) / sum(conversions), 0) AS cost_per_conversion
FROM campaign_metrics
WHERE tenant_id = %(tid)s
  AND date >= %(start)s
  AND date <= %(end)s
GROUP BY campaign_id, platform
ORDER BY total_spend DESC
