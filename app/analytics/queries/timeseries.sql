-- Time series: aggregated metrics over time.
-- Granularity is controlled by the {bucket} placeholder (toDate, toStartOfWeek, toStartOfMonth).
SELECT
    {bucket:Identifier}(date) AS period,
    sum(spend) AS spend,
    sum(impressions) AS impressions,
    sum(clicks) AS clicks,
    sum(conversions) AS conversions,
    sum(leads) AS leads,
    if(sum(impressions) > 0, (sum(clicks) / sum(impressions)) * 100, 0) AS ctr,
    if(sum(clicks) > 0, sum(spend) / sum(clicks), 0) AS cpc
FROM campaign_metrics
WHERE tenant_id = %(tid)s
  AND date >= %(start)s
  AND date <= %(end)s
GROUP BY period
ORDER BY period ASC
