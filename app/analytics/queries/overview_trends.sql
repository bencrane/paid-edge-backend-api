-- Overview trends: compare current period KPIs to previous period of same length.
-- Previous period: same number of days ending the day before start_date.
SELECT
    sum(spend) AS total_spend,
    sum(conversions) AS total_conversions,
    sum(leads) AS total_leads,
    sum(clicks) AS total_clicks,
    sum(impressions) AS total_impressions,
    if(sum(conversions) > 0, sum(spend) / sum(conversions), 0) AS avg_cac
FROM campaign_metrics
WHERE tenant_id = %(tid)s
  AND date >= %(prev_start)s
  AND date < %(start)s
