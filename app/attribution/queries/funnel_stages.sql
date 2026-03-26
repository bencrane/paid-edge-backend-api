-- Attribution funnel: campaigns → leads → opportunities → closed-won
-- Returns per-campaign funnel counts and conversion rates.
SELECT
    cm.campaign_id,
    cm.platform,
    sum(cm.spend) AS total_spend,
    sum(cm.leads) AS lead_count,
    countDistinct(opp.opportunity_id) AS opportunity_count,
    countDistinct(
        CASE WHEN opp.is_won = 1 THEN opp.opportunity_id ELSE NULL END
    ) AS closed_won_count,
    if(sum(cm.leads) > 0,
       countDistinct(opp.opportunity_id) / sum(cm.leads), 0
    ) AS lead_to_opportunity_rate,
    if(countDistinct(opp.opportunity_id) > 0,
       countDistinct(CASE WHEN opp.is_won = 1 THEN opp.opportunity_id ELSE NULL END)
       / countDistinct(opp.opportunity_id), 0
    ) AS opportunity_to_won_rate
FROM campaign_metrics AS cm
LEFT JOIN crm_opportunities AS opp
    ON opp.tenant_id = cm.tenant_id
    AND opp.source_campaign_id = cm.campaign_id
WHERE cm.tenant_id = %(tid)s
  AND cm.date >= %(start)s
  AND cm.date <= %(end)s
GROUP BY cm.campaign_id, cm.platform
ORDER BY total_spend DESC
