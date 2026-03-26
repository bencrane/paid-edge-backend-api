-- Cost-per-closed-won by campaign.
-- spend / closed-won opportunities for each campaign.
SELECT
    cm.campaign_id,
    cm.platform,
    sum(cm.spend) AS total_spend,
    countDistinct(
        CASE WHEN opp.is_won = 1 THEN opp.opportunity_id ELSE NULL END
    ) AS closed_won_count,
    if(countDistinct(CASE WHEN opp.is_won = 1 THEN opp.opportunity_id ELSE NULL END) > 0,
       sum(cm.spend) / countDistinct(CASE WHEN opp.is_won = 1 THEN opp.opportunity_id ELSE NULL END),
       0
    ) AS cost_per_closed_won
FROM campaign_metrics AS cm
LEFT JOIN crm_opportunities AS opp
    ON opp.tenant_id = cm.tenant_id
    AND opp.source_campaign_id = cm.campaign_id
WHERE cm.tenant_id = %(tid)s
  AND cm.date >= %(start)s
  AND cm.date <= %(end)s
GROUP BY cm.campaign_id, cm.platform
ORDER BY cost_per_closed_won ASC
