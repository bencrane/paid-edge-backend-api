-- Cost-per-opportunity by campaign.
-- spend / matched opportunities for each campaign.
SELECT
    cm.campaign_id,
    cm.platform,
    sum(cm.spend) AS total_spend,
    countDistinct(opp.opportunity_id) AS opportunity_count,
    if(countDistinct(opp.opportunity_id) > 0,
       sum(cm.spend) / countDistinct(opp.opportunity_id), 0
    ) AS cost_per_opportunity
FROM campaign_metrics AS cm
LEFT JOIN crm_opportunities AS opp
    ON opp.tenant_id = cm.tenant_id
    AND opp.source_campaign_id = cm.campaign_id
WHERE cm.tenant_id = %(tid)s
  AND cm.date >= %(start)s
  AND cm.date <= %(end)s
GROUP BY cm.campaign_id, cm.platform
ORDER BY cost_per_opportunity ASC
