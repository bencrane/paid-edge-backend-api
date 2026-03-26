-- Pipeline influenced: total pipeline $ from PaidEdge campaigns.
-- Shows total deal value attributed to each campaign via source_campaign_id.
SELECT
    opp.source_campaign_id AS campaign_id,
    countDistinct(opp.opportunity_id) AS opportunity_count,
    sum(opp.amount) AS pipeline_value,
    sumIf(opp.amount, opp.is_won = 1) AS closed_won_value,
    countDistinct(
        CASE WHEN opp.is_won = 1 THEN opp.opportunity_id ELSE NULL END
    ) AS closed_won_count
FROM crm_opportunities AS opp
WHERE opp.tenant_id = %(tid)s
  AND opp.source_campaign_id IS NOT NULL
  AND opp.created_at >= %(start)s
  AND opp.created_at <= %(end)s
GROUP BY opp.source_campaign_id
ORDER BY pipeline_value DESC
