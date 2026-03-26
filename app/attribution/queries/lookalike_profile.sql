-- Lookalike profile: firmographic breakdown of closed-won companies.
-- Feeds back into audience building for targeting similar companies.
SELECT
    opp.company_domain,
    opp.company_name,
    count() AS deal_count,
    sum(opp.amount) AS total_revenue,
    avg(opp.amount) AS avg_deal_size
FROM crm_opportunities AS opp
WHERE opp.tenant_id = %(tid)s
  AND opp.is_won = 1
  AND opp.source_campaign_id IS NOT NULL
  AND opp.created_at >= %(start)s
  AND opp.created_at <= %(end)s
GROUP BY opp.company_domain, opp.company_name
ORDER BY total_revenue DESC
