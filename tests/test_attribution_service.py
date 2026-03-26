"""Tests for Attribution matching service (PEX-70)."""

from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

from app.attribution.service import (
    calculate_attribution_metrics,
    match_contacts_to_events,
    resolve_campaign_ids,
    run_attribution_for_tenant,
    update_opportunity_attribution,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TENANT_ID = "org-test-789"


def _mock_named_results(rows: list[dict]):
    """Return a mock CH result whose named_results() yields dicts."""
    mock_result = MagicMock()
    mock_result.named_results.return_value = rows
    return mock_result


# ---------------------------------------------------------------------------
# match_contacts_to_events
# ---------------------------------------------------------------------------


class TestMatchContactsToEvents:
    def test_returns_matches(self):
        rows = [
            {
                "contact_email": "jane@acme.com",
                "contact_id": "c-1",
                "utm_source": "linkedin",
                "utm_medium": "paid",
                "utm_campaign": "q1-campaign",
                "utm_content": "ad-variant-a",
                "click_id": "click-123",
                "first_touch_at": datetime(2026, 1, 15, 10, 0),
            },
        ]
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_named_results(rows)

        result = match_contacts_to_events(TENANT_ID, clickhouse=mock_ch)

        assert len(result) == 1
        assert result[0]["contact_email"] == "jane@acme.com"
        assert result[0]["utm_campaign"] == "q1-campaign"

    def test_passes_tenant_id_param(self):
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_named_results([])

        match_contacts_to_events(TENANT_ID, clickhouse=mock_ch)

        params = mock_ch.query.call_args[1]["parameters"]
        assert params["tid"] == TENANT_ID

    def test_empty_result(self):
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_named_results([])

        result = match_contacts_to_events(TENANT_ID, clickhouse=mock_ch)
        assert result == []


# ---------------------------------------------------------------------------
# resolve_campaign_ids
# ---------------------------------------------------------------------------


class TestResolveCampaignIds:
    def test_resolves_by_campaign_id(self):
        ch_rows = [
            {
                "campaign_id": "uuid-camp-1",
                "platform_campaign_id": "li-camp-100",
            },
        ]
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_named_results(ch_rows)

        result = resolve_campaign_ids(
            TENANT_ID, ["uuid-camp-1"], clickhouse=mock_ch,
        )

        assert result == {"uuid-camp-1": "uuid-camp-1"}

    def test_resolves_by_platform_campaign_id(self):
        ch_rows = [
            {
                "campaign_id": "uuid-camp-1",
                "platform_campaign_id": "li-camp-100",
            },
        ]
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_named_results(ch_rows)

        result = resolve_campaign_ids(
            TENANT_ID, ["li-camp-100"], clickhouse=mock_ch,
        )

        assert result == {"li-camp-100": "uuid-camp-1"}

    def test_unresolved_campaigns_excluded(self):
        ch_rows = [
            {
                "campaign_id": "uuid-camp-1",
                "platform_campaign_id": "li-camp-100",
            },
        ]
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_named_results(ch_rows)

        result = resolve_campaign_ids(
            TENANT_ID, ["unknown-campaign"], clickhouse=mock_ch,
        )

        assert result == {}

    def test_empty_utm_list_returns_empty(self):
        mock_ch = MagicMock()
        result = resolve_campaign_ids(TENANT_ID, [], clickhouse=mock_ch)
        assert result == {}
        mock_ch.query.assert_not_called()


# ---------------------------------------------------------------------------
# update_opportunity_attribution
# ---------------------------------------------------------------------------


class TestUpdateOpportunityAttribution:
    def test_updates_opportunities(self):
        contact_matches = [
            {
                "contact_email": "jane@acme.com",
                "contact_id": "c-1",
                "utm_source": "linkedin",
                "utm_medium": "paid",
                "utm_campaign": "q1-campaign",
                "utm_content": "ad-a",
                "click_id": "click-1",
            },
        ]
        campaign_mapping = {"q1-campaign": "uuid-camp-1"}

        # Mock the opportunity query
        opp_rows = [
            {
                "opportunity_id": "opp-1",
                "contact_email": "jane@acme.com",
                "opportunity_name": "Acme Deal",
                "company_domain": "acme.com",
                "company_name": "Acme Corp",
                "amount": Decimal("50000.00"),
                "stage": "proposal",
                "is_won": 0,
                "is_lost": 0,
                "close_date": None,
                "created_at": datetime(2026, 2, 1),
                "crm_source": "hubspot",
            },
        ]
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_named_results(opp_rows)

        result = update_opportunity_attribution(
            TENANT_ID, contact_matches, campaign_mapping, clickhouse=mock_ch,
        )

        assert result == 1
        mock_ch.insert.assert_called_once()
        insert_args = mock_ch.insert.call_args
        assert insert_args[0][0] == "crm_opportunities"
        # Verify source_campaign_id is in the row data
        row_data = insert_args[0][1][0]
        assert row_data[13] == "uuid-camp-1"  # source_campaign_id

    def test_skips_when_no_matches(self):
        mock_ch = MagicMock()
        result = update_opportunity_attribution(
            TENANT_ID, [], {}, clickhouse=mock_ch,
        )
        assert result == 0
        mock_ch.insert.assert_not_called()

    def test_skips_when_no_campaign_mapping(self):
        contact_matches = [
            {
                "contact_email": "jane@acme.com",
                "utm_campaign": "unknown",
                "utm_source": "",
                "utm_medium": "",
                "utm_content": "",
                "click_id": "",
            },
        ]
        mock_ch = MagicMock()
        result = update_opportunity_attribution(
            TENANT_ID, contact_matches, {}, clickhouse=mock_ch,
        )
        assert result == 0

    def test_first_touch_attribution(self):
        """First match for an email wins (first-touch model)."""
        contact_matches = [
            {
                "contact_email": "jane@acme.com",
                "contact_id": "c-1",
                "utm_source": "linkedin",
                "utm_medium": "paid",
                "utm_campaign": "q1-campaign",
                "utm_content": "ad-a",
                "click_id": "click-1",
            },
            {
                "contact_email": "jane@acme.com",
                "contact_id": "c-1",
                "utm_source": "meta",
                "utm_medium": "paid",
                "utm_campaign": "q2-campaign",
                "utm_content": "ad-b",
                "click_id": "click-2",
            },
        ]
        campaign_mapping = {
            "q1-campaign": "uuid-camp-1",
            "q2-campaign": "uuid-camp-2",
        }

        opp_rows = [
            {
                "opportunity_id": "opp-1",
                "contact_email": "jane@acme.com",
                "opportunity_name": "Deal",
                "company_domain": "acme.com",
                "company_name": "Acme",
                "amount": Decimal("10000"),
                "stage": "proposal",
                "is_won": 0,
                "is_lost": 0,
                "close_date": None,
                "created_at": datetime(2026, 2, 1),
                "crm_source": "hubspot",
            },
        ]
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_named_results(opp_rows)

        update_opportunity_attribution(
            TENANT_ID, contact_matches, campaign_mapping, clickhouse=mock_ch,
        )

        row_data = mock_ch.insert.call_args[0][1][0]
        # First match (q1-campaign → uuid-camp-1) should win
        assert row_data[13] == "uuid-camp-1"


# ---------------------------------------------------------------------------
# calculate_attribution_metrics
# ---------------------------------------------------------------------------


class TestCalculateAttributionMetrics:
    def test_calculates_metrics(self):
        opp_row = {
            "attributed_opportunities": 10,
            "pipeline_influenced": Decimal("500000.00"),
            "closed_won_value": Decimal("200000.00"),
            "closed_won_count": 3,
        }
        spend_row = {"total_spend": Decimal("15000.00")}

        mock_ch = MagicMock()
        mock_ch.query.side_effect = [
            _mock_named_results([opp_row]),
            _mock_named_results([spend_row]),
        ]

        result = calculate_attribution_metrics(TENANT_ID, clickhouse=mock_ch)

        assert result["attributed_opportunities"] == 10
        assert result["pipeline_influenced"] == 500000.0
        assert result["cost_per_opportunity"] == 1500.0  # 15000/10
        assert result["cost_per_closed_won"] == 5000.0  # 15000/3

    def test_empty_data(self):
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_named_results([])

        result = calculate_attribution_metrics(TENANT_ID, clickhouse=mock_ch)

        assert result["attributed_opportunities"] == 0
        assert result["pipeline_influenced"] == 0


# ---------------------------------------------------------------------------
# run_attribution_for_tenant (integration of all steps)
# ---------------------------------------------------------------------------


class TestRunAttributionForTenant:
    def test_skips_when_no_matches(self):
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_named_results([])

        result = run_attribution_for_tenant(TENANT_ID, clickhouse=mock_ch)

        assert result["status"] == "skipped_no_matches"
        assert result["matches_found"] == 0

    def test_full_pipeline(self):
        """Test the full pipeline with mocked ClickHouse calls."""
        # Call 1: match_contacts_to_events
        contact_rows = [
            {
                "contact_email": "jane@acme.com",
                "contact_id": "c-1",
                "utm_source": "linkedin",
                "utm_medium": "paid",
                "utm_campaign": "camp-1",
                "utm_content": "ad-a",
                "click_id": "click-1",
                "first_touch_at": datetime(2026, 1, 15),
            },
        ]
        # Call 2: resolve_campaign_ids
        campaign_rows = [
            {"campaign_id": "uuid-1", "platform_campaign_id": "camp-1"},
        ]
        # Call 3: update_opportunity_attribution (query for opps)
        opp_rows = [
            {
                "opportunity_id": "opp-1",
                "contact_email": "jane@acme.com",
                "opportunity_name": "Deal",
                "company_domain": "acme.com",
                "company_name": "Acme",
                "amount": Decimal("50000"),
                "stage": "proposal",
                "is_won": 0,
                "is_lost": 0,
                "close_date": None,
                "created_at": datetime(2026, 2, 1),
                "crm_source": "hubspot",
            },
        ]
        # Call 4: calculate_attribution_metrics (opps query)
        metrics_rows = [
            {
                "attributed_opportunities": 1,
                "pipeline_influenced": Decimal("50000"),
                "closed_won_value": Decimal("0"),
                "closed_won_count": 0,
            },
        ]
        # Call 5: calculate_attribution_metrics (spend query)
        spend_rows = [{"total_spend": Decimal("5000")}]

        mock_ch = MagicMock()
        mock_ch.query.side_effect = [
            _mock_named_results(contact_rows),   # match_contacts_to_events
            _mock_named_results(campaign_rows),   # resolve_campaign_ids
            _mock_named_results(opp_rows),        # update_opportunity_attribution
            _mock_named_results(metrics_rows),    # calculate_attribution_metrics (opps)
            _mock_named_results(spend_rows),      # calculate_attribution_metrics (spend)
        ]

        result = run_attribution_for_tenant(TENANT_ID, clickhouse=mock_ch)

        assert result["status"] == "success"
        assert result["matches_found"] == 1
        assert result["campaigns_resolved"] == 1
        assert result["opportunities_updated"] == 1
        assert result["metrics"]["attributed_opportunities"] == 1
        assert result["metrics"]["cost_per_opportunity"] == 5000.0


# ---------------------------------------------------------------------------
# Trigger task
# ---------------------------------------------------------------------------


class TestAttributionMatchTask:
    def test_task_iterates_tenants(self):
        import asyncio

        # Mock supabase to return tenants
        mock_supabase_result = MagicMock()
        mock_supabase_result.data = [
            {"id": "org-1"},
            {"id": "org-2"},
        ]
        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.execute.return_value = (
            mock_supabase_result
        )

        # Mock ClickHouse to return no matches (simplest path)
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_named_results([])

        with patch(
            "trigger.attribution_match.get_supabase_client",
            return_value=mock_supabase,
        ), patch(
            "trigger.attribution_match.get_clickhouse_client",
            return_value=mock_ch,
        ):
            from trigger.attribution_match import attribution_match_task

            results = asyncio.get_event_loop().run_until_complete(
                attribution_match_task()
            )

        assert len(results) == 2
        assert all(r["status"] == "skipped_no_matches" for r in results)

    def test_task_isolates_tenant_errors(self):
        import asyncio

        mock_supabase_result = MagicMock()
        mock_supabase_result.data = [
            {"id": "org-1"},
            {"id": "org-2"},
        ]
        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.execute.return_value = (
            mock_supabase_result
        )

        # Make the first tenant fail, second succeed
        call_count = {"n": 0}

        def mock_run(tenant_id, clickhouse=None):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise RuntimeError("Simulated failure")
            return {
                "tenant_id": tenant_id,
                "matches_found": 0,
                "opportunities_updated": 0,
                "status": "skipped_no_matches",
            }

        mock_ch = MagicMock()

        with patch(
            "trigger.attribution_match.get_supabase_client",
            return_value=mock_supabase,
        ), patch(
            "trigger.attribution_match.get_clickhouse_client",
            return_value=mock_ch,
        ), patch(
            "trigger.attribution_match.run_attribution_for_tenant",
            side_effect=mock_run,
        ):
            from trigger.attribution_match import attribution_match_task

            results = asyncio.get_event_loop().run_until_complete(
                attribution_match_task()
            )

        # Both tenants should have results (one error, one success)
        assert len(results) == 2
        assert results[0]["status"] == "error"
        assert results[1]["status"] == "skipped_no_matches"
