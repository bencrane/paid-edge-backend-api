"""Tests for SalesforceSyncer normalization and BaseCRMSyncer implementation (BJC-189)."""

from unittest.mock import AsyncMock

import pytest

from app.integrations.salesforce_syncer import (
    SalesforceSyncer,
    parse_sf_bool,
    parse_sf_date,
    parse_sf_datetime,
    parse_sf_float,
    parse_sf_int,
)


# --- Parsing helpers ---


class TestParseSfFloat:
    def test_valid_float(self):
        assert parse_sf_float(123.45) == 123.45

    def test_valid_string(self):
        assert parse_sf_float("100.0") == 100.0

    def test_none(self):
        assert parse_sf_float(None) is None

    def test_invalid(self):
        assert parse_sf_float("not_a_number") is None


class TestParseSfDate:
    def test_iso_date(self):
        d = parse_sf_date("2026-03-15")
        assert d is not None
        assert d.year == 2026
        assert d.month == 3
        assert d.day == 15

    def test_datetime_string(self):
        d = parse_sf_date("2026-03-15T10:00:00.000Z")
        assert d is not None
        assert d.day == 15

    def test_none(self):
        assert parse_sf_date(None) is None

    def test_empty(self):
        assert parse_sf_date("") is None


class TestParseSfDatetime:
    def test_iso_datetime_z(self):
        dt = parse_sf_datetime("2026-03-15T10:30:00.000Z")
        assert dt is not None
        assert dt.hour == 10
        assert dt.minute == 30

    def test_none(self):
        assert parse_sf_datetime(None) is None


class TestParseSfBool:
    def test_true_bool(self):
        assert parse_sf_bool(True) is True

    def test_false_bool(self):
        assert parse_sf_bool(False) is False

    def test_string_true(self):
        assert parse_sf_bool("true") is True

    def test_string_false(self):
        assert parse_sf_bool("false") is False

    def test_none(self):
        assert parse_sf_bool(None) is False


class TestParseSfInt:
    def test_valid(self):
        assert parse_sf_int(100) == 100

    def test_none(self):
        assert parse_sf_int(None) is None

    def test_invalid(self):
        assert parse_sf_int("abc") is None


# --- SalesforceSyncer ---


SAMPLE_SF_CONTACT = {
    "Id": "003ABC",
    "Email": "jane@acme.com",
    "FirstName": "Jane",
    "LastName": "Doe",
    "Account": {"Name": "Acme Corp"},
    "AccountId": "001XYZ",
    "LeadSource": "Web",
    "Title": "VP Marketing",
    "Phone": "555-1234",
    "OwnerId": "005OWNER",
    "CreatedDate": "2026-01-15T08:00:00.000Z",
    "LastModifiedDate": "2026-03-20T12:00:00.000Z",
}

SAMPLE_SF_OPPORTUNITY = {
    "Id": "006OPP",
    "Name": "Acme - Enterprise Deal",
    "Amount": 50000.0,
    "CloseDate": "2026-06-30",
    "StageName": "Negotiation",
    "ForecastCategoryName": "Pipeline",
    "IsClosed": False,
    "IsWon": False,
    "AccountId": "001XYZ",
    "LeadSource": "Partner",
    "OwnerId": "005OWNER",
    "CreatedDate": "2026-02-01T10:00:00.000Z",
    "LastModifiedDate": "2026-03-18T14:00:00.000Z",
}

SAMPLE_CONTACT_ROLES = [
    {"Id": "ocr-1", "ContactId": "003A", "OpportunityId": "006OPP", "Role": "Decision Maker"},
    {"Id": "ocr-2", "ContactId": "003B", "OpportunityId": "006OPP", "Role": "Influencer"},
]

SAMPLE_PIPELINE_DATA = {
    "object_name": "Opportunity",
    "field_name": "StageName",
    "stages": [
        {"label": "Prospecting", "value": "Prospecting", "defaultProbability": 10},
        {"label": "Closed Won", "value": "Closed Won", "isClosed": True, "isWon": True, "defaultProbability": 100},
    ],
}


@pytest.fixture
def mock_client():
    return AsyncMock()


@pytest.fixture
def syncer(mock_client):
    return SalesforceSyncer(engine_client=mock_client)


class TestNormalizeContact:
    def test_full_contact(self, syncer):
        contact = syncer._normalize_contact(SAMPLE_SF_CONTACT)

        assert contact.crm_contact_id == "003ABC"
        assert contact.email == "jane@acme.com"
        assert contact.first_name == "Jane"
        assert contact.last_name == "Doe"
        assert contact.company_name == "Acme Corp"
        assert contact.account_id == "001XYZ"
        assert contact.lead_source == "Web"
        assert contact.job_title == "VP Marketing"
        assert contact.phone == "555-1234"
        assert contact.owner_id == "005OWNER"
        assert contact.created_at is not None
        assert contact.updated_at is not None

    def test_minimal_contact(self, syncer):
        record = {"Id": "003MIN", "Email": "min@test.com"}
        contact = syncer._normalize_contact(record)

        assert contact.crm_contact_id == "003MIN"
        assert contact.email == "min@test.com"
        assert contact.first_name is None
        assert contact.company_name is None


class TestNormalizeOpportunity:
    def test_full_opportunity(self, syncer):
        opp = syncer._normalize_opportunity(
            SAMPLE_SF_OPPORTUNITY,
            contact_ids=["003A", "003B"],
        )

        assert opp.crm_opportunity_id == "006OPP"
        assert opp.name == "Acme - Enterprise Deal"
        assert opp.amount == 50000.0
        assert opp.close_date is not None
        assert opp.close_date.year == 2026
        assert opp.stage == "Negotiation"
        assert opp.pipeline == "Pipeline"
        assert opp.is_closed is False
        assert opp.is_won is False
        assert opp.contact_ids == ["003A", "003B"]

    def test_closed_won_opportunity(self, syncer):
        record = {**SAMPLE_SF_OPPORTUNITY, "IsClosed": True, "IsWon": True}
        opp = syncer._normalize_opportunity(record)

        assert opp.is_closed is True
        assert opp.is_won is True


class TestNormalizePipelineStage:
    def test_stage_normalization(self, syncer):
        stage_data = {
            "label": "Closed Won",
            "value": "Closed Won",
            "isClosed": True,
            "isWon": True,
            "defaultProbability": 100,
        }
        stage = syncer._normalize_pipeline_stage(stage_data, display_order=5)

        assert stage.stage_id == "Closed Won"
        assert stage.label == "Closed Won"
        assert stage.display_order == 5
        assert stage.is_closed is True
        assert stage.is_won is True
        assert stage.probability == 100.0


class TestPullContacts:
    async def test_pull_contacts_incremental(self, syncer, mock_client):
        mock_client.query_all.return_value = [SAMPLE_SF_CONTACT]

        contacts = await syncer.pull_contacts("cl-1", since="2026-03-01T00:00:00Z")

        assert len(contacts) == 1
        assert contacts[0].email == "jane@acme.com"

        # Verify SOQL contains WHERE clause
        soql_arg = mock_client.query_all.call_args[0][1]
        assert "LastModifiedDate >= 2026-03-01T00:00:00Z" in soql_arg

    async def test_pull_contacts_full_sync(self, syncer, mock_client):
        mock_client.query_all.return_value = []

        contacts = await syncer.pull_contacts("cl-1", since=None)

        assert contacts == []
        soql_arg = mock_client.query_all.call_args[0][1]
        assert "WHERE" not in soql_arg


class TestPullOpportunities:
    async def test_pull_opportunities_with_contact_roles(self, syncer, mock_client):
        mock_client.query_all.return_value = [SAMPLE_SF_OPPORTUNITY]
        mock_client.get_contact_roles.return_value = SAMPLE_CONTACT_ROLES

        opps = await syncer.pull_opportunities("cl-1", since="2026-03-01T00:00:00Z")

        assert len(opps) == 1
        assert opps[0].crm_opportunity_id == "006OPP"
        assert set(opps[0].contact_ids) == {"003A", "003B"}

    async def test_pull_opportunities_empty(self, syncer, mock_client):
        mock_client.query_all.return_value = []

        opps = await syncer.pull_opportunities("cl-1")

        assert opps == []
        mock_client.get_contact_roles.assert_not_called()

    async def test_pull_opportunities_contact_role_failure_graceful(self, syncer, mock_client):
        mock_client.query_all.return_value = [SAMPLE_SF_OPPORTUNITY]
        mock_client.get_contact_roles.side_effect = Exception("SOQL error")

        opps = await syncer.pull_opportunities("cl-1")

        assert len(opps) == 1
        assert opps[0].contact_ids == []  # Graceful fallback


class TestPullPipelineStages:
    async def test_pull_pipeline_stages(self, syncer, mock_client):
        mock_client.get_pipelines.return_value = SAMPLE_PIPELINE_DATA

        stages = await syncer.pull_pipeline_stages("cl-1")

        assert len(stages) == 2
        assert stages[0].stage_id == "Prospecting"
        assert stages[1].is_closed is True
        assert stages[1].is_won is True


class TestPushLead:
    async def test_push_lead(self, syncer, mock_client):
        mock_client.push_records.return_value = [{"Id": "003NEW"}]

        result = await syncer.push_lead(
            "cl-1",
            {"email": "new@test.com", "first_name": "New", "last_name": "Lead"},
            attribution={"source": "PaidEdge"},
        )

        assert result == "003NEW"
        call_args = mock_client.push_records.call_args
        records = call_args[1]["records"]
        assert records[0]["Email"] == "new@test.com"
        assert records[0]["LeadSource"] == "PaidEdge"


class TestCheckConnection:
    async def test_connected(self, syncer, mock_client):
        mock_client.get_connection.return_value = {"status": "connected"}
        assert await syncer.check_connection("cl-1") is True

    async def test_disconnected(self, syncer, mock_client):
        mock_client.get_connection.return_value = {"status": "disconnected"}
        assert await syncer.check_connection("cl-1") is False

    async def test_connection_error(self, syncer, mock_client):
        mock_client.get_connection.side_effect = Exception("network error")
        assert await syncer.check_connection("cl-1") is False
