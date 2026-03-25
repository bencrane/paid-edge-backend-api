"""Tests for LinkedIn Lead Gen Forms — BJC-139."""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.integrations.linkedin import LinkedInAdsClient
from app.integrations.linkedin_leads import (
    LinkedInLeadProcessor,
    build_demo_request_form,
    build_lead_magnet_form,
)

# --- Helpers ---


def _mock_resp(status_code: int, json_data: dict | None = None):
    """Create a mock httpx.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    resp.text = str(json_data)
    return resp


def _mock_supabase_chain(select_data=None):
    """Create a Supabase mock that supports select + update chains."""
    mock_supabase = MagicMock()
    mock_chain = MagicMock()
    mock_chain.eq.return_value = mock_chain
    mock_chain.maybe_single.return_value = mock_chain
    mock_chain.select.return_value = mock_chain
    mock_chain.update.return_value = mock_chain
    if select_data is not None:
        mock_chain.execute.return_value = SimpleNamespace(data=select_data)
    else:
        mock_chain.execute.return_value = SimpleNamespace(data=None)
    mock_supabase.table.return_value = mock_chain
    return mock_supabase, mock_chain


# --- Lead gen form creation ---


class TestCreateLeadGenForm:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_create_form_with_standard_fields(self, client):
        """Should POST /leadGenerationForms with correct payload."""
        questions = [
            {"fieldType": "FIRST_NAME", "required": True},
            {"fieldType": "LAST_NAME", "required": True},
            {"fieldType": "EMAIL", "required": True},
        ]
        expected_resp = {
            "id": 123456,
            "name": "Test Form",
        }
        client._get_headers = AsyncMock(
            return_value={"Authorization": "Bearer tok"}
        )
        client._client = AsyncMock()
        client._client.request.return_value = _mock_resp(200, expected_resp)

        result = await client.create_lead_gen_form(
            account_id=507404993,
            name="Test Form",
            headline="Get Your Guide",
            description="Download now",
            privacy_policy_url="https://example.com/privacy",
            thank_you_message="Thanks!",
            questions=questions,
        )

        assert result["id"] == 123456
        call_kwargs = client._client.request.call_args
        body = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert body["name"] == "Test Form"
        assert body["headline"] == "Get Your Guide"
        assert body["privacyPolicyUrl"] == "https://example.com/privacy"
        assert len(body["questions"]) == 3

    @pytest.mark.asyncio
    async def test_create_form_with_custom_fields(self, client):
        """Should include custom question with answerType."""
        questions = [
            {"fieldType": "EMAIL", "required": True},
            {
                "fieldType": "CUSTOM",
                "customQuestionText": "What is your biggest challenge?",
                "required": False,
                "answerType": "FREE_TEXT",
            },
        ]
        client._get_headers = AsyncMock(
            return_value={"Authorization": "Bearer tok"}
        )
        client._client = AsyncMock()
        client._client.request.return_value = _mock_resp(200, {"id": 999})

        await client.create_lead_gen_form(
            account_id=507404993,
            name="Custom Form",
            headline="Test",
            description="Test",
            privacy_policy_url="https://example.com/privacy",
            thank_you_message="Thanks!",
            questions=questions,
        )

        call_kwargs = client._client.request.call_args
        body = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        custom_q = body["questions"][1]
        assert custom_q["fieldType"] == "CUSTOM"
        assert custom_q["customQuestionText"] == "What is your biggest challenge?"
        assert custom_q["answerType"] == "FREE_TEXT"

    @pytest.mark.asyncio
    async def test_create_form_with_landing_page(self, client):
        """Should include thankYouLandingPageUrl when provided."""
        client._get_headers = AsyncMock(
            return_value={"Authorization": "Bearer tok"}
        )
        client._client = AsyncMock()
        client._client.request.return_value = _mock_resp(200, {"id": 888})

        await client.create_lead_gen_form(
            account_id=507404993,
            name="Form",
            headline="H",
            description="D",
            privacy_policy_url="https://example.com/privacy",
            thank_you_message="Thanks!",
            thank_you_landing_page_url="https://example.com/thank-you",
        )

        call_kwargs = client._client.request.call_args
        body = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert body["thankYouLandingPageUrl"] == "https://example.com/thank-you"

    @pytest.mark.asyncio
    async def test_create_form_without_optional_fields(self, client):
        """Should omit optional fields when not provided."""
        client._get_headers = AsyncMock(
            return_value={"Authorization": "Bearer tok"}
        )
        client._client = AsyncMock()
        client._client.request.return_value = _mock_resp(200, {"id": 777})

        await client.create_lead_gen_form(
            account_id=507404993,
            name="Minimal Form",
            headline="H",
            description="D",
            privacy_policy_url="https://example.com/privacy",
            thank_you_message="Thanks!",
        )

        call_kwargs = client._client.request.call_args
        body = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert "thankYouLandingPageUrl" not in body
        assert "questions" not in body


class TestListLeadGenForms:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_list_forms(self, client):
        """Should GET /leadGenerationForms with account filter."""
        elements = [
            {"id": 1, "name": "Form A"},
            {"id": 2, "name": "Form B"},
        ]
        client._get_headers = AsyncMock(
            return_value={"Authorization": "Bearer tok"}
        )
        client._client = AsyncMock()
        client._client.request.return_value = _mock_resp(
            200, {"elements": elements}
        )

        result = await client.list_lead_gen_forms(account_id=507404993)

        assert len(result) == 2
        call_kwargs = client._client.request.call_args
        params = call_kwargs.kwargs.get("params") or call_kwargs[1].get("params")
        assert params["q"] == "account"

    @pytest.mark.asyncio
    async def test_list_forms_empty(self, client):
        """Should return empty list when no forms exist."""
        client._get_headers = AsyncMock(
            return_value={"Authorization": "Bearer tok"}
        )
        client._client = AsyncMock()
        client._client.request.return_value = _mock_resp(
            200, {"elements": []}
        )

        result = await client.list_lead_gen_forms(account_id=507404993)

        assert result == []


class TestGetLeadGenForm:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_get_form(self, client):
        """Should GET /leadGenerationForms/{id}."""
        expected = {"id": 123, "name": "My Form", "headline": "Test"}
        client._get_headers = AsyncMock(
            return_value={"Authorization": "Bearer tok"}
        )
        client._client = AsyncMock()
        client._client.request.return_value = _mock_resp(200, expected)

        result = await client.get_lead_gen_form(form_id=123)

        assert result["name"] == "My Form"
        call_args = client._client.request.call_args
        assert "/leadGenerationForms/123" in call_args[0][1]


# --- Lead submission retrieval ---


class TestGetLeadSubmissions:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_get_submissions(self, client):
        """Should GET /leadFormResponses with correct params."""
        submissions = [
            {
                "submittedAt": 1700000000000,
                "answers": [
                    {"fieldType": "EMAIL", "value": "user@test.com"},
                ],
            },
        ]
        client._get_headers = AsyncMock(
            return_value={"Authorization": "Bearer tok"}
        )
        client._client = AsyncMock()
        client._client.request.return_value = _mock_resp(
            200, {"elements": submissions}
        )

        result = await client.get_lead_submissions(
            account_id=507404993, form_id=123456
        )

        assert len(result) == 1
        call_kwargs = client._client.request.call_args
        params = call_kwargs.kwargs.get("params") or call_kwargs[1].get("params")
        assert params["q"] == "owner"
        assert "507404993" in params["owner"]
        assert "123456" in params["leadGenerationForm"]

    @pytest.mark.asyncio
    async def test_get_submissions_with_incremental_polling(self, client):
        """Should include submittedAfter param for incremental sync."""
        client._get_headers = AsyncMock(
            return_value={"Authorization": "Bearer tok"}
        )
        client._client = AsyncMock()
        client._client.request.return_value = _mock_resp(
            200, {"elements": []}
        )

        await client.get_lead_submissions(
            account_id=507404993,
            form_id=123456,
            submitted_after=1700000000000,
        )

        call_kwargs = client._client.request.call_args
        params = call_kwargs.kwargs.get("params") or call_kwargs[1].get("params")
        assert params["submittedAfter"] == "1700000000000"

    @pytest.mark.asyncio
    async def test_get_submissions_empty(self, client):
        """Should return empty list when no submissions."""
        client._get_headers = AsyncMock(
            return_value={"Authorization": "Bearer tok"}
        )
        client._client = AsyncMock()
        client._client.request.return_value = _mock_resp(
            200, {"elements": []}
        )

        result = await client.get_lead_submissions(
            account_id=507404993, form_id=123456
        )

        assert result == []


# --- Answer parsing ---


class TestParseLeadAnswers:
    @pytest.mark.asyncio
    async def test_parse_standard_fields(self):
        """Should map standard field types to snake_case keys."""
        processor = LinkedInLeadProcessor(
            client=AsyncMock(), supabase=MagicMock()
        )
        answers = [
            {"fieldType": "FIRST_NAME", "value": "John"},
            {"fieldType": "LAST_NAME", "value": "Doe"},
            {"fieldType": "EMAIL", "value": "john@example.com"},
            {"fieldType": "COMPANY_NAME", "value": "Acme Corp"},
            {"fieldType": "JOB_TITLE", "value": "VP Marketing"},
        ]

        result = await processor.parse_lead_answers(answers)

        assert result["first_name"] == "John"
        assert result["last_name"] == "Doe"
        assert result["email"] == "john@example.com"
        assert result["company_name"] == "Acme Corp"
        assert result["job_title"] == "VP Marketing"

    @pytest.mark.asyncio
    async def test_parse_custom_fields(self):
        """Should use custom question text as key."""
        processor = LinkedInLeadProcessor(
            client=AsyncMock(), supabase=MagicMock()
        )
        answers = [
            {"fieldType": "EMAIL", "value": "user@test.com"},
            {
                "fieldType": "CUSTOM",
                "customQuestionText": "What is your biggest challenge?",
                "value": "Scaling",
            },
        ]

        result = await processor.parse_lead_answers(answers)

        assert result["email"] == "user@test.com"
        assert result["what_is_your_biggest_challenge"] == "Scaling"

    @pytest.mark.asyncio
    async def test_parse_empty_answers(self):
        """Should return empty dict for no answers."""
        processor = LinkedInLeadProcessor(
            client=AsyncMock(), supabase=MagicMock()
        )
        result = await processor.parse_lead_answers([])
        assert result == {}


# --- Form templates ---


class TestFormTemplates:
    def test_build_lead_magnet_form(self):
        """Should create standard lead magnet form."""
        config = {
            "asset_title": "B2B Guide 2026",
            "privacy_policy_url": "https://example.com/privacy",
        }
        form = build_lead_magnet_form(config)

        assert form["name"] == "LM: B2B Guide 2026"
        assert form["headline"] == "Get Your Free Guide"
        assert form["privacyPolicyUrl"] == "https://example.com/privacy"
        assert len(form["questions"]) == 5
        field_types = [q["fieldType"] for q in form["questions"]]
        assert "FIRST_NAME" in field_types
        assert "EMAIL" in field_types
        assert "COMPANY_NAME" in field_types

    def test_build_lead_magnet_form_custom_headline(self):
        """Should use custom headline when provided."""
        config = {
            "asset_title": "Report",
            "privacy_policy_url": "https://example.com/privacy",
            "headline": "Download the Report",
        }
        form = build_lead_magnet_form(config)
        assert form["headline"] == "Download the Report"

    def test_build_demo_request_form(self):
        """Should create demo request form with phone."""
        config = {
            "product_name": "PaidEdge",
            "privacy_policy_url": "https://example.com/privacy",
        }
        form = build_demo_request_form(config)

        assert form["name"] == "Demo: PaidEdge"
        assert form["headline"] == "Request a Demo"
        field_types = [q["fieldType"] for q in form["questions"]]
        assert "PHONE_NUMBER" in field_types
        assert "CUSTOM" not in field_types

    def test_build_demo_request_form_with_custom_question(self):
        """Should add custom question when provided."""
        config = {
            "product_name": "PaidEdge",
            "privacy_policy_url": "https://example.com/privacy",
            "custom_question": "How many employees?",
        }
        form = build_demo_request_form(config)

        field_types = [q["fieldType"] for q in form["questions"]]
        assert "CUSTOM" in field_types
        custom_q = [
            q for q in form["questions"] if q["fieldType"] == "CUSTOM"
        ][0]
        assert custom_q["customQuestionText"] == "How many employees?"
        assert custom_q["answerType"] == "FREE_TEXT"


# --- Lead processor sync flow ---


class TestSyncLeads:
    @pytest.mark.asyncio
    async def test_sync_leads_success(self):
        """Should pull, parse, and return new leads."""
        mock_client = AsyncMock(spec=LinkedInAdsClient)
        mock_client.get_lead_submissions.return_value = [
            {
                "submittedAt": 1700000000000,
                "associatedEntity": "urn:li:sponsoredCampaign:100",
                "answers": [
                    {"fieldType": "FIRST_NAME", "value": "John"},
                    {"fieldType": "EMAIL", "value": "john@test.com"},
                ],
            },
            {
                "submittedAt": 1700000001000,
                "associatedEntity": "urn:li:sponsoredCampaign:200",
                "answers": [
                    {"fieldType": "FIRST_NAME", "value": "Jane"},
                    {"fieldType": "EMAIL", "value": "jane@test.com"},
                ],
            },
        ]

        mock_supabase, mock_chain = _mock_supabase_chain(
            select_data={"config": {}}
        )

        processor = LinkedInLeadProcessor(
            client=mock_client, supabase=mock_supabase
        )

        result = await processor.sync_leads(
            tenant_id="tenant-1",
            account_id=507404993,
            form_id=123456,
        )

        assert result["leads_processed"] == 2
        assert len(result["new_leads"]) == 2
        assert result["new_leads"][0]["first_name"] == "John"
        assert result["new_leads"][1]["email"] == "jane@test.com"
        assert result["errors"] == []

    @pytest.mark.asyncio
    async def test_sync_leads_with_since(self):
        """Should pass submitted_after for incremental polling."""
        mock_client = AsyncMock(spec=LinkedInAdsClient)
        mock_client.get_lead_submissions.return_value = []

        mock_supabase, _ = _mock_supabase_chain(
            select_data={"config": {}}
        )

        processor = LinkedInLeadProcessor(
            client=mock_client, supabase=mock_supabase
        )

        since = datetime(2026, 3, 24, 0, 0, 0, tzinfo=UTC)
        await processor.sync_leads(
            tenant_id="tenant-1",
            account_id=507404993,
            form_id=123456,
            since=since,
        )

        mock_client.get_lead_submissions.assert_called_once_with(
            account_id=507404993,
            form_id=123456,
            submitted_after=int(since.timestamp() * 1000),
        )

    @pytest.mark.asyncio
    async def test_sync_leads_updates_last_synced(self):
        """Should update last_synced_at in provider_configs."""
        mock_client = AsyncMock(spec=LinkedInAdsClient)
        mock_client.get_lead_submissions.return_value = [
            {
                "submittedAt": 1700000000000,
                "answers": [
                    {"fieldType": "EMAIL", "value": "x@test.com"},
                ],
            },
        ]

        mock_supabase, mock_chain = _mock_supabase_chain(
            select_data={"config": {}}
        )

        processor = LinkedInLeadProcessor(
            client=mock_client, supabase=mock_supabase
        )

        await processor.sync_leads(
            tenant_id="tenant-1",
            account_id=507404993,
            form_id=123456,
        )

        # Verify update was called with lead_sync timestamp
        update_call = mock_chain.update.call_args[0][0]
        assert "lead_sync" in update_call["config"]
        assert "123456" in update_call["config"]["lead_sync"]
        assert "last_synced_at" in update_call["config"]["lead_sync"]["123456"]

    @pytest.mark.asyncio
    async def test_sync_leads_empty(self):
        """Should handle no submissions gracefully."""
        mock_client = AsyncMock(spec=LinkedInAdsClient)
        mock_client.get_lead_submissions.return_value = []

        mock_supabase, _ = _mock_supabase_chain(
            select_data={"config": {}}
        )

        processor = LinkedInLeadProcessor(
            client=mock_client, supabase=mock_supabase
        )

        result = await processor.sync_leads(
            tenant_id="tenant-1",
            account_id=507404993,
            form_id=123456,
        )

        assert result["leads_processed"] == 0
        assert result["new_leads"] == []


# --- Lead sync task ---


class TestLinkedInLeadSyncTask:
    @pytest.mark.asyncio
    async def test_full_sync_flow(self):
        """Should discover tenants and sync leads per tenant."""
        from trigger.linkedin_lead_sync import linkedin_lead_sync_task

        mock_supabase = MagicMock()
        mock_chain = MagicMock()
        mock_chain.select.return_value = mock_chain
        mock_chain.eq.return_value = mock_chain
        mock_chain.execute.return_value = SimpleNamespace(
            data=[
                {
                    "organization_id": "org-1",
                    "config": {"lead_gen_forms": []},
                },
            ]
        )
        mock_supabase.table.return_value = mock_chain

        with patch(
            "trigger.linkedin_lead_sync.get_supabase_client",
            return_value=mock_supabase,
        ):
            results = await linkedin_lead_sync_task()

        assert len(results) == 1
        assert results[0]["status"] == "skipped_no_forms"

    @pytest.mark.asyncio
    async def test_per_tenant_error_isolation(self):
        """One tenant failure should not stop other tenants."""
        from trigger.linkedin_lead_sync import linkedin_lead_sync_task

        mock_supabase = MagicMock()
        mock_chain = MagicMock()
        mock_chain.select.return_value = mock_chain
        mock_chain.eq.return_value = mock_chain
        mock_chain.execute.return_value = SimpleNamespace(
            data=[
                {"organization_id": "org-fail", "config": {}},
                {"organization_id": "org-ok", "config": {}},
            ]
        )
        mock_supabase.table.return_value = mock_chain

        call_count = 0

        async def mock_sync(tenant_config, supabase):
            nonlocal call_count
            call_count += 1
            if tenant_config["organization_id"] == "org-fail":
                raise RuntimeError("Connection failed")
            return {
                "task": "linkedin_lead_sync",
                "tenant_id": "org-ok",
                "leads_processed": 0,
                "status": "skipped_no_forms",
            }

        with (
            patch(
                "trigger.linkedin_lead_sync.get_supabase_client",
                return_value=mock_supabase,
            ),
            patch(
                "trigger.linkedin_lead_sync.sync_tenant_leads",
                side_effect=mock_sync,
            ),
        ):
            results = await linkedin_lead_sync_task()

        assert call_count == 2
        assert len(results) == 2
        statuses = [r["status"] for r in results]
        assert "error" in statuses
        assert "skipped_no_forms" in statuses

    @pytest.mark.asyncio
    async def test_no_tenants(self):
        """Should handle empty tenant list."""
        from trigger.linkedin_lead_sync import linkedin_lead_sync_task

        mock_supabase = MagicMock()
        mock_chain = MagicMock()
        mock_chain.select.return_value = mock_chain
        mock_chain.eq.return_value = mock_chain
        mock_chain.execute.return_value = SimpleNamespace(data=[])
        mock_supabase.table.return_value = mock_chain

        with patch(
            "trigger.linkedin_lead_sync.get_supabase_client",
            return_value=mock_supabase,
        ):
            results = await linkedin_lead_sync_task()

        assert results == []
