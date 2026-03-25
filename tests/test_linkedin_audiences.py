"""Tests for LinkedIn Matched Audiences — DMP segment management + streaming upload (BJC-134)."""

import hashlib
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.integrations.linkedin import (
    LinkedInAdsClient,
    LinkedInAPIError,
    LinkedInPermissionError,
    hash_email_for_linkedin,
    make_account_urn,
)
from app.integrations.linkedin_models import LinkedInDMPSegment

# --- Helpers ---


def _mock_resp(
    status_code: int,
    json_data: dict | None = None,
):
    """Create a mock httpx.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    resp.text = str(json_data)
    return resp


# --- Email hashing utility ---


class TestHashEmailForLinkedIn:
    def test_basic_email(self):
        """Should produce correct SHA256 for a simple email."""
        result = hash_email_for_linkedin("test@example.com")
        expected = hashlib.sha256(
            b"test@example.com"
        ).hexdigest()
        assert result == expected

    def test_uppercase_is_lowered(self):
        """Should lowercase before hashing."""
        result = hash_email_for_linkedin("Test@Example.COM")
        expected = hashlib.sha256(
            b"test@example.com"
        ).hexdigest()
        assert result == expected

    def test_whitespace_is_stripped(self):
        """Should strip whitespace before hashing."""
        result = hash_email_for_linkedin("  test@example.com  ")
        expected = hashlib.sha256(
            b"test@example.com"
        ).hexdigest()
        assert result == expected

    def test_lowercase_then_strip_order(self):
        """Order: lowercase then strip. Both operations are idempotent
        so order doesn't affect result, but verify both are applied."""
        result = hash_email_for_linkedin("  TEST@EXAMPLE.COM  ")
        expected = hashlib.sha256(
            b"test@example.com"
        ).hexdigest()
        assert result == expected

    def test_known_sha256_vector(self):
        """Test against a known SHA256 test vector."""
        # SHA256 of "password" is a well-known hash
        result = hash_email_for_linkedin("password")
        expected = (
            "5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8"
        )
        assert result == expected

    def test_empty_string(self):
        """Empty string (after strip) should still produce valid hash."""
        result = hash_email_for_linkedin("  ")
        expected = hashlib.sha256(b"").hexdigest()
        assert result == expected


# --- DMP Segment creation ---


class TestCreateDMPSegment:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_create_company_segment(self, client):
        """Should POST /dmpSegments with COMPANY type."""
        api_resp = {
            "id": "urn:li:dmpSegment:12345",
            "name": "Q2 Target Companies",
            "type": "COMPANY",
            "status": "BUILDING",
            "account": make_account_urn(507404993),
        }

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=_mock_resp(200, api_resp),
            ) as mock_req,
        ):
            result = await client.create_dmp_segment(
                account_id=507404993,
                name="Q2 Target Companies",
                segment_type="COMPANY",
            )

        assert result["id"] == "urn:li:dmpSegment:12345"
        assert result["status"] == "BUILDING"
        body = mock_req.call_args.kwargs["json"]
        assert body["name"] == "Q2 Target Companies"
        assert body["type"] == "COMPANY"
        assert body["account"] == make_account_urn(507404993)
        assert body["sources"] == ["FIRST_PARTY"]

    @pytest.mark.asyncio
    async def test_create_user_segment(self, client):
        """Should POST /dmpSegments with USER type."""
        api_resp = {
            "id": "urn:li:dmpSegment:67890",
            "type": "USER",
            "status": "BUILDING",
        }

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=_mock_resp(200, api_resp),
            ) as mock_req,
        ):
            result = await client.create_dmp_segment(
                account_id=1,
                name="Email List",
                segment_type="USER",
            )

        assert result["type"] == "USER"
        body = mock_req.call_args.kwargs["json"]
        assert body["type"] == "USER"


# --- Stream companies ---


class TestStreamCompanies:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_stream_companies_single_batch(self, client):
        """Should POST companies with correct element structure."""
        companies = [
            {"companyName": "Acme Corp", "companyDomain": "acme.com"},
            {"companyName": "Globex", "companyDomain": "globex.com"},
        ]

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=_mock_resp(200, {}),
            ) as mock_req,
        ):
            result = await client.stream_companies(
                segment_id="seg-1", companies=companies
            )

        assert result["total_sent"] == 2
        assert result["batches_completed"] == 1
        assert result["errors"] == []

        body = mock_req.call_args.kwargs["json"]
        assert len(body["elements"]) == 2
        el0 = body["elements"][0]
        assert el0["action"] == "ADD"
        assert el0["companyIdentifiers"]["companyName"] == "Acme Corp"
        assert el0["companyIdentifiers"]["companyDomain"] == "acme.com"

    @pytest.mark.asyncio
    async def test_stream_companies_remove_action(self, client):
        """Should use REMOVE action when specified."""
        companies = [
            {"companyName": "Old Corp", "companyDomain": "old.com"},
        ]

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=_mock_resp(200, {}),
            ) as mock_req,
        ):
            await client.stream_companies(
                segment_id="seg-1",
                companies=companies,
                action="REMOVE",
            )

        body = mock_req.call_args.kwargs["json"]
        assert body["elements"][0]["action"] == "REMOVE"

    @pytest.mark.asyncio
    async def test_stream_companies_auto_batching(self, client):
        """Should split into multiple batches at 5000 items."""
        companies = [
            {"companyName": f"Co-{i}", "companyDomain": f"co{i}.com"}
            for i in range(7500)
        ]

        call_count = 0

        async def request_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return _mock_resp(200, {})

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                side_effect=request_side_effect,
            ),
            patch("app.integrations.linkedin.asyncio.sleep", new_callable=AsyncMock),
        ):
            result = await client.stream_companies(
                segment_id="seg-1", companies=companies
            )

        assert result["total_sent"] == 7500
        assert result["batches_completed"] == 2
        # 2 POST calls (5000 + 2500)
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_stream_companies_no_action_key_in_identifiers(self, client):
        """The _action tag should not leak into companyIdentifiers."""
        companies = [
            {"companyName": "Test", "companyDomain": "test.com"},
        ]

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=_mock_resp(200, {}),
            ) as mock_req,
        ):
            await client.stream_companies(
                segment_id="seg-1", companies=companies
            )

        body = mock_req.call_args.kwargs["json"]
        identifiers = body["elements"][0]["companyIdentifiers"]
        assert "_action" not in identifiers


# --- Stream contacts ---


class TestStreamContacts:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_stream_contacts_hashes_emails(self, client):
        """Should hash emails with SHA256 before sending."""
        emails = ["test@example.com", "USER@EXAMPLE.COM"]

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=_mock_resp(200, {}),
            ) as mock_req,
        ):
            result = await client.stream_contacts(
                segment_id="seg-1", emails=emails
            )

        assert result["total_sent"] == 2
        body = mock_req.call_args.kwargs["json"]
        el0 = body["elements"][0]
        assert el0["action"] == "ADD"
        hashed = el0["userIdentifiers"]["hashedEmail"]
        assert hashed["hashType"] == "SHA256"
        expected_hash = hashlib.sha256(
            b"test@example.com"
        ).hexdigest()
        assert hashed["hashValue"] == expected_hash

        # Second email should be lowercased before hashing
        el1 = body["elements"][1]
        hashed1 = el1["userIdentifiers"]["hashedEmail"]
        expected_hash_upper = hashlib.sha256(
            b"user@example.com"
        ).hexdigest()
        assert hashed1["hashValue"] == expected_hash_upper

    @pytest.mark.asyncio
    async def test_stream_contacts_remove_action(self, client):
        """Should use REMOVE action when specified."""
        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=_mock_resp(200, {}),
            ) as mock_req,
        ):
            await client.stream_contacts(
                segment_id="seg-1",
                emails=["a@b.com"],
                action="REMOVE",
            )

        body = mock_req.call_args.kwargs["json"]
        assert body["elements"][0]["action"] == "REMOVE"

    @pytest.mark.asyncio
    async def test_stream_contacts_auto_batching(self, client):
        """Should split into batches at 5000 contacts."""
        emails = [f"user{i}@example.com" for i in range(6000)]

        call_count = 0

        async def request_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return _mock_resp(200, {})

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                side_effect=request_side_effect,
            ),
            patch("app.integrations.linkedin.asyncio.sleep", new_callable=AsyncMock),
        ):
            result = await client.stream_contacts(
                segment_id="seg-1", emails=emails
            )

        assert result["total_sent"] == 6000
        assert result["batches_completed"] == 2
        assert call_count == 2


# --- Batch upload delay ---


class TestBatchUploadDelay:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_delay_between_batches(self, client):
        """Should sleep between batches for rate limiting."""
        companies = [
            {"companyName": f"Co-{i}", "companyDomain": f"co{i}.com"}
            for i in range(7500)
        ]

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=_mock_resp(200, {}),
            ),
            patch(
                "app.integrations.linkedin.asyncio.sleep",
                new_callable=AsyncMock,
            ) as mock_sleep,
        ):
            await client.stream_companies(
                segment_id="seg-1", companies=companies
            )

        # Sleep called once between 2 batches (not after last)
        mock_sleep.assert_called_once_with(0.2)

    @pytest.mark.asyncio
    async def test_no_delay_for_single_batch(self, client):
        """Should not sleep when data fits in a single batch."""
        companies = [
            {"companyName": "Co", "companyDomain": "co.com"},
        ]

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=_mock_resp(200, {}),
            ),
            patch(
                "app.integrations.linkedin.asyncio.sleep",
                new_callable=AsyncMock,
            ) as mock_sleep,
        ):
            await client.stream_companies(
                segment_id="seg-1", companies=companies
            )

        mock_sleep.assert_not_called()


# --- Segment status ---


class TestGetDMPSegmentStatus:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_returns_parsed_segment(self, client):
        """Should return LinkedInDMPSegment with all fields."""
        api_resp = {
            "id": "urn:li:dmpSegment:12345",
            "name": "Q2 Companies",
            "type": "COMPANY",
            "status": "READY",
            "matchedMemberCount": 1500,
            "destinationSegmentId": "urn:li:adSegment:99999",
            "account": make_account_urn(507404993),
        }

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=_mock_resp(200, api_resp),
            ),
        ):
            segment = await client.get_dmp_segment_status(
                "urn:li:dmpSegment:12345"
            )

        assert isinstance(segment, LinkedInDMPSegment)
        assert segment.id == "urn:li:dmpSegment:12345"
        assert segment.name == "Q2 Companies"
        assert segment.type == "COMPANY"
        assert segment.status == "READY"
        assert segment.matched_member_count == 1500
        assert segment.destination_segment_id == "urn:li:adSegment:99999"

    @pytest.mark.asyncio
    async def test_building_status_no_match_count(self, client):
        """BUILDING status should have no matched count yet."""
        api_resp = {
            "id": "urn:li:dmpSegment:111",
            "name": "New Segment",
            "type": "USER",
            "status": "BUILDING",
            "account": "urn:li:sponsoredAccount:1",
        }

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=_mock_resp(200, api_resp),
            ),
        ):
            segment = await client.get_dmp_segment_status(
                "urn:li:dmpSegment:111"
            )

        assert segment.status == "BUILDING"
        assert segment.matched_member_count is None


# --- Segment polling ---


class TestWaitForSegmentReady:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_returns_on_ready(self, client):
        """Should return immediately when segment is READY."""
        ready_resp = _mock_resp(
            200,
            {
                "id": "seg-1",
                "name": "Test",
                "type": "COMPANY",
                "status": "READY",
                "matchedMemberCount": 500,
                "account": "urn:li:sponsoredAccount:1",
            },
        )

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=ready_resp,
            ),
            patch(
                "app.integrations.linkedin.asyncio.sleep",
                new_callable=AsyncMock,
            ) as mock_sleep,
        ):
            segment = await client.wait_for_segment_ready(
                "seg-1", max_wait_minutes=1
            )

        assert segment.status == "READY"
        # Should not sleep since first poll returns READY
        mock_sleep.assert_not_called()

    @pytest.mark.asyncio
    async def test_polls_until_ready(self, client):
        """Should poll multiple times until status changes to READY."""
        building_resp = _mock_resp(
            200,
            {
                "id": "seg-1",
                "name": "Test",
                "type": "USER",
                "status": "BUILDING",
                "account": "urn:li:sponsoredAccount:1",
            },
        )
        ready_resp = _mock_resp(
            200,
            {
                "id": "seg-1",
                "name": "Test",
                "type": "USER",
                "status": "READY",
                "matchedMemberCount": 800,
                "account": "urn:li:sponsoredAccount:1",
            },
        )

        responses = iter([building_resp, building_resp, ready_resp])

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                side_effect=lambda *a, **kw: next(responses),
            ),
            patch(
                "app.integrations.linkedin.asyncio.sleep",
                new_callable=AsyncMock,
            ) as mock_sleep,
        ):
            segment = await client.wait_for_segment_ready(
                "seg-1", max_wait_minutes=5, poll_interval_seconds=30
            )

        assert segment.status == "READY"
        assert mock_sleep.call_count == 2
        mock_sleep.assert_called_with(30)

    @pytest.mark.asyncio
    async def test_returns_on_failed(self, client):
        """Should return immediately on FAILED status."""
        failed_resp = _mock_resp(
            200,
            {
                "id": "seg-1",
                "name": "Bad",
                "type": "USER",
                "status": "FAILED",
                "account": "urn:li:sponsoredAccount:1",
            },
        )

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=failed_resp,
            ),
        ):
            segment = await client.wait_for_segment_ready("seg-1")

        assert segment.status == "FAILED"

    @pytest.mark.asyncio
    async def test_timeout_returns_last_status(self, client):
        """Should return last status on timeout without error."""
        building_resp = _mock_resp(
            200,
            {
                "id": "seg-1",
                "name": "Test",
                "type": "COMPANY",
                "status": "BUILDING",
                "account": "urn:li:sponsoredAccount:1",
            },
        )

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=building_resp,
            ),
            patch(
                "app.integrations.linkedin.asyncio.sleep",
                new_callable=AsyncMock,
            ),
        ):
            # Very short timeout: 0 minutes → only the final GET
            segment = await client.wait_for_segment_ready(
                "seg-1", max_wait_minutes=0
            )

        assert segment.status == "BUILDING"


# --- Delete and list segments ---


class TestDeleteDMPSegment:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_delete_segment(self, client):
        """Should DELETE /dmpSegments/{id}."""
        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=_mock_resp(204),
            ) as mock_req,
        ):
            await client.delete_dmp_segment("urn:li:dmpSegment:12345")

        assert mock_req.call_args.args[0] == "DELETE"
        url = mock_req.call_args.args[1]
        assert "/dmpSegments/urn:li:dmpSegment:12345" in url


class TestListDMPSegments:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_list_segments(self, client):
        """Should GET /dmpSegments with account filter and parse results."""
        api_resp = {
            "elements": [
                {
                    "id": "urn:li:dmpSegment:111",
                    "name": "Company List",
                    "type": "COMPANY",
                    "status": "READY",
                    "matchedMemberCount": 1200,
                    "destinationSegmentId": "urn:li:adSegment:aaa",
                    "account": make_account_urn(507404993),
                },
                {
                    "id": "urn:li:dmpSegment:222",
                    "name": "Email List",
                    "type": "USER",
                    "status": "BUILDING",
                    "account": make_account_urn(507404993),
                },
            ]
        }

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=_mock_resp(200, api_resp),
            ) as mock_req,
        ):
            segments = await client.list_dmp_segments(
                account_id=507404993
            )

        assert len(segments) == 2
        assert isinstance(segments[0], LinkedInDMPSegment)
        assert segments[0].name == "Company List"
        assert segments[0].status == "READY"
        assert segments[0].matched_member_count == 1200
        assert segments[1].type == "USER"
        assert segments[1].status == "BUILDING"

        params = mock_req.call_args.kwargs["params"]
        assert params["q"] == "account"
        assert params["account"] == make_account_urn(507404993)

    @pytest.mark.asyncio
    async def test_list_segments_empty(self, client):
        """Should return empty list when no segments exist."""
        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=_mock_resp(200, {"elements": []}),
            ),
        ):
            segments = await client.list_dmp_segments(account_id=1)

        assert segments == []


# --- Error handling ---


class TestAudienceErrors:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_audience_size_too_small(self, client):
        """Should raise LinkedInAPIError for AUDIENCE_SIZE_TOO_SMALL."""
        error_resp = _mock_resp(
            400,
            {
                "status": 400,
                "serviceErrorCode": 100,
                "message": "AUDIENCE_SIZE_TOO_SMALL: "
                "Minimum 300 matched members required",
            },
        )

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=error_resp,
            ),
        ):
            with pytest.raises(
                LinkedInAPIError,
                match="AUDIENCE_SIZE_TOO_SMALL",
            ):
                await client.create_dmp_segment(
                    account_id=1,
                    name="Tiny Segment",
                    segment_type="COMPANY",
                )

    @pytest.mark.asyncio
    async def test_missing_rw_dmp_segments_scope(self, client):
        """Should raise LinkedInPermissionError for missing scope."""
        error_resp = _mock_resp(
            403,
            {
                "status": 403,
                "serviceErrorCode": 100,
                "message": "Not enough permissions to access: "
                "POST /dmpSegments. Required scope: rw_dmp_segments",
            },
        )

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=error_resp,
            ),
        ):
            with pytest.raises(
                LinkedInPermissionError,
                match="rw_dmp_segments",
            ):
                await client.create_dmp_segment(
                    account_id=1,
                    name="Test",
                    segment_type="COMPANY",
                )

    @pytest.mark.asyncio
    async def test_batch_upload_partial_failure(self, client):
        """Should capture errors from individual batches."""
        companies = [
            {"companyName": f"Co-{i}", "companyDomain": f"co{i}.com"}
            for i in range(7500)
        ]

        call_count = 0

        async def request_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                # Second batch fails
                return _mock_resp(
                    400,
                    {
                        "status": 400,
                        "message": "Invalid company data",
                    },
                )
            return _mock_resp(200, {})

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                side_effect=request_side_effect,
            ),
            patch("app.integrations.linkedin.asyncio.sleep", new_callable=AsyncMock),
        ):
            result = await client.stream_companies(
                segment_id="seg-1", companies=companies
            )

        # First batch succeeded (5000), second failed (2500)
        assert result["total_sent"] == 5000
        assert result["batches_completed"] == 1
        assert len(result["errors"]) == 1
        assert "Invalid company data" in result["errors"][0]
