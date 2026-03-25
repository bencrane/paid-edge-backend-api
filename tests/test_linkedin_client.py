"""Tests for LinkedIn Ads base client and account discovery (BJC-130)."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.integrations.linkedin import (
    LinkedInAdsClient,
    LinkedInAPIError,
    LinkedInNotFoundError,
    LinkedInPermissionError,
    LinkedInRateLimitError,
    LinkedInVersionError,
    extract_id_from_urn,
    make_account_urn,
    make_campaign_urn,
    make_org_urn,
)
from app.integrations.linkedin_models import LinkedInAdAccount, LinkedInCampaignGroup

# --- URN utilities ---


class TestURNUtilities:
    def test_extract_id_from_sponsored_account_urn(self):
        assert extract_id_from_urn("urn:li:sponsoredAccount:507404993") == 507404993

    def test_extract_id_from_campaign_urn(self):
        assert extract_id_from_urn("urn:li:sponsoredCampaign:12345") == 12345

    def test_extract_id_from_person_urn(self):
        assert extract_id_from_urn("urn:li:person:99999") == 99999

    def test_make_account_urn(self):
        assert make_account_urn(507404993) == "urn:li:sponsoredAccount:507404993"

    def test_make_campaign_urn(self):
        assert make_campaign_urn(12345) == "urn:li:sponsoredCampaign:12345"

    def test_make_org_urn(self):
        assert make_org_urn(789) == "urn:li:organization:789"


# --- Base client ---


class TestLinkedInAdsClient:
    @pytest.fixture
    def mock_supabase(self):
        return MagicMock()

    @pytest.fixture
    def client(self, mock_supabase):
        return LinkedInAdsClient(org_id="org-1", supabase=mock_supabase)

    @pytest.mark.asyncio
    async def test_get_headers_includes_required_headers(self, client):
        """All requests include required LinkedIn headers."""
        with patch(
            "app.integrations.linkedin.get_valid_linkedin_token",
            new_callable=AsyncMock,
            return_value="test-token",
        ):
            headers = await client._get_headers()

        assert headers["Authorization"] == "Bearer test-token"
        assert headers["LinkedIn-Version"] == "202603"
        assert headers["X-Restli-Protocol-Version"] == "2.0.0"
        assert headers["Content-Type"] == "application/json"

    @pytest.mark.asyncio
    async def test_get_request(self, client):
        """GET convenience method should work correctly."""
        mock_resp = _mock_httpx_response(200, {"elements": []})

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="test-token",
            ),
            patch.object(client._client, "request", new_callable=AsyncMock, return_value=mock_resp),
        ):
            result = await client.get("/adAccounts", params={"q": "search"})

        assert result == {"elements": []}

    @pytest.mark.asyncio
    async def test_post_request(self, client):
        """POST convenience method should work correctly."""
        mock_resp = _mock_httpx_response(200, {"id": "urn:li:adCampaignGroup:123"})

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="test-token",
            ),
            patch.object(client._client, "request", new_callable=AsyncMock, return_value=mock_resp),
        ):
            result = await client.post("/adAccounts/123/adCampaignGroups", json={"name": "Test"})

        assert result["id"] == "urn:li:adCampaignGroup:123"

    @pytest.mark.asyncio
    async def test_delete_request(self, client):
        """DELETE convenience method should return None."""
        mock_resp = _mock_httpx_response(204)

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="test-token",
            ),
            patch.object(client._client, "request", new_callable=AsyncMock, return_value=mock_resp),
        ):
            result = await client.delete("/some/resource")

        assert result is None

    @pytest.mark.asyncio
    async def test_context_manager(self, mock_supabase):
        """Client should work as an async context manager."""
        async with LinkedInAdsClient("org-1", mock_supabase) as client:
            assert client.org_id == "org-1"


# --- Rate limit retry ---


class TestRateLimitRetry:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_retries_on_429_then_succeeds(self, client):
        """Should retry with backoff on 429 and succeed on subsequent attempt."""
        resp_429 = _mock_httpx_response(429, {"message": "Rate limit"})
        resp_200 = _mock_httpx_response(200, {"ok": True})

        call_count = 0

        async def mock_request(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                return resp_429
            return resp_200

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="test-token",
            ),
            patch.object(client._client, "request", side_effect=mock_request),
            patch("app.integrations.linkedin.asyncio.sleep", new_callable=AsyncMock),
        ):
            result = await client.get("/test")

        assert result == {"ok": True}
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_raises_after_max_retries(self, client):
        """Should raise LinkedInRateLimitError after max retries exhausted."""
        resp_429 = _mock_httpx_response(429, {"message": "Rate limit"})

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="test-token",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=resp_429,
            ),
            patch("app.integrations.linkedin.asyncio.sleep", new_callable=AsyncMock),
        ):
            with pytest.raises(LinkedInRateLimitError):
                await client.get("/test")

    @pytest.mark.asyncio
    async def test_backoff_delays_are_exponential(self, client):
        """Backoff should follow 2s, 4s, 8s, 16s, 32s pattern."""
        resp_429 = _mock_httpx_response(429, {"message": "Rate limit"})

        sleep_calls = []

        async def mock_sleep(seconds):
            sleep_calls.append(seconds)

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="test-token",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=resp_429,
            ),
            patch("app.integrations.linkedin.asyncio.sleep", side_effect=mock_sleep),
        ):
            with pytest.raises(LinkedInRateLimitError):
                await client.get("/test")

        assert sleep_calls == [2, 4, 8, 16, 32]


# --- Error handling ---


class TestErrorHandling:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_403_raises_permission_error(self, client):
        resp = _mock_httpx_response(
            403, {"status": 403, "serviceErrorCode": 100, "message": "Forbidden"}
        )

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="test-token",
            ),
            patch.object(client._client, "request", new_callable=AsyncMock, return_value=resp),
        ):
            with pytest.raises(LinkedInPermissionError):
                await client.get("/test")

    @pytest.mark.asyncio
    async def test_404_raises_not_found_error(self, client):
        resp = _mock_httpx_response(
            404, {"status": 404, "message": "Not found"}
        )

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="test-token",
            ),
            patch.object(client._client, "request", new_callable=AsyncMock, return_value=resp),
        ):
            with pytest.raises(LinkedInNotFoundError):
                await client.get("/test")

    @pytest.mark.asyncio
    async def test_version_error_on_400_with_version_message(self, client):
        resp = _mock_httpx_response(
            400,
            {"status": 400, "message": "VERSION_MISSING in request headers"},
        )

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="test-token",
            ),
            patch.object(client._client, "request", new_callable=AsyncMock, return_value=resp),
        ):
            with pytest.raises(LinkedInVersionError):
                await client.get("/test")

    @pytest.mark.asyncio
    async def test_generic_error_on_500(self, client):
        resp = _mock_httpx_response(
            500, {"status": 500, "message": "Internal server error"}
        )

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="test-token",
            ),
            patch.object(client._client, "request", new_callable=AsyncMock, return_value=resp),
        ):
            with pytest.raises(LinkedInAPIError) as exc_info:
                await client.get("/test")
            assert exc_info.value.status_code == 500

    @pytest.mark.asyncio
    async def test_error_parsing_with_malformed_json(self, client):
        """Should handle non-JSON error responses gracefully."""
        resp = MagicMock()
        resp.status_code = 502
        resp.json.side_effect = Exception("Not JSON")
        resp.text = "Bad Gateway"

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="test-token",
            ),
            patch.object(client._client, "request", new_callable=AsyncMock, return_value=resp),
        ):
            with pytest.raises(LinkedInAPIError) as exc_info:
                await client.get("/test")
            assert exc_info.value.status_code == 502
            assert "Bad Gateway" in exc_info.value.message


# --- Account discovery ---


class TestAccountDiscovery:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_get_ad_accounts(self, client):
        """Should parse ad accounts from API response."""
        api_response = {
            "elements": [
                {
                    "id": "urn:li:sponsoredAccount:507404993",
                    "name": "Acme Corp",
                    "currency": "USD",
                    "status": "ACTIVE",
                    "reference": "urn:li:organization:12345",
                }
            ]
        }

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="test-token",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=_mock_httpx_response(200, api_response),
            ),
        ):
            accounts = await client.get_ad_accounts()

        assert len(accounts) == 1
        assert isinstance(accounts[0], LinkedInAdAccount)
        assert accounts[0].id == 507404993
        assert accounts[0].name == "Acme Corp"
        assert accounts[0].currency == "USD"

    @pytest.mark.asyncio
    async def test_get_ad_account_users(self, client):
        """Should return raw elements from adAccountUsers API."""
        api_response = {
            "elements": [
                {"account": "urn:li:sponsoredAccount:123", "role": "CAMPAIGN_MANAGER"}
            ]
        }

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="test-token",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=_mock_httpx_response(200, api_response),
            ),
        ):
            users = await client.get_ad_account_users()

        assert len(users) == 1
        assert users[0]["role"] == "CAMPAIGN_MANAGER"

    @pytest.mark.asyncio
    async def test_get_campaign_groups(self, client):
        """Should parse campaign groups from API response."""
        api_response = {
            "elements": [
                {
                    "id": "urn:li:adCampaignGroup:999",
                    "name": "Q1 Campaign",
                    "status": "ACTIVE",
                    "account": "urn:li:sponsoredAccount:507404993",
                    "totalBudget": {"amount": "5000", "currencyCode": "USD"},
                    "runSchedule": {"start": 1700000000000},
                }
            ]
        }

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="test-token",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=_mock_httpx_response(200, api_response),
            ),
        ):
            groups = await client.get_campaign_groups(account_id=507404993)

        assert len(groups) == 1
        assert isinstance(groups[0], LinkedInCampaignGroup)
        assert groups[0].id == 999
        assert groups[0].name == "Q1 Campaign"
        assert groups[0].total_budget is not None

    @pytest.mark.asyncio
    async def test_create_campaign_group(self, client):
        """Should POST campaign group with correct body."""
        api_response = {"id": "urn:li:adCampaignGroup:888"}

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="test-token",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=_mock_httpx_response(200, api_response),
            ) as mock_request,
        ):
            result = await client.create_campaign_group(
                account_id=507404993,
                name="New Group",
                budget={"amount": "1000", "currencyCode": "USD"},
            )

        assert result["id"] == "urn:li:adCampaignGroup:888"
        # Verify the request body
        call_kwargs = mock_request.call_args
        assert call_kwargs.kwargs["json"]["name"] == "New Group"
        assert call_kwargs.kwargs["json"]["account"] == "urn:li:sponsoredAccount:507404993"
        assert call_kwargs.kwargs["json"]["status"] == "DRAFT"

    @pytest.mark.asyncio
    async def test_get_selected_account_id_returns_id(self, client):
        """Should return selected account ID from provider_configs."""
        _mock_sb_select(
            client.supabase,
            {"config": {"selected_ad_account_id": 507404993}},
        )

        result = await client.get_selected_account_id()
        assert result == 507404993

    @pytest.mark.asyncio
    async def test_get_selected_account_id_raises_when_not_set(self, client):
        """Should raise error when no account selected."""
        _mock_sb_select(
            client.supabase,
            {"config": {"selected_ad_account_id": None}},
        )

        with pytest.raises(
            LinkedInAPIError, match="No LinkedIn ad account selected"
        ):
            await client.get_selected_account_id()

    @pytest.mark.asyncio
    async def test_get_selected_account_id_raises_when_not_connected(
        self, client
    ):
        """Should raise error when LinkedIn not connected."""
        _mock_sb_select(client.supabase, None)

        with pytest.raises(
            LinkedInAPIError, match="LinkedIn not connected"
        ):
            await client.get_selected_account_id()


# --- Helpers ---


def _mock_httpx_response(status_code: int, json_data: dict | None = None):
    """Create a mock httpx.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    resp.text = str(json_data)
    return resp


def _mock_sb_select(mock_sb, data):
    """Set up supabase select→eq→eq→maybe_single→execute chain."""
    chain = mock_sb.table.return_value.select.return_value
    chain = chain.eq.return_value.eq.return_value
    chain.maybe_single.return_value.execute.return_value = MagicMock(
        data=data
    )
