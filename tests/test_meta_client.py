"""Tests for Meta base API client, rate limiter, and ad account management (BJC-149)."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.integrations.meta_client import (
    MetaAdsClient,
    MetaAPIError,
    MetaPermissionError,
    MetaRateLimitError,
    MetaRateLimiter,
    MetaTokenError,
)


def _mock_resp(status_code, json_data, headers=None):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.text = str(json_data)
    resp.headers = headers or {}
    return resp


class TestMetaRateLimiter:
    """Tests for rate limit header parsing and throttling."""

    def test_update_from_ad_account_usage(self):
        limiter = MetaRateLimiter()
        limiter.update_from_headers({
            "x-ad-account-usage": json.dumps({"acc_id_util_pct": 50.0, "reset_time_duration": 300}),
        })
        assert limiter._ad_account_util == 50.0
        assert limiter._reset_time == 300.0
        assert not limiter.should_throttle()

    def test_throttle_at_75_percent(self):
        limiter = MetaRateLimiter()
        limiter.update_from_headers({
            "x-ad-account-usage": json.dumps({"acc_id_util_pct": 80.0, "reset_time_duration": 10}),
        })
        assert limiter.should_throttle()

    def test_update_from_buc_usage(self):
        limiter = MetaRateLimiter()
        limiter.update_from_headers({
            "x-business-use-case-usage": json.dumps({
                "act_123": [{"call_count": 60, "total_cputime": 30, "total_time": 40}],
            }),
        })
        assert limiter._buc_call_count == 60.0

    def test_update_from_insights_throttle(self):
        limiter = MetaRateLimiter()
        limiter.update_from_headers({
            "x-fb-ads-insights-throttle": json.dumps({
                "app_id_util_pct": 20, "acc_id_util_pct": 30,
            }),
        })
        assert limiter._insights_app_util == 20.0
        assert limiter._insights_acc_util == 30.0

    def test_no_throttle_below_threshold(self):
        limiter = MetaRateLimiter()
        limiter.update_from_headers({
            "x-ad-account-usage": json.dumps({"acc_id_util_pct": 40}),
            "x-business-use-case-usage": json.dumps({
                "act_123": [{"call_count": 50, "total_cputime": 50, "total_time": 50}],
            }),
        })
        assert not limiter.should_throttle()

    @pytest.mark.asyncio
    async def test_wait_if_needed_throttled(self):
        limiter = MetaRateLimiter()
        limiter._ad_account_util = 80.0
        limiter._reset_time = 0.01  # Very short for test

        with patch("app.integrations.meta_client.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await limiter.wait_if_needed()
            mock_sleep.assert_called_once()


class TestMetaAdsClient:
    """Tests for the base MetaAdsClient."""

    def _make_client(self):
        return MetaAdsClient(
            access_token="TEST_TOKEN",
            app_id="APP_123",
            app_secret="SECRET_456",
            ad_account_id="act_789",
        )

    def test_appsecret_proof_computed(self):
        client = self._make_client()
        assert client._appsecret_proof is not None
        assert len(client._appsecret_proof) == 64

    @pytest.mark.asyncio
    async def test_context_manager(self):
        client = self._make_client()
        async with client:
            assert client._http_client is not None
        assert client._http_client is None

    @pytest.mark.asyncio
    async def test_get_request(self):
        client = self._make_client()
        mock_resp = _mock_resp(200, {"data": [{"id": "123"}]})
        mock_http = AsyncMock()
        mock_http.get = AsyncMock(return_value=mock_resp)
        client._http_client = mock_http

        result = await client._request("GET", "me/adaccounts")
        assert result["data"][0]["id"] == "123"

    @pytest.mark.asyncio
    async def test_post_request_with_json(self):
        client = self._make_client()
        mock_resp = _mock_resp(200, {"id": "campaign_123"})
        mock_http = AsyncMock()
        mock_http.post = AsyncMock(return_value=mock_resp)
        client._http_client = mock_http

        result = await client._request(
            "POST", "act_789/campaigns", json_data={"name": "Test"}
        )
        assert result["id"] == "campaign_123"

    @pytest.mark.asyncio
    async def test_delete_request(self):
        client = self._make_client()
        mock_resp = _mock_resp(200, {"success": True})
        mock_http = AsyncMock()
        mock_http.delete = AsyncMock(return_value=mock_resp)
        client._http_client = mock_http

        result = await client._request("DELETE", "123456")
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_rate_limit_retry(self):
        """Rate limit errors should trigger retry with backoff."""
        client = self._make_client()
        rate_limit_resp = _mock_resp(
            200,
            {"error": {"code": 17, "message": "Rate limit hit"}},
        )
        success_resp = _mock_resp(200, {"data": []})

        mock_http = AsyncMock()
        mock_http.get = AsyncMock(side_effect=[rate_limit_resp, success_resp])
        client._http_client = mock_http

        with patch("app.integrations.meta_client.asyncio.sleep", new_callable=AsyncMock):
            result = await client._request("GET", "test")
        assert "data" in result

    @pytest.mark.asyncio
    async def test_token_error_not_retried(self):
        """Token errors (190) should not be retried."""
        client = self._make_client()
        token_error_resp = _mock_resp(
            200,
            {"error": {"code": 190, "message": "Invalid token"}},
        )
        mock_http = AsyncMock()
        mock_http.get = AsyncMock(return_value=token_error_resp)
        client._http_client = mock_http

        with pytest.raises(MetaTokenError):
            await client._request("GET", "test")

    @pytest.mark.asyncio
    async def test_permission_error(self):
        """Permission errors (10, 200) raise MetaPermissionError."""
        client = self._make_client()
        perm_error_resp = _mock_resp(
            200,
            {"error": {"code": 10, "message": "Permission denied"}},
        )
        mock_http = AsyncMock()
        mock_http.get = AsyncMock(return_value=perm_error_resp)
        client._http_client = mock_http

        with pytest.raises(MetaPermissionError):
            await client._request("GET", "test")

    @pytest.mark.asyncio
    async def test_generic_api_error(self):
        """Unknown error codes raise MetaAPIError."""
        client = self._make_client()
        error_resp = _mock_resp(
            200,
            {"error": {"code": 100, "message": "Invalid parameter"}},
        )
        mock_http = AsyncMock()
        mock_http.get = AsyncMock(return_value=error_resp)
        client._http_client = mock_http

        with pytest.raises(MetaAPIError):
            await client._request("GET", "test")


class TestMetaAdsClientFactory:
    """Tests for the for_tenant factory method."""

    @pytest.mark.asyncio
    async def test_for_tenant(self):
        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.maybe_single.return_value.execute.return_value = MagicMock(
            data={
                "config": {
                    "access_token": "TOKEN",
                    "token_type": "system_user_non_expiring",
                    "app_id": "APP_ID",
                    "ad_accounts": [{"id": "act_123"}],
                    "selected_ad_account_id": "act_123",
                }
            }
        )

        with patch("app.integrations.meta_client.get_valid_meta_token", new_callable=AsyncMock, return_value="TOKEN"):
            client = await MetaAdsClient.for_tenant("org-123", mock_supabase)

        assert client.access_token == "TOKEN"
        assert client.ad_account_id == "act_123"


class TestBatchRequest:
    """Tests for batch request handling."""

    @pytest.mark.asyncio
    async def test_batch_under_50(self):
        client = MetaAdsClient("TOKEN", "APP", "SECRET", "act_123")
        mock_http = AsyncMock()
        mock_http.post = AsyncMock(
            return_value=_mock_resp(200, [{"code": 200, "body": "{}"}])
        )
        client._http_client = mock_http

        requests = [{"method": "GET", "relative_url": "me"}]
        result = await client.batch_request(requests)
        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_batch_auto_chunks(self):
        """Batches > 50 should be auto-chunked."""
        client = MetaAdsClient("TOKEN", "APP", "SECRET", "act_123")
        mock_http = AsyncMock()
        mock_http.post = AsyncMock(
            return_value=_mock_resp(200, [{"code": 200, "body": "{}"}])
        )
        client._http_client = mock_http

        requests = [{"method": "GET", "relative_url": f"obj/{i}"} for i in range(75)]
        await client.batch_request(requests)

        # Should have made 2 calls (50 + 25)
        assert mock_http.post.call_count == 2


class TestAdAccountManagement:
    """Tests for ad account operations."""

    @pytest.mark.asyncio
    async def test_list_ad_accounts(self):
        client = MetaAdsClient("TOKEN", "APP", "SECRET", "act_123")
        mock_http = AsyncMock()
        mock_http.get = AsyncMock(
            return_value=_mock_resp(200, {
                "data": [
                    {"id": "act_111", "name": "Account 1", "currency": "USD",
                     "timezone_name": "US/Eastern", "account_status": 1}
                ],
            })
        )
        client._http_client = mock_http

        accounts = await client.list_ad_accounts()
        assert len(accounts) == 1
        assert accounts[0]["id"] == "act_111"

    @pytest.mark.asyncio
    async def test_get_ad_account(self):
        client = MetaAdsClient("TOKEN", "APP", "SECRET", "act_123")
        mock_http = AsyncMock()
        mock_http.get = AsyncMock(
            return_value=_mock_resp(200, {
                "name": "Test Account",
                "account_status": 1,
                "currency": "USD",
            })
        )
        client._http_client = mock_http

        account = await client.get_ad_account()
        assert account["name"] == "Test Account"

    @pytest.mark.asyncio
    async def test_validate_ad_account_access(self):
        client = MetaAdsClient("TOKEN", "APP", "SECRET", "act_123")
        mock_http = AsyncMock()
        mock_http.get = AsyncMock(
            return_value=_mock_resp(200, {"name": "Test", "account_status": 1})
        )
        client._http_client = mock_http

        valid = await client.validate_ad_account_access()
        assert valid is True

    @pytest.mark.asyncio
    async def test_validate_ad_account_access_disabled(self):
        client = MetaAdsClient("TOKEN", "APP", "SECRET", "act_123")
        mock_http = AsyncMock()
        mock_http.get = AsyncMock(
            return_value=_mock_resp(200, {"name": "Test", "account_status": 2})
        )
        client._http_client = mock_http

        valid = await client.validate_ad_account_access()
        assert valid is False
