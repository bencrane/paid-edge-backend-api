"""Tests for Google Ads OAuth flow and token management (BJC-140)."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import jwt
import pytest

from app.config import settings


# --- OAuth authorize endpoint ---


class TestGoogleAdsAuthorize:
    """Tests for GET /auth/google-ads/authorize."""

    @pytest.fixture
    def mock_user(self):
        user = MagicMock()
        user.id = "user-123"
        user.email = "test@example.com"
        user.full_name = "Test User"
        return user

    @pytest.mark.asyncio
    async def test_authorize_generates_redirect_url(self, mock_user):
        """Authorize endpoint should redirect to Google with correct params."""
        from app.auth.google_ads import google_ads_authorize

        request = MagicMock()
        response = await google_ads_authorize(request, org_id="org-456", user=mock_user)

        assert response.status_code == 307
        location = response.headers["location"]
        assert "https://accounts.google.com/o/oauth2/v2/auth" in location
        assert "response_type=code" in location
        assert "access_type=offline" in location
        assert "prompt=consent" in location
        assert "scope=" in location
        assert "adwords" in location
        assert "state=" in location

    @pytest.mark.asyncio
    async def test_authorize_state_contains_org_and_user(self, mock_user):
        """State JWT should contain org_id, user_id, and nonce."""
        from urllib.parse import parse_qs, urlparse

        from app.auth.google_ads import google_ads_authorize

        request = MagicMock()
        response = await google_ads_authorize(request, org_id="org-456", user=mock_user)

        location = response.headers["location"]
        parsed = urlparse(location)
        qs = parse_qs(parsed.query)
        state_token = qs["state"][0]

        payload = jwt.decode(
            state_token,
            settings.SUPABASE_JWT_SECRET,
            algorithms=["HS256"],
        )
        assert payload["org_id"] == "org-456"
        assert payload["user_id"] == "user-123"
        assert "nonce" in payload


# --- OAuth callback endpoint ---


class TestGoogleAdsCallback:
    """Tests for GET /auth/google-ads/callback."""

    def _make_state(self, org_id="org-456", user_id="user-123"):
        return jwt.encode(
            {
                "org_id": org_id,
                "user_id": user_id,
                "nonce": "test-nonce",
                "exp": datetime.now(UTC) + timedelta(minutes=30),
            },
            settings.SUPABASE_JWT_SECRET,
            algorithm="HS256",
        )

    @pytest.mark.asyncio
    async def test_callback_error_redirects_to_frontend(self):
        """If Google returns an error, redirect to frontend with error param."""
        from app.auth.google_ads import google_ads_callback

        mock_sb = MagicMock()
        response = await google_ads_callback(
            code=None,
            state=None,
            error="access_denied",
            error_description="The user denied access",
            supabase=mock_sb,
        )

        assert response.status_code == 307
        assert "google_ads_error=access_denied" in response.headers["location"]

    @pytest.mark.asyncio
    async def test_callback_missing_code_raises(self):
        """Missing code parameter should raise BadRequestError."""
        from app.auth.google_ads import google_ads_callback
        from app.shared.errors import BadRequestError

        mock_sb = MagicMock()
        with pytest.raises(BadRequestError):
            await google_ads_callback(
                code=None,
                state="some-state",
                error=None,
                error_description=None,
                supabase=mock_sb,
            )

    @pytest.mark.asyncio
    async def test_callback_invalid_state_raises(self):
        """Invalid state JWT should raise BadRequestError."""
        from app.auth.google_ads import google_ads_callback
        from app.shared.errors import BadRequestError

        mock_sb = MagicMock()
        with pytest.raises(BadRequestError, match="Invalid OAuth state"):
            await google_ads_callback(
                code="auth-code",
                state="invalid-jwt-token",
                error=None,
                error_description=None,
                supabase=mock_sb,
            )

    @pytest.mark.asyncio
    async def test_callback_expired_state_raises(self):
        """Expired state JWT should raise BadRequestError."""
        from app.auth.google_ads import google_ads_callback
        from app.shared.errors import BadRequestError

        expired_state = jwt.encode(
            {
                "org_id": "org-456",
                "user_id": "user-123",
                "nonce": "test",
                "exp": datetime.now(UTC) - timedelta(minutes=5),
            },
            settings.SUPABASE_JWT_SECRET,
            algorithm="HS256",
        )

        mock_sb = MagicMock()
        with pytest.raises(BadRequestError, match="expired"):
            await google_ads_callback(
                code="auth-code",
                state=expired_state,
                error=None,
                error_description=None,
                supabase=mock_sb,
            )

    @pytest.mark.asyncio
    async def test_callback_exchanges_code_and_stores_tokens(self):
        """Successful callback should exchange code and upsert config."""
        from app.auth.google_ads import google_ads_callback

        state = self._make_state()
        mock_sb = MagicMock()
        mock_upsert = MagicMock()
        mock_upsert.execute.return_value = MagicMock(data=[{}])
        mock_sb.table.return_value.upsert.return_value = mock_upsert

        token_response = {
            "access_token": "access-tok",
            "refresh_token": "refresh-tok",
            "expires_in": 3600,
            "token_type": "Bearer",
        }
        list_customers_response = {
            "resourceNames": ["customers/1234567890"]
        }
        search_stream_response = [
            {
                "results": [
                    {
                        "customer": {
                            "id": "1234567890",
                            "descriptiveName": "Acme Corp",
                            "currencyCode": "USD",
                            "timeZone": "America/New_York",
                            "manager": False,
                            "testAccount": False,
                        }
                    }
                ]
            }
        ]

        with patch("app.auth.google_ads.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client_cls.return_value.__aenter__ = AsyncMock(
                return_value=mock_client
            )
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            # First httpx.AsyncClient context: token exchange (POST)
            mock_client.post.side_effect = [
                _mock_httpx_response(200, token_response),
                # searchStream call in _get_account_details
                _mock_httpx_response(200, search_stream_response),
            ]
            # GET for listAccessibleCustomers
            mock_client.get.return_value = _mock_httpx_response(
                200, list_customers_response
            )

            response = await google_ads_callback(
                code="auth-code",
                state=state,
                error=None,
                error_description=None,
                supabase=mock_sb,
            )

        # Should redirect to frontend with success
        assert response.status_code == 307
        assert "google_ads_connected=true" in response.headers["location"]

        # Should upsert provider config
        mock_sb.table.assert_called_with("provider_configs")
        upsert_call = mock_sb.table.return_value.upsert.call_args
        upsert_data = upsert_call[0][0]
        assert upsert_data["provider"] == "google_ads"
        assert upsert_data["organization_id"] == "org-456"
        assert upsert_data["config"]["refresh_token"] == "refresh-tok"
        assert upsert_data["config"]["selected_customer_id"] is None

    @pytest.mark.asyncio
    async def test_callback_no_refresh_token_raises(self):
        """Should raise when Google doesn't return a refresh token."""
        from app.auth.google_ads import google_ads_callback
        from app.shared.errors import BadRequestError

        state = self._make_state()
        mock_sb = MagicMock()

        token_response = {
            "access_token": "access-tok",
            "expires_in": 3600,
            "token_type": "Bearer",
            # No refresh_token
        }

        with patch("app.auth.google_ads.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client_cls.return_value.__aenter__ = AsyncMock(
                return_value=mock_client
            )
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)
            mock_client.post.return_value = _mock_httpx_response(200, token_response)

            with pytest.raises(BadRequestError, match="refresh token"):
                await google_ads_callback(
                    code="auth-code",
                    state=state,
                    error=None,
                    error_description=None,
                    supabase=mock_sb,
                )


# --- Token refresh utility ---


class TestGetGoogleAdsCredentials:
    """Tests for get_google_ads_credentials()."""

    @pytest.mark.asyncio
    async def test_returns_credentials_with_fresh_token(self):
        """Should return credentials with a fresh access token."""
        from app.integrations.google_ads_auth import get_google_ads_credentials

        config = {
            "refresh_token": "refresh-tok",
            "selected_customer_id": "1234567890",
        }

        mock_sb = MagicMock()
        _mock_sb_select(mock_sb, {"config": config})

        refresh_response = {"access_token": "new-access-tok", "expires_in": 3600}

        with patch(
            "app.integrations.google_ads_auth.httpx.AsyncClient"
        ) as mock_client_cls:
            mock_client = AsyncMock()
            mock_client_cls.return_value.__aenter__ = AsyncMock(
                return_value=mock_client
            )
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)
            mock_client.post.return_value = _mock_httpx_response(200, refresh_response)

            creds = await get_google_ads_credentials("org-1", mock_sb)

        assert creds["access_token"] == "new-access-tok"
        assert creds["refresh_token"] == "refresh-tok"
        assert creds["customer_id"] == "1234567890"

    @pytest.mark.asyncio
    async def test_raises_when_no_config(self):
        """Should raise GoogleAdsReauthRequiredError when no provider config."""
        from app.integrations.google_ads_auth import (
            GoogleAdsReauthRequiredError,
            get_google_ads_credentials,
        )

        mock_sb = MagicMock()
        _mock_sb_select(mock_sb, None)

        with pytest.raises(GoogleAdsReauthRequiredError):
            await get_google_ads_credentials("org-1", mock_sb)

    @pytest.mark.asyncio
    async def test_raises_on_invalid_grant(self):
        """Should raise GoogleAdsReauthRequiredError on invalid_grant error."""
        from app.integrations.google_ads_auth import (
            GoogleAdsReauthRequiredError,
            get_google_ads_credentials,
        )

        config = {
            "refresh_token": "revoked-token",
            "selected_customer_id": "1234567890",
        }

        mock_sb = MagicMock()
        _mock_sb_select(mock_sb, {"config": config})

        with patch(
            "app.integrations.google_ads_auth.httpx.AsyncClient"
        ) as mock_client_cls:
            mock_client = AsyncMock()
            mock_client_cls.return_value.__aenter__ = AsyncMock(
                return_value=mock_client
            )
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)
            mock_client.post.return_value = _mock_httpx_response(
                400, {"error": "invalid_grant"}
            )

            with pytest.raises(GoogleAdsReauthRequiredError):
                await get_google_ads_credentials("org-1", mock_sb)

    @pytest.mark.asyncio
    async def test_raises_when_no_refresh_token_in_config(self):
        """Should raise when config has no refresh_token."""
        from app.integrations.google_ads_auth import (
            GoogleAdsReauthRequiredError,
            get_google_ads_credentials,
        )

        config = {"selected_customer_id": "1234567890"}

        mock_sb = MagicMock()
        _mock_sb_select(mock_sb, {"config": config})

        with pytest.raises(GoogleAdsReauthRequiredError):
            await get_google_ads_credentials("org-1", mock_sb)


# --- Status endpoint ---


class TestGoogleAdsStatus:
    """Tests for GET /auth/google-ads/status."""

    @pytest.mark.asyncio
    async def test_status_not_connected(self):
        """Should return connected=False when no config exists."""
        from app.auth.google_ads import google_ads_status

        tenant = MagicMock()
        tenant.id = "org-1"
        mock_sb = MagicMock()
        _mock_sb_select(mock_sb, None)

        result = await google_ads_status(tenant=tenant, supabase=mock_sb)
        assert result.connected is False

    @pytest.mark.asyncio
    async def test_status_connected(self):
        """Should return connection health when config exists."""
        from app.auth.google_ads import google_ads_status

        tenant = MagicMock()
        tenant.id = "org-1"
        mock_sb = MagicMock()
        _mock_sb_select(
            mock_sb,
            {
                "is_active": True,
                "config": {
                    "accessible_accounts": [
                        {
                            "customer_id": "1234567890",
                            "name": "Acme Corp",
                            "currency": "USD",
                            "timezone": "America/New_York",
                            "is_manager": False,
                            "is_test": False,
                        }
                    ],
                    "selected_customer_id": "1234567890",
                },
            },
        )

        result = await google_ads_status(tenant=tenant, supabase=mock_sb)
        assert result.connected is True
        assert result.selected_customer_id == "1234567890"
        assert len(result.accessible_accounts) == 1
        assert result.needs_reauth is False


# --- Account selection endpoint ---


class TestGoogleAdsSelectAccount:
    """Tests for PUT /auth/google-ads/account."""

    @pytest.mark.asyncio
    async def test_select_valid_account(self):
        """Should update selected_customer_id when valid."""
        from app.auth.google_ads import AccountSelectRequest, google_ads_select_account

        tenant = MagicMock()
        tenant.id = "org-1"
        mock_sb = MagicMock()
        _mock_sb_select(
            mock_sb,
            {
                "config": {
                    "accessible_accounts": [
                        {"customer_id": "1234567890", "name": "Acme"}
                    ],
                    "selected_customer_id": None,
                },
            },
        )
        update_chain = mock_sb.table.return_value.update.return_value
        update_chain.eq.return_value.eq.return_value.execute.return_value = MagicMock()

        body = AccountSelectRequest(customer_id="1234567890")
        result = await google_ads_select_account(
            body=body, tenant=tenant, supabase=mock_sb
        )

        assert result["selected_customer_id"] == "1234567890"

    @pytest.mark.asyncio
    async def test_select_invalid_account_raises(self):
        """Should raise BadRequestError for invalid customer_id."""
        from app.auth.google_ads import AccountSelectRequest, google_ads_select_account
        from app.shared.errors import BadRequestError

        tenant = MagicMock()
        tenant.id = "org-1"
        mock_sb = MagicMock()
        _mock_sb_select(
            mock_sb,
            {
                "config": {
                    "accessible_accounts": [
                        {"customer_id": "1234567890", "name": "Acme"}
                    ],
                    "selected_customer_id": None,
                },
            },
        )

        body = AccountSelectRequest(customer_id="9999999999")
        with pytest.raises(BadRequestError, match="not in accessible accounts"):
            await google_ads_select_account(
                body=body, tenant=tenant, supabase=mock_sb
            )

    @pytest.mark.asyncio
    async def test_select_account_strips_hyphens(self):
        """Customer IDs with hyphens should be normalized."""
        from app.auth.google_ads import AccountSelectRequest, google_ads_select_account

        tenant = MagicMock()
        tenant.id = "org-1"
        mock_sb = MagicMock()
        _mock_sb_select(
            mock_sb,
            {
                "config": {
                    "accessible_accounts": [
                        {"customer_id": "1234567890", "name": "Acme"}
                    ],
                    "selected_customer_id": None,
                },
            },
        )
        update_chain = mock_sb.table.return_value.update.return_value
        update_chain.eq.return_value.eq.return_value.execute.return_value = MagicMock()

        body = AccountSelectRequest(customer_id="123-456-7890")
        result = await google_ads_select_account(
            body=body, tenant=tenant, supabase=mock_sb
        )

        assert result["selected_customer_id"] == "1234567890"

    @pytest.mark.asyncio
    async def test_select_not_connected_raises(self):
        """Should raise NotFoundError when Google Ads not connected."""
        from app.auth.google_ads import AccountSelectRequest, google_ads_select_account
        from app.shared.errors import NotFoundError

        tenant = MagicMock()
        tenant.id = "org-1"
        mock_sb = MagicMock()
        _mock_sb_select(mock_sb, None)

        body = AccountSelectRequest(customer_id="1234567890")
        with pytest.raises(NotFoundError):
            await google_ads_select_account(
                body=body, tenant=tenant, supabase=mock_sb
            )


# --- Helpers ---


def _mock_httpx_response(status_code: int, json_data: dict | list | None = None):
    """Create a mock httpx.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data if json_data is not None else {}
    resp.text = str(json_data)
    return resp


def _mock_sb_select(mock_sb, data):
    """Set up the common supabase select→eq→eq→maybe_single→execute chain."""
    chain = mock_sb.table.return_value.select.return_value
    chain = chain.eq.return_value.eq.return_value
    chain.maybe_single.return_value.execute.return_value = MagicMock(data=data)
