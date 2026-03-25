"""Tests for LinkedIn OAuth flow and token management (BJC-129)."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import jwt
import pytest

from app.config import settings

# --- OAuth authorize endpoint ---


class TestLinkedInAuthorize:
    """Tests for GET /auth/linkedin/authorize."""

    @pytest.fixture
    def mock_user(self):
        user = MagicMock()
        user.id = "user-123"
        user.email = "test@example.com"
        user.full_name = "Test User"
        return user

    @pytest.mark.asyncio
    async def test_authorize_generates_redirect_url(self, mock_user):
        """Authorize endpoint should redirect to LinkedIn with correct params."""
        from app.auth.linkedin import linkedin_authorize

        request = MagicMock()
        response = await linkedin_authorize(request, org_id="org-456", user=mock_user)

        assert response.status_code == 307
        location = response.headers["location"]
        assert "https://www.linkedin.com/oauth/v2/authorization" in location
        assert "response_type=code" in location
        assert "state=" in location
        assert "scope=" in location
        assert "r_ads" in location

    @pytest.mark.asyncio
    async def test_authorize_state_contains_org_and_user(self, mock_user):
        """State JWT should contain org_id, user_id, and nonce."""
        from urllib.parse import parse_qs, urlparse

        from app.auth.linkedin import linkedin_authorize

        request = MagicMock()
        response = await linkedin_authorize(request, org_id="org-456", user=mock_user)

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


class TestLinkedInCallback:
    """Tests for GET /auth/linkedin/callback."""

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
        """If LinkedIn returns an error, redirect to frontend with error param."""
        from app.auth.linkedin import linkedin_callback

        mock_sb = MagicMock()
        response = await linkedin_callback(
            code=None,
            state=None,
            error="user_cancelled_authorize",
            error_description="The member refused",
            supabase=mock_sb,
        )

        assert response.status_code == 307
        assert "linkedin_error=user_cancelled_authorize" in response.headers["location"]

    @pytest.mark.asyncio
    async def test_callback_missing_code_raises(self):
        """Missing code parameter should raise BadRequestError."""
        from app.auth.linkedin import linkedin_callback
        from app.shared.errors import BadRequestError

        mock_sb = MagicMock()
        with pytest.raises(BadRequestError):
            await linkedin_callback(
                code=None,
                state="some-state",
                error=None,
                error_description=None,
                supabase=mock_sb,
            )

    @pytest.mark.asyncio
    async def test_callback_invalid_state_raises(self):
        """Invalid state JWT should raise BadRequestError."""
        from app.auth.linkedin import linkedin_callback
        from app.shared.errors import BadRequestError

        mock_sb = MagicMock()
        with pytest.raises(BadRequestError, match="Invalid OAuth state"):
            await linkedin_callback(
                code="auth-code",
                state="invalid-jwt-token",
                error=None,
                error_description=None,
                supabase=mock_sb,
            )

    @pytest.mark.asyncio
    async def test_callback_expired_state_raises(self):
        """Expired state JWT should raise BadRequestError."""
        from app.auth.linkedin import linkedin_callback
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
            await linkedin_callback(
                code="auth-code",
                state=expired_state,
                error=None,
                error_description=None,
                supabase=mock_sb,
            )

    @pytest.mark.asyncio
    async def test_callback_exchanges_code_and_stores_tokens(self):
        """Successful callback should exchange code, fetch profile, and upsert config."""
        from app.auth.linkedin import linkedin_callback

        state = self._make_state()
        mock_sb = MagicMock()
        mock_upsert = MagicMock()
        mock_upsert.execute.return_value = MagicMock(data=[{}])
        mock_sb.table.return_value.upsert.return_value = mock_upsert

        token_response = {
            "access_token": "access-tok",
            "expires_in": 5184000,
            "refresh_token": "refresh-tok",
            "refresh_token_expires_in": 31536000,
            "scope": "r_ads,rw_ads",
        }
        me_response = {"id": "urn:li:person:abc123"}
        accounts_response = {
            "elements": [
                {
                    "account": "urn:li:sponsoredAccount:507404993",
                    "role": "CAMPAIGN_MANAGER",
                }
            ]
        }

        mock_http_responses = [
            _mock_httpx_response(200, token_response),
            _mock_httpx_response(200, me_response),
            _mock_httpx_response(200, accounts_response),
        ]

        with patch("app.auth.linkedin.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            mock_client.post.return_value = mock_http_responses[0]
            mock_client.get.side_effect = mock_http_responses[1:]

            response = await linkedin_callback(
                code="auth-code",
                state=state,
                error=None,
                error_description=None,
                supabase=mock_sb,
            )

        # Should redirect to frontend with success
        assert response.status_code == 307
        assert "linkedin_connected=true" in response.headers["location"]

        # Should upsert provider config
        mock_sb.table.assert_called_with("provider_configs")
        upsert_call = mock_sb.table.return_value.upsert.call_args
        upsert_data = upsert_call[0][0]
        assert upsert_data["provider"] == "linkedin_ads"
        assert upsert_data["organization_id"] == "org-456"
        assert upsert_data["config"]["access_token"] == "access-tok"
        assert upsert_data["config"]["member_urn"] == "urn:li:person:abc123"
        assert len(upsert_data["config"]["ad_accounts"]) == 1
        assert upsert_data["config"]["ad_accounts"][0]["id"] == 507404993


# --- Token refresh utility ---


class TestGetValidLinkedInToken:
    """Tests for get_valid_linkedin_token()."""

    @pytest.mark.asyncio
    async def test_returns_token_when_fresh(self):
        """Should return access token when not near expiry."""
        from app.integrations.linkedin_auth import get_valid_linkedin_token

        now = datetime.now(UTC)
        config = {
            "access_token": "fresh-token",
            "access_token_expires_at": (now + timedelta(days=30)).isoformat(),
            "refresh_token": "refresh-tok",
            "refresh_token_expires_at": (now + timedelta(days=300)).isoformat(),
        }

        mock_sb = MagicMock()
        _mock_sb_select(mock_sb, {"config": config})

        token = await get_valid_linkedin_token("org-1", mock_sb)
        assert token == "fresh-token"

    @pytest.mark.asyncio
    async def test_raises_when_no_config(self):
        """Should raise LinkedInReauthRequiredError when no provider config exists."""
        from app.integrations.linkedin_auth import (
            LinkedInReauthRequiredError,
            get_valid_linkedin_token,
        )

        mock_sb = MagicMock()
        _mock_sb_select(mock_sb, None)

        with pytest.raises(LinkedInReauthRequiredError):
            await get_valid_linkedin_token("org-1", mock_sb)

    @pytest.mark.asyncio
    async def test_raises_when_refresh_token_expired(self):
        """Should raise LinkedInReauthRequiredError when refresh token has expired."""
        from app.integrations.linkedin_auth import (
            LinkedInReauthRequiredError,
            get_valid_linkedin_token,
        )

        now = datetime.now(UTC)
        config = {
            "access_token": "tok",
            "access_token_expires_at": (now + timedelta(days=30)).isoformat(),
            "refresh_token": "ref-tok",
            "refresh_token_expires_at": (now - timedelta(days=1)).isoformat(),
        }

        mock_sb = MagicMock()
        _mock_sb_select(mock_sb, {"config": config})

        with pytest.raises(LinkedInReauthRequiredError):
            await get_valid_linkedin_token("org-1", mock_sb)

    @pytest.mark.asyncio
    async def test_proactively_refreshes_when_near_expiry(self):
        """Should refresh access token when it expires within 7 days."""
        from app.integrations.linkedin_auth import get_valid_linkedin_token

        now = datetime.now(UTC)
        config = {
            "access_token": "old-token",
            "access_token_expires_at": (now + timedelta(days=3)).isoformat(),
            "refresh_token": "refresh-tok",
            "refresh_token_expires_at": (now + timedelta(days=300)).isoformat(),
        }

        mock_sb = MagicMock()
        _mock_sb_select(mock_sb, {"config": config})
        update_chain = mock_sb.table.return_value.update.return_value
        update_chain.eq.return_value.eq.return_value.execute.return_value = (
            MagicMock()
        )

        refresh_response = {
            "access_token": "new-token",
            "expires_in": 5184000,
            "refresh_token": "new-refresh-tok",
            "refresh_token_expires_in": 31536000,
        }

        with patch("app.integrations.linkedin_auth.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)
            mock_client.post.return_value = _mock_httpx_response(200, refresh_response)

            token = await get_valid_linkedin_token("org-1", mock_sb)

        assert token == "new-token"

    @pytest.mark.asyncio
    async def test_warns_when_refresh_token_near_expiry(self):
        """Should log warning when refresh token expires within 14 days."""

        from app.integrations.linkedin_auth import get_valid_linkedin_token

        now = datetime.now(UTC)
        config = {
            "access_token": "tok",
            "access_token_expires_at": (now + timedelta(days=30)).isoformat(),
            "refresh_token": "ref-tok",
            "refresh_token_expires_at": (now + timedelta(days=10)).isoformat(),
        }

        mock_sb = MagicMock()
        _mock_sb_select(mock_sb, {"config": config})

        with patch("app.integrations.linkedin_auth.logger") as mock_logger:
            await get_valid_linkedin_token("org-1", mock_sb)
            mock_logger.warning.assert_called_once()
            assert "expires in" in mock_logger.warning.call_args[0][0]


# --- LinkedIn status endpoint ---


class TestLinkedInStatus:
    """Tests for GET /auth/linkedin/status."""

    @pytest.mark.asyncio
    async def test_status_not_connected(self):
        """Should return connected=False when no config exists."""
        from app.auth.linkedin import linkedin_status

        tenant = MagicMock()
        tenant.id = "org-1"
        mock_sb = MagicMock()
        _mock_sb_select(mock_sb, None)

        result = await linkedin_status(tenant=tenant, supabase=mock_sb)
        assert result.connected is False

    @pytest.mark.asyncio
    async def test_status_connected(self):
        """Should return connection health when config exists."""
        from app.auth.linkedin import linkedin_status

        now = datetime.now(UTC)
        tenant = MagicMock()
        tenant.id = "org-1"
        mock_sb = MagicMock()
        _mock_sb_select(mock_sb, {
            "is_active": True,
            "config": {
                "access_token_expires_at": (
                    now + timedelta(days=45)
                ).isoformat(),
                "refresh_token_expires_at": (
                    now + timedelta(days=300)
                ).isoformat(),
                "ad_accounts": [
                    {"id": 123, "name": "Test", "role": "ADMIN"}
                ],
                "selected_ad_account_id": 123,
            },
        })

        result = await linkedin_status(tenant=tenant, supabase=mock_sb)
        assert result.connected is True
        assert result.expires_in_days >= 44  # timedelta(days=45) may be 44 due to sub-day offset
        assert result.needs_reauth is False
        assert len(result.ad_accounts) == 1
        assert result.selected_ad_account_id == 123


# --- Helpers ---


def _mock_httpx_response(status_code: int, json_data: dict | None = None):
    """Create a mock httpx.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    resp.text = str(json_data)
    return resp


def _mock_sb_select(mock_sb, data):
    """Set up the common supabase select→eq→eq→maybe_single→execute chain."""
    chain = mock_sb.table.return_value.select.return_value
    chain = chain.eq.return_value.eq.return_value
    chain.maybe_single.return_value.execute.return_value = MagicMock(
        data=data
    )
