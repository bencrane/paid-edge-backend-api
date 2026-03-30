"""Tests for Meta OAuth flow and token management (BJC-147)."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import jwt
import pytest

from app.config import settings


# --- OAuth authorize endpoint ---


class TestMetaAuthorize:
    """Tests for GET /auth/meta/authorize."""

    @pytest.fixture
    def mock_user(self):
        user = MagicMock()
        user.id = "user-123"
        user.email = "test@example.com"
        user.full_name = "Test User"
        return user

    @pytest.mark.asyncio
    async def test_authorize_generates_redirect_url(self, mock_user):
        """Authorize endpoint should redirect to Meta with correct params."""
        from app.auth.meta import meta_authorize

        request = MagicMock()
        response = await meta_authorize(request, org_id="org-456", user=mock_user)

        assert response.status_code == 307
        location = response.headers["location"]
        assert "https://www.facebook.com/v25.0/dialog/oauth" in location
        assert "client_id=" in location
        assert "state=" in location
        assert "scope=" in location
        assert "ads_management" in location

    @pytest.mark.asyncio
    async def test_authorize_state_contains_org_and_user(self, mock_user):
        """State JWT should contain org_id, user_id, and nonce."""
        from urllib.parse import parse_qs, urlparse

        from app.auth.meta import meta_authorize

        request = MagicMock()
        response = await meta_authorize(request, org_id="org-456", user=mock_user)

        location = response.headers["location"]
        parsed = urlparse(location)
        qs = parse_qs(parsed.query)
        state_token = qs["state"][0]

        payload = jwt.decode(
            state_token,
            settings.SUPABASE_SERVICE_ROLE_KEY,
            algorithms=["HS256"],
        )
        assert payload["org_id"] == "org-456"
        assert payload["user_id"] == "user-123"
        assert "nonce" in payload


# --- OAuth callback endpoint ---


class TestMetaCallback:
    """Tests for GET /auth/meta/callback."""

    def _make_state(self, org_id="org-456", user_id="user-123"):
        return jwt.encode(
            {
                "org_id": org_id,
                "user_id": user_id,
                "nonce": "test-nonce",
                "exp": datetime.now(UTC) + timedelta(minutes=30),
            },
            settings.SUPABASE_SERVICE_ROLE_KEY,
            algorithm="HS256",
        )

    def _mock_resp(self, status_code, json_data):
        resp = MagicMock()
        resp.status_code = status_code
        resp.json.return_value = json_data
        resp.text = str(json_data)
        return resp

    @pytest.mark.asyncio
    async def test_callback_exchanges_code_and_stores_config(self):
        """Callback should exchange code, get long-lived token, discover accounts, store config."""
        from app.auth.meta import meta_callback

        state = self._make_state()
        mock_supabase = MagicMock()
        mock_supabase.table.return_value.upsert.return_value.execute.return_value = (
            MagicMock(data={})
        )

        short_token_resp = self._mock_resp(200, {"access_token": "SHORT_TOKEN"})
        long_token_resp = self._mock_resp(
            200, {"access_token": "LONG_TOKEN", "expires_in": 5184000}
        )
        accounts_resp = self._mock_resp(
            200,
            {
                "data": [
                    {
                        "id": "act_123456",
                        "name": "Test Account",
                        "currency": "USD",
                        "timezone_name": "US/Eastern",
                        "account_status": 1,
                    }
                ]
            },
        )

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.get = AsyncMock(
            side_effect=[short_token_resp, long_token_resp, accounts_resp]
        )

        with patch("app.auth.meta.httpx.AsyncClient", return_value=mock_client):
            response = await meta_callback(
                code="AUTH_CODE", state=state, supabase=mock_supabase
            )

        assert response.status_code == 307
        assert "meta_connected=true" in response.headers["location"]

        # Verify upsert was called with correct provider
        upsert_call = mock_supabase.table.return_value.upsert.call_args
        upsert_data = upsert_call[0][0]
        assert upsert_data["provider"] == "meta_ads"
        assert upsert_data["config"]["access_token"] == "LONG_TOKEN"
        assert upsert_data["config"]["token_type"] == "long_lived_user"
        assert len(upsert_data["config"]["ad_accounts"]) == 1
        assert upsert_data["config"]["ad_accounts"][0]["id"] == "act_123456"

    @pytest.mark.asyncio
    async def test_callback_handles_error_from_meta(self):
        """Callback should redirect with error if Meta sends error."""
        from app.auth.meta import meta_callback

        mock_supabase = MagicMock()
        response = await meta_callback(
            error="access_denied",
            error_description="User denied access",
            supabase=mock_supabase,
        )

        assert response.status_code == 307
        assert "meta_error=access_denied" in response.headers["location"]

    @pytest.mark.asyncio
    async def test_callback_rejects_expired_state(self):
        """Callback should reject expired state JWT."""
        from app.auth.meta import meta_callback

        expired_state = jwt.encode(
            {
                "org_id": "org-456",
                "user_id": "user-123",
                "nonce": "test",
                "exp": datetime.now(UTC) - timedelta(minutes=5),
            },
            settings.SUPABASE_SERVICE_ROLE_KEY,
            algorithm="HS256",
        )

        mock_supabase = MagicMock()
        with pytest.raises(Exception) as exc_info:
            await meta_callback(
                code="CODE", state=expired_state, supabase=mock_supabase
            )
        assert "expired" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_callback_rejects_missing_code(self):
        """Callback should reject missing code."""
        from app.auth.meta import meta_callback

        mock_supabase = MagicMock()
        with pytest.raises(Exception):
            await meta_callback(code=None, state=None, supabase=mock_supabase)


# --- Token management ---


class TestMetaTokenManagement:
    """Tests for Meta token lifecycle."""

    @pytest.mark.asyncio
    async def test_get_valid_meta_token_returns_active_token(self):
        """Should return token when it's still valid."""
        from app.integrations.meta_auth import get_valid_meta_token

        mock_supabase = MagicMock()
        future_expiry = (datetime.now(UTC) + timedelta(days=30)).isoformat()
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.maybe_single.return_value.execute.return_value = MagicMock(
            data={
                "config": {
                    "access_token": "VALID_TOKEN",
                    "token_type": "long_lived_user",
                    "token_expires_at": future_expiry,
                }
            }
        )

        token = await get_valid_meta_token("org-123", mock_supabase)
        assert token == "VALID_TOKEN"

    @pytest.mark.asyncio
    async def test_get_valid_meta_token_refreshes_near_expiry(self):
        """Should refresh token when < 7 days remaining."""
        from app.integrations.meta_auth import get_valid_meta_token

        mock_supabase = MagicMock()
        near_expiry = (datetime.now(UTC) + timedelta(days=3)).isoformat()
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.maybe_single.return_value.execute.return_value = MagicMock(
            data={
                "config": {
                    "access_token": "OLD_TOKEN",
                    "token_type": "long_lived_user",
                    "token_expires_at": near_expiry,
                }
            }
        )
        mock_supabase.table.return_value.update.return_value.eq.return_value.eq.return_value.execute.return_value = (
            MagicMock()
        )

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "access_token": "NEW_TOKEN",
            "expires_in": 5184000,
        }
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.get = AsyncMock(return_value=mock_resp)

        with patch("app.integrations.meta_auth.httpx.AsyncClient", return_value=mock_client):
            token = await get_valid_meta_token("org-123", mock_supabase)

        assert token == "NEW_TOKEN"

    @pytest.mark.asyncio
    async def test_get_valid_meta_token_raises_on_expired(self):
        """Should raise when token is expired."""
        from app.integrations.meta_auth import (
            MetaReauthRequiredError,
            get_valid_meta_token,
        )

        mock_supabase = MagicMock()
        expired = (datetime.now(UTC) - timedelta(days=1)).isoformat()
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.maybe_single.return_value.execute.return_value = MagicMock(
            data={
                "config": {
                    "access_token": "EXPIRED_TOKEN",
                    "token_type": "long_lived_user",
                    "token_expires_at": expired,
                }
            }
        )

        with pytest.raises(MetaReauthRequiredError):
            await get_valid_meta_token("org-123", mock_supabase)

    @pytest.mark.asyncio
    async def test_get_valid_meta_token_non_expiring_system_user(self):
        """Non-expiring system user tokens should always be returned."""
        from app.integrations.meta_auth import get_valid_meta_token

        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.maybe_single.return_value.execute.return_value = MagicMock(
            data={
                "config": {
                    "access_token": "SYSTEM_TOKEN",
                    "token_type": "system_user_non_expiring",
                }
            }
        )

        token = await get_valid_meta_token("org-123", mock_supabase)
        assert token == "SYSTEM_TOKEN"

    @pytest.mark.asyncio
    async def test_get_valid_meta_token_raises_on_no_config(self):
        """Should raise when no config exists."""
        from app.integrations.meta_auth import (
            MetaReauthRequiredError,
            get_valid_meta_token,
        )

        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.maybe_single.return_value.execute.return_value = MagicMock(
            data=None
        )

        with pytest.raises(MetaReauthRequiredError):
            await get_valid_meta_token("org-123", mock_supabase)


class TestAppsecretProof:
    """Tests for appsecret_proof computation."""

    def test_compute_appsecret_proof(self):
        """Should produce correct HMAC-SHA256 hash."""
        from app.integrations.meta_auth import compute_appsecret_proof

        result = compute_appsecret_proof("my_secret", "my_token")
        assert isinstance(result, str)
        assert len(result) == 64  # SHA-256 hex digest

    def test_compute_appsecret_proof_deterministic(self):
        """Same inputs should produce same output."""
        from app.integrations.meta_auth import compute_appsecret_proof

        result1 = compute_appsecret_proof("secret", "token")
        result2 = compute_appsecret_proof("secret", "token")
        assert result1 == result2

    def test_compute_appsecret_proof_different_inputs(self):
        """Different inputs should produce different outputs."""
        from app.integrations.meta_auth import compute_appsecret_proof

        result1 = compute_appsecret_proof("secret1", "token")
        result2 = compute_appsecret_proof("secret2", "token")
        assert result1 != result2


class TestSystemUserTokenGeneration:
    """Tests for system user token generation."""

    @pytest.mark.asyncio
    async def test_generate_system_user_token(self):
        """Should install app and generate token."""
        from app.integrations.meta_auth import generate_system_user_token

        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.maybe_single.return_value.execute.return_value = MagicMock(
            data={
                "config": {
                    "access_token": "ADMIN_TOKEN",
                    "token_type": "long_lived_user",
                }
            }
        )
        mock_supabase.table.return_value.update.return_value.eq.return_value.eq.return_value.execute.return_value = (
            MagicMock()
        )

        install_resp = MagicMock(status_code=200, json=lambda: {"success": True})
        token_resp = MagicMock(
            status_code=200,
            json=lambda: {"access_token": "SYSTEM_USER_TOKEN"},
        )

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(side_effect=[install_resp, token_resp])

        with patch("app.integrations.meta_auth.httpx.AsyncClient", return_value=mock_client):
            token = await generate_system_user_token("org-123", mock_supabase)

        assert token == "SYSTEM_USER_TOKEN"


class TestPageTokenAcquisition:
    """Tests for page token acquisition."""

    @pytest.mark.asyncio
    async def test_acquire_page_token(self):
        """Should fetch and store page access token."""
        from app.integrations.meta_auth import acquire_page_token

        mock_supabase = MagicMock()
        future_expiry = (datetime.now(UTC) + timedelta(days=30)).isoformat()

        # For get_valid_meta_token
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.maybe_single.return_value.execute.return_value = MagicMock(
            data={
                "config": {
                    "access_token": "USER_TOKEN",
                    "token_type": "long_lived_user",
                    "token_expires_at": future_expiry,
                }
            }
        )
        mock_supabase.table.return_value.update.return_value.eq.return_value.eq.return_value.execute.return_value = (
            MagicMock()
        )

        page_resp = MagicMock()
        page_resp.status_code = 200
        page_resp.json.return_value = {"access_token": "PAGE_TOKEN_123"}

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.get = AsyncMock(return_value=page_resp)

        with patch("app.integrations.meta_auth.httpx.AsyncClient", return_value=mock_client):
            token = await acquire_page_token("org-123", "page-456", mock_supabase)

        assert token == "PAGE_TOKEN_123"


class TestMetaStatus:
    """Tests for GET /auth/meta/status."""

    @pytest.mark.asyncio
    async def test_status_connected(self):
        """Should return connected status."""
        from app.auth.meta import meta_status

        mock_supabase = MagicMock()
        future_expiry = (datetime.now(UTC) + timedelta(days=30)).isoformat()
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.maybe_single.return_value.execute.return_value = MagicMock(
            data={
                "is_active": True,
                "config": {
                    "token_type": "long_lived_user",
                    "token_expires_at": future_expiry,
                    "ad_accounts": [{"id": "act_123"}],
                    "selected_ad_account_id": "act_123",
                    "page_id": "page-789",
                },
            }
        )

        mock_tenant = MagicMock()
        mock_tenant.id = "org-123"

        result = await meta_status(tenant=mock_tenant, supabase=mock_supabase)
        assert result.connected is True
        assert result.token_type == "long_lived_user"
        assert result.page_connected is True
        assert result.needs_reauth is False

    @pytest.mark.asyncio
    async def test_status_not_connected(self):
        """Should return not connected when no config."""
        from app.auth.meta import meta_status

        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.maybe_single.return_value.execute.return_value = MagicMock(
            data=None
        )

        mock_tenant = MagicMock()
        mock_tenant.id = "org-123"

        result = await meta_status(tenant=mock_tenant, supabase=mock_supabase)
        assert result.connected is False
