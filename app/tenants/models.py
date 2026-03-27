from datetime import datetime

from pydantic import BaseModel, EmailStr

# --- Request models ---


class CreateOrgRequest(BaseModel):
    name: str
    slug: str
    domain: str | None = None
    logo_url: str | None = None


class UpdateOrgRequest(BaseModel):
    name: str | None = None
    slug: str | None = None
    domain: str | None = None
    logo_url: str | None = None
    plan: str | None = None


class InviteMemberRequest(BaseModel):
    email: EmailStr
    role: str = "member"


class ProviderConfigRequest(BaseModel):
    config: dict
    is_active: bool = True


class SelectAdAccountRequest(BaseModel):
    ad_account_id: int


# --- Response models ---


class Organization(BaseModel):
    id: str
    name: str
    slug: str
    domain: str | None = None
    logo_url: str | None = None
    plan: str | None = None
    created_at: datetime
    updated_at: datetime | None = None


class Membership(BaseModel):
    id: str
    user_id: str
    organization_id: str
    role: str
    created_at: datetime


class ProviderConfig(BaseModel):
    id: str
    organization_id: str
    provider: str
    config: dict
    is_active: bool
    created_at: datetime
    updated_at: datetime | None = None


# Keys in provider config that contain secrets and must be masked in API responses
_SECRET_CONFIG_KEYS = {
    "access_token",
    "refresh_token",
    "client_secret",
    "api_key",
    "app_secret",
}


def mask_provider_config(config: dict) -> dict:
    """Return a copy of the provider config dict with secret values masked."""
    masked = {}
    for key, value in config.items():
        if key in _SECRET_CONFIG_KEYS and isinstance(value, str) and len(value) > 8:
            masked[key] = value[:4] + "****" + value[-4:]
        else:
            masked[key] = value
    return masked
