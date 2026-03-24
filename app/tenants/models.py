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
