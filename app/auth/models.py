from datetime import datetime

from pydantic import BaseModel, EmailStr

# --- Request models ---


class SignupRequest(BaseModel):
    email: EmailStr
    password: str
    full_name: str


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class RefreshRequest(BaseModel):
    refresh_token: str


# --- Response models ---


class AuthTokens(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int


class UserProfile(BaseModel):
    id: str
    email: str
    full_name: str | None = None
    avatar_url: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class MembershipInfo(BaseModel):
    organization_id: str
    organization_name: str
    role: str


class MeResponse(BaseModel):
    user: UserProfile
    memberships: list[MembershipInfo]
