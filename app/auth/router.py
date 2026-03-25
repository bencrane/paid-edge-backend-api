from fastapi import APIRouter, Depends
from supabase_auth.errors import AuthApiError

from app.auth.models import (
    AuthTokens,
    LoginRequest,
    MembershipInfo,
    MeResponse,
    RefreshRequest,
    SignupRequest,
    UserProfile,
)
from app.dependencies import get_current_user, get_supabase
from app.shared.errors import BadRequestError, UnauthorizedError

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/signup", response_model=AuthTokens)
async def signup(body: SignupRequest, supabase=Depends(get_supabase)):
    try:
        res = supabase.auth.sign_up(
            {"email": body.email, "password": body.password, "options": {"data": {"full_name": body.full_name}}}
        )
    except AuthApiError as e:
        raise BadRequestError(detail=str(e)) from e

    if not res.session:
        raise BadRequestError(detail="Signup failed — check email for confirmation link")

    # Create user_profiles row
    supabase.table("user_profiles").upsert(
        {"id": res.user.id, "full_name": body.full_name}
    ).execute()

    return AuthTokens(
        access_token=res.session.access_token,
        refresh_token=res.session.refresh_token,
        expires_in=res.session.expires_in,
    )


@router.post("/login", response_model=AuthTokens)
async def login(body: LoginRequest, supabase=Depends(get_supabase)):
    try:
        res = supabase.auth.sign_in_with_password(
            {"email": body.email, "password": body.password}
        )
    except AuthApiError as e:
        raise UnauthorizedError(detail=str(e)) from e

    return AuthTokens(
        access_token=res.session.access_token,
        refresh_token=res.session.refresh_token,
        expires_in=res.session.expires_in,
    )


@router.post("/logout", status_code=204)
async def logout(
    user: UserProfile = Depends(get_current_user),
    supabase=Depends(get_supabase),
):
    supabase.auth.sign_out()


@router.get("/me", response_model=MeResponse)
async def me(
    user: UserProfile = Depends(get_current_user),
    supabase=Depends(get_supabase),
):
    # Fetch memberships with org names
    memberships_res = (
        supabase.table("memberships")
        .select("role, organization_id, organizations(name)")
        .eq("user_id", user.id)
        .execute()
    )

    memberships = [
        MembershipInfo(
            organization_id=m["organization_id"],
            organization_name=m["organizations"]["name"],
            role=m["role"],
        )
        for m in memberships_res.data
    ]

    return MeResponse(user=user, memberships=memberships)


@router.post("/refresh", response_model=AuthTokens)
async def refresh(body: RefreshRequest, supabase=Depends(get_supabase)):
    try:
        res = supabase.auth.refresh_session(body.refresh_token)
    except AuthApiError as e:
        raise UnauthorizedError(detail=str(e)) from e

    return AuthTokens(
        access_token=res.session.access_token,
        refresh_token=res.session.refresh_token,
        expires_in=res.session.expires_in,
    )
