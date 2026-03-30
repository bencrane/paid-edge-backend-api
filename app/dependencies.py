from clickhouse_connect.driver import Client as CHClient
from fastapi import Depends, Request
from supabase import Client as SupabaseClient

from app.auth.models import UserProfile
from app.db.clickhouse import get_clickhouse_client
from app.db.supabase import get_supabase_client
from app.integrations.claude_ai import ClaudeClient
from app.shared.errors import NotFoundError, UnauthorizedError
from app.tenants.models import Organization
from app.tenants.service import resolve_tenant


async def get_supabase() -> SupabaseClient:
    return get_supabase_client()


async def get_clickhouse() -> CHClient:
    return get_clickhouse_client()


async def get_current_user(
    request: Request,
    supabase: SupabaseClient = Depends(get_supabase),
) -> UserProfile:
    user_id: str | None = getattr(request.state, "user_id", None)
    if not user_id:
        raise UnauthorizedError()

    res = (
        supabase.table("user_profiles")
        .select("id, full_name, avatar_url, created_at, updated_at")
        .eq("id", user_id)
        .maybe_single()
        .execute()
    )
    if not res.data:
        raise NotFoundError(detail="User profile not found")

    # Fetch email from JWT payload stored in request state
    email = getattr(request.state, "jwt_payload", {}).get("email", "")

    return UserProfile(email=email, **res.data)


async def get_claude() -> ClaudeClient:
    return ClaudeClient()


async def get_tenant(
    request: Request,
    user: UserProfile = Depends(get_current_user),
    supabase: SupabaseClient = Depends(get_supabase),
) -> Organization:
    org_id = getattr(request.state, "org_id", None)
    return await resolve_tenant(user.id, org_id, supabase)
