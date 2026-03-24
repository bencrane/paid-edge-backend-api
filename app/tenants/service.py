from supabase import Client

from app.shared.errors import ForbiddenError, NotFoundError
from app.tenants.models import Organization


async def resolve_tenant(
    user_id: str,
    org_id: str | None,
    supabase: Client,
) -> Organization:
    """Resolve the active tenant for the current request.

    If org_id is provided (from X-Organization-Id header), verify membership.
    Otherwise fall back to the user's first organization.
    """
    if org_id:
        # Verify membership
        membership = (
            supabase.table("memberships")
            .select("id")
            .eq("user_id", user_id)
            .eq("organization_id", org_id)
            .maybe_single()
            .execute()
        )
        if not membership.data:
            raise ForbiddenError(detail="Not a member of this organization")

        org = (
            supabase.table("organizations")
            .select("*")
            .eq("id", org_id)
            .single()
            .execute()
        )
        return Organization(**org.data)

    # Default to first org
    membership = (
        supabase.table("memberships")
        .select("organization_id, organizations(*)")
        .eq("user_id", user_id)
        .limit(1)
        .maybe_single()
        .execute()
    )
    if not membership.data:
        raise NotFoundError(detail="User has no organization memberships")

    return Organization(**membership.data["organizations"])


async def require_admin(user_id: str, org_id: str, supabase: Client) -> None:
    """Raise ForbiddenError if user is not an admin of the organization."""
    membership = (
        supabase.table("memberships")
        .select("role")
        .eq("user_id", user_id)
        .eq("organization_id", org_id)
        .maybe_single()
        .execute()
    )
    if not membership.data or membership.data["role"] != "admin":
        raise ForbiddenError(detail="Admin access required")
