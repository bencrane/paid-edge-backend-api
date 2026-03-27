from fastapi import APIRouter, Depends

from app.auth.models import UserProfile
from app.dependencies import get_current_user, get_supabase, get_tenant
from app.shared.errors import BadRequestError, NotFoundError
from app.tenants.models import (
    CreateOrgRequest,
    InviteMemberRequest,
    Membership,
    Organization,
    ProviderConfig,
    ProviderConfigRequest,
    SelectAdAccountRequest,
    UpdateOrgRequest,
    mask_provider_config,
)
from app.tenants.service import require_admin, require_membership

router = APIRouter(prefix="/orgs", tags=["organizations"])


# --- Org CRUD ---


@router.get("", response_model=list[Organization])
async def list_orgs(
    user: UserProfile = Depends(get_current_user),
    supabase=Depends(get_supabase),
):
    res = (
        supabase.table("memberships")
        .select("organizations(*)")
        .eq("user_id", user.id)
        .execute()
    )
    return [Organization(**m["organizations"]) for m in res.data]


@router.post("", response_model=Organization, status_code=201)
async def create_org(
    body: CreateOrgRequest,
    user: UserProfile = Depends(get_current_user),
    supabase=Depends(get_supabase),
):
    org_res = (
        supabase.table("organizations")
        .insert(body.model_dump(exclude_none=True))
        .execute()
    )
    org = org_res.data[0]

    # Creator becomes admin
    supabase.table("memberships").insert(
        {"user_id": user.id, "organization_id": org["id"], "role": "admin"}
    ).execute()

    return Organization(**org)


@router.get("/{org_id}", response_model=Organization)
async def get_org(
    org_id: str,
    tenant: Organization = Depends(get_tenant),
):
    # get_tenant already verified membership and resolved the org
    if tenant.id != org_id:
        raise NotFoundError(detail="Organization not found")
    return tenant


@router.patch("/{org_id}", response_model=Organization)
async def update_org(
    org_id: str,
    body: UpdateOrgRequest,
    user: UserProfile = Depends(get_current_user),
    supabase=Depends(get_supabase),
):
    await require_admin(user.id, org_id, supabase)

    update_data = body.model_dump(exclude_none=True)
    if not update_data:
        # Nothing to update — just return the current org
        res = supabase.table("organizations").select("*").eq("id", org_id).single().execute()
        return Organization(**res.data)

    res = (
        supabase.table("organizations")
        .update(update_data)
        .eq("id", org_id)
        .execute()
    )
    return Organization(**res.data[0])


# --- Member management ---


@router.post("/{org_id}/members", response_model=Membership, status_code=201)
async def invite_member(
    org_id: str,
    body: InviteMemberRequest,
    user: UserProfile = Depends(get_current_user),
    supabase=Depends(get_supabase),
):
    await require_admin(user.id, org_id, supabase)

    # Look up the invited user by email
    profile_res = (
        supabase.table("user_profiles")
        .select("id")
        .eq("email", body.email)
        .maybe_single()
        .execute()
    )

    if not profile_res.data:
        # Try auth.users via admin API
        users_res = supabase.auth.admin.list_users()
        target_user = next(
            (u for u in users_res if u.email == body.email),
            None,
        )
        if not target_user:
            raise NotFoundError(
                detail="Unable to invite user. "
                "They may need to create an account first."
            )
        target_user_id = target_user.id
    else:
        target_user_id = profile_res.data["id"]

    membership_res = (
        supabase.table("memberships")
        .insert({"user_id": target_user_id, "organization_id": org_id, "role": body.role})
        .execute()
    )
    return Membership(**membership_res.data[0])


@router.delete("/{org_id}/members/{user_id}", status_code=204)
async def remove_member(
    org_id: str,
    user_id: str,
    user: UserProfile = Depends(get_current_user),
    supabase=Depends(get_supabase),
):
    await require_admin(user.id, org_id, supabase)

    supabase.table("memberships").delete().eq(
        "user_id", user_id
    ).eq("organization_id", org_id).execute()


# --- Provider config management ---


@router.get("/{org_id}/providers", response_model=list[ProviderConfig])
async def list_providers(
    org_id: str,
    user: UserProfile = Depends(get_current_user),
    supabase=Depends(get_supabase),
):
    await require_membership(user.id, org_id, supabase)

    res = (
        supabase.table("provider_configs")
        .select("*")
        .eq("organization_id", org_id)
        .execute()
    )
    # Mask secret values (tokens, keys) in the config dict
    return [
        ProviderConfig(**{**p, "config": mask_provider_config(p["config"])})
        for p in res.data
    ]


@router.put("/{org_id}/providers/{provider}", response_model=ProviderConfig)
async def upsert_provider(
    org_id: str,
    provider: str,
    body: ProviderConfigRequest,
    user: UserProfile = Depends(get_current_user),
    supabase=Depends(get_supabase),
):
    await require_admin(user.id, org_id, supabase)

    res = (
        supabase.table("provider_configs")
        .upsert(
            {
                "organization_id": org_id,
                "provider": provider,
                "config": body.config,
                "is_active": body.is_active,
            },
            on_conflict="organization_id,provider",
        )
        .execute()
    )
    return ProviderConfig(**res.data[0])


@router.delete("/{org_id}/providers/{provider}", status_code=204)
async def remove_provider(
    org_id: str,
    provider: str,
    user: UserProfile = Depends(get_current_user),
    supabase=Depends(get_supabase),
):
    await require_admin(user.id, org_id, supabase)

    supabase.table("provider_configs").delete().eq(
        "organization_id", org_id
    ).eq("provider", provider).execute()


# --- LinkedIn ad account selection ---


@router.put(
    "/{org_id}/providers/linkedin_ads/account",
    response_model=ProviderConfig,
)
async def select_linkedin_ad_account(
    org_id: str,
    body: SelectAdAccountRequest,
    user: UserProfile = Depends(get_current_user),
    supabase=Depends(get_supabase),
):
    """Set the active LinkedIn ad account for this organization."""
    await require_admin(user.id, org_id, supabase)

    res = (
        supabase.table("provider_configs")
        .select("*")
        .eq("organization_id", org_id)
        .eq("provider", "linkedin_ads")
        .maybe_single()
        .execute()
    )
    if not res.data:
        raise NotFoundError(detail="LinkedIn not connected for this organization")

    config = res.data["config"]

    # Validate the account ID exists in the stored ad_accounts list
    valid_ids = [a["id"] for a in config.get("ad_accounts", [])]
    if body.ad_account_id not in valid_ids:
        raise BadRequestError(
            detail=f"Ad account {body.ad_account_id} not found in connected accounts. "
            f"Valid IDs: {valid_ids}"
        )

    config["selected_ad_account_id"] = body.ad_account_id

    updated = (
        supabase.table("provider_configs")
        .update({"config": config})
        .eq("organization_id", org_id)
        .eq("provider", "linkedin_ads")
        .execute()
    )
    return ProviderConfig(**updated.data[0])
