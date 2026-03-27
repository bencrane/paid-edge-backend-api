import uuid
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends
from jinja2 import Environment, FileSystemLoader
from pydantic import BaseModel
from supabase import Client as SupabaseClient

from app.assets.models import (
    DocumentAdInput,
    LandingPageInput,
    LeadMagnetPDFInput,
)
from app.assets.renderers.document_ad_pdf import render_document_ad_pdf
from app.assets.renderers.lead_magnet_pdf import render_lead_magnet_pdf
from app.assets.storage import upload_asset
from app.auth.models import UserProfile
from app.dependencies import get_current_user, get_supabase, get_tenant
from app.tenants.models import Organization

router = APIRouter(prefix="/render", tags=["assets"])

_TEMPLATES_DIR = Path(__file__).parent / "templates"
_jinja_env = Environment(
    loader=FileSystemLoader(str(_TEMPLATES_DIR)),
    autoescape=True,
)


class RenderLPResponse(BaseModel):
    asset_id: str
    content_url: str
    template_used: str
    slug: str


class RenderPDFResponse(BaseModel):
    asset_id: str
    content_url: str


def _generate_slug() -> str:
    return uuid.uuid4().hex[:12]


@router.post("/landing-page", response_model=RenderLPResponse)
async def render_landing_page(
    payload: LandingPageInput,
    user: Annotated[UserProfile, Depends(get_current_user)],
    tenant: Annotated[Organization, Depends(get_tenant)],
    supabase: Annotated[SupabaseClient, Depends(get_supabase)],
):
    template_name = payload.template
    template = _jinja_env.get_template(f"{template_name}.html")

    slug = _generate_slug()
    html = template.render(slug=slug, **payload.model_dump())

    asset_id = str(uuid.uuid4())
    filename = f"landing-pages/{asset_id}.html"
    content_url = await upload_asset(
        html.encode("utf-8"), filename, "text/html"
    )

    # Persist to generated_assets
    supabase.table("generated_assets").insert({
        "id": asset_id,
        "organization_id": str(tenant.id),
        "asset_type": "landing_page",
        "template_used": template_name,
        "input_data": payload.model_dump(),
        "content_url": content_url,
        "slug": slug,
    }).execute()

    return RenderLPResponse(
        asset_id=asset_id,
        content_url=content_url,
        template_used=template_name,
        slug=slug,
    )


@router.post("/lead-magnet", response_model=RenderPDFResponse)
async def render_lead_magnet(
    payload: LeadMagnetPDFInput,
    user: Annotated[UserProfile, Depends(get_current_user)],
    tenant: Annotated[Organization, Depends(get_tenant)],
    supabase: Annotated[SupabaseClient, Depends(get_supabase)],
):
    pdf_bytes = render_lead_magnet_pdf(payload)

    asset_id = str(uuid.uuid4())
    filename = f"lead-magnets/{asset_id}.pdf"
    content_url = await upload_asset(pdf_bytes, filename, "application/pdf")

    supabase.table("generated_assets").insert({
        "id": asset_id,
        "organization_id": str(tenant.id),
        "asset_type": "lead_magnet_pdf",
        "template_used": "lead_magnet_pdf",
        "input_data": payload.model_dump(),
        "content_url": content_url,
    }).execute()

    return RenderPDFResponse(asset_id=asset_id, content_url=content_url)


@router.post("/document-ad", response_model=RenderPDFResponse)
async def render_document_ad(
    payload: DocumentAdInput,
    user: Annotated[UserProfile, Depends(get_current_user)],
    tenant: Annotated[Organization, Depends(get_tenant)],
    supabase: Annotated[SupabaseClient, Depends(get_supabase)],
):
    pdf_bytes = render_document_ad_pdf(payload)

    asset_id = str(uuid.uuid4())
    filename = f"document-ads/{asset_id}.pdf"
    content_url = await upload_asset(pdf_bytes, filename, "application/pdf")

    supabase.table("generated_assets").insert({
        "id": asset_id,
        "organization_id": str(tenant.id),
        "asset_type": "document_ad",
        "template_used": "document_ad_pdf",
        "input_data": payload.model_dump(),
        "content_url": content_url,
    }).execute()

    return RenderPDFResponse(asset_id=asset_id, content_url=content_url)
