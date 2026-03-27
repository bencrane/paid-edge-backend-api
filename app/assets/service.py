"""Asset generation orchestrator.

Coordinates multi-asset generation: build context, dispatch to generators,
render renderable assets, persist to generated_assets table.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import Any

from pydantic import BaseModel
from supabase import Client as SupabaseClient

from app.assets.context import AssetContext, build_asset_context
from app.assets.generators.ad_copy import generate_ad_copy
from app.assets.generators.case_study_page import generate_case_study_page
from app.assets.generators.document_ad import generate_document_ad
from app.assets.generators.email_copy import generate_email_sequence
from app.assets.generators.image_brief import generate_image_briefs
from app.assets.generators.landing_page import generate_landing_page
from app.assets.generators.lead_magnet import generate_lead_magnet
from app.assets.generators.video_script import generate_video_script
from app.assets.renderers.document_ad_pdf import render_document_ad_pdf
from app.assets.renderers.lead_magnet_pdf import render_lead_magnet_pdf
from app.assets.storage import upload_asset
from app.integrations.claude_ai import ClaudeClient
from app.shared.errors import BadRequestError, NotFoundError
from app.shared.usage import UsageEvent, record_usage_event

logger = logging.getLogger(__name__)

# Asset types that produce a rendered file (PDF / HTML)
_RENDERABLE_TYPES = {"lead_magnet", "document_ad", "landing_page", "case_study_page"}

# Asset types that store content as JSON only
_TEXT_ONLY_TYPES = {"ad_copy", "email_copy", "video_script", "image_brief"}

VALID_ASSET_TYPES = _RENDERABLE_TYPES | _TEXT_ONLY_TYPES


class AssetGenerationService:
    """Orchestrates multi-asset generation, rendering, and persistence."""

    def __init__(self, claude: ClaudeClient, supabase: SupabaseClient):
        self.claude = claude
        self.supabase = supabase

    # ------------------------------------------------------------------
    # Main generation entry point
    # ------------------------------------------------------------------

    async def generate(
        self,
        org_id: str,
        campaign_id: str,
        asset_types: list[str],
        *,
        user_id: str | None = None,
        platforms: list[str] | None = None,
        angle: str | None = None,
        tone: str | None = None,
        cta: str | None = None,
        lead_magnet_format: str | None = None,
        landing_page_template: str | None = None,
        document_ad_pattern: str | None = None,
        video_duration: str | None = None,
        email_trigger: str | None = None,
    ) -> list[dict]:
        """Orchestrate generation for multiple asset types in parallel."""
        # Validate asset types
        for at in asset_types:
            if at not in VALID_ASSET_TYPES:
                raise BadRequestError(
                    detail=f"Unknown asset type '{at}'. "
                    f"Valid: {sorted(VALID_ASSET_TYPES)}"
                )

        # Build context once — shared across all generators
        ctx = await build_asset_context(org_id, campaign_id, self.supabase)

        # Apply overrides
        if angle:
            ctx.angle = angle
        if tone:
            ctx.brand_voice = tone
        if platforms:
            ctx.platforms = platforms

        # Build generation tasks
        tasks = []
        for at in asset_types:
            tasks.append(
                self._generate_single(
                    ctx=ctx,
                    org_id=org_id,
                    campaign_id=campaign_id,
                    asset_type=at,
                    user_id=user_id,
                    platforms=platforms,
                    cta=cta,
                    lead_magnet_format=lead_magnet_format,
                    landing_page_template=landing_page_template,
                    document_ad_pattern=document_ad_pattern,
                    video_duration=video_duration,
                    email_trigger=email_trigger,
                )
            )

        # Run all in parallel — each handles its own errors
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Convert exceptions to failed responses
        final: list[dict] = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(
                    "Generation failed for %s: %s",
                    asset_types[i],
                    result,
                )
                final.append({
                    "id": str(uuid.uuid4()),
                    "asset_type": asset_types[i],
                    "status": "failed",
                    "content_url": None,
                    "content_preview": None,
                    "template_used": None,
                    "error": str(result),
                })
            else:
                final.append(result)

        return final

    # ------------------------------------------------------------------
    # Single asset generation
    # ------------------------------------------------------------------

    async def _generate_single(
        self,
        ctx: AssetContext,
        org_id: str,
        campaign_id: str,
        asset_type: str,
        *,
        user_id: str | None = None,
        **kwargs: Any,
    ) -> dict:
        """Generate, render (if needed), persist, and return a single asset."""
        asset_id = str(uuid.uuid4())
        t0 = time.monotonic()

        # Create row with status='generating'
        self.supabase.table("generated_assets").insert({
            "id": asset_id,
            "organization_id": org_id,
            "campaign_id": campaign_id,
            "asset_type": asset_type,
            "status": "generating",
        }).execute()

        try:
            result = await self._dispatch_generator(ctx, asset_type, **kwargs)
            content_url = None
            content_json = None
            template_used = None

            if asset_type in _RENDERABLE_TYPES:
                content_url, template_used = await self._render_and_upload(
                    asset_id, asset_type, result
                )
            else:
                # Text-only: serialize to JSON
                if isinstance(result, BaseModel):
                    content_json = result.model_dump()
                elif isinstance(result, dict):
                    # ad_copy returns dict[str, BaseModel]
                    content_json = {
                        k: v.model_dump() if isinstance(v, BaseModel) else v
                        for k, v in result.items()
                    }
                else:
                    content_json = result

            # Update row to 'draft'
            update_data: dict[str, Any] = {"status": "draft"}
            if content_url:
                update_data["content_url"] = content_url
            if template_used:
                update_data["template_used"] = template_used
            if content_json is not None:
                update_data["content_json"] = content_json

            self.supabase.table("generated_assets").update(
                update_data
            ).eq("id", asset_id).execute()

            # Build preview for text-only assets
            content_preview = None
            if content_json is not None:
                content_preview = _build_preview(content_json)

            # Record usage event (fire-and-forget)
            if user_id:
                duration_ms = int((time.monotonic() - t0) * 1000)
                record_usage_event(
                    UsageEvent(
                        org_id=org_id,
                        user_id=user_id,
                        asset_type=asset_type,
                        status="success",
                        duration_ms=duration_ms,
                    ),
                    self.supabase,
                )

            return {
                "id": asset_id,
                "asset_type": asset_type,
                "status": "draft",
                "content_url": content_url,
                "content_preview": content_preview,
                "template_used": template_used,
                "error": None,
            }

        except Exception as exc:
            logger.error("Generation error for %s (%s): %s", asset_type, asset_id, exc)
            # Update row to 'failed'
            self.supabase.table("generated_assets").update(
                {"status": "failed", "error_message": str(exc)}
            ).eq("id", asset_id).execute()

            # Record failed usage event
            if user_id:
                duration_ms = int((time.monotonic() - t0) * 1000)
                record_usage_event(
                    UsageEvent(
                        org_id=org_id,
                        user_id=user_id,
                        asset_type=asset_type,
                        status="failed",
                        duration_ms=duration_ms,
                    ),
                    self.supabase,
                )

            raise

    # ------------------------------------------------------------------
    # Generator dispatch
    # ------------------------------------------------------------------

    async def _dispatch_generator(
        self, ctx: AssetContext, asset_type: str, **kwargs: Any
    ) -> Any:
        """Call the appropriate convenience function for the asset type."""
        if asset_type == "lead_magnet":
            fmt = kwargs.get("lead_magnet_format") or "checklist"
            return await generate_lead_magnet(self.claude, ctx, format=fmt)

        if asset_type == "landing_page":
            template = kwargs.get("landing_page_template") or "lead_magnet_download"
            return await generate_landing_page(self.claude, ctx, template_type=template)

        if asset_type == "document_ad":
            pattern = kwargs.get("document_ad_pattern") or "problem_solution"
            return await generate_document_ad(self.claude, ctx, pattern=pattern)

        if asset_type == "case_study_page":
            return await generate_case_study_page(self.claude, ctx)

        if asset_type == "ad_copy":
            platforms = kwargs.get("platforms") or ctx.platforms or ["linkedin"]
            return await generate_ad_copy(self.claude, ctx, platforms=platforms)

        if asset_type == "email_copy":
            trigger = kwargs.get("email_trigger") or "lead_magnet_download"
            return await generate_email_sequence(self.claude, ctx, trigger=trigger)

        if asset_type == "video_script":
            duration = kwargs.get("video_duration") or "30s"
            platform = (ctx.platforms[0] if ctx.platforms else "linkedin")
            return await generate_video_script(
                self.claude, ctx, duration=duration, platform=platform
            )

        if asset_type == "image_brief":
            platforms = kwargs.get("platforms") or ["linkedin_sponsored"]
            return await generate_image_briefs(self.claude, ctx, platforms=platforms)

        raise BadRequestError(detail=f"No generator for asset type '{asset_type}'")

    # ------------------------------------------------------------------
    # Rendering & upload
    # ------------------------------------------------------------------

    async def _render_and_upload(
        self, asset_id: str, asset_type: str, content: Any
    ) -> tuple[str, str]:
        """Render a renderable asset and upload, return (content_url, template_used)."""
        if asset_type == "lead_magnet":
            pdf_bytes = render_lead_magnet_pdf(content)
            filename = f"lead-magnets/{asset_id}.pdf"
            url = await upload_asset(pdf_bytes, filename, "application/pdf")
            return url, "lead_magnet_pdf"

        if asset_type == "document_ad":
            pdf_bytes = render_document_ad_pdf(content)
            filename = f"document-ads/{asset_id}.pdf"
            url = await upload_asset(pdf_bytes, filename, "application/pdf")
            return url, "document_ad_pdf"

        if asset_type == "landing_page":
            # Landing page content is already a rendering input model.
            # For now, store as JSON — full HTML rendering happens in render router.
            return "", content.template if hasattr(content, "template") else "landing_page"

        if asset_type == "case_study_page":
            return "", "case_study"

        raise BadRequestError(detail=f"Cannot render asset type '{asset_type}'")

    # ------------------------------------------------------------------
    # Revision
    # ------------------------------------------------------------------

    async def revise_asset(
        self, asset_id: str, revision_instructions: str, org_id: str
    ) -> dict:
        """Re-generate an asset with revision instructions appended."""
        row = self._get_asset_or_404(asset_id, org_id)

        asset_type = row["asset_type"]
        campaign_id = row.get("campaign_id")

        # Rebuild context
        ctx = await build_asset_context(org_id, campaign_id, self.supabase)

        # Append revision instructions to context
        original_angle = ctx.angle or ""
        ctx.angle = f"{original_angle}\n\nREVISION INSTRUCTIONS: {revision_instructions}"

        # Update status to generating
        self.supabase.table("generated_assets").update(
            {"status": "generating"}
        ).eq("id", asset_id).execute()

        try:
            result = await self._dispatch_generator(ctx, asset_type)
            content_url = None
            content_json = None
            template_used = None

            if asset_type in _RENDERABLE_TYPES:
                content_url, template_used = await self._render_and_upload(
                    asset_id, asset_type, result
                )
            else:
                if isinstance(result, BaseModel):
                    content_json = result.model_dump()
                elif isinstance(result, dict):
                    content_json = {
                        k: v.model_dump() if isinstance(v, BaseModel) else v
                        for k, v in result.items()
                    }
                else:
                    content_json = result

            update_data: dict[str, Any] = {"status": "draft"}
            if content_url:
                update_data["content_url"] = content_url
            if template_used:
                update_data["template_used"] = template_used
            if content_json is not None:
                update_data["content_json"] = content_json

            self.supabase.table("generated_assets").update(
                update_data
            ).eq("id", asset_id).execute()

            content_preview = None
            if content_json is not None:
                content_preview = _build_preview(content_json)

            return {
                "id": asset_id,
                "asset_type": asset_type,
                "status": "draft",
                "content_url": content_url,
                "content_preview": content_preview,
                "template_used": template_used,
                "error": None,
            }

        except Exception as exc:
            self.supabase.table("generated_assets").update(
                {"status": "failed", "error_message": str(exc)}
            ).eq("id", asset_id).execute()
            raise

    # ------------------------------------------------------------------
    # Approve
    # ------------------------------------------------------------------

    def approve_asset(self, asset_id: str, org_id: str) -> dict:
        """Transition asset from draft → approved."""
        row = self._get_asset_or_404(asset_id, org_id)

        if row["status"] != "draft":
            raise BadRequestError(
                detail=f"Can only approve draft assets. Current status: {row['status']}"
            )

        self.supabase.table("generated_assets").update(
            {"status": "approved"}
        ).eq("id", asset_id).execute()

        row["status"] = "approved"
        return row

    # ------------------------------------------------------------------
    # Read helpers
    # ------------------------------------------------------------------

    def get_asset(self, asset_id: str, org_id: str) -> dict:
        """Get full asset detail."""
        return self._get_asset_or_404(asset_id, org_id)

    def list_campaign_assets(
        self,
        campaign_id: str,
        org_id: str,
        *,
        status: str | None = None,
        asset_type: str | None = None,
    ) -> list[dict]:
        """List all assets for a campaign with optional filters."""
        query = (
            self.supabase.table("generated_assets")
            .select("*")
            .eq("campaign_id", campaign_id)
            .eq("organization_id", org_id)
        )
        if status:
            query = query.eq("status", status)
        if asset_type:
            query = query.eq("asset_type", asset_type)
        res = query.order("created_at", desc=True).execute()
        return res.data or []

    def update_asset(
        self,
        asset_id: str,
        org_id: str,
        *,
        content_json: dict | None = None,
        content_url: str | None = None,
        status: str | None = None,
    ) -> dict:
        """Direct content update — edit stored content without AI re-generation."""
        row = self._get_asset_or_404(asset_id, org_id)

        update_data: dict[str, Any] = {}
        if content_json is not None:
            update_data["content_json"] = content_json
        if content_url is not None:
            update_data["content_url"] = content_url
        if status is not None:
            _VALID_STATUSES = {"draft", "approved", "generating", "failed"}
            if status not in _VALID_STATUSES:
                raise BadRequestError(
                    detail=f"Invalid status '{status}'. "
                    f"Valid: {sorted(_VALID_STATUSES)}"
                )
            update_data["status"] = status

        if not update_data:
            return row

        self.supabase.table("generated_assets").update(
            update_data
        ).eq("id", asset_id).execute()

        row.update(update_data)
        return row

    def _get_asset_or_404(self, asset_id: str, org_id: str) -> dict:
        """Fetch a single asset scoped to org, raise 404 if missing."""
        res = (
            self.supabase.table("generated_assets")
            .select("*")
            .eq("id", asset_id)
            .eq("organization_id", org_id)
            .maybe_single()
            .execute()
        )
        if not res.data:
            raise NotFoundError(detail="Asset not found")
        return res.data


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_preview(content: Any, max_chars: int = 500) -> dict:
    """Build a preview dict from content JSON for the response."""
    if isinstance(content, dict):
        preview_str = str(content)[:max_chars]
        return {"summary": preview_str, "type": "json"}
    return {"summary": str(content)[:max_chars], "type": "text"}
