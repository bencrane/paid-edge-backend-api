"""Brand context ingestion — tenant_context loading and prompt injection.

BJC-167: Loads all tenant context, campaign data, and formats it for Claude prompts.
"""

from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel
from supabase import Client

logger = logging.getLogger(__name__)

# Maximum approximate token budget for context injection (~8K tokens ≈ 32K chars)
_MAX_CONTEXT_CHARS = 32_000


# ---------------------------------------------------------------------------
# AssetContext model
# ---------------------------------------------------------------------------

class AssetContext(BaseModel):
    """All context needed for Claude to generate a client-specific asset."""

    organization_id: str
    campaign_id: str | None = None

    # Brand
    company_name: str = ""
    brand_voice: str = ""
    brand_guidelines: dict | None = None
    value_proposition: str = ""

    # ICP
    icp_definition: dict | None = None
    target_persona: str = ""

    # Content inputs
    case_studies: list[dict] = []
    testimonials: list[dict] = []
    customer_logos: list[str] = []
    competitor_differentiators: list[str] = []

    # Campaign-specific
    angle: str | None = None
    objective: str | None = None
    platforms: list[str] = []
    industry: str | None = None


# ---------------------------------------------------------------------------
# Context assembly
# ---------------------------------------------------------------------------

async def build_asset_context(
    org_id: str,
    campaign_id: str | None,
    supabase: Client,
) -> AssetContext:
    """Load all tenant_context rows + campaign data into a single AssetContext."""

    ctx = AssetContext(organization_id=org_id)

    # --- Load tenant_context rows ---
    try:
        res = (
            supabase.table("tenant_context")
            .select("*")
            .eq("organization_id", org_id)
            .execute()
        )
        rows = res.data or []
    except Exception:
        logger.warning("Failed to load tenant_context for org %s", org_id)
        rows = []

    # Group by context_type
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        ct = row.get("context_type", "unknown")
        grouped.setdefault(ct, []).append(row)

    # --- Brand guidelines ---
    brand_rows = grouped.get("brand_guidelines", [])
    if brand_rows:
        data = brand_rows[0].get("context_data", {})
        ctx.brand_guidelines = data
        ctx.brand_voice = data.get("voice", data.get("tone", ""))
        ctx.company_name = data.get("company_name", "")

    # --- Positioning ---
    positioning_rows = grouped.get("positioning", [])
    if positioning_rows:
        data = positioning_rows[0].get("context_data", {})
        ctx.value_proposition = data.get("value_proposition", "")
        if not ctx.company_name:
            ctx.company_name = data.get("company_name", "")

    # --- ICP Definition ---
    icp_rows = grouped.get("icp_definition", [])
    if icp_rows:
        data = icp_rows[0].get("context_data", {})
        ctx.icp_definition = data
        ctx.target_persona = _format_icp_summary(data)
        ctx.industry = data.get("industry", None)

    # --- Case studies ---
    for row in grouped.get("case_study", []):
        data = row.get("context_data", {})
        ctx.case_studies.append(data)

    # --- Testimonials ---
    for row in grouped.get("testimonial", []):
        data = row.get("context_data", {})
        ctx.testimonials.append(data)

    # --- Customer logos ---
    for row in grouped.get("customers", []):
        data = row.get("context_data", {})
        logos = data.get("logos", [])
        if isinstance(logos, list):
            ctx.customer_logos.extend(logos)
        elif data.get("logo_url"):
            ctx.customer_logos.append(data["logo_url"])

    # --- Competitors ---
    for row in grouped.get("competitors", []):
        data = row.get("context_data", {})
        diffs = data.get("differentiators", [])
        if isinstance(diffs, list):
            ctx.competitor_differentiators.extend(diffs)
        elif isinstance(diffs, str):
            ctx.competitor_differentiators.append(diffs)

    # --- Load campaign data if provided ---
    if campaign_id:
        try:
            camp_res = (
                supabase.table("campaigns")
                .select("*")
                .eq("id", campaign_id)
                .eq("organization_id", org_id)
                .maybe_single()
                .execute()
            )
            if camp_res.data:
                camp = camp_res.data
                ctx.campaign_id = campaign_id
                ctx.angle = camp.get("angle")
                ctx.objective = camp.get("objective")
                ctx.platforms = camp.get("platforms", [])

                # Load audience segment for additional persona context
                segment_id = camp.get("audience_segment_id")
                if segment_id:
                    seg_res = (
                        supabase.table("audience_segments")
                        .select("*")
                        .eq("id", segment_id)
                        .maybe_single()
                        .execute()
                    )
                    if seg_res.data:
                        seg_data = seg_res.data
                        seg_name = seg_data.get("name", "")
                        seg_desc = seg_data.get("description", "")
                        if seg_name or seg_desc:
                            ctx.target_persona += (
                                f"\n\nAudience Segment: {seg_name}\n{seg_desc}"
                            )
        except Exception:
            logger.warning(
                "Failed to load campaign %s for org %s", campaign_id, org_id
            )

    # --- Log warnings for missing critical context ---
    if not ctx.brand_guidelines:
        logger.warning("Missing brand_guidelines for org %s", org_id)
    if not ctx.icp_definition:
        logger.warning("Missing icp_definition for org %s", org_id)
    if not ctx.company_name:
        logger.warning("Missing company_name for org %s", org_id)
        # Try to load from organization table
        try:
            org_res = (
                supabase.table("organizations")
                .select("name")
                .eq("id", org_id)
                .maybe_single()
                .execute()
            )
            if org_res.data:
                ctx.company_name = org_res.data.get("name", "")
        except Exception:
            pass

    return ctx


def _format_icp_summary(icp: dict) -> str:
    """Format ICP data into a readable persona summary."""
    parts = []
    if icp.get("job_titles"):
        titles = icp["job_titles"]
        if isinstance(titles, list):
            parts.append(f"Job titles: {', '.join(titles)}")
        else:
            parts.append(f"Job titles: {titles}")
    if icp.get("company_size"):
        parts.append(f"Company size: {icp['company_size']}")
    if icp.get("industry"):
        parts.append(f"Industry: {icp['industry']}")
    if icp.get("pain_points"):
        pps = icp["pain_points"]
        if isinstance(pps, list):
            parts.append(f"Pain points: {'; '.join(pps)}")
        else:
            parts.append(f"Pain points: {pps}")
    if icp.get("goals"):
        goals = icp["goals"]
        if isinstance(goals, list):
            parts.append(f"Goals: {'; '.join(goals)}")
        else:
            parts.append(f"Goals: {goals}")
    if icp.get("decision_criteria"):
        parts.append(f"Decision criteria: {icp['decision_criteria']}")
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Prompt formatting utilities
# ---------------------------------------------------------------------------

def format_brand_context_block(ctx: AssetContext) -> str:
    """Format brand context as a structured text block for system prompts.

    Goes in the system prompt — persistent across the conversation.
    """
    lines = []
    if ctx.company_name:
        lines.append(f"COMPANY: {ctx.company_name}")
    if ctx.value_proposition:
        lines.append(f"VALUE PROPOSITION: {ctx.value_proposition}")
    if ctx.brand_voice:
        lines.append(f"BRAND VOICE: {ctx.brand_voice}")
    if ctx.brand_guidelines:
        # Include relevant non-visual guidelines
        for key in ("tone", "messaging_pillars", "dos", "donts", "key_messages"):
            if key in ctx.brand_guidelines:
                val = ctx.brand_guidelines[key]
                if isinstance(val, list):
                    val = "; ".join(str(v) for v in val)
                lines.append(f"{key.upper().replace('_', ' ')}: {val}")
    if ctx.competitor_differentiators:
        lines.append(
            "KEY DIFFERENTIATORS:\n- " + "\n- ".join(ctx.competitor_differentiators)
        )

    block = "\n".join(lines)
    return _truncate_block(block, "brand context")


def format_persona_block(ctx: AssetContext) -> str:
    """Format ICP/persona context for user prompts.

    Goes in the user message — specific to this generation request.
    """
    lines = []
    if ctx.target_persona:
        lines.append(ctx.target_persona)
    if ctx.icp_definition:
        for key in ("seniority", "buying_triggers", "objections"):
            if key in ctx.icp_definition:
                val = ctx.icp_definition[key]
                if isinstance(val, list):
                    val = "; ".join(str(v) for v in val)
                lines.append(f"{key.upper().replace('_', ' ')}: {val}")

    block = "\n".join(lines)
    return _truncate_block(block, "persona")


def format_social_proof_block(ctx: AssetContext) -> str:
    """Format case studies, testimonials, logos for content generation.

    Included as reference material when the asset type needs social proof.
    """
    lines = []

    if ctx.case_studies:
        lines.append("CASE STUDIES:")
        for cs in ctx.case_studies[:3]:  # Limit to 3 for token budget
            name = cs.get("customer_name", "Customer")
            industry = cs.get("customer_industry", "")
            problem = cs.get("problem", "")
            solution = cs.get("solution", "")
            results = cs.get("results", {})
            quote = cs.get("quote", {})

            entry = [f"- {name}"]
            if industry:
                entry.append(f"  Industry: {industry}")
            if problem:
                entry.append(f"  Challenge: {problem}")
            if solution:
                entry.append(f"  Solution: {solution}")
            if results:
                if isinstance(results, dict):
                    metrics = ", ".join(f"{k}: {v}" for k, v in results.items())
                    entry.append(f"  Results: {metrics}")
                else:
                    entry.append(f"  Results: {results}")
            if quote and isinstance(quote, dict):
                q_text = quote.get("text", "")
                q_author = quote.get("author", "")
                q_title = quote.get("title", "")
                if q_text:
                    entry.append(f'  Quote: "{q_text}" — {q_author}, {q_title}')

            lines.extend(entry)

    if ctx.testimonials:
        lines.append("\nTESTIMONIALS:")
        for t in ctx.testimonials[:3]:
            q = t.get("quote", "")
            author = t.get("author", "")
            title = t.get("title", "")
            company = t.get("company", "")
            if q:
                lines.append(f'- "{q}" — {author}, {title}, {company}')

    if ctx.customer_logos:
        lines.append(
            f"\nCUSTOMER LOGOS: {len(ctx.customer_logos)} logos available"
        )

    block = "\n".join(lines)
    return _truncate_block(block, "social proof")


def _truncate_block(text: str, label: str, max_chars: int = 10_000) -> str:
    """Truncate a context block if it exceeds the character budget.

    Priority order for truncation (truncate least important first):
    competitor info → testimonials → case studies → ICP → brand guidelines
    """
    if len(text) <= max_chars:
        return text
    logger.warning(
        "Context block '%s' truncated: %d chars → %d chars",
        label,
        len(text),
        max_chars,
    )
    return text[:max_chars] + "\n[...truncated]"
