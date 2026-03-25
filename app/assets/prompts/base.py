"""Base prompt template system for asset generation.

BJC-168: Base templates, output schemas, format enforcement.
"""

from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from typing import Any, TypeVar

from pydantic import BaseModel

from app.assets.context import (
    AssetContext,
    format_brand_context_block,
    format_persona_block,
    format_social_proof_block,
)
from app.integrations.claude_ai import ClaudeClient

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


class PromptTemplate(ABC):
    """Base class for all asset generation prompt templates.

    Each generator subclasses this and provides:
    - asset_type: str identifier
    - model: which Claude model to use
    - output_schema: Pydantic model for output validation
    - temperature: generation temperature (0.3 structured, 0.7 creative)
    - build_asset_specific_instructions(): asset-specific user prompt section
    """

    asset_type: str
    model: str
    output_schema: type[BaseModel]
    temperature: float = 0.5

    def build_system_prompt(self, ctx: AssetContext) -> str:
        """Build system prompt with brand context.

        System prompt structure:
        - Role definition
        - Brand context block (persistent across conversation)
        - Tone and voice guidelines
        - Output rules (XML tags + JSON schema)
        """
        brand_block = format_brand_context_block(ctx)
        company = ctx.company_name or "the client"

        system = (
            f"You are an expert B2B content strategist creating {self.asset_type.replace('_', ' ')} "
            f"content for {company}.\n\n"
        )

        if brand_block:
            system += f"BRAND CONTEXT:\n{brand_block}\n\n"

        if ctx.brand_voice:
            system += f"TONE AND VOICE:\n{ctx.brand_voice}\n\n"

        system += (
            "IMPORTANT RULES:\n"
            "- All content must be specific to this company and audience — never generic\n"
            "- Use concrete examples, real numbers, and specific outcomes\n"
            "- Match the brand voice consistently\n"
            "- Focus on value to the reader, not features of the product\n"
        )

        return system

    def build_user_prompt(self, ctx: AssetContext, **kwargs: Any) -> str:
        """Build user prompt with persona, campaign context, and asset-specific instructions.

        User prompt structure:
        - Target audience (persona block)
        - Campaign context (angle, objective, platforms)
        - Asset-specific instructions (from subclass)
        - Social proof (when asset type needs it)
        """
        parts: list[str] = []

        # Persona
        persona_block = format_persona_block(ctx)
        if persona_block:
            parts.append(f"TARGET AUDIENCE:\n{persona_block}")

        # Campaign context
        campaign_lines = []
        if ctx.angle:
            campaign_lines.append(f"- Angle: {ctx.angle}")
        if ctx.objective:
            campaign_lines.append(f"- Objective: {ctx.objective}")
        if ctx.platforms:
            campaign_lines.append(f"- Platform(s): {', '.join(ctx.platforms)}")
        if ctx.industry:
            campaign_lines.append(f"- Industry: {ctx.industry}")
        if campaign_lines:
            parts.append("CAMPAIGN CONTEXT:\n" + "\n".join(campaign_lines))

        # Asset-specific instructions from subclass
        specific = self.build_asset_specific_instructions(ctx, **kwargs)
        if specific:
            parts.append(specific)

        # Social proof for asset types that need it
        if self._needs_social_proof():
            proof_block = format_social_proof_block(ctx)
            if proof_block:
                parts.append(f"AVAILABLE SOCIAL PROOF:\n{proof_block}")

        return "\n\n".join(parts)

    @abstractmethod
    def build_asset_specific_instructions(self, ctx: AssetContext, **kwargs: Any) -> str:
        """Return asset-specific instructions for the user prompt.

        Subclasses must implement this with their format-specific prompts.
        """
        ...

    def _needs_social_proof(self) -> bool:
        """Override in subclasses that need social proof in prompts.

        Default: True for landing pages, case studies, carousels.
        """
        return self.asset_type in {
            "landing_page",
            "case_study_page",
            "document_ad",
            "lead_magnet",
        }

    async def generate(
        self,
        claude: ClaudeClient,
        ctx: AssetContext,
        **kwargs: Any,
    ) -> BaseModel:
        """Full generation pipeline: build prompts → call Claude → validate output."""
        system_prompt = self.build_system_prompt(ctx)
        user_prompt = self.build_user_prompt(ctx, **kwargs)

        result = await claude.generate_structured(
            model=self.model,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            output_schema=self.output_schema,
            temperature=self.temperature,
            asset_type=self.asset_type,
        )
        return result


# ---------------------------------------------------------------------------
# Generator registry
# ---------------------------------------------------------------------------

GENERATOR_REGISTRY: dict[str, PromptTemplate] = {}


def register_generator(template: PromptTemplate) -> PromptTemplate:
    """Register a generator instance in the global registry."""
    GENERATOR_REGISTRY[template.asset_type] = template
    logger.info("Registered generator: %s", template.asset_type)
    return template


def get_generator(asset_type: str) -> PromptTemplate:
    """Look up a registered generator by asset type."""
    if asset_type not in GENERATOR_REGISTRY:
        raise ValueError(
            f"No generator registered for asset type '{asset_type}'. "
            f"Available: {list(GENERATOR_REGISTRY.keys())}"
        )
    return GENERATOR_REGISTRY[asset_type]
