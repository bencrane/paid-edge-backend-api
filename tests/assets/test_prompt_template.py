"""Tests for BJC-168: Prompt template system."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import BaseModel

from app.assets.context import AssetContext
from app.assets.prompts.base import (
    GENERATOR_REGISTRY,
    PromptTemplate,
    get_generator,
    register_generator,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


class SampleOutput(BaseModel):
    title: str
    body: str


class SampleGenerator(PromptTemplate):
    asset_type = "sample"
    model = "claude-sonnet-4-20250514"
    output_schema = SampleOutput
    temperature = 0.5

    def build_asset_specific_instructions(self, ctx: AssetContext, **kwargs: Any) -> str:
        topic = kwargs.get("topic", "general")
        return f"Generate a sample asset about {topic}."


def _ctx(**overrides: Any) -> AssetContext:
    defaults = dict(
        organization_id="org-1",
        company_name="TestCo",
        brand_voice="Professional and clear",
        value_proposition="We make testing easy",
        target_persona="Job titles: QA Lead\nIndustry: SaaS",
        angle="Automated testing",
        objective="lead_generation",
        platforms=["linkedin"],
        industry="SaaS",
    )
    defaults.update(overrides)
    return AssetContext(**defaults)


# ---------------------------------------------------------------------------
# System prompt tests
# ---------------------------------------------------------------------------


class TestSystemPrompt:
    def test_contains_role_and_company(self):
        gen = SampleGenerator()
        ctx = _ctx()
        prompt = gen.build_system_prompt(ctx)
        assert "TestCo" in prompt
        assert "sample" in prompt
        assert "B2B content strategist" in prompt

    def test_contains_brand_context(self):
        gen = SampleGenerator()
        ctx = _ctx(value_proposition="We simplify compliance")
        prompt = gen.build_system_prompt(ctx)
        assert "We simplify compliance" in prompt

    def test_contains_brand_voice(self):
        gen = SampleGenerator()
        ctx = _ctx(brand_voice="Bold and direct")
        prompt = gen.build_system_prompt(ctx)
        assert "Bold and direct" in prompt


# ---------------------------------------------------------------------------
# User prompt tests
# ---------------------------------------------------------------------------


class TestUserPrompt:
    def test_contains_persona(self):
        gen = SampleGenerator()
        ctx = _ctx()
        prompt = gen.build_user_prompt(ctx, topic="testing")
        assert "QA Lead" in prompt

    def test_contains_campaign_context(self):
        gen = SampleGenerator()
        ctx = _ctx()
        prompt = gen.build_user_prompt(ctx)
        assert "Automated testing" in prompt
        assert "lead_generation" in prompt
        assert "linkedin" in prompt

    def test_contains_asset_specific_instructions(self):
        gen = SampleGenerator()
        ctx = _ctx()
        prompt = gen.build_user_prompt(ctx, topic="CI/CD pipelines")
        assert "CI/CD pipelines" in prompt

    def test_no_persona_when_empty(self):
        gen = SampleGenerator()
        ctx = _ctx(target_persona="")
        prompt = gen.build_user_prompt(ctx)
        assert "TARGET AUDIENCE" not in prompt


# ---------------------------------------------------------------------------
# Registry tests
# ---------------------------------------------------------------------------


class TestRegistry:
    def setup_method(self):
        # Clean registry before each test
        GENERATOR_REGISTRY.clear()

    def test_register_and_get(self):
        gen = SampleGenerator()
        register_generator(gen)
        retrieved = get_generator("sample")
        assert retrieved is gen

    def test_get_unknown_raises(self):
        with pytest.raises(ValueError, match="No generator registered"):
            get_generator("nonexistent")

    def test_register_overwrites(self):
        gen1 = SampleGenerator()
        gen2 = SampleGenerator()
        register_generator(gen1)
        register_generator(gen2)
        assert get_generator("sample") is gen2


# ---------------------------------------------------------------------------
# Generate pipeline test
# ---------------------------------------------------------------------------


class TestGeneratePipeline:
    @pytest.mark.asyncio
    async def test_calls_claude_with_correct_params(self):
        gen = SampleGenerator()
        ctx = _ctx()

        mock_claude = MagicMock()
        mock_claude.generate_structured = AsyncMock(
            return_value=SampleOutput(title="Test", body="Body")
        )

        result = await gen.generate(mock_claude, ctx, topic="testing")
        assert isinstance(result, SampleOutput)
        assert result.title == "Test"

        call_kwargs = mock_claude.generate_structured.call_args.kwargs
        assert call_kwargs["model"] == "claude-sonnet-4-20250514"
        assert call_kwargs["temperature"] == 0.5
        assert call_kwargs["asset_type"] == "sample"
        assert call_kwargs["output_schema"] is SampleOutput
