"""Tests for BJC-166: Claude API client setup."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel

from app.integrations.claude_ai import (
    MODEL_FAST,
    MODEL_QUALITY,
    ClaudeClient,
    get_model_for_asset_type,
    parse_json_from_response,
    validate_against_schema,
)


# ---------------------------------------------------------------------------
# Model selection tests
# ---------------------------------------------------------------------------


class TestModelSelection:
    def test_quality_types_return_opus(self):
        for t in ("lead_magnet", "landing_page", "case_study_page", "document_ad"):
            assert get_model_for_asset_type(t) == MODEL_QUALITY

    def test_fast_types_return_sonnet(self):
        for t in ("ad_copy", "email_copy", "video_script", "image_brief"):
            assert get_model_for_asset_type(t) == MODEL_FAST

    def test_unknown_type_defaults_to_fast(self):
        assert get_model_for_asset_type("unknown_thing") == MODEL_FAST


# ---------------------------------------------------------------------------
# JSON parsing tests
# ---------------------------------------------------------------------------


class TestParseJsonFromResponse:
    def test_xml_tags(self):
        text = '<output>{"title": "hello", "count": 5}</output>'
        result = parse_json_from_response(text)
        assert result == {"title": "hello", "count": 5}

    def test_xml_tags_with_surrounding_text(self):
        text = 'Here is the output:\n<output>\n{"title": "hello"}\n</output>\nDone.'
        result = parse_json_from_response(text)
        assert result == {"title": "hello"}

    def test_markdown_code_fence(self):
        text = '```json\n{"title": "hello"}\n```'
        result = parse_json_from_response(text)
        assert result == {"title": "hello"}

    def test_markdown_fence_no_lang(self):
        text = '```\n{"title": "hello"}\n```'
        result = parse_json_from_response(text)
        assert result == {"title": "hello"}

    def test_raw_json(self):
        text = '{"title": "hello", "items": [1, 2, 3]}'
        result = parse_json_from_response(text)
        assert result == {"title": "hello", "items": [1, 2, 3]}

    def test_json_with_leading_text(self):
        text = 'Sure, here is the result:\n\n{"title": "hello"}'
        result = parse_json_from_response(text)
        assert result == {"title": "hello"}

    def test_nested_json(self):
        data = {"sections": [{"heading": "A", "body": "B"}], "title": "Test"}
        text = f"<output>{json.dumps(data)}</output>"
        result = parse_json_from_response(text)
        assert result == data

    def test_no_json_raises(self):
        with pytest.raises((ValueError, json.JSONDecodeError)):
            parse_json_from_response("This has no JSON at all.")


# ---------------------------------------------------------------------------
# Schema validation tests
# ---------------------------------------------------------------------------


class SampleSchema(BaseModel):
    title: str
    count: int


class TestValidateAgainstSchema:
    def test_valid_data(self):
        data = {"title": "Test", "count": 42}
        result = validate_against_schema(data, SampleSchema)
        assert isinstance(result, SampleSchema)
        assert result.title == "Test"
        assert result.count == 42

    def test_invalid_data_raises(self):
        data = {"title": "Test"}  # missing count
        with pytest.raises(Exception):
            validate_against_schema(data, SampleSchema)

    def test_extra_fields_ignored(self):
        data = {"title": "Test", "count": 1, "extra": "stuff"}
        result = validate_against_schema(data, SampleSchema)
        assert result.title == "Test"


# ---------------------------------------------------------------------------
# ClaudeClient tests (mocked API)
# ---------------------------------------------------------------------------


def _make_mock_response(text: str, input_tokens: int = 100, output_tokens: int = 200):
    """Create a mock Anthropic API response."""
    mock = MagicMock()
    mock.content = [MagicMock(type="text", text=text)]
    mock.usage = MagicMock(input_tokens=input_tokens, output_tokens=output_tokens)
    return mock


class TestClaudeClient:
    @pytest.mark.asyncio
    async def test_generate_structured_success(self):
        client = ClaudeClient(api_key="test-key")
        mock_resp = _make_mock_response(
            '<output>{"title": "Hello", "count": 5}</output>'
        )
        with patch.object(client._client.messages, "create", return_value=mock_resp):
            result = await client.generate_structured(
                model=MODEL_FAST,
                system_prompt="You are a test.",
                user_prompt="Generate.",
                output_schema=SampleSchema,
                asset_type="test",
            )
        assert isinstance(result, SampleSchema)
        assert result.title == "Hello"
        assert result.count == 5

    @pytest.mark.asyncio
    async def test_generate_text_success(self):
        client = ClaudeClient(api_key="test-key")
        mock_resp = _make_mock_response("Here is some plain text output.")
        with patch.object(client._client.messages, "create", return_value=mock_resp):
            result = await client.generate_text(
                model=MODEL_FAST,
                system_prompt="You are a test.",
                user_prompt="Generate.",
                asset_type="test",
            )
        assert result == "Here is some plain text output."

    @pytest.mark.asyncio
    async def test_generate_structured_retries_on_bad_json(self):
        client = ClaudeClient(api_key="test-key")
        bad_resp = _make_mock_response("Not valid json at all")
        good_resp = _make_mock_response('<output>{"title": "Fixed", "count": 1}</output>')
        with patch.object(
            client._client.messages,
            "create",
            side_effect=[bad_resp, good_resp],
        ):
            result = await client.generate_structured(
                model=MODEL_FAST,
                system_prompt="You are a test.",
                user_prompt="Generate.",
                output_schema=SampleSchema,
                asset_type="test",
            )
        assert result.title == "Fixed"

    @pytest.mark.asyncio
    async def test_timeout_configured_per_model(self):
        client = ClaudeClient(api_key="test-key")
        mock_resp = _make_mock_response('<output>{"title":"T","count":1}</output>')
        with patch.object(
            client._client.messages, "create", return_value=mock_resp
        ) as mock_create:
            await client.generate_structured(
                model=MODEL_QUALITY,
                system_prompt="sys",
                user_prompt="usr",
                output_schema=SampleSchema,
                asset_type="lead_magnet",
            )
            call_kwargs = mock_create.call_args
            assert call_kwargs.kwargs["timeout"] == 120.0

            await client.generate_structured(
                model=MODEL_FAST,
                system_prompt="sys",
                user_prompt="usr",
                output_schema=SampleSchema,
                asset_type="ad_copy",
            )
            call_kwargs = mock_create.call_args
            assert call_kwargs.kwargs["timeout"] == 30.0
