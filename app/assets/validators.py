"""Post-generation output validators.

Each validator takes a generated output and returns
(is_valid, warnings, errors) where:
- is_valid: True if no hard errors
- warnings: list of soft issues (limits exceeded but within tolerance)
- errors: list of hard failures (output is unusable)
"""

from __future__ import annotations

import re
from typing import Any

from app.assets.prompts.schemas import (
    DocumentAdOutput,
    EmailSequenceOutput,
    GoogleRSACopyOutput,
    ImageBriefSetOutput,
    LeadMagnetOutput,
    LinkedInAdCopyOutput,
    MetaAdCopyOutput,
    VideoScriptOutput,
)

ValidationResult = tuple[bool, list[str], list[str]]

# ---------------------------------------------------------------------------
# Known image brief dimensions
# ---------------------------------------------------------------------------

KNOWN_DIMENSIONS = {
    "1200x628", "1080x1080", "1080x1920", "1920x1080",
}

_HEX_RE = re.compile(r"^#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})$")

# ---------------------------------------------------------------------------
# Lead magnet word-count ranges by format
# ---------------------------------------------------------------------------

LEAD_MAGNET_WORD_RANGES: dict[str, tuple[int, int]] = {
    "checklist": (2000, 4000),
    "ultimate_guide": (5500, 8500),
    "benchmark_report": (4000, 8000),
    "template_toolkit": (3000, 6000),
    "state_of_industry": (6000, 10000),
}

LEAD_MAGNET_SECTION_RANGES: dict[str, tuple[int, int]] = {
    "checklist": (3, 10),
    "ultimate_guide": (5, 10),
    "benchmark_report": (4, 10),
    "template_toolkit": (3, 8),
    "state_of_industry": (5, 10),
}


# ---------------------------------------------------------------------------
# Ad copy validators
# ---------------------------------------------------------------------------


def validate_linkedin_ad_copy(output: LinkedInAdCopyOutput) -> ValidationResult:
    """Validate LinkedIn ad copy output."""
    warnings: list[str] = []
    errors: list[str] = []

    for i, v in enumerate(output.variants):
        prefix = f"Variant {i+1}"

        # introductory_text: hard limit 600, warn >150
        if len(v.introductory_text) > 600:
            errors.append(f"{prefix}: introductory_text exceeds 600 chars ({len(v.introductory_text)})")
        elif len(v.introductory_text) > 150:
            warnings.append(f"{prefix}: introductory_text exceeds 150-char fold cutoff ({len(v.introductory_text)})")

        # headline: hard limit 200, warn >70
        if len(v.headline) > 200:
            errors.append(f"{prefix}: headline exceeds 200 chars ({len(v.headline)})")
        elif len(v.headline) > 70:
            warnings.append(f"{prefix}: headline exceeds 70-char recommended limit ({len(v.headline)})")

        # description: hard limit 100
        if len(v.description) > 100:
            errors.append(f"{prefix}: description exceeds 100 chars ({len(v.description)})")

    return (len(errors) == 0, warnings, errors)


def validate_meta_ad_copy(output: MetaAdCopyOutput) -> ValidationResult:
    """Validate Meta ad copy output."""
    warnings: list[str] = []
    errors: list[str] = []

    for i, v in enumerate(output.variants):
        prefix = f"Variant {i+1}"

        if len(v.primary_text) > 125:
            warnings.append(f"{prefix}: primary_text exceeds 125-char recommended limit ({len(v.primary_text)})")

        if len(v.headline) > 40:
            warnings.append(f"{prefix}: headline exceeds 40-char recommended limit ({len(v.headline)})")

        if len(v.description) > 30:
            warnings.append(f"{prefix}: description exceeds 30-char recommended limit ({len(v.description)})")

    return (True, warnings, errors)


def validate_google_rsa_copy(output: GoogleRSACopyOutput) -> ValidationResult:
    """Validate Google RSA ad copy output."""
    warnings: list[str] = []
    errors: list[str] = []

    # Headline count: 3-15
    if len(output.headlines) < 3:
        errors.append(f"Too few headlines: {len(output.headlines)} (minimum 3)")
    if len(output.headlines) > 15:
        errors.append(f"Too many headlines: {len(output.headlines)} (maximum 15)")

    # Headline char limit: 30
    for i, h in enumerate(output.headlines):
        if len(h) > 30:
            errors.append(f"Headline {i+1} exceeds 30 chars ({len(h)}): '{h}'")

    # Description count: 2-4
    if len(output.descriptions) < 2:
        errors.append(f"Too few descriptions: {len(output.descriptions)} (minimum 2)")
    if len(output.descriptions) > 4:
        errors.append(f"Too many descriptions: {len(output.descriptions)} (maximum 4)")

    # Description char limit: 90
    for i, d in enumerate(output.descriptions):
        if len(d) > 90:
            errors.append(f"Description {i+1} exceeds 90 chars ({len(d)})")

    # Path limits: 15
    if len(output.path1) > 15:
        errors.append(f"path1 exceeds 15 chars ({len(output.path1)})")
    if len(output.path2) > 15:
        errors.append(f"path2 exceeds 15 chars ({len(output.path2)})")

    return (len(errors) == 0, warnings, errors)


# ---------------------------------------------------------------------------
# Lead magnet validator
# ---------------------------------------------------------------------------


def validate_lead_magnet(
    output: LeadMagnetOutput, format: str = "checklist"
) -> ValidationResult:
    """Validate lead magnet output against format-specific rules."""
    warnings: list[str] = []
    errors: list[str] = []

    # Word count
    total_words = sum(len(s.body.split()) for s in output.sections)
    word_range = LEAD_MAGNET_WORD_RANGES.get(format, (2000, 8000))
    if total_words < word_range[0]:
        warnings.append(
            f"Word count {total_words} below target {word_range[0]} for {format}"
        )
    if total_words > word_range[1]:
        warnings.append(
            f"Word count {total_words} above target {word_range[1]} for {format}"
        )

    # Section count
    section_range = LEAD_MAGNET_SECTION_RANGES.get(format, (3, 10))
    if len(output.sections) < section_range[0]:
        errors.append(
            f"Too few sections: {len(output.sections)} (minimum {section_range[0]} for {format})"
        )
    if len(output.sections) > section_range[1]:
        warnings.append(
            f"Too many sections: {len(output.sections)} (maximum {section_range[1]} for {format})"
        )

    # All sections must have heading and body
    for i, s in enumerate(output.sections):
        if not s.heading.strip():
            errors.append(f"Section {i+1}: empty heading")
        if not s.body.strip():
            errors.append(f"Section {i+1}: empty body")

    # Checklist items should start with imperative verbs
    if format == "checklist":
        imperative_re = re.compile(r"^[A-Z][a-z]+\b")
        for i, s in enumerate(output.sections):
            for j, bullet in enumerate(s.bullets):
                if not imperative_re.match(bullet):
                    warnings.append(
                        f"Section {i+1}, bullet {j+1}: may not start with imperative verb: '{bullet[:30]}'"
                    )

    return (len(errors) == 0, warnings, errors)


# ---------------------------------------------------------------------------
# Document ad validator
# ---------------------------------------------------------------------------


def validate_document_ad(output: DocumentAdOutput) -> ValidationResult:
    """Validate document ad carousel output."""
    warnings: list[str] = []
    errors: list[str] = []

    # Slide count: 5-8
    if len(output.slides) < 5:
        errors.append(f"Too few slides: {len(output.slides)} (minimum 5)")
    if len(output.slides) > 8:
        errors.append(f"Too many slides: {len(output.slides)} (maximum 8)")

    # Last slide must be CTA
    if output.slides and not output.slides[-1].is_cta_slide:
        errors.append("Last slide must have is_cta_slide=True")

    # Character limits
    for i, s in enumerate(output.slides):
        if len(s.headline) > 50:
            warnings.append(f"Slide {i+1}: headline exceeds 50 chars ({len(s.headline)})")
        if s.body and len(s.body) > 120:
            warnings.append(f"Slide {i+1}: body exceeds 120 chars ({len(s.body)})")

    # At least one stat callout in non-CTA slides
    non_cta = [s for s in output.slides if not s.is_cta_slide]
    has_stat = any(s.stat_callout for s in non_cta)
    if not has_stat:
        warnings.append("No stat callouts found in non-CTA slides")

    return (len(errors) == 0, warnings, errors)


# ---------------------------------------------------------------------------
# Email sequence validator
# ---------------------------------------------------------------------------


def validate_email_sequence(output: EmailSequenceOutput) -> ValidationResult:
    """Validate email nurture sequence output."""
    warnings: list[str] = []
    errors: list[str] = []

    # Email count: 3-5
    if len(output.emails) < 3:
        errors.append(f"Too few emails: {len(output.emails)} (minimum 3)")
    if len(output.emails) > 5:
        errors.append(f"Too many emails: {len(output.emails)} (maximum 5)")

    prev_delay = -1
    for i, email in enumerate(output.emails):
        prefix = f"Email {i+1}"

        # Subject line ≤60 chars
        if len(email.subject_line) > 60:
            warnings.append(f"{prefix}: subject line exceeds 60 chars ({len(email.subject_line)})")

        # Preview text ≤90 chars
        if len(email.preview_text) > 90:
            warnings.append(f"{prefix}: preview text exceeds 90 chars ({len(email.preview_text)})")

        # send_delay_days must be sequential
        if email.send_delay_days < prev_delay:
            errors.append(
                f"{prefix}: send_delay_days ({email.send_delay_days}) is not sequential "
                f"(previous: {prev_delay})"
            )
        prev_delay = email.send_delay_days

        # Reasonable delay: max 30 days
        if email.send_delay_days > 30:
            warnings.append(f"{prefix}: send_delay_days ({email.send_delay_days}) seems too long")

    return (len(errors) == 0, warnings, errors)


# ---------------------------------------------------------------------------
# Video script validator
# ---------------------------------------------------------------------------


def _parse_timestamp_seconds(ts: str) -> float:
    """Parse 'M:SS' timestamp to seconds."""
    parts = ts.split(":")
    if len(parts) == 2:
        return int(parts[0]) * 60 + int(parts[1])
    return 0.0


def validate_video_script(output: VideoScriptOutput) -> ValidationResult:
    """Validate video script output."""
    warnings: list[str] = []
    errors: list[str] = []

    # Word count vs duration (±20% tolerance)
    target = 75 if output.duration == "30s" else 150
    tolerance = target * 0.20
    if output.total_word_count < target - tolerance:
        warnings.append(
            f"Word count {output.total_word_count} below target "
            f"{target} for {output.duration} (min {int(target - tolerance)})"
        )
    if output.total_word_count > target + tolerance:
        warnings.append(
            f"Word count {output.total_word_count} above target "
            f"{target} for {output.duration} (max {int(target + tolerance)})"
        )

    # Hook segment ≤3 seconds
    hook_start = _parse_timestamp_seconds(output.hook.timestamp_start)
    hook_end = _parse_timestamp_seconds(output.hook.timestamp_end)
    hook_duration = hook_end - hook_start
    if hook_duration > 3:
        warnings.append(f"Hook segment is {hook_duration}s (should be ≤3s)")

    # Timestamps sequential across all segments
    all_segments = [output.hook] + output.body + [output.cta]
    for i in range(1, len(all_segments)):
        prev_end = _parse_timestamp_seconds(all_segments[i - 1].timestamp_end)
        curr_start = _parse_timestamp_seconds(all_segments[i].timestamp_start)
        if curr_start < prev_end:
            warnings.append(
                f"Segment {i+1} starts at {all_segments[i].timestamp_start} "
                f"before previous ends at {all_segments[i-1].timestamp_end}"
            )

    # Total duration within expected range
    duration_secs = 30 if output.duration == "30s" else 60
    last_end = _parse_timestamp_seconds(output.cta.timestamp_end)
    if last_end > duration_secs + 5:
        errors.append(
            f"Script extends beyond duration: ends at {output.cta.timestamp_end} "
            f"but duration is {output.duration}"
        )

    return (len(errors) == 0, warnings, errors)


# ---------------------------------------------------------------------------
# Landing page validator
# ---------------------------------------------------------------------------


def validate_landing_page(output: Any, template_type: str = "lead_magnet_download") -> ValidationResult:
    """Validate landing page output."""
    warnings: list[str] = []
    errors: list[str] = []

    headline = getattr(output, "headline", "")
    if not headline or not headline.strip():
        errors.append("Headline is empty")
    elif len(headline) < 5:
        warnings.append(f"Headline too short: {len(headline)} chars (minimum 5)")
    elif len(headline) > 80:
        warnings.append(f"Headline exceeds 80 chars ({len(headline)})")

    # Check sections if present
    sections = getattr(output, "sections", None) or getattr(output, "benefits", None)
    if sections is not None:
        if len(sections) == 0:
            errors.append("Sections list is empty")
        for i, s in enumerate(sections):
            heading = getattr(s, "heading", "")
            body = getattr(s, "body", "")
            if not heading.strip():
                errors.append(f"Section {i+1}: empty heading")
            if not body.strip():
                errors.append(f"Section {i+1}: empty body")

    return (len(errors) == 0, warnings, errors)


# ---------------------------------------------------------------------------
# Image brief validator
# ---------------------------------------------------------------------------


def validate_image_brief(output: ImageBriefSetOutput) -> ValidationResult:
    """Validate image brief set output."""
    warnings: list[str] = []
    errors: list[str] = []

    if not output.briefs:
        errors.append("No briefs generated")
        return (False, warnings, errors)

    for i, brief in enumerate(output.briefs):
        prefix = f"Brief {i+1}"

        # Required fields populated
        if not brief.concept_name.strip():
            errors.append(f"{prefix}: empty concept_name")
        if not brief.visual_description.strip():
            errors.append(f"{prefix}: empty visual_description")
        if not brief.mood.strip():
            errors.append(f"{prefix}: empty mood")
        if not brief.style_reference.strip():
            errors.append(f"{prefix}: empty style_reference")

        # Dimensions match known formats
        if brief.dimensions and brief.dimensions not in KNOWN_DIMENSIONS:
            warnings.append(
                f"{prefix}: dimensions '{brief.dimensions}' not in known formats"
            )

        # Color palette has valid hex values
        for j, color in enumerate(brief.color_palette):
            if not _HEX_RE.match(color):
                warnings.append(f"{prefix}: color {j+1} '{color}' is not valid hex")

    return (len(errors) == 0, warnings, errors)


# ---------------------------------------------------------------------------
# Master dispatcher
# ---------------------------------------------------------------------------


def validate_asset_output(asset_type: str, output: Any, **kwargs: Any) -> ValidationResult:
    """Dispatch to the correct validator based on asset_type."""
    if asset_type == "linkedin_ad_copy":
        return validate_linkedin_ad_copy(output)
    if asset_type == "meta_ad_copy":
        return validate_meta_ad_copy(output)
    if asset_type == "google_rsa_copy":
        return validate_google_rsa_copy(output)
    if asset_type == "lead_magnet":
        fmt = kwargs.get("format", "checklist")
        return validate_lead_magnet(output, format=fmt)
    if asset_type == "document_ad":
        return validate_document_ad(output)
    if asset_type == "email_copy":
        return validate_email_sequence(output)
    if asset_type == "video_script":
        return validate_video_script(output)
    if asset_type == "landing_page":
        template = kwargs.get("template_type", "lead_magnet_download")
        return validate_landing_page(output, template_type=template)
    if asset_type == "image_brief":
        return validate_image_brief(output)

    # Unknown type — pass through
    return (True, [], [])
