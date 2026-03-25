"""Tests for post-generation output validators."""

from __future__ import annotations

import pytest

from app.assets.validators import (
    KNOWN_DIMENSIONS,
    validate_asset_output,
    validate_document_ad,
    validate_email_sequence,
    validate_google_rsa_copy,
    validate_image_brief,
    validate_landing_page,
    validate_lead_magnet,
    validate_linkedin_ad_copy,
    validate_meta_ad_copy,
    validate_video_script,
)
from app.assets.prompts.schemas import (
    DocumentAdOutput,
    EmailSequenceOutput,
    GoogleRSACopyOutput,
    ImageBriefOutput,
    ImageBriefSetOutput,
    LeadMagnetOutput,
    LeadMagnetSectionOutput,
    LeadMagnetPageOutput,
    LinkedInAdCopyOutput,
    LinkedInAdCopyVariant,
    MetaAdCopyOutput,
    MetaAdCopyVariant,
    NurtureEmail,
    ScriptSegment,
    SlideOutput,
    VideoScriptOutput,
)


# ===================================================================
# Helpers
# ===================================================================


def _linkedin_variant(
    intro: str = "Short intro",
    headline: str = "Headline",
    desc: str = "Description text",
    cta: str = "Learn More",
) -> LinkedInAdCopyVariant:
    return LinkedInAdCopyVariant(
        introductory_text=intro,
        headline=headline,
        description=desc,
        cta=cta,
    )


def _meta_variant(
    primary: str = "Short text",
    headline: str = "Head",
    desc: str = "Desc",
    cta: str = "LEARN_MORE",
) -> MetaAdCopyVariant:
    return MetaAdCopyVariant(
        primary_text=primary,
        headline=headline,
        description=desc,
        cta=cta,
    )


def _slide(
    headline: str = "Slide headline",
    body: str | None = "Slide body",
    stat_callout: str | None = None,
    stat_label: str | None = None,
    is_cta: bool = False,
    cta_text: str | None = None,
) -> SlideOutput:
    return SlideOutput(
        headline=headline,
        body=body,
        stat_callout=stat_callout,
        stat_label=stat_label,
        is_cta_slide=is_cta,
        cta_text=cta_text,
    )


def _email(
    subject: str = "Subject line",
    preview: str = "Preview text",
    body: str = "<p>Body</p>",
    delay: int = 0,
    purpose: str = "value_delivery",
) -> NurtureEmail:
    return NurtureEmail(
        subject_line=subject,
        preview_text=preview,
        body_html=body,
        send_delay_days=delay,
        purpose=purpose,
    )


def _segment(
    start: str = "0:00",
    end: str = "0:03",
    spoken: str = "Some spoken text here",
    visual: str = "Close-up shot",
    caption: str = "Caption text",
) -> ScriptSegment:
    return ScriptSegment(
        timestamp_start=start,
        timestamp_end=end,
        spoken_text=spoken,
        visual_direction=visual,
        caption_text=caption,
    )


def _lead_magnet_section(
    heading: str = "Section Heading",
    body: str = "word " * 400,
    bullets: list[str] | None = None,
) -> LeadMagnetSectionOutput:
    return LeadMagnetSectionOutput(
        heading=heading,
        body=body.strip(),
        bullets=bullets or [],
    )


def _image_brief(
    concept_name: str = "Concept A",
    dimensions: str = "1200x628",
    visual_description: str = "Overhead shot of a whiteboard",
    mood: str = "confident",
    style_reference: str = "Apple product photography",
    color_palette: list[str] | None = None,
) -> ImageBriefOutput:
    return ImageBriefOutput(
        concept_name=concept_name,
        intended_use="linkedin_sponsored",
        dimensions=dimensions,
        visual_description=visual_description,
        mood=mood,
        style_reference=style_reference,
        color_palette=color_palette or ["#1A73E8", "#34A853", "#FFFFFF"],
    )


# ===================================================================
# LinkedIn Ad Copy
# ===================================================================


class TestLinkedInAdCopy:
    def test_valid_passes(self):
        output = LinkedInAdCopyOutput(
            variants=[_linkedin_variant() for _ in range(3)]
        )
        valid, warnings, errors = validate_linkedin_ad_copy(output)
        assert valid is True
        assert errors == []

    def test_intro_over_600_errors(self):
        output = LinkedInAdCopyOutput(
            variants=[
                _linkedin_variant(intro="x" * 601),
                _linkedin_variant(),
                _linkedin_variant(),
            ]
        )
        valid, warnings, errors = validate_linkedin_ad_copy(output)
        assert valid is False
        assert any("600" in e for e in errors)

    def test_intro_over_150_warns(self):
        output = LinkedInAdCopyOutput(
            variants=[
                _linkedin_variant(intro="x" * 200),
                _linkedin_variant(),
                _linkedin_variant(),
            ]
        )
        valid, warnings, errors = validate_linkedin_ad_copy(output)
        assert valid is True
        assert any("150" in w for w in warnings)

    def test_headline_over_200_errors(self):
        output = LinkedInAdCopyOutput(
            variants=[
                _linkedin_variant(headline="x" * 201),
                _linkedin_variant(),
                _linkedin_variant(),
            ]
        )
        valid, warnings, errors = validate_linkedin_ad_copy(output)
        assert valid is False
        assert any("200" in e for e in errors)

    def test_headline_over_70_warns(self):
        output = LinkedInAdCopyOutput(
            variants=[
                _linkedin_variant(headline="x" * 80),
                _linkedin_variant(),
                _linkedin_variant(),
            ]
        )
        valid, warnings, errors = validate_linkedin_ad_copy(output)
        assert valid is True
        assert any("70" in w for w in warnings)

    def test_description_over_100_errors(self):
        output = LinkedInAdCopyOutput(
            variants=[
                _linkedin_variant(desc="x" * 101),
                _linkedin_variant(),
                _linkedin_variant(),
            ]
        )
        valid, warnings, errors = validate_linkedin_ad_copy(output)
        assert valid is False
        assert any("100" in e for e in errors)


# ===================================================================
# Meta Ad Copy
# ===================================================================


class TestMetaAdCopy:
    def test_valid_passes(self):
        output = MetaAdCopyOutput(
            variants=[_meta_variant() for _ in range(3)]
        )
        valid, warnings, errors = validate_meta_ad_copy(output)
        assert valid is True
        assert warnings == []

    def test_headline_over_40_warns(self):
        output = MetaAdCopyOutput(
            variants=[
                _meta_variant(headline="x" * 45),
                _meta_variant(),
                _meta_variant(),
            ]
        )
        valid, warnings, errors = validate_meta_ad_copy(output)
        assert valid is True
        assert any("40" in w for w in warnings)

    def test_primary_text_over_125_warns(self):
        output = MetaAdCopyOutput(
            variants=[
                _meta_variant(primary="x" * 130),
                _meta_variant(),
                _meta_variant(),
            ]
        )
        valid, warnings, errors = validate_meta_ad_copy(output)
        assert any("125" in w for w in warnings)

    def test_description_over_30_warns(self):
        output = MetaAdCopyOutput(
            variants=[
                _meta_variant(desc="x" * 35),
                _meta_variant(),
                _meta_variant(),
            ]
        )
        valid, warnings, errors = validate_meta_ad_copy(output)
        assert any("30" in w for w in warnings)


# ===================================================================
# Google RSA Copy
# ===================================================================


class TestGoogleRSACopy:
    def test_valid_passes(self):
        output = GoogleRSACopyOutput(
            headlines=["Headline One", "Headline Two", "Headline Three"],
            descriptions=["A short description.", "Another description."],
            path1="products",
            path2="demo",
        )
        valid, warnings, errors = validate_google_rsa_copy(output)
        assert valid is True
        assert errors == []

    def test_headline_over_30_errors(self):
        output = GoogleRSACopyOutput(
            headlines=["x" * 31, "Short", "OK"],
            descriptions=["Desc one.", "Desc two."],
            path1="p1",
            path2="p2",
        )
        valid, warnings, errors = validate_google_rsa_copy(output)
        assert valid is False
        assert any("30" in e for e in errors)

    def test_too_few_headlines_errors(self):
        """Pydantic enforces min_length=3, so we test the validator with a manually built object."""
        output = GoogleRSACopyOutput.model_construct(
            headlines=["One", "Two"],
            descriptions=["Desc.", "Desc2."],
            path1="p1",
            path2="p2",
        )
        valid, warnings, errors = validate_google_rsa_copy(output)
        assert valid is False
        assert any("few headlines" in e.lower() for e in errors)

    def test_path_over_15_errors(self):
        output = GoogleRSACopyOutput(
            headlines=["A", "B", "C"],
            descriptions=["D.", "E."],
            path1="x" * 16,
            path2="ok",
        )
        valid, warnings, errors = validate_google_rsa_copy(output)
        assert valid is False
        assert any("path1" in e for e in errors)

    def test_description_over_90_errors(self):
        output = GoogleRSACopyOutput(
            headlines=["A", "B", "C"],
            descriptions=["x" * 91, "Short."],
            path1="p1",
            path2="p2",
        )
        valid, warnings, errors = validate_google_rsa_copy(output)
        assert valid is False
        assert any("90" in e for e in errors)


# ===================================================================
# Lead Magnet
# ===================================================================


class TestLeadMagnet:
    def test_valid_checklist_passes(self):
        sections = [_lead_magnet_section(body="word " * 500) for _ in range(5)]
        output = LeadMagnetOutput(title="T", subtitle="S", sections=sections)
        valid, warnings, errors = validate_lead_magnet(output, format="checklist")
        assert valid is True
        assert errors == []

    def test_word_count_below_range_warns(self):
        sections = [_lead_magnet_section(body="word " * 50) for _ in range(3)]
        output = LeadMagnetOutput(title="T", subtitle="S", sections=sections)
        valid, warnings, errors = validate_lead_magnet(output, format="checklist")
        assert any("below target" in w.lower() for w in warnings)

    def test_word_count_above_range_warns(self):
        sections = [_lead_magnet_section(body="word " * 2000) for _ in range(5)]
        output = LeadMagnetOutput(title="T", subtitle="S", sections=sections)
        valid, warnings, errors = validate_lead_magnet(output, format="checklist")
        assert any("above target" in w.lower() for w in warnings)

    def test_too_few_sections_errors(self):
        sections = [_lead_magnet_section(body="word " * 500)]
        output = LeadMagnetOutput(title="T", subtitle="S", sections=sections)
        valid, warnings, errors = validate_lead_magnet(output, format="ultimate_guide")
        assert valid is False
        assert any("few sections" in e.lower() for e in errors)

    def test_empty_heading_errors(self):
        sections = [_lead_magnet_section(heading="", body="word " * 400) for _ in range(5)]
        output = LeadMagnetOutput(title="T", subtitle="S", sections=sections)
        valid, warnings, errors = validate_lead_magnet(output, format="checklist")
        assert valid is False
        assert any("empty heading" in e.lower() for e in errors)

    def test_empty_body_errors(self):
        sections = [_lead_magnet_section(heading="H", body="") for _ in range(5)]
        output = LeadMagnetOutput(title="T", subtitle="S", sections=sections)
        valid, warnings, errors = validate_lead_magnet(output, format="checklist")
        assert valid is False
        assert any("empty body" in e.lower() for e in errors)

    def test_checklist_imperative_verb_warning(self):
        section = _lead_magnet_section(bullets=["review your process", "123 do this"])
        output = LeadMagnetOutput(title="T", subtitle="S", sections=[section] * 4)
        valid, warnings, errors = validate_lead_magnet(output, format="checklist")
        assert any("imperative" in w.lower() for w in warnings)


# ===================================================================
# Document Ad
# ===================================================================


class TestDocumentAd:
    def test_valid_passes(self):
        slides = [_slide(stat_callout="3x", stat_label="ROI") for _ in range(4)]
        slides.append(_slide(is_cta=True, cta_text="Download"))
        output = DocumentAdOutput(slides=slides)
        valid, warnings, errors = validate_document_ad(output)
        assert valid is True
        assert errors == []

    def test_too_few_slides_errors(self):
        slides = [_slide(), _slide(is_cta=True, cta_text="Go")]
        output = DocumentAdOutput.model_construct(slides=slides, aspect_ratio="1:1")
        valid, warnings, errors = validate_document_ad(output)
        assert valid is False
        assert any("few slides" in e.lower() for e in errors)

    def test_last_slide_not_cta_errors(self):
        slides = [_slide() for _ in range(5)]
        output = DocumentAdOutput(slides=slides)
        valid, warnings, errors = validate_document_ad(output)
        assert valid is False
        assert any("is_cta_slide" in e for e in errors)

    def test_headline_over_50_warns(self):
        slides = [
            _slide(headline="x" * 55),
            _slide(),
            _slide(),
            _slide(stat_callout="5x", stat_label="Growth"),
            _slide(is_cta=True, cta_text="Go"),
        ]
        output = DocumentAdOutput(slides=slides)
        valid, warnings, errors = validate_document_ad(output)
        assert any("50" in w for w in warnings)

    def test_body_over_120_warns(self):
        slides = [
            _slide(body="x" * 125),
            _slide(),
            _slide(),
            _slide(stat_callout="3x", stat_label="ROI"),
            _slide(is_cta=True, cta_text="Go"),
        ]
        output = DocumentAdOutput(slides=slides)
        valid, warnings, errors = validate_document_ad(output)
        assert any("120" in w for w in warnings)

    def test_no_stat_callout_warns(self):
        slides = [_slide() for _ in range(4)]
        slides.append(_slide(is_cta=True, cta_text="Go"))
        output = DocumentAdOutput(slides=slides)
        valid, warnings, errors = validate_document_ad(output)
        assert any("stat callout" in w.lower() for w in warnings)


# ===================================================================
# Email Sequence
# ===================================================================


class TestEmailSequence:
    def test_valid_passes(self):
        output = EmailSequenceOutput(
            sequence_name="Seq",
            trigger="lead_magnet_download",
            emails=[
                _email(delay=0, purpose="value_delivery"),
                _email(delay=2, purpose="education"),
                _email(delay=5, purpose="social_proof"),
            ],
        )
        valid, warnings, errors = validate_email_sequence(output)
        assert valid is True
        assert errors == []

    def test_subject_over_60_warns(self):
        output = EmailSequenceOutput(
            sequence_name="S",
            trigger="t",
            emails=[
                _email(subject="x" * 65, delay=0),
                _email(delay=2, purpose="education"),
                _email(delay=5, purpose="social_proof"),
            ],
        )
        valid, warnings, errors = validate_email_sequence(output)
        assert any("60" in w for w in warnings)

    def test_preview_over_90_warns(self):
        output = EmailSequenceOutput(
            sequence_name="S",
            trigger="t",
            emails=[
                _email(preview="x" * 95, delay=0),
                _email(delay=2, purpose="education"),
                _email(delay=5, purpose="social_proof"),
            ],
        )
        valid, warnings, errors = validate_email_sequence(output)
        assert any("90" in w for w in warnings)

    def test_too_few_emails_errors(self):
        output = EmailSequenceOutput.model_construct(
            sequence_name="S",
            trigger="t",
            emails=[_email()],
        )
        valid, warnings, errors = validate_email_sequence(output)
        assert valid is False
        assert any("few emails" in e.lower() for e in errors)

    def test_non_sequential_delay_errors(self):
        output = EmailSequenceOutput(
            sequence_name="S",
            trigger="t",
            emails=[
                _email(delay=5, purpose="value_delivery"),
                _email(delay=2, purpose="education"),
                _email(delay=10, purpose="social_proof"),
            ],
        )
        valid, warnings, errors = validate_email_sequence(output)
        assert valid is False
        assert any("sequential" in e.lower() for e in errors)


# ===================================================================
# Video Script
# ===================================================================


class TestVideoScript:
    def _build_30s(self, word_count: int = 75, hook_end: str = "0:03") -> VideoScriptOutput:
        return VideoScriptOutput(
            title="Test",
            duration="30s",
            aspect_ratio="4:5",
            hook=_segment("0:00", hook_end, "Hook words"),
            body=[
                _segment("0:03", "0:10", "Problem text goes here now"),
                _segment("0:10", "0:20", "Solution text goes here now"),
            ],
            cta=_segment("0:20", "0:30", "Call to action"),
            total_word_count=word_count,
            music_direction="Upbeat",
            target_platform="linkedin",
        )

    def test_valid_30s_passes(self):
        output = self._build_30s(word_count=75)
        valid, warnings, errors = validate_video_script(output)
        assert valid is True
        assert errors == []

    def test_word_count_too_low_warns(self):
        output = self._build_30s(word_count=40)
        valid, warnings, errors = validate_video_script(output)
        assert any("below target" in w.lower() for w in warnings)

    def test_word_count_too_high_warns(self):
        output = self._build_30s(word_count=120)
        valid, warnings, errors = validate_video_script(output)
        assert any("above target" in w.lower() for w in warnings)

    def test_hook_over_3s_warns(self):
        output = self._build_30s(hook_end="0:05")
        # Fix body to start after hook
        output.body[0].timestamp_start = "0:05"
        valid, warnings, errors = validate_video_script(output)
        assert any("hook" in w.lower() and "3s" in w for w in warnings)

    def test_60s_valid(self):
        output = VideoScriptOutput(
            title="Test 60",
            duration="60s",
            aspect_ratio="16:9",
            hook=_segment("0:00", "0:03"),
            body=[
                _segment("0:03", "0:15"),
                _segment("0:15", "0:35"),
                _segment("0:35", "0:50"),
            ],
            cta=_segment("0:50", "1:00"),
            total_word_count=150,
            music_direction="Ambient",
            target_platform="youtube",
        )
        valid, warnings, errors = validate_video_script(output)
        assert valid is True

    def test_script_exceeds_duration_errors(self):
        output = VideoScriptOutput(
            title="Test",
            duration="30s",
            aspect_ratio="4:5",
            hook=_segment("0:00", "0:03"),
            body=[_segment("0:03", "0:20")],
            cta=_segment("0:20", "0:40"),
            total_word_count=75,
            music_direction="Up",
            target_platform="linkedin",
        )
        valid, warnings, errors = validate_video_script(output)
        assert valid is False
        assert any("extends beyond" in e.lower() for e in errors)


# ===================================================================
# Landing Page
# ===================================================================


class TestLandingPage:
    def test_valid_passes(self):
        output = LeadMagnetPageOutput(
            headline="Download Our Free Guide",
            subhead="Get started today",
            value_props=["Fast setup", "Easy to use"],
        )
        valid, warnings, errors = validate_landing_page(output)
        assert valid is True

    def test_empty_headline_errors(self):
        output = LeadMagnetPageOutput(
            headline="",
            subhead="Sub",
            value_props=["A"],
        )
        valid, warnings, errors = validate_landing_page(output)
        assert valid is False
        assert any("empty" in e.lower() for e in errors)

    def test_headline_over_80_warns(self):
        output = LeadMagnetPageOutput(
            headline="x" * 85,
            subhead="Sub",
            value_props=["A"],
        )
        valid, warnings, errors = validate_landing_page(output)
        assert any("80" in w for w in warnings)

    def test_headline_too_short_warns(self):
        output = LeadMagnetPageOutput(
            headline="Hi",
            subhead="Sub",
            value_props=["A"],
        )
        valid, warnings, errors = validate_landing_page(output)
        assert any("short" in w.lower() for w in warnings)


# ===================================================================
# Image Brief
# ===================================================================


class TestImageBrief:
    def test_valid_passes(self):
        output = ImageBriefSetOutput(briefs=[_image_brief()])
        valid, warnings, errors = validate_image_brief(output)
        assert valid is True
        assert errors == []

    def test_no_briefs_errors(self):
        output = ImageBriefSetOutput(briefs=[])
        valid, warnings, errors = validate_image_brief(output)
        assert valid is False
        assert any("no briefs" in e.lower() for e in errors)

    def test_empty_concept_name_errors(self):
        output = ImageBriefSetOutput(briefs=[_image_brief(concept_name="")])
        valid, warnings, errors = validate_image_brief(output)
        assert valid is False
        assert any("concept_name" in e for e in errors)

    def test_empty_visual_description_errors(self):
        output = ImageBriefSetOutput(briefs=[_image_brief(visual_description="")])
        valid, warnings, errors = validate_image_brief(output)
        assert valid is False
        assert any("visual_description" in e for e in errors)

    def test_unknown_dimensions_warns(self):
        output = ImageBriefSetOutput(briefs=[_image_brief(dimensions="800x600")])
        valid, warnings, errors = validate_image_brief(output)
        assert any("800x600" in w for w in warnings)

    def test_invalid_hex_color_warns(self):
        output = ImageBriefSetOutput(
            briefs=[_image_brief(color_palette=["#1A73E8", "not-hex", "rgb(0,0,0)"])]
        )
        valid, warnings, errors = validate_image_brief(output)
        assert valid is True
        assert any("not-hex" in w for w in warnings)
        assert any("rgb" in w for w in warnings)

    def test_valid_hex_colors_pass(self):
        output = ImageBriefSetOutput(
            briefs=[_image_brief(color_palette=["#1A73E8", "#FFF", "#000000"])]
        )
        valid, warnings, errors = validate_image_brief(output)
        assert valid is True
        assert not any("hex" in w.lower() for w in warnings)


# ===================================================================
# Master Dispatcher
# ===================================================================


class TestMasterDispatcher:
    def test_routes_linkedin(self):
        output = LinkedInAdCopyOutput(
            variants=[_linkedin_variant() for _ in range(3)]
        )
        valid, _, _ = validate_asset_output("linkedin_ad_copy", output)
        assert valid is True

    def test_routes_meta(self):
        output = MetaAdCopyOutput(
            variants=[_meta_variant() for _ in range(3)]
        )
        valid, _, _ = validate_asset_output("meta_ad_copy", output)
        assert valid is True

    def test_routes_google_rsa(self):
        output = GoogleRSACopyOutput(
            headlines=["A", "B", "C"],
            descriptions=["D.", "E."],
            path1="p1",
            path2="p2",
        )
        valid, _, _ = validate_asset_output("google_rsa_copy", output)
        assert valid is True

    def test_routes_lead_magnet(self):
        sections = [_lead_magnet_section(body="word " * 500) for _ in range(5)]
        output = LeadMagnetOutput(title="T", subtitle="S", sections=sections)
        valid, _, _ = validate_asset_output("lead_magnet", output, format="checklist")
        assert valid is True

    def test_routes_document_ad(self):
        slides = [_slide(stat_callout="3x", stat_label="ROI") for _ in range(4)]
        slides.append(_slide(is_cta=True, cta_text="Go"))
        output = DocumentAdOutput(slides=slides)
        valid, _, _ = validate_asset_output("document_ad", output)
        assert valid is True

    def test_routes_email_copy(self):
        output = EmailSequenceOutput(
            sequence_name="S",
            trigger="t",
            emails=[
                _email(delay=0, purpose="value_delivery"),
                _email(delay=2, purpose="education"),
                _email(delay=5, purpose="social_proof"),
            ],
        )
        valid, _, _ = validate_asset_output("email_copy", output)
        assert valid is True

    def test_routes_video_script(self):
        output = VideoScriptOutput(
            title="T",
            duration="30s",
            aspect_ratio="4:5",
            hook=_segment("0:00", "0:03"),
            body=[_segment("0:03", "0:20")],
            cta=_segment("0:20", "0:30"),
            total_word_count=75,
            music_direction="Up",
            target_platform="linkedin",
        )
        valid, _, _ = validate_asset_output("video_script", output)
        assert valid is True

    def test_routes_landing_page(self):
        output = LeadMagnetPageOutput(
            headline="Download Our Guide",
            subhead="Free",
            value_props=["A"],
        )
        valid, _, _ = validate_asset_output("landing_page", output)
        assert valid is True

    def test_routes_image_brief(self):
        output = ImageBriefSetOutput(briefs=[_image_brief()])
        valid, _, _ = validate_asset_output("image_brief", output)
        assert valid is True

    def test_unknown_type_passes(self):
        valid, warnings, errors = validate_asset_output("unknown_type", None)
        assert valid is True
        assert warnings == []
        assert errors == []


# ===================================================================
# Conftest fixture test
# ===================================================================


class TestRealisticContext:
    def test_fixture_loads(self, realistic_context):
        """Verify conftest fixture is usable."""
        assert realistic_context.company_name == "SecureStack"
        assert realistic_context.industry == "Healthcare SaaS"
        assert len(realistic_context.case_studies) == 1
        assert len(realistic_context.testimonials) == 2
        assert realistic_context.brand_guidelines is not None
