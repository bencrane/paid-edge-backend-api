"""Output schemas for Claude generation — distinct from rendering input models.

BJC-168: These are the generation output schemas that Claude produces.
They get mapped to the rendering input models in app/assets/models.py.

Each generator issue (BJC-169 through BJC-176) defines its specific schemas here.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# BJC-169: Lead Magnet PDF
# ---------------------------------------------------------------------------

class LeadMagnetSectionOutput(BaseModel):
    heading: str
    body: str
    bullets: list[str] = Field(default_factory=list)
    callout_box: str | None = None


class LeadMagnetOutput(BaseModel):
    title: str
    subtitle: str
    sections: list[LeadMagnetSectionOutput]


# ---------------------------------------------------------------------------
# BJC-170: Landing Page
# ---------------------------------------------------------------------------

class LandingPageSectionOutput(BaseModel):
    heading: str
    body: str
    bullets: list[str] | None = None
    callout: str | None = None


class LeadMagnetPageOutput(BaseModel):
    """LP-1: Lead Magnet Download Page"""
    headline: str
    subhead: str
    value_props: list[str]
    cta_text: str = "Download Now"


class CaseStudyPageOutput(BaseModel):
    """LP-2: Case Study Page"""
    customer_name: str
    headline: str
    sections: list[LandingPageSectionOutput]
    metrics: list[dict]  # [{value, label}]
    quote_text: str | None = None
    quote_author: str | None = None
    quote_title: str | None = None
    cta_text: str = "Get Similar Results"


class WebinarPageOutput(BaseModel):
    """LP-3: Webinar Registration"""
    event_name: str
    headline: str
    agenda: list[str]
    cta_text: str = "Register Now"


class DemoRequestPageOutput(BaseModel):
    """LP-4: Demo Request"""
    headline: str
    subhead: str
    benefits: list[LandingPageSectionOutput]
    cta_text: str = "Request Demo"


# ---------------------------------------------------------------------------
# BJC-171: Ad Copy
# ---------------------------------------------------------------------------

class LinkedInAdCopyVariant(BaseModel):
    introductory_text: str = Field(..., description="Max 600 chars (150 visible before fold)")
    headline: str = Field(..., description="Max 70 chars recommended")
    description: str = Field(..., description="Max 100 chars")
    cta: Literal[
        "Apply", "Download", "Get Quote", "Learn More", "Sign Up",
        "Subscribe", "Register", "Join", "Attend", "Request Demo",
    ]


class LinkedInAdCopyOutput(BaseModel):
    variants: list[LinkedInAdCopyVariant] = Field(..., min_length=3, max_length=3)


class MetaAdCopyVariant(BaseModel):
    primary_text: str = Field(..., description="Max 125 chars recommended")
    headline: str = Field(..., description="Max 40 chars")
    description: str = Field(..., description="Max 30 chars")
    cta: Literal[
        "LEARN_MORE", "SIGN_UP", "DOWNLOAD", "GET_QUOTE",
        "CONTACT_US", "APPLY_NOW", "SUBSCRIBE", "BOOK_NOW",
    ]


class MetaAdCopyOutput(BaseModel):
    variants: list[MetaAdCopyVariant] = Field(..., min_length=3, max_length=3)


class GoogleRSACopyOutput(BaseModel):
    headlines: list[str] = Field(
        ..., min_length=3, max_length=15,
        description="3-15 headlines, each max 30 chars",
    )
    descriptions: list[str] = Field(
        ..., min_length=2, max_length=4,
        description="2-4 descriptions, each max 90 chars",
    )
    path1: str = Field(..., description="Max 15 chars")
    path2: str = Field(..., description="Max 15 chars")


class AdCopyOutput(BaseModel):
    """Wrapper for multi-platform ad copy generation."""
    linkedin: LinkedInAdCopyOutput | None = None
    meta: MetaAdCopyOutput | None = None
    google: GoogleRSACopyOutput | None = None


# ---------------------------------------------------------------------------
# BJC-172: Email Nurture Sequence
# ---------------------------------------------------------------------------

class NurtureEmail(BaseModel):
    subject_line: str = Field(..., description="Max 60 chars")
    preview_text: str = Field(..., description="Max 90 chars")
    body_html: str
    send_delay_days: int = Field(..., description="Days after trigger (0 = immediate)")
    purpose: Literal[
        "value_delivery", "education", "social_proof", "soft_pitch", "direct_cta",
    ]


class EmailSequenceOutput(BaseModel):
    sequence_name: str
    trigger: str
    emails: list[NurtureEmail] = Field(..., min_length=3, max_length=5)


# ---------------------------------------------------------------------------
# BJC-173: Image Concept Brief
# ---------------------------------------------------------------------------

class ImageBriefOutput(BaseModel):
    concept_name: str
    intended_use: str
    dimensions: str
    visual_description: str
    text_overlay: str | None = None
    color_palette: list[str] = Field(default_factory=list, description="Hex colors")
    mood: str
    style_reference: str
    do_not_include: list[str] = Field(default_factory=list)


class ImageBriefSetOutput(BaseModel):
    briefs: list[ImageBriefOutput]


# ---------------------------------------------------------------------------
# BJC-174: LinkedIn Document Ad (Carousel)
# ---------------------------------------------------------------------------

class SlideOutput(BaseModel):
    headline: str = Field(..., description="Max 50 chars")
    body: str | None = Field(None, description="Max 120 chars")
    stat_callout: str | None = None
    stat_label: str | None = None
    is_cta_slide: bool = False
    cta_text: str | None = None


class DocumentAdOutput(BaseModel):
    slides: list[SlideOutput] = Field(..., min_length=5, max_length=8)
    aspect_ratio: Literal["1:1", "4:5"] = "1:1"


# ---------------------------------------------------------------------------
# BJC-175: Video Script
# ---------------------------------------------------------------------------

class ScriptSegment(BaseModel):
    timestamp_start: str
    timestamp_end: str
    spoken_text: str
    visual_direction: str
    text_overlay: str | None = None
    caption_text: str


class VideoScriptOutput(BaseModel):
    title: str
    duration: Literal["30s", "60s"]
    aspect_ratio: str
    hook: ScriptSegment
    body: list[ScriptSegment]
    cta: ScriptSegment
    total_word_count: int
    music_direction: str
    target_platform: str


# ---------------------------------------------------------------------------
# BJC-176: Case Study Page Content
# ---------------------------------------------------------------------------

class CaseStudyNarrativeSection(BaseModel):
    heading: str
    body: str
    bullets: list[str] | None = None


class CaseStudyMetricOutput(BaseModel):
    value: str
    label: str


class CaseStudyContentOutput(BaseModel):
    headline: str
    sections: list[CaseStudyNarrativeSection]
    metrics: list[CaseStudyMetricOutput]
    quote_text: str | None = None
    quote_author: str | None = None
    quote_title: str | None = None
    cta_text: str = "Get Similar Results"
