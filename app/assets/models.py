from typing import Literal, Union

from pydantic import BaseModel


class BrandingConfig(BaseModel):
    logo_url: str | None = None
    primary_color: str = "#00e87b"
    secondary_color: str = "#09090b"
    font_family: str = "Inter, sans-serif"
    company_name: str = ""


class TrackingConfig(BaseModel):
    rudderstack_write_key: str | None = None
    rudderstack_data_plane_url: str | None = None
    utm_source: str | None = None
    utm_medium: str | None = None
    utm_campaign: str | None = None


class FormField(BaseModel):
    name: str
    label: str
    type: str = "text"
    required: bool = True


class Section(BaseModel):
    heading: str
    body: str
    bullets: list[str] | None = None
    callout: str | None = None


class MetricCallout(BaseModel):
    value: str
    label: str


class SocialProofConfig(BaseModel):
    type: Literal["logos", "quote", "stats"]
    logos: list[str] | None = None
    quote_text: str | None = None
    quote_author: str | None = None
    quote_title: str | None = None
    stats: list[MetricCallout] | None = None


class LeadMagnetPageInput(BaseModel):
    template: Literal["lead_magnet_download"] = "lead_magnet_download"
    headline: str
    subhead: str
    value_props: list[str]
    form_fields: list[FormField]
    cta_text: str = "Download Now"
    branding: BrandingConfig
    tracking: TrackingConfig = TrackingConfig()
    social_proof: SocialProofConfig | None = None
    hero_image_url: str | None = None


class CaseStudyPageInput(BaseModel):
    template: Literal["case_study"] = "case_study"
    customer_name: str
    customer_logo_url: str | None = None
    headline: str
    sections: list[Section]
    metrics: list[MetricCallout]
    quote_text: str | None = None
    quote_author: str | None = None
    quote_title: str | None = None
    cta_text: str = "Get Similar Results"
    form_fields: list[FormField] = []
    branding: BrandingConfig
    tracking: TrackingConfig = TrackingConfig()


class WebinarPageInput(BaseModel):
    template: Literal["webinar"] = "webinar"
    event_name: str
    event_date: str
    headline: str
    speakers: list[dict]
    agenda: list[str]
    form_fields: list[FormField]
    cta_text: str = "Register Now"
    branding: BrandingConfig
    tracking: TrackingConfig = TrackingConfig()


class DemoRequestPageInput(BaseModel):
    template: Literal["demo_request"] = "demo_request"
    headline: str
    subhead: str
    benefits: list[Section]
    trust_signals: SocialProofConfig | None = None
    form_fields: list[FormField]
    cta_text: str = "Request Demo"
    branding: BrandingConfig
    tracking: TrackingConfig = TrackingConfig()


LandingPageInput = Union[
    LeadMagnetPageInput, CaseStudyPageInput, WebinarPageInput, DemoRequestPageInput
]


class PDFSection(BaseModel):
    heading: str
    body: str
    bullets: list[str] | None = None
    callout_box: str | None = None


class LeadMagnetPDFInput(BaseModel):
    title: str
    subtitle: str | None = None
    sections: list[PDFSection]
    branding: BrandingConfig


class Slide(BaseModel):
    headline: str
    body: str | None = None
    stat_callout: str | None = None
    stat_label: str | None = None
    is_cta_slide: bool = False
    cta_text: str | None = None


class DocumentAdInput(BaseModel):
    slides: list[Slide]
    branding: BrandingConfig
    aspect_ratio: Literal["1:1", "4:5"] = "1:1"
