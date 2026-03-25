import io

from reportlab.lib.colors import Color, HexColor, white
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.styles import ParagraphStyle
from reportlab.platypus import (
    BaseDocTemplate,
    Frame,
    PageTemplate,
    Paragraph,
    Spacer,
)

from app.assets.models import DocumentAdInput

DIMENSIONS = {
    "1:1": (1080, 1080),
    "4:5": (1080, 1350),
}

# reportlab uses points (72 per inch). We scale 1080px to a reasonable PDF page size.
# Use 7.5 inches as the base width for readability.
SCALE_BASE = 7.5 * 72  # 540 points


def _hex_to_color(hex_str: str) -> Color:
    return HexColor(hex_str)


def _page_size(aspect_ratio: str):
    px_w, px_h = DIMENSIONS[aspect_ratio]
    w = SCALE_BASE
    h = w * (px_h / px_w)
    return (w, h)


def _build_slide_styles(branding):
    secondary = _hex_to_color(branding.secondary_color)
    return {
        "headline": ParagraphStyle(
            "SlideHeadline",
            fontSize=32,
            leading=40,
            textColor=white,
            fontName="Helvetica-Bold",
            alignment=TA_LEFT,
            spaceAfter=16,
        ),
        "body": ParagraphStyle(
            "SlideBody",
            fontSize=16,
            leading=24,
            textColor=HexColor("#dddddd"),
            fontName="Helvetica",
            alignment=TA_LEFT,
            spaceAfter=12,
        ),
        "stat": ParagraphStyle(
            "SlideStat",
            fontSize=72,
            leading=80,
            textColor=_hex_to_color(branding.primary_color),
            fontName="Helvetica-Bold",
            alignment=TA_CENTER,
            spaceAfter=8,
        ),
        "stat_label": ParagraphStyle(
            "SlideStatLabel",
            fontSize=18,
            leading=24,
            textColor=HexColor("#cccccc"),
            fontName="Helvetica",
            alignment=TA_CENTER,
            spaceAfter=16,
        ),
        "cta_headline": ParagraphStyle(
            "CTAHeadline",
            fontSize=36,
            leading=44,
            textColor=white,
            fontName="Helvetica-Bold",
            alignment=TA_CENTER,
            spaceAfter=24,
        ),
        "cta_text": ParagraphStyle(
            "CTAText",
            fontSize=20,
            leading=28,
            textColor=secondary,
            fontName="Helvetica-Bold",
            alignment=TA_CENTER,
        ),
    }


class _SlideDoc(BaseDocTemplate):
    def __init__(self, buf, slides, branding, aspect_ratio, **kwargs):
        super().__init__(buf, **kwargs)
        self.slide_list = slides
        self.branding = branding
        self.aspect_ratio = aspect_ratio
        self._slide_idx = 0

    def afterPage(self):
        self._slide_idx += 1


def _draw_slide_bg(canvas, doc):
    """Dark background with branding accent."""
    secondary = _hex_to_color(doc.branding.secondary_color)
    primary = _hex_to_color(doc.branding.primary_color)
    w, h = doc.pagesize

    canvas.setFillColor(secondary)
    canvas.rect(0, 0, w, h, fill=1, stroke=0)

    # Accent bar at top
    canvas.setFillColor(primary)
    canvas.rect(0, h - 6, w, 6, fill=1, stroke=0)

    # Company name bottom-right
    if doc.branding.company_name:
        canvas.setFillColor(HexColor("#666666"))
        canvas.setFont("Helvetica", 9)
        canvas.drawRightString(w - 28, 20, doc.branding.company_name)

    # Slide number bottom-left
    canvas.setFillColor(HexColor("#555555"))
    canvas.setFont("Helvetica", 9)
    page_num = canvas.getPageNumber()
    total = len(doc.slide_list)
    canvas.drawString(28, 20, f"{page_num}/{total}")


def _draw_cta_bg(canvas, doc):
    """CTA slide with accent color background."""
    primary = _hex_to_color(doc.branding.primary_color)
    secondary = _hex_to_color(doc.branding.secondary_color)
    w, h = doc.pagesize

    canvas.setFillColor(secondary)
    canvas.rect(0, 0, w, h, fill=1, stroke=0)

    # Large accent circle in center as visual element
    canvas.setFillColor(Color(primary.red, primary.green, primary.blue, alpha=0.12))
    canvas.circle(w / 2, h / 2, w * 0.35, fill=1, stroke=0)

    # Company name
    if doc.branding.company_name:
        canvas.setFillColor(HexColor("#666666"))
        canvas.setFont("Helvetica", 9)
        canvas.drawRightString(w - 28, 20, doc.branding.company_name)


def render_document_ad_pdf(input_data: DocumentAdInput) -> bytes:
    buf = io.BytesIO()
    page_w, page_h = _page_size(input_data.aspect_ratio)

    doc = _SlideDoc(
        buf,
        slides=input_data.slides,
        branding=input_data.branding,
        aspect_ratio=input_data.aspect_ratio,
        pagesize=(page_w, page_h),
        leftMargin=40,
        rightMargin=40,
        topMargin=40,
        bottomMargin=48,
    )

    margin = 40
    frame_w = page_w - 2 * margin
    frame_h = page_h - 88  # top + bottom margins

    templates = []
    for i, slide in enumerate(input_data.slides):
        bg_fn = _draw_cta_bg if slide.is_cta_slide else _draw_slide_bg
        templates.append(
            PageTemplate(
                id=f"slide_{i}",
                frames=[Frame(margin, 48, frame_w, frame_h, showBoundary=0)],
                onPage=bg_fn,
            )
        )
    doc.addPageTemplates(templates)

    styles = _build_slide_styles(input_data.branding)
    primary = _hex_to_color(input_data.branding.primary_color)
    story = []

    from reportlab.platypus import NextPageTemplate, PageBreak

    for i, slide in enumerate(input_data.slides):
        if i > 0:
            story.append(NextPageTemplate(f"slide_{i}"))
            story.append(PageBreak())

        if slide.is_cta_slide:
            story.append(Spacer(1, frame_h * 0.25))
            story.append(Paragraph(slide.headline, styles["cta_headline"]))
            if slide.cta_text:
                story.append(Spacer(1, 16))
                story.append(Paragraph(slide.cta_text, styles["cta_text"]))
        else:
            # Content slide
            story.append(Spacer(1, frame_h * 0.08))

            if slide.stat_callout:
                story.append(Paragraph(slide.stat_callout, styles["stat"]))
                if slide.stat_label:
                    story.append(Paragraph(slide.stat_label, styles["stat_label"]))
                story.append(Spacer(1, 16))

            story.append(Paragraph(slide.headline, styles["headline"]))

            if slide.body:
                story.append(Paragraph(slide.body, styles["body"]))

    doc.build(story)
    return buf.getvalue()
