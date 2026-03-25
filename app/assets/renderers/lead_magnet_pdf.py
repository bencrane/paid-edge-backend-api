import io

from reportlab.lib.colors import Color, HexColor, white
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.platypus import (
    BaseDocTemplate,
    Frame,
    NextPageTemplate,
    PageBreak,
    PageTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
)

from app.assets.models import LeadMagnetPDFInput


def _hex_to_color(hex_str: str) -> Color:
    return HexColor(hex_str)


def _build_styles(branding):
    primary = _hex_to_color(branding.primary_color)
    secondary = _hex_to_color(branding.secondary_color)
    base = getSampleStyleSheet()

    return {
        "title": ParagraphStyle(
            "PDFTitle",
            parent=base["Title"],
            fontSize=28,
            leading=34,
            textColor=white,
            alignment=TA_CENTER,
            spaceAfter=12,
            fontName="Helvetica-Bold",
        ),
        "subtitle": ParagraphStyle(
            "PDFSubtitle",
            parent=base["Normal"],
            fontSize=14,
            leading=20,
            textColor=HexColor("#cccccc"),
            alignment=TA_CENTER,
            spaceAfter=24,
            fontName="Helvetica",
        ),
        "toc_title": ParagraphStyle(
            "TOCTitle",
            parent=base["Heading1"],
            fontSize=22,
            leading=28,
            textColor=secondary,
            spaceAfter=20,
            fontName="Helvetica-Bold",
        ),
        "toc_entry": ParagraphStyle(
            "TOCEntry",
            parent=base["Normal"],
            fontSize=12,
            leading=22,
            textColor=HexColor("#333333"),
            leftIndent=8,
            fontName="Helvetica",
        ),
        "section_heading": ParagraphStyle(
            "SectionHeading",
            parent=base["Heading1"],
            fontSize=20,
            leading=26,
            textColor=secondary,
            spaceBefore=16,
            spaceAfter=12,
            fontName="Helvetica-Bold",
        ),
        "body": ParagraphStyle(
            "PDFBody",
            parent=base["Normal"],
            fontSize=11,
            leading=17,
            textColor=HexColor("#333333"),
            spaceAfter=10,
            fontName="Helvetica",
        ),
        "bullet": ParagraphStyle(
            "PDFBullet",
            parent=base["Normal"],
            fontSize=10.5,
            leading=16,
            textColor=HexColor("#444444"),
            leftIndent=20,
            bulletIndent=8,
            spaceAfter=4,
            fontName="Helvetica",
        ),
        "callout": ParagraphStyle(
            "PDFCallout",
            parent=base["Normal"],
            fontSize=10.5,
            leading=16,
            textColor=secondary,
            fontName="Helvetica-Bold",
        ),
        "footer": ParagraphStyle(
            "PDFFooter",
            parent=base["Normal"],
            fontSize=8,
            textColor=HexColor("#999999"),
            alignment=TA_CENTER,
            fontName="Helvetica",
        ),
        "_primary": primary,
        "_secondary": secondary,
    }


class _PDFDoc(BaseDocTemplate):
    def __init__(self, buf, branding, **kwargs):
        super().__init__(buf, **kwargs)
        self.branding = branding
        self.page_count = 0

    def afterPage(self):
        self.page_count += 1


def _cover_page(canvas, doc):
    """Draw the cover page background."""
    secondary = _hex_to_color(doc.branding.secondary_color)
    primary = _hex_to_color(doc.branding.primary_color)
    w, h = letter

    # Full page dark bg
    canvas.setFillColor(secondary)
    canvas.rect(0, 0, w, h, fill=1, stroke=0)

    # Accent bar at bottom
    canvas.setFillColor(primary)
    canvas.rect(0, 0, w, 8, fill=1, stroke=0)

    # Company name in top-left
    if doc.branding.company_name:
        canvas.setFillColor(HexColor("#888888"))
        canvas.setFont("Helvetica", 10)
        canvas.drawString(40, h - 40, doc.branding.company_name)


def _content_page(canvas, doc):
    """Draw header/footer for content pages."""
    primary = _hex_to_color(doc.branding.primary_color)
    w, h = letter

    # Top accent line
    canvas.setStrokeColor(primary)
    canvas.setLineWidth(2)
    canvas.line(40, h - 36, w - 40, h - 36)

    # Header company name
    if doc.branding.company_name:
        canvas.setFillColor(HexColor("#999999"))
        canvas.setFont("Helvetica", 8)
        canvas.drawString(40, h - 30, doc.branding.company_name)

    # Footer page number
    canvas.setFillColor(HexColor("#999999"))
    canvas.setFont("Helvetica", 8)
    canvas.drawCentredString(w / 2, 28, f"Page {canvas.getPageNumber()}")

    # Bottom accent line
    canvas.setStrokeColor(HexColor("#e0e0e0"))
    canvas.setLineWidth(0.5)
    canvas.line(40, 44, w - 40, 44)


def render_lead_magnet_pdf(input_data: LeadMagnetPDFInput) -> bytes:
    buf = io.BytesIO()
    w, h = letter

    doc = _PDFDoc(
        buf,
        branding=input_data.branding,
        pagesize=letter,
        leftMargin=40,
        rightMargin=40,
        topMargin=50,
        bottomMargin=60,
    )

    cover_frame = Frame(
        40, 60, w - 80, h - 120,
        id="cover",
        showBoundary=0,
    )
    content_frame = Frame(
        40, 60, w - 80, h - 110,
        id="content",
        showBoundary=0,
    )

    doc.addPageTemplates([
        PageTemplate(id="cover", frames=[cover_frame], onPage=_cover_page),
        PageTemplate(id="content", frames=[content_frame], onPage=_content_page),
    ])

    styles = _build_styles(input_data.branding)
    primary = styles["_primary"]
    story = []

    # --- Cover page ---
    story.append(Spacer(1, h * 0.3))
    story.append(Paragraph(input_data.title, styles["title"]))
    if input_data.subtitle:
        story.append(Paragraph(input_data.subtitle, styles["subtitle"]))
    story.append(NextPageTemplate("content"))
    story.append(PageBreak())

    # --- Table of contents ---
    story.append(Paragraph("Table of Contents", styles["toc_title"]))
    story.append(Spacer(1, 8))
    for i, section in enumerate(input_data.sections, 1):
        entry = f"{i}. &nbsp;&nbsp;{section.heading}"
        story.append(Paragraph(entry, styles["toc_entry"]))
    story.append(PageBreak())

    # --- Content sections ---
    for i, section in enumerate(input_data.sections):
        story.append(Paragraph(section.heading, styles["section_heading"]))
        story.append(Spacer(1, 4))
        story.append(Paragraph(section.body, styles["body"]))

        if section.bullets:
            story.append(Spacer(1, 6))
            for bullet in section.bullets:
                story.append(
                    Paragraph(f"<bullet>&bull;</bullet> {bullet}", styles["bullet"])
                )
            story.append(Spacer(1, 6))

        if section.callout_box:
            # Callout as a tinted table cell
            callout_para = Paragraph(section.callout_box, styles["callout"])
            callout_table = Table(
                [[callout_para]],
                colWidths=[w - 100],
            )
            light_primary = Color(
                primary.red, primary.green, primary.blue, alpha=0.08
            )
            callout_table.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, -1), light_primary),
                ("BOX", (0, 0), (-1, -1), 1.5, primary),
                ("TOPPADDING", (0, 0), (-1, -1), 12),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 12),
                ("LEFTPADDING", (0, 0), (-1, -1), 16),
                ("RIGHTPADDING", (0, 0), (-1, -1), 16),
            ]))
            story.append(callout_table)
            story.append(Spacer(1, 12))

        # Page break between sections (except last)
        if i < len(input_data.sections) - 1:
            story.append(PageBreak())

    doc.build(story)
    return buf.getvalue()
