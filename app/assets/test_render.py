#!/usr/bin/env python3
"""
Render all templates with test content and verify output.
Run: python -m app.assets.test_render
"""

import sys
from pathlib import Path

# Ensure project root is on the path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from jinja2 import Environment, FileSystemLoader

from app.assets.renderers.document_ad_pdf import render_document_ad_pdf
from app.assets.renderers.lead_magnet_pdf import render_lead_magnet_pdf
from app.assets.test_content import (
    CASE_STUDY_PAGE,
    DEMO_REQUEST_PAGE,
    DOCUMENT_AD,
    LEAD_MAGNET_PAGE,
    LEAD_MAGNET_PDF,
    WEBINAR_PAGE,
)

OUTPUT_DIR = Path(__file__).resolve().parents[2] / "test_output"
TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"

_jinja_env = Environment(
    loader=FileSystemLoader(str(TEMPLATES_DIR)),
    autoescape=False,
)


def ensure_output_dir():
    OUTPUT_DIR.mkdir(exist_ok=True)
    print(f"Output directory: {OUTPUT_DIR}")


def test_landing_page(name: str, template_name: str, data: dict):
    """Render a landing page template and verify it's valid HTML."""
    template = _jinja_env.get_template(f"{template_name}.html")
    html = template.render(slug="test-slug", **data)

    out_path = OUTPUT_DIR / f"{name}.html"
    out_path.write_text(html, encoding="utf-8")

    # Basic validation
    assert "<!DOCTYPE html>" in html, f"{name}: missing DOCTYPE"
    assert "<html" in html, f"{name}: missing html tag"
    assert "</html>" in html, f"{name}: missing closing html tag"
    assert "<form" in html, f"{name}: missing form element"
    assert '/lp/test-slug/submit' in html, f"{name}: missing form action"

    size_kb = len(html.encode("utf-8")) / 1024
    print(f"  [PASS] {name}.html — {size_kb:.1f} KB")
    return True


def test_lead_magnet_pdf():
    """Render lead magnet PDF and verify it's valid."""
    pdf_bytes = render_lead_magnet_pdf(LEAD_MAGNET_PDF)

    out_path = OUTPUT_DIR / "lead_magnet.pdf"
    out_path.write_bytes(pdf_bytes)

    assert pdf_bytes[:5] == b"%PDF-", "Lead magnet PDF: invalid PDF header"
    assert len(pdf_bytes) > 1000, "Lead magnet PDF: suspiciously small"

    size_kb = len(pdf_bytes) / 1024
    print(f"  [PASS] lead_magnet.pdf — {size_kb:.1f} KB")
    return True


def test_document_ad_pdf():
    """Render document ad PDF and verify it's valid."""
    pdf_bytes = render_document_ad_pdf(DOCUMENT_AD)

    out_path = OUTPUT_DIR / "document_ad.pdf"
    out_path.write_bytes(pdf_bytes)

    assert pdf_bytes[:5] == b"%PDF-", "Document ad PDF: invalid PDF header"
    assert len(pdf_bytes) > 500, "Document ad PDF: suspiciously small"

    size_kb = len(pdf_bytes) / 1024
    print(f"  [PASS] document_ad.pdf — {size_kb:.1f} KB")
    return True


def test_document_ad_4x5():
    """Test 4:5 aspect ratio variant."""
    input_4x5 = DOCUMENT_AD.model_copy(update={"aspect_ratio": "4:5"})
    pdf_bytes = render_document_ad_pdf(input_4x5)

    out_path = OUTPUT_DIR / "document_ad_4x5.pdf"
    out_path.write_bytes(pdf_bytes)

    assert pdf_bytes[:5] == b"%PDF-", "Document ad 4:5 PDF: invalid PDF header"

    size_kb = len(pdf_bytes) / 1024
    print(f"  [PASS] document_ad_4x5.pdf — {size_kb:.1f} KB")
    return True


def main():
    ensure_output_dir()
    passed = 0
    failed = 0
    tests = []

    print("\n--- Landing Page Templates ---")
    lp_tests = [
        ("lead_magnet_download", "lead_magnet_download", LEAD_MAGNET_PAGE.model_dump()),
        ("case_study", "case_study", CASE_STUDY_PAGE.model_dump()),
        ("webinar", "webinar", WEBINAR_PAGE.model_dump()),
        ("demo_request", "demo_request", DEMO_REQUEST_PAGE.model_dump()),
    ]
    for name, tmpl, data in lp_tests:
        try:
            test_landing_page(name, tmpl, data)
            passed += 1
        except Exception as e:
            print(f"  [FAIL] {name}: {e}")
            failed += 1

    print("\n--- PDF Renderers ---")
    pdf_tests = [
        ("lead_magnet_pdf", test_lead_magnet_pdf),
        ("document_ad_pdf", test_document_ad_pdf),
        ("document_ad_4x5", test_document_ad_4x5),
    ]
    for name, fn in pdf_tests:
        try:
            fn()
            passed += 1
        except Exception as e:
            print(f"  [FAIL] {name}: {e}")
            failed += 1

    print(f"\n{'='*40}")
    print(f"Results: {passed} passed, {failed} failed")
    print(f"Output files: {OUTPUT_DIR}")

    if failed > 0:
        sys.exit(1)
    print("\nAll tests passed!")


if __name__ == "__main__":
    main()
