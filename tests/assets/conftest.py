"""Shared fixtures for asset tests."""

from __future__ import annotations

import pytest

from app.assets.context import AssetContext


@pytest.fixture
def realistic_context() -> AssetContext:
    """Full tenant context for integration tests."""
    return AssetContext(
        organization_id="org-test",
        campaign_id="camp-test",
        company_name="SecureStack",
        brand_voice="Authoritative but approachable, data-driven",
        brand_guidelines={
            "tone": "professional",
            "primary_color": "#00e87b",
            "secondary_color": "#09090b",
            "dos": ["Be specific", "Use data", "Address pain points"],
            "donts": ["No jargon", "No fear-mongering"],
        },
        value_proposition="AI-powered compliance automation that cuts SOC 2 prep time by 80%",
        icp_definition={
            "job_titles": ["CISO", "VP Engineering", "Head of Security"],
            "company_size": "100-1000 employees",
            "industry": "Healthcare SaaS",
            "pain_points": [
                "Audit fatigue",
                "Manual evidence collection",
                "Compliance debt",
            ],
            "goals": [
                "SOC 2 Type II in 90 days",
                "Continuous compliance monitoring",
            ],
        },
        target_persona=(
            "CISOs and VP Engineering at mid-market Healthcare SaaS "
            "companies (100-1000 employees)"
        ),
        case_studies=[
            {
                "customer_name": "MedFlow",
                "customer_industry": "Healthcare SaaS",
                "problem": (
                    "Failed SOC 2 audit twice, manual evidence collection "
                    "taking 20hrs/week"
                ),
                "solution": (
                    "Automated evidence collection and continuous monitoring "
                    "with SecureStack"
                ),
                "results": {
                    "pipeline_generated": "$2.1M",
                    "roi": "3.2x",
                    "time_saved": "80%",
                    "audit_pass": "First attempt",
                },
                "quote": {
                    "text": (
                        "SecureStack turned our compliance nightmare into "
                        "a competitive advantage"
                    ),
                    "author": "Sarah Chen",
                    "title": "VP Engineering",
                },
            }
        ],
        testimonials=[
            {
                "quote": "Cut our audit prep from 6 months to 6 weeks",
                "author": "James Liu",
                "title": "CISO",
                "company": "HealthBridge",
            },
            {
                "quote": (
                    "The automated evidence collection alone saved us "
                    "20 hours a week"
                ),
                "author": "Maria Garcia",
                "title": "Head of Security",
                "company": "CareStack",
            },
        ],
        customer_logos=[
            "https://example.com/medflow.png",
            "https://example.com/healthbridge.png",
            "https://example.com/carestack.png",
        ],
        competitor_differentiators=[
            "Only platform with automated SOC 2 evidence collection for healthcare",
            "Built-in HIPAA + SOC 2 crosswalk",
        ],
        angle="SOC 2 compliance readiness for healthtech startups",
        objective="lead_generation",
        platforms=["linkedin", "meta"],
        industry="Healthcare SaaS",
    )
