"""Meta Conversions API (CAPI) — server-side event tracking + Pixel dedup (BJC-164)."""

import hashlib
import logging
import re
import time
from uuid import uuid4

from app.integrations.meta_audiences import _normalize

logger = logging.getLogger(__name__)

# --- Standard event types ---

META_STANDARD_EVENTS = {
    "Lead": "Lead form submitted",
    "Purchase": "Completed purchase",
    "CompleteRegistration": "Registration completed",
    "ViewContent": "Key page viewed",
    "Contact": "Contact form submitted",
    "Schedule": "Appointment scheduled",
    "StartTrial": "Free trial started",
    "SubmitApplication": "Application submitted",
    "Search": "Search performed",
}

PAIDEDGE_CUSTOM_EVENTS = [
    "DemoRequested",
    "PricingPageViewed",
    "LeadMagnetDownloaded",
    "CaseStudyViewed",
]

# --- User data builder ---


def _hash_pii(value: str, field_type: str) -> str:
    """Hash a PII field for CAPI. Same normalization as Custom Audiences."""
    if not value:
        return ""
    normalized = _normalize(value, field_type)
    if not normalized:
        return ""
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def build_user_data(
    email: str | None = None,
    phone: str | None = None,
    first_name: str | None = None,
    last_name: str | None = None,
    date_of_birth: str | None = None,
    gender: str | None = None,
    city: str | None = None,
    state: str | None = None,
    zip_code: str | None = None,
    country: str | None = None,
    client_ip_address: str | None = None,
    client_user_agent: str | None = None,
    fbc: str | None = None,
    fbp: str | None = None,
    external_id: str | None = None,
) -> dict:
    """Build user_data object with proper hashing.

    Hashed: em, ph, fn, ln, db, ge, ct, st, zp, country, external_id
    Raw: client_ip_address, client_user_agent, fbc, fbp
    """
    user_data = {}

    if email:
        user_data["em"] = [_hash_pii(email, "EMAIL")]
    if phone:
        user_data["ph"] = [_hash_pii(phone, "PHONE")]
    if first_name:
        user_data["fn"] = _hash_pii(first_name, "FN")
    if last_name:
        user_data["ln"] = _hash_pii(last_name, "LN")
    if date_of_birth:
        user_data["db"] = _hash_pii(date_of_birth, "DOBY")
    if gender:
        user_data["ge"] = _hash_pii(gender, "GEN")
    if city:
        user_data["ct"] = _hash_pii(city, "CT")
    if state:
        user_data["st"] = _hash_pii(state, "ST")
    if zip_code:
        user_data["zp"] = _hash_pii(zip_code, "ZIP")
    if country:
        user_data["country"] = _hash_pii(country, "COUNTRY")
    if external_id:
        user_data["external_id"] = [_hash_pii(external_id, "EXTERN_ID")]

    # Raw fields (NOT hashed)
    if client_ip_address:
        user_data["client_ip_address"] = client_ip_address
    if client_user_agent:
        user_data["client_user_agent"] = client_user_agent
    if fbc:
        user_data["fbc"] = fbc
    if fbp:
        user_data["fbp"] = fbp

    return user_data


# --- Event builder ---


def generate_event_id(prefix: str = "pe") -> str:
    """Generate a unique event_id for Pixel deduplication."""
    return f"{prefix}_{uuid4().hex[:12]}"


def build_event(
    event_name: str,
    action_source: str,
    event_source_url: str,
    user_data: dict,
    custom_data: dict | None = None,
    event_id: str | None = None,
) -> dict:
    """Construct a single CAPI event payload."""
    event = {
        "event_name": event_name,
        "event_time": int(time.time()),
        "action_source": action_source,
        "event_source_url": event_source_url,
        "user_data": user_data,
    }
    if event_id:
        event["event_id"] = event_id
    if custom_data:
        event["custom_data"] = custom_data
    return event


# --- Data processing options ---


def build_data_processing_options(
    ldu: bool = False,
    country: int = 0,
    state: int = 0,
) -> dict:
    """Limited Data Use for CCPA compliance."""
    if ldu:
        return {
            "data_processing_options": ["LDU"],
            "data_processing_options_country": 1,
            "data_processing_options_state": 1000,
        }
    return {"data_processing_options": []}


# --- CAPI client methods (mixin for MetaAdsClient) ---


class MetaConversionsMixin:
    """Conversions API methods for MetaAdsClient."""

    async def send_events(
        self,
        pixel_id: str,
        events: list[dict],
        test_event_code: str | None = None,
    ) -> dict:
        """POST /{PIXEL_ID}/events — up to 1,000 events per request."""
        import json

        data = {"data": json.dumps(events)}
        if test_event_code:
            data["test_event_code"] = test_event_code

        return await self._request("POST", f"{pixel_id}/events", data=data)


# --- Convenience functions ---


async def send_landing_page_conversion(
    tenant_id: str,
    form_data: dict,
    page_url: str,
    client_ip: str,
    user_agent: str,
    fbc: str | None = None,
    fbp: str | None = None,
    event_id: str | None = None,
    supabase=None,
    meta_client=None,
) -> dict:
    """Send a Lead event when a form is submitted on a PaidEdge landing page."""
    user_data = build_user_data(
        email=form_data.get("email"),
        phone=form_data.get("phone"),
        first_name=form_data.get("first_name"),
        last_name=form_data.get("last_name"),
        client_ip_address=client_ip,
        client_user_agent=user_agent,
        fbc=fbc,
        fbp=fbp,
    )

    event = build_event(
        event_name="Lead",
        action_source="website",
        event_source_url=page_url,
        user_data=user_data,
        event_id=event_id or generate_event_id(),
    )

    # Get pixel_id from provider_configs
    pixel_id = await get_tenant_pixel_id(tenant_id, supabase)
    if not pixel_id:
        logger.warning("No pixel_id configured for tenant %s", tenant_id)
        return {"error": "No pixel_id configured"}

    return await meta_client.send_events(pixel_id, [event])


async def get_tenant_pixel_id(org_id: str, supabase) -> str:
    """Load pixel_id from provider_configs for meta_ads provider."""
    if not supabase:
        return ""
    res = (
        supabase.table("provider_configs")
        .select("config")
        .eq("organization_id", org_id)
        .eq("provider", "meta_ads")
        .maybe_single()
        .execute()
    )
    if not res.data:
        return ""
    return res.data.get("config", {}).get("pixel_id", "")
