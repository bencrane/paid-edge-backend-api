"""Google Ads offline conversion import — server-side conversion tracking (BJC-156).

Uploads gclid-based click conversions and enhanced conversions (hashed PII)
to Google Ads via ConversionUploadService. Creates and manages conversion actions.
"""

import asyncio
import hashlib
import logging
import re
from datetime import datetime, timezone
from functools import partial as functools_partial

from app.integrations.google_ads import GoogleAdsService

logger = logging.getLogger(__name__)

# gclid validity window: 90 days
GCLID_MAX_AGE_DAYS = 90


class GoogleAdsConversionClient:
    """Uploads offline conversions to Google Ads via ConversionUploadService."""

    def __init__(self, service: GoogleAdsService):
        self.service = service
        self.customer_id = service.customer_id

    async def upload_click_conversions(
        self,
        conversions: list[dict],
    ) -> dict:
        """Upload gclid-based click conversions.

        Each conversion dict should have:
            - gclid: str (required)
            - conversion_action_id: str (required)
            - conversion_time: str or datetime (required)
            - value: float (optional, conversion value in dollars)
            - currency: str (optional, default "USD")
        """
        if not conversions:
            return {"uploaded": 0, "failed": 0, "errors": []}

        conversion_upload_service = self.service._get_service(
            "ConversionUploadService"
        )
        operations = []

        for conv in conversions:
            click_conversion = self.service._get_type("ClickConversion")
            click_conversion.gclid = conv["gclid"]
            click_conversion.conversion_action = (
                f"customers/{self.customer_id}"
                f"/conversionActions/{conv['conversion_action_id']}"
            )
            click_conversion.conversion_date_time = _format_datetime(
                conv["conversion_time"]
            )

            if conv.get("value"):
                click_conversion.conversion_value = float(conv["value"])
                click_conversion.currency_code = conv.get("currency", "USD")

            operations.append(click_conversion)

        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            functools_partial(
                conversion_upload_service.upload_click_conversions,
                customer_id=self.customer_id,
                conversions=operations,
                partial_failure=True,
            ),
        )

        result = _parse_upload_response(response)
        logger.info(
            "Uploaded %d click conversions (%d failed)",
            result["uploaded"],
            result["failed"],
        )
        return result

    async def upload_enhanced_conversions(
        self,
        conversions: list[dict],
    ) -> dict:
        """Upload enhanced conversions using hashed PII for cookieless matching.

        Each conversion dict should have:
            - conversion_action_id: str (required)
            - conversion_time: str or datetime (required)
            - value: float (optional)
            - currency: str (optional, default "USD")
            - order_id: str (optional, for dedup)
            - email: str (optional, will be hashed)
            - phone: str (optional, will be hashed)
        """
        if not conversions:
            return {"uploaded": 0, "failed": 0, "errors": []}

        conversion_upload_service = self.service._get_service(
            "ConversionUploadService"
        )
        operations = []

        for conv in conversions:
            click_conversion = self.service._get_type("ClickConversion")
            click_conversion.conversion_action = (
                f"customers/{self.customer_id}"
                f"/conversionActions/{conv['conversion_action_id']}"
            )
            click_conversion.conversion_date_time = _format_datetime(
                conv["conversion_time"]
            )

            if conv.get("value"):
                click_conversion.conversion_value = float(conv["value"])
                click_conversion.currency_code = conv.get("currency", "USD")

            if conv.get("order_id"):
                click_conversion.order_id = conv["order_id"]

            # Enhanced conversion: hashed PII for matching
            if conv.get("email") or conv.get("phone"):
                user_identifier = click_conversion.user_identifiers.add()
                if conv.get("email"):
                    user_identifier.hashed_email = _hash_value(
                        conv["email"].strip().lower()
                    )
                if conv.get("phone"):
                    user_identifier.hashed_phone_number = _hash_value(
                        _normalize_phone(conv["phone"])
                    )

            operations.append(click_conversion)

        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            functools_partial(
                conversion_upload_service.upload_click_conversions,
                customer_id=self.customer_id,
                conversions=operations,
                partial_failure=True,
            ),
        )

        result = _parse_upload_response(response)
        logger.info(
            "Uploaded %d enhanced conversions (%d failed)",
            result["uploaded"],
            result["failed"],
        )
        return result

    async def create_conversion_action(
        self,
        name: str,
        category: str = "PURCHASE",
        value_settings: dict | None = None,
    ) -> str:
        """Create a conversion action in Google Ads.

        Returns the conversion action resource name.
        """
        operation = self.service._get_type("ConversionActionOperation")
        action = operation.create
        action.name = name
        action.type_ = (
            self.service.enums.ConversionActionTypeEnum.UPLOAD_CLICKS
        )
        action.category = getattr(
            self.service.enums.ConversionActionCategoryEnum, category
        )
        action.status = self.service.enums.ConversionActionStatusEnum.ENABLED

        if value_settings:
            action.value_settings.default_value = value_settings.get(
                "default_value", 0
            )
            action.value_settings.always_use_default_value = value_settings.get(
                "always_use_default", False
            )

        response = await self.service.mutate(
            "ConversionActionService", [operation]
        )
        resource_name = response.results[0].resource_name
        logger.info("Created conversion action: %s (%s)", name, resource_name)
        return resource_name

    async def list_conversion_actions(self) -> list[dict]:
        """List all conversion actions for the customer."""
        query = """
            SELECT
                conversion_action.id,
                conversion_action.name,
                conversion_action.type,
                conversion_action.category,
                conversion_action.status
            FROM conversion_action
            WHERE conversion_action.status != 'REMOVED'
        """
        rows = await self.service.search_stream(query)
        results = []
        for row in rows:
            results.append({
                "id": str(row.conversion_action.id),
                "name": row.conversion_action.name,
                "type": row.conversion_action.type_.name,
                "category": row.conversion_action.category.name,
                "status": row.conversion_action.status.name,
            })
        return results

    async def get_conversion_action(self, action_id: str) -> dict | None:
        """Get a single conversion action by ID."""
        query = f"""
            SELECT
                conversion_action.id,
                conversion_action.name,
                conversion_action.type,
                conversion_action.category,
                conversion_action.status
            FROM conversion_action
            WHERE conversion_action.id = {action_id}
        """
        rows = await self.service.search_stream(query)
        for row in rows:
            return {
                "id": str(row.conversion_action.id),
                "name": row.conversion_action.name,
                "type": row.conversion_action.type_.name,
                "category": row.conversion_action.category.name,
                "status": row.conversion_action.status.name,
            }
        return None


# --- CRM event mapping ---

# Default mapping of PaidEdge CRM events to Google Ads conversion action categories
CRM_EVENT_CATEGORY_MAP = {
    "lead_created": "LEAD",
    "opportunity_created": "LEAD",
    "deal_closed_won": "PURCHASE",
    "form_submission": "SUBMIT_LEAD_FORM",
    "demo_booked": "BOOK_APPOINTMENT",
    "trial_started": "SIGNUP",
}


def map_crm_event_to_conversion(
    crm_event: dict,
    conversion_action_map: dict[str, str],
) -> dict | None:
    """Map a PaidEdge CRM event to a Google Ads conversion upload payload.

    Args:
        crm_event: Dict with keys: event_type, gclid, timestamp, value, currency, email, phone
        conversion_action_map: Mapping of event_type → conversion_action_id

    Returns:
        Conversion payload dict for upload, or None if not mappable.
    """
    event_type = crm_event.get("event_type", "")
    conversion_action_id = conversion_action_map.get(event_type)
    if not conversion_action_id:
        logger.debug("No conversion action mapped for event type: %s", event_type)
        return None

    gclid = crm_event.get("gclid")
    if not gclid:
        # Fall back to enhanced conversion if no gclid
        if not crm_event.get("email") and not crm_event.get("phone"):
            logger.debug("No gclid or PII for conversion mapping")
            return None

    payload = {
        "conversion_action_id": conversion_action_id,
        "conversion_time": crm_event.get("timestamp", datetime.now(timezone.utc).isoformat()),
    }

    if gclid:
        payload["gclid"] = gclid

    if crm_event.get("value"):
        payload["value"] = crm_event["value"]
        payload["currency"] = crm_event.get("currency", "USD")

    if crm_event.get("email"):
        payload["email"] = crm_event["email"]
    if crm_event.get("phone"):
        payload["phone"] = crm_event["phone"]
    if crm_event.get("order_id"):
        payload["order_id"] = crm_event["order_id"]

    return payload


# --- gclid helpers ---


def extract_gclid(url: str) -> str | None:
    """Extract gclid parameter from a URL."""
    match = re.search(r'[?&]gclid=([^&]+)', url)
    return match.group(1) if match else None


def is_gclid_valid(gclid_captured_at: datetime) -> bool:
    """Check if a gclid is still within its 90-day validity window."""
    now = datetime.now(timezone.utc)
    if gclid_captured_at.tzinfo is None:
        gclid_captured_at = gclid_captured_at.replace(tzinfo=timezone.utc)
    age_days = (now - gclid_captured_at).days
    return age_days <= GCLID_MAX_AGE_DAYS


# --- Deduplication ---


def build_dedup_key(gclid: str, action_id: str, conversion_time: str) -> str:
    """Build a deduplication key for a conversion.

    Google Ads deduplicates by gclid + conversion_action + conversion_time.
    """
    return f"{gclid}:{action_id}:{conversion_time}"


# --- Internal helpers ---


def _format_datetime(dt) -> str:
    """Format datetime to Google Ads expected format: yyyy-mm-dd hh:mm:ss+|-hh:mm"""
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt)
        except ValueError:
            return dt  # Return as-is if unparseable
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S+00:00")


def _hash_value(value: str) -> str:
    """SHA-256 hash a string value."""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _normalize_phone(phone: str) -> str:
    """Normalize phone to E.164 format (digits only with country code)."""
    digits = re.sub(r"[^\d+]", "", phone)
    if not digits.startswith("+"):
        digits = "+1" + digits  # Default US
    return digits


def _parse_upload_response(response) -> dict:
    """Parse upload response including partial failures."""
    result = {"uploaded": 0, "failed": 0, "errors": []}

    if hasattr(response, "partial_failure_error") and response.partial_failure_error:
        for error in response.partial_failure_error.details:
            result["errors"].append(str(error))
            result["failed"] += 1

    total_results = len(response.results) if hasattr(response, "results") else 0
    result["uploaded"] = total_results - result["failed"]
    if result["uploaded"] < 0:
        result["uploaded"] = 0

    return result
