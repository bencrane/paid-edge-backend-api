"""Meta Custom Audiences + Lookalike Audiences — SHA-256 hashing + session upload (BJC-160)."""

import hashlib
import json
import logging
import re
import time

from pydantic import BaseModel

logger = logging.getLogger(__name__)

# --- Hashing ---

NO_HASH_FIELDS = {"MADID", "EXTERN_ID"}


def _normalize(value: str, field_type: str) -> str:
    """Normalize a value per Meta's field-specific rules."""
    if not value:
        return ""

    field_type = field_type.upper()

    if field_type == "EMAIL":
        return value.strip().lower()

    if field_type == "PHONE":
        # Remove symbols/letters, keep digits, prefix country code
        digits = re.sub(r"[^\d+]", "", value)
        if not digits.startswith("+") and not digits.startswith("1"):
            digits = "1" + digits  # Default US country code
        return digits.lstrip("+")

    if field_type in ("FN", "LN"):
        # Lowercase, a-z only (UTF-8 special chars OK per Meta docs)
        return re.sub(r"[^a-z\u00C0-\u024F]", "", value.strip().lower())

    if field_type == "FI":
        return value.strip().lower()[:1]

    if field_type == "GEN":
        v = value.strip().lower()
        if v in ("male", "m"):
            return "m"
        if v in ("female", "f"):
            return "f"
        return v

    if field_type == "DOBY":
        return value.strip()[:4]

    if field_type in ("DOBM", "DOBD"):
        return value.strip().zfill(2)

    if field_type == "CT":
        return re.sub(r"[^a-z]", "", value.strip().lower())

    if field_type == "ST":
        return value.strip().lower()[:2]

    if field_type == "ZIP":
        return value.strip().lower().replace(" ", "")[:5]

    if field_type == "COUNTRY":
        return value.strip().lower()[:2]

    return value.strip().lower()


def hash_for_meta(value: str, field_type: str) -> str:
    """Normalize and SHA-256 hash a value per Meta's rules.

    Returns raw value for MADID and EXTERN_ID (not hashed).
    """
    if field_type.upper() in NO_HASH_FIELDS:
        return value

    normalized = _normalize(value, field_type)
    if not normalized:
        return ""
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def prepare_audience_data(
    members: list[dict],
    schema: list[str],
) -> list[list[str]]:
    """Transform PaidEdge audience members to hashed rows for Meta upload."""
    rows = []
    for member in members:
        row = []
        for field in schema:
            field_key = field.lower()
            value = member.get(field_key, "")
            if not value:
                # Try alternate key names
                alt_keys = {
                    "EMAIL": "email",
                    "FN": "first_name",
                    "LN": "last_name",
                    "PHONE": "phone",
                    "EXTERN_ID": "entity_id",
                }
                value = member.get(alt_keys.get(field.upper(), ""), "")
            row.append(hash_for_meta(str(value), field) if value else "")
        rows.append(row)
    return rows


# --- Pydantic models ---


class MetaCustomAudience(BaseModel):
    id: str
    name: str
    subtype: str = "CUSTOM"
    approximate_count: int | None = None
    operation_status: int | None = None
    delivery_status: dict | None = None
    time_created: str | None = None


class MetaAudienceUploadResult(BaseModel):
    audience_id: str
    num_received: int
    num_invalid_entries: int
    invalid_entry_samples: list[dict] = []
    session_id: int | None = None


# --- LDU / CCPA ---


def build_ldu_payload(data: list[list[str]], schema: list[str]) -> dict:
    """For California residents, include LDU data processing options."""
    return {
        "schema": schema,
        "data": data,
        "data_processing_options": ["LDU"],
        "data_processing_options_country": 1,
        "data_processing_options_state": 1000,
    }


# --- Audience CRUD methods (mixin for MetaAdsClient) ---


class MetaAudiencesMixin:
    """Custom Audience + Lookalike Audience methods for MetaAdsClient."""

    async def create_custom_audience(
        self,
        name: str,
        subtype: str = "CUSTOM",
        customer_file_source: str = "USER_PROVIDED_ONLY",
    ) -> dict:
        """POST /act_{AD_ACCOUNT_ID}/customaudiences — creates empty audience."""
        return await self._request(
            "POST",
            f"{self.ad_account_id}/customaudiences",
            data={
                "name": name,
                "subtype": subtype,
                "customer_file_source": customer_file_source,
            },
        )

    async def upload_users(
        self,
        audience_id: str,
        schema: list[str],
        data: list[list[str]],
        session_id: int | None = None,
    ) -> dict:
        """POST /{AUDIENCE_ID}/users — upload hashed user data.

        Max 10,000 users per request. Auto-batches larger lists.
        """
        if len(data) > 10_000:
            return await self._batch_upload_with_sessions(
                audience_id, schema, data
            )

        payload = {
            "payload": json.dumps({
                "schema": schema,
                "data": data,
            }),
        }
        if session_id is not None:
            payload["session"] = json.dumps({
                "session_id": session_id,
                "batch_seq": 1,
                "last_batch_flag": True,
                "estimated_num_total": len(data),
            })

        return await self._request("POST", f"{audience_id}/users", data=payload)

    async def remove_users(
        self,
        audience_id: str,
        schema: list[str],
        data: list[list[str]],
    ) -> dict:
        """DELETE /{AUDIENCE_ID}/users — remove specific users."""
        payload = {
            "payload": json.dumps({
                "schema": schema,
                "data": data,
            }),
        }
        return await self._request("DELETE", f"{audience_id}/users", data=payload)

    async def replace_users(
        self,
        audience_id: str,
        schema: list[str],
        data: list[list[str]],
    ) -> dict:
        """POST /{AUDIENCE_ID}/usersreplace — replace entire audience."""
        session_id = int(time.time())
        all_results = {"total_sent": 0, "total_received": 0, "total_invalid": 0}

        for batch_idx in range(0, len(data), 10_000):
            batch = data[batch_idx : batch_idx + 10_000]
            is_last = (batch_idx + 10_000) >= len(data)
            batch_seq = (batch_idx // 10_000) + 1

            payload = {
                "payload": json.dumps({"schema": schema, "data": batch}),
                "session": json.dumps({
                    "session_id": session_id,
                    "batch_seq": batch_seq,
                    "last_batch_flag": is_last,
                    "estimated_num_total": len(data),
                }),
            }
            resp = await self._request(
                "POST", f"{audience_id}/usersreplace", data=payload
            )
            all_results["total_sent"] += len(batch)
            all_results["total_received"] += resp.get("num_received", 0)
            all_results["total_invalid"] += resp.get("num_invalid_entries", 0)

        return all_results

    async def _batch_upload_with_sessions(
        self,
        audience_id: str,
        schema: list[str],
        data: list[list[str]],
        batch_size: int = 10_000,
    ) -> dict:
        """Split data into batches and upload with session tracking."""
        session_id = int(time.time())
        total_batches = (len(data) + batch_size - 1) // batch_size
        aggregate = {
            "total_sent": 0,
            "total_received": 0,
            "total_invalid": 0,
            "session_id": session_id,
        }

        for batch_idx in range(0, len(data), batch_size):
            batch = data[batch_idx : batch_idx + batch_size]
            batch_seq = (batch_idx // batch_size) + 1
            is_last = batch_seq == total_batches

            payload = {
                "payload": json.dumps({"schema": schema, "data": batch}),
                "session": json.dumps({
                    "session_id": session_id,
                    "batch_seq": batch_seq,
                    "last_batch_flag": is_last,
                    "estimated_num_total": len(data),
                }),
            }
            resp = await self._request(
                "POST", f"{audience_id}/users", data=payload
            )
            aggregate["total_sent"] += len(batch)
            aggregate["total_received"] += resp.get("num_received", 0)
            aggregate["total_invalid"] += resp.get("num_invalid_entries", 0)

        return aggregate

    async def get_audience(self, audience_id: str) -> dict:
        """GET /{AUDIENCE_ID} with fields."""
        return await self._request(
            "GET",
            audience_id,
            params={
                "fields": "name,subtype,approximate_count,operation_status,"
                "delivery_status,time_created,time_updated"
            },
        )

    async def delete_audience(self, audience_id: str) -> None:
        """DELETE /{AUDIENCE_ID}"""
        await self._request("DELETE", audience_id)

    async def list_audiences(self, limit: int = 25) -> list[dict]:
        """GET /act_{AD_ACCOUNT_ID}/customaudiences"""
        return await self._paginate(
            f"{self.ad_account_id}/customaudiences",
            params={
                "fields": "name,subtype,approximate_count,operation_status,"
                "delivery_status,time_created"
            },
            limit=limit,
        )

    # --- Lookalike Audiences ---

    async def create_lookalike_audience(
        self,
        seed_audience_id: str,
        name: str,
        country: str = "US",
        ratio: float = 0.01,
        audience_type: str = "similarity",
    ) -> dict:
        """POST /act_{AD_ACCOUNT_ID}/customaudiences with subtype=LOOKALIKE."""
        spec = {
            "type": audience_type.upper() if audience_type == "reach" else "SIMILARITY",
            "ratio": ratio,
            "country": country,
        }
        return await self._request(
            "POST",
            f"{self.ad_account_id}/customaudiences",
            data={
                "name": name,
                "subtype": "LOOKALIKE",
                "origin_audience_id": seed_audience_id,
                "lookalike_spec": json.dumps(spec),
            },
        )

    async def create_multi_country_lookalike(
        self,
        seed_audience_id: str,
        name: str,
        countries: list[str],
        ratio: float = 0.02,
    ) -> dict:
        """Lookalike across multiple countries via location_spec."""
        spec = {
            "type": "SIMILARITY",
            "ratio": ratio,
            "location_spec": {
                "geo_locations": {"countries": countries},
                "type": "recent",
            },
        }
        return await self._request(
            "POST",
            f"{self.ad_account_id}/customaudiences",
            data={
                "name": name,
                "subtype": "LOOKALIKE",
                "origin_audience_id": seed_audience_id,
                "lookalike_spec": json.dumps(spec),
            },
        )

    async def create_conversion_lookalike(
        self,
        campaign_id: str,
        name: str,
        country: str = "US",
        ratio: float = 0.05,
    ) -> dict:
        """Lookalike from campaign conversions."""
        spec = {
            "type": "SIMILARITY",
            "ratio": ratio,
            "country": country,
        }
        return await self._request(
            "POST",
            f"{self.ad_account_id}/customaudiences",
            data={
                "name": name,
                "subtype": "LOOKALIKE",
                "origin_audience_id": campaign_id,
                "lookalike_spec": json.dumps(spec),
            },
        )
