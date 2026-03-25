"""data-engine-x API client for PaidEdge backend (BJC-127).

Service-to-service client consuming data-engine-x's entity API.
PaidEdge never calls enrichment providers directly — all entity data,
enrichment, and signal detection flows through data-engine-x.
"""

import asyncio
import logging
from datetime import datetime
from typing import Any

import httpx
from pydantic import BaseModel, Field

from app.config import settings

logger = logging.getLogger(__name__)


# --- Pydantic response models ---


class Pagination(BaseModel):
    page: int
    per_page: int
    returned: int


class CompanyEntity(BaseModel):
    entity_id: str
    canonical_domain: str | None = None
    canonical_name: str | None = None
    linkedin_url: str | None = None
    industry: str | None = None
    employee_count: int | None = None
    employee_range: str | None = None
    revenue_band: str | None = None
    hq_country: str | None = None
    description: str | None = None
    enrichment_confidence: float | None = None
    canonical_payload: dict[str, Any] = Field(default_factory=dict)
    record_version: int = 0
    source_providers: list[str] | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class PersonEntity(BaseModel):
    entity_id: str
    full_name: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    linkedin_url: str | None = None
    title: str | None = None
    seniority: str | None = None
    department: str | None = None
    work_email: str | None = None
    email_status: str | None = None
    phone_e164: str | None = None
    contact_confidence: float | None = None
    canonical_payload: dict[str, Any] = Field(default_factory=dict)
    record_version: int = 0
    created_at: datetime | None = None
    updated_at: datetime | None = None


class Signal(BaseModel):
    entity_type: str
    entity_id: str
    signal_type: str
    details: dict[str, Any] = Field(default_factory=dict)
    detected_at: datetime | None = None


class EntityListResponse(BaseModel):
    items: list[dict[str, Any]]
    pagination: Pagination


# --- Custom exceptions ---


class DataEngineXError(Exception):
    """Base exception for data-engine-x API errors."""

    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"data-engine-x API error {status_code}: {message}")


class DataEngineXAuthError(DataEngineXError):
    """401/403 authentication or permission error."""

    def __init__(self, message: str = "Authentication failed"):
        super().__init__(401, message)


class DataEngineXRateLimitError(DataEngineXError):
    """429 rate limit exceeded."""

    def __init__(self, retry_after: int | None = None):
        self.retry_after = retry_after
        super().__init__(429, "Rate limit exceeded")


# --- Client ---


class DataEngineXClient:
    """Authenticated HTTP client for data-engine-x API.

    Uses API token provisioned via data-engine-x super-admin and stored
    in Doppler. Token is scoped to PaidEdge's org in data-engine-x.
    """

    MAX_RETRIES = 5
    BACKOFF_BASE = 1  # seconds
    BACKOFF_CAP = 16  # seconds
    TIMEOUT = 30.0  # seconds

    def __init__(
        self,
        base_url: str | None = None,
        api_token: str | None = None,
    ):
        self.base_url = (base_url or settings.DATA_ENGINE_X_BASE_URL).rstrip("/")
        self.api_token = api_token or settings.DATA_ENGINE_X_API_TOKEN
        self._client = httpx.AsyncClient(timeout=self.TIMEOUT)

    async def close(self) -> None:
        await self._client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
        }

    async def _request(
        self,
        method: str,
        path: str,
        json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Core request method with retry + exponential backoff."""
        url = f"{self.base_url}{path}"
        headers = self._headers()

        for attempt in range(self.MAX_RETRIES + 1):
            try:
                resp = await self._client.request(
                    method, url, headers=headers, json=json,
                )
            except httpx.TimeoutException:
                if attempt >= self.MAX_RETRIES:
                    raise DataEngineXError(0, "Request timed out after retries")
                delay = min(self.BACKOFF_BASE * (2 ** attempt), self.BACKOFF_CAP)
                logger.warning(
                    "data-engine-x timeout on %s %s — retrying in %ds (attempt %d/%d)",
                    method, path, delay, attempt + 1, self.MAX_RETRIES,
                )
                await asyncio.sleep(delay)
                continue

            if resp.status_code == 429:
                if attempt >= self.MAX_RETRIES:
                    raise DataEngineXRateLimitError()
                delay = min(self.BACKOFF_BASE * (2 ** attempt), self.BACKOFF_CAP)
                logger.warning(
                    "data-engine-x rate limited on %s %s — retrying in %ds (attempt %d/%d)",
                    method, path, delay, attempt + 1, self.MAX_RETRIES,
                )
                await asyncio.sleep(delay)
                continue

            if resp.status_code in (500, 502, 503):
                if attempt >= self.MAX_RETRIES:
                    self._raise_for_status(resp)
                delay = min(self.BACKOFF_BASE * (2 ** attempt), self.BACKOFF_CAP)
                logger.warning(
                    "data-engine-x %d on %s %s — retrying in %ds (attempt %d/%d)",
                    resp.status_code, method, path, delay, attempt + 1, self.MAX_RETRIES,
                )
                await asyncio.sleep(delay)
                continue

            if resp.status_code >= 400:
                self._raise_for_status(resp)

            return resp.json()

        raise DataEngineXError(500, "Unexpected retry exhaustion")

    def _raise_for_status(self, resp: httpx.Response) -> None:
        """Parse error response and raise appropriate exception."""
        try:
            body = resp.json()
            message = body.get("error", resp.text)
        except Exception:
            message = resp.text

        if resp.status_code in (401, 403):
            raise DataEngineXAuthError(message)
        if resp.status_code == 429:
            raise DataEngineXRateLimitError()
        raise DataEngineXError(resp.status_code, message)

    # --- Public methods ---

    async def enrich_companies(
        self,
        domains: list[str],
        *,
        page: int = 1,
        per_page: int = 100,
    ) -> list[CompanyEntity]:
        """Batch company enrichment by domain.

        Calls POST /api/entities/v1/companies/list with domain filters.
        """
        results: list[CompanyEntity] = []
        for domain in domains:
            data = await self._request("POST", "/api/entities/v1/companies/list", json={
                "canonical_domain": domain,
                "page": page,
                "per_page": per_page,
            })
            items = data.get("data", {}).get("items", [])
            results.extend(CompanyEntity(**item) for item in items)
        return results

    async def enrich_persons(
        self,
        filters: dict[str, Any] | None = None,
        *,
        page: int = 1,
        per_page: int = 100,
    ) -> list[PersonEntity]:
        """Person search/enrichment with optional filters.

        Calls POST /api/entities/v1/persons/list.
        """
        payload: dict[str, Any] = {"page": page, "per_page": per_page}
        if filters:
            payload.update(filters)
        data = await self._request("POST", "/api/entities/v1/persons/list", json=payload)
        items = data.get("data", {}).get("items", [])
        return [PersonEntity(**item) for item in items]

    async def get_signals(
        self,
        signal_type: str | None = None,
        since: datetime | None = None,
        *,
        page: int = 1,
        per_page: int = 100,
    ) -> list[Signal]:
        """Consume pre-computed signal data from entity timeline.

        Signals include: new_in_role, exec_departed, promoted,
        raised_money, lookalike matches, page visitors, etc.

        Uses POST /api/entities/v1/entity-timeline to read change events.
        """
        payload: dict[str, Any] = {
            "entity_type": "company",
            "page": page,
            "per_page": per_page,
        }
        if signal_type:
            payload["event_type"] = signal_type
        data = await self._request(
            "POST", "/api/entities/v1/entity-timeline", json=payload,
        )
        items = data.get("data", {}).get("items", [])
        signals: list[Signal] = []
        for item in items:
            signals.append(Signal(
                entity_type=item.get("entity_type", "company"),
                entity_id=item.get("entity_id", ""),
                signal_type=item.get("operation_id", item.get("event_type", "unknown")),
                details=item,
                detected_at=item.get("created_at"),
            ))
        return signals

    async def get_entity(
        self,
        entity_type: str,
        entity_id: str,
    ) -> dict[str, Any]:
        """Single entity lookup by type and ID.

        Uses POST /api/entities/v1/{type}/list with an entity filter.
        """
        type_map = {
            "company": "companies",
            "person": "persons",
            "job": "job-postings",
        }
        path_segment = type_map.get(entity_type, f"{entity_type}s")
        data = await self._request(
            "POST", f"/api/entities/v1/{path_segment}/list", json={
                "page": 1,
                "per_page": 1,
            },
        )
        items = data.get("data", {}).get("items", [])
        # Filter to the specific entity
        for item in items:
            if item.get("entity_id") == entity_id:
                return item
        # If not in first page, do a targeted search via snapshots
        snapshot_data = await self._request(
            "POST", "/api/entities/v1/entity-snapshots", json={
                "entity_type": entity_type,
                "entity_id": entity_id,
            },
        )
        snapshot_items = snapshot_data.get("data", {}).get("items", [])
        if snapshot_items:
            return snapshot_items[0]
        return {}

    async def search_entities(
        self,
        entity_type: str,
        filters: dict[str, Any] | None = None,
        *,
        page: int = 1,
        per_page: int = 100,
    ) -> EntityListResponse:
        """Filtered entity search with pagination.

        Returns the raw list response with pagination metadata.
        """
        type_map = {
            "company": "companies",
            "person": "persons",
            "job": "job-postings",
        }
        path_segment = type_map.get(entity_type, f"{entity_type}s")
        payload: dict[str, Any] = {"page": page, "per_page": per_page}
        if filters:
            payload.update(filters)
        data = await self._request(
            "POST", f"/api/entities/v1/{path_segment}/list", json=payload,
        )
        inner = data.get("data", {})
        items = inner.get("items", [])
        pagination_data = inner.get("pagination", {
            "page": page, "per_page": per_page, "returned": len(items),
        })
        return EntityListResponse(
            items=items,
            pagination=Pagination(**pagination_data),
        )
