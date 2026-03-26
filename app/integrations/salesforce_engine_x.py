"""sfdc-engine-x API client for PaidEdge backend (BJC-189).

Service-to-service client consuming sfdc-engine-x's CRM proxy API.
PaidEdge never calls Salesforce directly — all CRM reads/writes flow through
sfdc-engine-x, which manages OAuth tokens via Nango.

Pattern: mirrors HubSpotEngineClient (async httpx, Bearer auth, exponential
backoff retry on timeout/429/5xx).
"""

import asyncio
import logging
from typing import Any

import httpx

from app.config import settings
from app.integrations.crm_base import (
    CRMEngineAuthError,
    CRMEngineError,
    CRMEngineRateLimitError,
)

logger = logging.getLogger(__name__)


class SalesforceEngineClient:
    """Authenticated HTTP client for sfdc-engine-x API.

    All CRM read endpoints use POST with JSON bodies.
    Auth via Bearer token provisioned in sfdc-engine-x and stored in Doppler.
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
        self.base_url = (base_url or settings.SFDC_ENGINE_X_BASE_URL).rstrip("/")
        self.api_token = api_token or settings.SFDC_ENGINE_X_API_TOKEN
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
                    raise CRMEngineError(0, "Request timed out after retries")
                delay = min(self.BACKOFF_BASE * (2 ** attempt), self.BACKOFF_CAP)
                logger.warning(
                    "sfdc-engine-x timeout on %s %s — retrying in %ds (attempt %d/%d)",
                    method, path, delay, attempt + 1, self.MAX_RETRIES,
                )
                await asyncio.sleep(delay)
                continue

            if resp.status_code == 429:
                if attempt >= self.MAX_RETRIES:
                    raise CRMEngineRateLimitError()
                delay = min(self.BACKOFF_BASE * (2 ** attempt), self.BACKOFF_CAP)
                logger.warning(
                    "sfdc-engine-x rate limited on %s %s — retrying in %ds (attempt %d/%d)",
                    method, path, delay, attempt + 1, self.MAX_RETRIES,
                )
                await asyncio.sleep(delay)
                continue

            if resp.status_code in (500, 502, 503):
                if attempt >= self.MAX_RETRIES:
                    self._raise_for_status(resp)
                delay = min(self.BACKOFF_BASE * (2 ** attempt), self.BACKOFF_CAP)
                logger.warning(
                    "sfdc-engine-x %d on %s %s — retrying in %ds (attempt %d/%d)",
                    resp.status_code, method, path, delay, attempt + 1, self.MAX_RETRIES,
                )
                await asyncio.sleep(delay)
                continue

            if resp.status_code >= 400:
                self._raise_for_status(resp)

            return resp.json()

        raise CRMEngineError(500, "Unexpected retry exhaustion")

    def _raise_for_status(self, resp: httpx.Response) -> None:
        """Parse error response and raise appropriate exception."""
        try:
            body = resp.json()
            message = body.get("error", resp.text)
        except Exception:
            message = resp.text

        if resp.status_code in (401, 403):
            raise CRMEngineAuthError(message)
        if resp.status_code == 429:
            raise CRMEngineRateLimitError()
        raise CRMEngineError(resp.status_code, message)

    # --- Connection ---

    async def get_connection(self, client_id: str) -> dict[str, Any]:
        """Check a client's Salesforce connection status."""
        return await self._request("POST", "/api/connections/get", json={
            "client_id": client_id,
        })

    # --- SOQL queries ---

    async def query_soql(
        self,
        client_id: str,
        soql: str,
    ) -> dict[str, Any]:
        """Execute a raw SOQL query. Returns {total_size, done, records, next_records_path}."""
        return await self._request("POST", "/api/query/soql", json={
            "client_id": client_id,
            "soql": soql,
        })

    async def query_more(
        self,
        client_id: str,
        next_records_path: str,
    ) -> dict[str, Any]:
        """Fetch the next page of SOQL results."""
        return await self._request("POST", "/api/query/more", json={
            "client_id": client_id,
            "next_records_path": next_records_path,
        })

    async def query_all(
        self,
        client_id: str,
        soql: str,
    ) -> list[dict[str, Any]]:
        """Auto-paginate through all SOQL query results."""
        all_records: list[dict[str, Any]] = []

        result = await self.query_soql(client_id, soql)
        all_records.extend(result.get("records", []))

        while not result.get("done", True) and result.get("next_records_path"):
            result = await self.query_more(client_id, result["next_records_path"])
            all_records.extend(result.get("records", []))

        return all_records

    # --- Structured CRM reads ---

    async def search(
        self,
        client_id: str,
        object_name: str,
        filters: list[dict] | None = None,
        fields: list[str] | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> dict[str, Any]:
        """Search CRM objects with filters. Returns {total_size, done, records}."""
        payload: dict[str, Any] = {
            "client_id": client_id,
            "object_name": object_name,
        }
        if filters:
            payload["filters"] = filters
        if fields:
            payload["fields"] = fields
        if limit is not None:
            payload["limit"] = limit
        if offset is not None:
            payload["offset"] = offset
        return await self._request("POST", "/api/crm/search", json=payload)

    async def count(
        self,
        client_id: str,
        object_name: str,
        filters: list[dict] | None = None,
    ) -> int:
        """Count CRM objects matching filters."""
        payload: dict[str, Any] = {
            "client_id": client_id,
            "object_name": object_name,
        }
        if filters:
            payload["filters"] = filters
        data = await self._request("POST", "/api/crm/count", json=payload)
        return data.get("count", 0)

    # --- Associations ---

    async def get_contact_roles(
        self,
        client_id: str,
        opportunity_ids: list[str],
    ) -> list[dict[str, Any]]:
        """Get OpportunityContactRole records for given opportunity IDs.

        Returns list of {Id, ContactId, OpportunityId, Role, IsPrimary}.
        """
        data = await self._request("POST", "/api/crm/contact-roles", json={
            "client_id": client_id,
            "opportunity_ids": opportunity_ids,
        })
        return data.get("records", [])

    async def get_associations(
        self,
        client_id: str,
        source_object: str,
        source_ids: list[str],
        related_object: str,
        related_fields: list[str],
    ) -> list[dict[str, Any]]:
        """Get associated records via junction object query."""
        data = await self._request("POST", "/api/crm/associations", json={
            "client_id": client_id,
            "source_object": source_object,
            "source_ids": source_ids,
            "related_object": related_object,
            "related_fields": related_fields,
        })
        return data.get("records", [])

    # --- Pipelines ---

    async def get_pipelines(
        self,
        client_id: str,
        object_name: str = "Opportunity",
        field_name: str = "StageName",
    ) -> dict[str, Any]:
        """Get pipeline stage definitions (picklist values).

        Returns {object_name, field_name, stages: [{label, value, ...}]}.
        """
        return await self._request("POST", "/api/crm/pipelines", json={
            "client_id": client_id,
            "object_name": object_name,
            "field_name": field_name,
        })

    # --- Push / Mappings (passthrough to existing sfdc-engine-x endpoints) ---

    async def push_records(
        self,
        client_id: str,
        object_type: str,
        records: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Push records to Salesforce via sfdc-engine-x."""
        data = await self._request("POST", "/api/push/records", json={
            "client_id": client_id,
            "object_type": object_type,
            "records": records,
        })
        return data.get("results", [])

    async def get_mapping(
        self,
        client_id: str,
        object_type: str,
    ) -> dict[str, str]:
        """Get field mapping for a Salesforce object type."""
        data = await self._request("POST", "/api/mappings/get", json={
            "client_id": client_id,
            "object_type": object_type,
        })
        return data.get("mapping", {})

    async def set_mapping(
        self,
        client_id: str,
        object_type: str,
        mapping: dict[str, str],
    ) -> dict[str, Any]:
        """Set field mapping for a Salesforce object type."""
        return await self._request("POST", "/api/mappings/set", json={
            "client_id": client_id,
            "object_type": object_type,
            "mapping": mapping,
        })
