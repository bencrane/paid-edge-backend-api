"""Google Ads base API client factory + service wrapper (BJC-141).

Uses the google-ads Python SDK (v25.x, API v18) for all API interactions.
"""

import asyncio
import logging
from functools import partial

from supabase import Client

from app.config import settings
from app.integrations.google_ads_auth import get_google_ads_credentials

logger = logging.getLogger(__name__)


# --- Custom exceptions ---


class GoogleAdsAPIError(Exception):
    """Base error for Google Ads API failures."""

    def __init__(self, message: str, status_code: int = 0, request_id: str | None = None):
        self.message = message
        self.status_code = status_code
        self.request_id = request_id
        super().__init__(message)


class GoogleAdsQuotaError(GoogleAdsAPIError):
    """Raised when API quota/rate limit is exceeded."""
    pass


class GoogleAdsPermissionError(GoogleAdsAPIError):
    """Raised when insufficient permissions."""
    pass


class GoogleAdsNotFoundError(GoogleAdsAPIError):
    """Raised when a resource is not found."""
    pass


class GoogleAdsAuthError(GoogleAdsAPIError):
    """Raised when authentication fails (triggers reauth flow)."""
    pass


# --- Helpers ---


def customer_id_to_str(customer_id: int | str) -> str:
    """Ensure customer ID is a string with no hyphens."""
    return str(customer_id).replace("-", "")


def micros_to_dollars(micros: int) -> float:
    """Convert Google Ads micros to dollars. 5000000 -> $5.00"""
    return micros / 1_000_000


def dollars_to_micros(dollars: float) -> int:
    """Convert dollars to Google Ads micros. $5.00 -> 5000000"""
    return int(dollars * 1_000_000)


# --- Client factory ---


class GoogleAdsClientFactory:
    """Creates per-tenant GoogleAdsClient instances."""

    def __init__(self):
        self.developer_token = settings.GOOGLE_ADS_DEVELOPER_TOKEN
        self.client_id = settings.GOOGLE_ADS_CLIENT_ID
        self.client_secret = settings.GOOGLE_ADS_CLIENT_SECRET
        self.mcc_id = settings.GOOGLE_ADS_MCC_ID

    async def get_client(self, org_id: str, supabase: Client):
        """Get a configured GoogleAdsClient for a specific tenant."""
        creds = await get_google_ads_credentials(org_id, supabase)

        # Import here to avoid hard dependency at module level for testing
        from google.ads.googleads.client import GoogleAdsClient

        config_dict = {
            "developer_token": self.developer_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": creds["refresh_token"],
            "login_customer_id": self.mcc_id,
            "use_proto_plus": True,
        }
        return GoogleAdsClient.load_from_dict(config_dict)

    async def get_customer_id(self, org_id: str, supabase: Client) -> str:
        """Get the tenant's selected Google Ads customer ID from provider_configs."""
        creds = await get_google_ads_credentials(org_id, supabase)
        customer_id = creds.get("customer_id")
        if not customer_id:
            raise ValueError(f"No Google Ads account selected for org {org_id}")
        return customer_id


# --- Service wrapper ---


class GoogleAdsService:
    """High-level wrapper around GoogleAdsClient with retry + error handling."""

    MAX_RETRIES = 5
    INITIAL_BACKOFF = 1  # seconds

    def __init__(self, client, customer_id: str):
        self.client = client
        self.customer_id = customer_id

    def _get_service(self, service_name: str):
        """Get a service from the client."""
        return self.client.get_service(service_name)

    def _get_type(self, type_name: str):
        """Get a protobuf type from the client."""
        return self.client.get_type(type_name)

    @property
    def enums(self):
        """Access Google Ads enums."""
        return self.client.enums

    async def search_stream(self, query: str) -> list:
        """Execute GAQL query via search_stream with retry logic."""
        ga_service = self._get_service("GoogleAdsService")

        for attempt in range(self.MAX_RETRIES + 1):
            try:
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    partial(
                        ga_service.search_stream,
                        customer_id=self.customer_id,
                        query=query,
                    ),
                )
                results = []
                for batch in response:
                    for row in batch.results:
                        results.append(row)
                return results

            except Exception as e:
                if self._is_quota_error(e) and attempt < self.MAX_RETRIES:
                    backoff = self.INITIAL_BACKOFF * (2 ** attempt)
                    logger.warning(
                        "Google Ads quota error (attempt %d/%d), retrying in %ds: %s",
                        attempt + 1,
                        self.MAX_RETRIES,
                        backoff,
                        str(e),
                    )
                    await asyncio.sleep(backoff)
                    continue
                raise self._map_exception(e) from e

    async def mutate(self, service_name: str, operations: list, partial_failure: bool = True):
        """Execute mutate operations with retry + partial failure handling."""
        service = self._get_service(service_name)
        mutate_method_name = self._get_mutate_method(service_name)
        mutate_fn = getattr(service, mutate_method_name)

        for attempt in range(self.MAX_RETRIES + 1):
            try:
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    partial(
                        mutate_fn,
                        customer_id=self.customer_id,
                        operations=operations,
                        partial_failure=partial_failure,
                    ),
                )
                if partial_failure and hasattr(response, "partial_failure_error") and response.partial_failure_error:
                    logger.warning(
                        "Google Ads partial failure in %s: %s",
                        service_name,
                        response.partial_failure_error,
                    )
                return response

            except Exception as e:
                if self._is_quota_error(e) and attempt < self.MAX_RETRIES:
                    backoff = self.INITIAL_BACKOFF * (2 ** attempt)
                    logger.warning(
                        "Google Ads quota error on mutate (attempt %d/%d), retrying in %ds",
                        attempt + 1,
                        self.MAX_RETRIES,
                        backoff,
                    )
                    await asyncio.sleep(backoff)
                    continue
                raise self._map_exception(e) from e

    async def list_accessible_customers(self) -> list[str]:
        """List all Google Ads accounts accessible to the authenticated user."""
        customer_service = self._get_service("CustomerService")
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None, customer_service.list_accessible_customers
        )
        return [rn.split("/")[-1] for rn in response.resource_names]

    async def get_account_details(self, customer_id: str | None = None) -> dict:
        """Get detailed info about a specific account."""
        cid = customer_id or self.customer_id
        query = """
            SELECT customer.id, customer.descriptive_name, customer.currency_code,
                   customer.time_zone, customer.manager, customer.test_account, customer.status
            FROM customer LIMIT 1
        """
        ga_service = self._get_service("GoogleAdsService")
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            partial(ga_service.search_stream, customer_id=cid, query=query),
        )
        for batch in response:
            for row in batch.results:
                return {
                    "id": str(row.customer.id),
                    "name": row.customer.descriptive_name,
                    "currency": row.customer.currency_code,
                    "timezone": row.customer.time_zone,
                    "is_manager": row.customer.manager,
                    "is_test": row.customer.test_account,
                    "status": row.customer.status.name if hasattr(row.customer.status, "name") else str(row.customer.status),
                }
        return {}

    async def list_clients_under_mcc(self) -> list[dict]:
        """List all client accounts under the MCC."""
        query = """
            SELECT customer_client.id, customer_client.descriptive_name,
                   customer_client.level, customer_client.manager,
                   customer_client.status, customer_client.currency_code
            FROM customer_client
            WHERE customer_client.status = 'ENABLED'
        """
        rows = await self.search_stream(query)
        clients = []
        for row in rows:
            if not row.customer_client.manager:
                clients.append({
                    "id": str(row.customer_client.id),
                    "name": row.customer_client.descriptive_name,
                    "level": row.customer_client.level,
                    "currency": row.customer_client.currency_code,
                })
        return clients

    async def get_account_hierarchy(self) -> dict:
        """Get the full account hierarchy for display in settings."""
        clients = await self.list_clients_under_mcc()
        return {"mcc_id": self.customer_id, "client_accounts": clients}

    @staticmethod
    def _get_mutate_method(service_name: str) -> str:
        """Derive the mutate method name from the service name."""
        # CampaignBudgetService -> mutate_campaign_budgets
        name = service_name.replace("Service", "")
        # Convert CamelCase to snake_case
        import re
        snake = re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()
        return f"mutate_{snake}s"

    @staticmethod
    def _is_quota_error(exception: Exception) -> bool:
        """Check if exception is a quota/rate limit error."""
        error_str = str(exception).lower()
        return "quota" in error_str or "rate" in error_str or "resource_exhausted" in error_str

    @staticmethod
    def _map_exception(exception: Exception) -> GoogleAdsAPIError:
        """Map SDK exceptions to our custom error hierarchy."""
        error_str = str(exception).lower()
        request_id = getattr(exception, "request_id", None)

        if "quota" in error_str or "rate" in error_str or "resource_exhausted" in error_str:
            return GoogleAdsQuotaError(str(exception), request_id=request_id)
        if "permission" in error_str or "authorization" in error_str:
            return GoogleAdsPermissionError(str(exception), request_id=request_id)
        if "not_found" in error_str:
            return GoogleAdsNotFoundError(str(exception), request_id=request_id)
        if "authentication" in error_str or "unauthenticated" in error_str:
            return GoogleAdsAuthError(str(exception), request_id=request_id)

        return GoogleAdsAPIError(str(exception), request_id=request_id)
