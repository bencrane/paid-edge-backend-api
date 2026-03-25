"""Tests for data-engine-x API client (BJC-127)."""

from unittest.mock import AsyncMock, patch

import httpx
import pytest

from app.integrations.data_engine_x import (
    CompanyEntity,
    DataEngineXAuthError,
    DataEngineXClient,
    DataEngineXError,
    DataEngineXRateLimitError,
    PersonEntity,
    Signal,
)


@pytest.fixture
def client():
    """Create a DataEngineXClient with test config."""
    return DataEngineXClient(
        base_url="https://data-engine-x.test",
        api_token="test-token-123",
    )


# --- Auth headers ---


class TestHeaders:
    def test_bearer_token_in_headers(self, client):
        """Should include API token as Bearer auth."""
        headers = client._headers()
        assert headers["Authorization"] == "Bearer test-token-123"
        assert headers["Content-Type"] == "application/json"


# --- enrich_companies ---


class TestEnrichCompanies:
    @pytest.mark.asyncio
    async def test_returns_company_entities(self, client):
        """Should call companies/list for each domain and return parsed entities."""
        mock_response = httpx.Response(
            200,
            json={
                "data": {
                    "items": [
                        {
                            "entity_id": "eid-1",
                            "canonical_domain": "acme.com",
                            "canonical_name": "Acme Corp",
                            "industry": "Software",
                            "employee_range": "51-200",
                            "canonical_payload": {"canonical_domain": "acme.com"},
                            "record_version": 4,
                            "source_providers": ["clay", "clearbit"],
                        }
                    ],
                    "pagination": {"page": 1, "per_page": 100, "returned": 1},
                }
            },
        )
        client._client = AsyncMock()
        client._client.request = AsyncMock(return_value=mock_response)

        result = await client.enrich_companies(["acme.com"])

        assert len(result) == 1
        assert isinstance(result[0], CompanyEntity)
        assert result[0].entity_id == "eid-1"
        assert result[0].canonical_domain == "acme.com"
        assert result[0].industry == "Software"

    @pytest.mark.asyncio
    async def test_multiple_domains(self, client):
        """Should make one request per domain."""
        mock_response = httpx.Response(
            200,
            json={"data": {"items": [{"entity_id": "eid-1"}], "pagination": {"page": 1, "per_page": 100, "returned": 1}}},
        )
        client._client = AsyncMock()
        client._client.request = AsyncMock(return_value=mock_response)

        result = await client.enrich_companies(["a.com", "b.com", "c.com"])

        assert len(result) == 3
        assert client._client.request.call_count == 3

    @pytest.mark.asyncio
    async def test_empty_results(self, client):
        """Should return empty list when no matches."""
        mock_response = httpx.Response(
            200,
            json={"data": {"items": [], "pagination": {"page": 1, "per_page": 100, "returned": 0}}},
        )
        client._client = AsyncMock()
        client._client.request = AsyncMock(return_value=mock_response)

        result = await client.enrich_companies(["unknown.com"])

        assert result == []


# --- enrich_persons ---


class TestEnrichPersons:
    @pytest.mark.asyncio
    async def test_returns_person_entities(self, client):
        """Should call persons/list and return parsed entities."""
        mock_response = httpx.Response(
            200,
            json={
                "data": {
                    "items": [
                        {
                            "entity_id": "pid-1",
                            "full_name": "Jane Doe",
                            "title": "VP Engineering",
                            "seniority": "VP",
                            "work_email": "jane@acme.com",
                        }
                    ],
                    "pagination": {"page": 1, "per_page": 100, "returned": 1},
                }
            },
        )
        client._client = AsyncMock()
        client._client.request = AsyncMock(return_value=mock_response)

        result = await client.enrich_persons({"seniority": "VP"})

        assert len(result) == 1
        assert isinstance(result[0], PersonEntity)
        assert result[0].full_name == "Jane Doe"
        assert result[0].seniority == "VP"

    @pytest.mark.asyncio
    async def test_no_filters(self, client):
        """Should work without filters."""
        mock_response = httpx.Response(
            200,
            json={"data": {"items": [], "pagination": {"page": 1, "per_page": 100, "returned": 0}}},
        )
        client._client = AsyncMock()
        client._client.request = AsyncMock(return_value=mock_response)

        result = await client.enrich_persons()

        assert result == []


# --- get_signals ---


class TestGetSignals:
    @pytest.mark.asyncio
    async def test_returns_signals(self, client):
        """Should call entity-timeline and parse into Signal objects."""
        mock_response = httpx.Response(
            200,
            json={
                "data": {
                    "items": [
                        {
                            "entity_type": "company",
                            "entity_id": "eid-1",
                            "operation_id": "company.enrich.profile",
                            "created_at": "2026-03-25T09:15:00Z",
                        },
                        {
                            "entity_type": "person",
                            "entity_id": "pid-1",
                            "operation_id": "person.derive.detect_changes",
                            "created_at": "2026-03-25T10:00:00Z",
                        },
                    ],
                    "pagination": {"page": 1, "per_page": 100, "returned": 2},
                }
            },
        )
        client._client = AsyncMock()
        client._client.request = AsyncMock(return_value=mock_response)

        result = await client.get_signals()

        assert len(result) == 2
        assert isinstance(result[0], Signal)
        assert result[0].signal_type == "company.enrich.profile"
        assert result[0].entity_id == "eid-1"

    @pytest.mark.asyncio
    async def test_filter_by_signal_type(self, client):
        """Should pass signal_type as event_type filter."""
        mock_response = httpx.Response(
            200,
            json={"data": {"items": [], "pagination": {"page": 1, "per_page": 100, "returned": 0}}},
        )
        client._client = AsyncMock()
        client._client.request = AsyncMock(return_value=mock_response)

        await client.get_signals(signal_type="new_in_role")

        call_args = client._client.request.call_args
        payload = call_args.kwargs.get("json") or call_args[1].get("json")
        assert payload["event_type"] == "new_in_role"


# --- search_entities ---


class TestSearchEntities:
    @pytest.mark.asyncio
    async def test_returns_list_response(self, client):
        """Should return EntityListResponse with items and pagination."""
        mock_response = httpx.Response(
            200,
            json={
                "data": {
                    "items": [
                        {"entity_id": "eid-1", "canonical_domain": "acme.com"},
                        {"entity_id": "eid-2", "canonical_domain": "beta.io"},
                    ],
                    "pagination": {"page": 1, "per_page": 50, "returned": 2},
                }
            },
        )
        client._client = AsyncMock()
        client._client.request = AsyncMock(return_value=mock_response)

        result = await client.search_entities("company", {"industry": "Software"})

        assert len(result.items) == 2
        assert result.pagination.returned == 2
        assert result.pagination.page == 1

    @pytest.mark.asyncio
    async def test_person_entity_type(self, client):
        """Should use correct path for person entity type."""
        mock_response = httpx.Response(
            200,
            json={"data": {"items": [], "pagination": {"page": 1, "per_page": 100, "returned": 0}}},
        )
        client._client = AsyncMock()
        client._client.request = AsyncMock(return_value=mock_response)

        await client.search_entities("person", {"seniority": "VP"})

        call_args = client._client.request.call_args
        url = call_args[0][1] if len(call_args[0]) > 1 else call_args.kwargs.get("url", "")
        assert "/persons/list" in url

    @pytest.mark.asyncio
    async def test_job_entity_type(self, client):
        """Should use correct path for job entity type."""
        mock_response = httpx.Response(
            200,
            json={"data": {"items": [], "pagination": {"page": 1, "per_page": 100, "returned": 0}}},
        )
        client._client = AsyncMock()
        client._client.request = AsyncMock(return_value=mock_response)

        await client.search_entities("job")

        call_args = client._client.request.call_args
        url = call_args[0][1] if len(call_args[0]) > 1 else call_args.kwargs.get("url", "")
        assert "/job-postings/list" in url


# --- Error handling ---


class TestErrorHandling:
    @pytest.mark.asyncio
    async def test_401_raises_auth_error(self, client):
        """Should raise DataEngineXAuthError on 401."""
        mock_response = httpx.Response(
            401,
            json={"error": "Invalid or expired authentication token"},
        )
        client._client = AsyncMock()
        client._client.request = AsyncMock(return_value=mock_response)

        with pytest.raises(DataEngineXAuthError):
            await client.enrich_companies(["acme.com"])

    @pytest.mark.asyncio
    async def test_403_raises_auth_error(self, client):
        """Should raise DataEngineXAuthError on 403."""
        mock_response = httpx.Response(
            403,
            json={"error": "Insufficient permissions"},
        )
        client._client = AsyncMock()
        client._client.request = AsyncMock(return_value=mock_response)

        with pytest.raises(DataEngineXAuthError):
            await client.enrich_companies(["acme.com"])

    @pytest.mark.asyncio
    async def test_400_raises_generic_error(self, client):
        """Should raise DataEngineXError on 400."""
        mock_response = httpx.Response(
            400,
            json={"error": "Bad request"},
        )
        client._client = AsyncMock()
        client._client.request = AsyncMock(return_value=mock_response)

        with pytest.raises(DataEngineXError) as exc_info:
            await client.enrich_companies(["acme.com"])
        assert exc_info.value.status_code == 400

    @pytest.mark.asyncio
    async def test_429_retries_then_raises(self, client):
        """Should retry on 429 then raise after max retries."""
        client.MAX_RETRIES = 1
        mock_response = httpx.Response(429, json={"error": "Rate limited"})
        client._client = AsyncMock()
        client._client.request = AsyncMock(return_value=mock_response)

        with (
            patch("asyncio.sleep", new_callable=AsyncMock),
            pytest.raises(DataEngineXRateLimitError),
        ):
            await client.enrich_companies(["acme.com"])

    @pytest.mark.asyncio
    async def test_500_retries_then_raises(self, client):
        """Should retry on 500 then raise after max retries."""
        client.MAX_RETRIES = 1
        mock_response = httpx.Response(500, json={"error": "Internal error"})
        client._client = AsyncMock()
        client._client.request = AsyncMock(return_value=mock_response)

        with (
            patch("asyncio.sleep", new_callable=AsyncMock),
            pytest.raises(DataEngineXError),
        ):
            await client.enrich_companies(["acme.com"])

    @pytest.mark.asyncio
    async def test_timeout_retries(self, client):
        """Should retry on timeout then raise after max retries."""
        client.MAX_RETRIES = 1
        client._client = AsyncMock()
        client._client.request = AsyncMock(side_effect=httpx.TimeoutException("timed out"))

        with (
            patch("asyncio.sleep", new_callable=AsyncMock),
            pytest.raises(DataEngineXError, match="timed out"),
        ):
            await client.enrich_companies(["acme.com"])

    @pytest.mark.asyncio
    async def test_retry_success_after_failure(self, client):
        """Should succeed if a retry works."""
        success_response = httpx.Response(
            200,
            json={"data": {"items": [{"entity_id": "eid-1"}], "pagination": {"page": 1, "per_page": 100, "returned": 1}}},
        )
        fail_response = httpx.Response(500, json={"error": "temporary"})
        client._client = AsyncMock()
        client._client.request = AsyncMock(side_effect=[fail_response, success_response])

        with patch("asyncio.sleep", new_callable=AsyncMock):
            result = await client.enrich_companies(["acme.com"])

        assert len(result) == 1
        assert client._client.request.call_count == 2


# --- get_entity ---


class TestGetEntity:
    @pytest.mark.asyncio
    async def test_returns_entity_from_list(self, client):
        """Should return entity when found in list response."""
        mock_response = httpx.Response(
            200,
            json={
                "data": {
                    "items": [{"entity_id": "eid-1", "canonical_domain": "acme.com"}],
                    "pagination": {"page": 1, "per_page": 1, "returned": 1},
                }
            },
        )
        client._client = AsyncMock()
        client._client.request = AsyncMock(return_value=mock_response)

        result = await client.get_entity("company", "eid-1")

        assert result["entity_id"] == "eid-1"

    @pytest.mark.asyncio
    async def test_falls_back_to_snapshots(self, client):
        """Should try snapshots if entity not in initial list."""
        list_response = httpx.Response(
            200,
            json={"data": {"items": [{"entity_id": "other"}], "pagination": {"page": 1, "per_page": 1, "returned": 1}}},
        )
        snapshot_response = httpx.Response(
            200,
            json={
                "data": {
                    "items": [{"entity_id": "eid-1", "canonical_payload": {"name": "Acme"}}],
                    "pagination": {"page": 1, "per_page": 10, "returned": 1},
                }
            },
        )
        client._client = AsyncMock()
        client._client.request = AsyncMock(side_effect=[list_response, snapshot_response])

        result = await client.get_entity("company", "eid-1")

        assert result["entity_id"] == "eid-1"
        assert client._client.request.call_count == 2
