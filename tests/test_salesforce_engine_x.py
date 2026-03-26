"""Tests for SalesforceEngineClient (BJC-189)."""

from unittest.mock import AsyncMock, patch

import httpx
import pytest

from app.integrations.crm_base import (
    CRMEngineAuthError,
    CRMEngineError,
    CRMEngineRateLimitError,
)
from app.integrations.salesforce_engine_x import SalesforceEngineClient


@pytest.fixture
def client():
    return SalesforceEngineClient(
        base_url="https://sfdc-engine-x.test",
        api_token="test-token",
    )


def _mock_response(status_code=200, json_data=None):
    resp = AsyncMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    resp.text = ""
    return resp


# --- Core request ---


class TestRequestRetry:
    async def test_success(self, client):
        with patch.object(client._client, "request", new_callable=AsyncMock) as mock_req:
            mock_req.return_value = _mock_response(200, {"ok": True})
            result = await client._request("POST", "/api/test", json={"a": 1})

        assert result == {"ok": True}
        mock_req.assert_called_once()

    async def test_retries_on_429(self, client):
        with patch.object(client._client, "request", new_callable=AsyncMock) as mock_req:
            mock_req.side_effect = [
                _mock_response(429),
                _mock_response(200, {"ok": True}),
            ]
            with patch("app.integrations.salesforce_engine_x.asyncio.sleep"):
                result = await client._request("POST", "/api/test")

        assert result == {"ok": True}
        assert mock_req.call_count == 2

    async def test_retries_on_5xx(self, client):
        with patch.object(client._client, "request", new_callable=AsyncMock) as mock_req:
            mock_req.side_effect = [
                _mock_response(503),
                _mock_response(200, {"data": 1}),
            ]
            with patch("app.integrations.salesforce_engine_x.asyncio.sleep"):
                result = await client._request("POST", "/api/test")

        assert result == {"data": 1}

    async def test_retries_on_timeout(self, client):
        with patch.object(client._client, "request", new_callable=AsyncMock) as mock_req:
            mock_req.side_effect = [
                httpx.TimeoutException("timeout"),
                _mock_response(200, {"ok": True}),
            ]
            with patch("app.integrations.salesforce_engine_x.asyncio.sleep"):
                result = await client._request("POST", "/api/test")

        assert result == {"ok": True}

    async def test_raises_auth_error_on_401(self, client):
        with patch.object(client._client, "request", new_callable=AsyncMock) as mock_req:
            mock_req.return_value = _mock_response(401, {"error": "unauthorized"})

            with pytest.raises(CRMEngineAuthError):
                await client._request("POST", "/api/test")

    async def test_raises_on_400(self, client):
        with patch.object(client._client, "request", new_callable=AsyncMock) as mock_req:
            mock_req.return_value = _mock_response(400, {"error": "bad request"})

            with pytest.raises(CRMEngineError) as exc_info:
                await client._request("POST", "/api/test")
            assert exc_info.value.status_code == 400

    async def test_exhausts_retries_on_persistent_429(self, client):
        with patch.object(client._client, "request", new_callable=AsyncMock) as mock_req:
            mock_req.return_value = _mock_response(429)
            with patch("app.integrations.salesforce_engine_x.asyncio.sleep"):
                with pytest.raises(CRMEngineRateLimitError):
                    await client._request("POST", "/api/test")


# --- SOQL queries ---


class TestQuerySOQL:
    async def test_query_soql(self, client):
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_req:
            mock_req.return_value = {
                "total_size": 2,
                "done": True,
                "records": [{"Id": "001"}, {"Id": "002"}],
            }

            result = await client.query_soql("cl-1", "SELECT Id FROM Contact")

        mock_req.assert_called_once_with("POST", "/api/query/soql", json={
            "client_id": "cl-1",
            "soql": "SELECT Id FROM Contact",
        })
        assert result["total_size"] == 2

    async def test_query_more(self, client):
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_req:
            mock_req.return_value = {
                "total_size": 10,
                "done": True,
                "records": [{"Id": "003"}],
            }

            result = await client.query_more("cl-1", "/services/data/v59.0/query/next")

        mock_req.assert_called_once_with("POST", "/api/query/more", json={
            "client_id": "cl-1",
            "next_records_path": "/services/data/v59.0/query/next",
        })
        assert result["done"] is True

    async def test_query_all_auto_paginates(self, client):
        with patch.object(client, "query_soql", new_callable=AsyncMock) as mock_soql, \
             patch.object(client, "query_more", new_callable=AsyncMock) as mock_more:

            mock_soql.return_value = {
                "total_size": 3,
                "done": False,
                "records": [{"Id": "001"}, {"Id": "002"}],
                "next_records_path": "/services/data/v59.0/query/next1",
            }
            mock_more.return_value = {
                "total_size": 3,
                "done": True,
                "records": [{"Id": "003"}],
            }

            records = await client.query_all("cl-1", "SELECT Id FROM Contact")

        assert len(records) == 3
        assert records[2]["Id"] == "003"
        mock_more.assert_called_once_with("cl-1", "/services/data/v59.0/query/next1")


# --- Structured reads ---


class TestSearch:
    async def test_search_with_filters(self, client):
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_req:
            mock_req.return_value = {"total_size": 1, "done": True, "records": []}

            await client.search(
                "cl-1", "Contact",
                filters=[{"field": "Email", "op": "eq", "value": "a@b.com"}],
                fields=["Id", "Email"],
                limit=10,
            )

        call_json = mock_req.call_args[1]["json"]
        assert call_json["object_name"] == "Contact"
        assert call_json["limit"] == 10

    async def test_count(self, client):
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_req:
            mock_req.return_value = {"count": 42}

            result = await client.count("cl-1", "Opportunity")

        assert result == 42


# --- Associations ---


class TestContactRoles:
    async def test_get_contact_roles(self, client):
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_req:
            mock_req.return_value = {
                "total_size": 2,
                "done": True,
                "records": [
                    {"Id": "ocr-1", "ContactId": "c-1", "OpportunityId": "o-1"},
                    {"Id": "ocr-2", "ContactId": "c-2", "OpportunityId": "o-1"},
                ],
            }

            roles = await client.get_contact_roles("cl-1", ["o-1"])

        assert len(roles) == 2
        assert roles[0]["ContactId"] == "c-1"


# --- Pipelines ---


class TestPipelines:
    async def test_get_pipelines(self, client):
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_req:
            mock_req.return_value = {
                "object_name": "Opportunity",
                "field_name": "StageName",
                "stages": [
                    {"label": "Prospecting", "value": "Prospecting"},
                    {"label": "Closed Won", "value": "Closed Won"},
                ],
            }

            result = await client.get_pipelines("cl-1")

        assert result["stages"][0]["label"] == "Prospecting"
        mock_req.assert_called_once_with("POST", "/api/crm/pipelines", json={
            "client_id": "cl-1",
            "object_name": "Opportunity",
            "field_name": "StageName",
        })


# --- Context manager ---


class TestLifecycle:
    async def test_context_manager(self):
        async with SalesforceEngineClient(
            base_url="https://test.local",
            api_token="tok",
        ) as client:
            assert client.base_url == "https://test.local"
