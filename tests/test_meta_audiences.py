"""Tests for Meta Custom Audiences + Lookalike Audiences (BJC-160)."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.integrations.meta_audiences import (
    MetaAudiencesMixin,
    MetaAudienceUploadResult,
    MetaCustomAudience,
    build_ldu_payload,
    hash_for_meta,
    prepare_audience_data,
)


class FakeClient(MetaAudiencesMixin):
    def __init__(self):
        self.ad_account_id = "act_123"
        self._request = AsyncMock()
        self._paginate = AsyncMock()


class TestHashForMeta:
    def test_email_normalization(self):
        h1 = hash_for_meta("  Test@Example.Com  ", "EMAIL")
        h2 = hash_for_meta("test@example.com", "EMAIL")
        assert h1 == h2
        assert len(h1) == 64  # SHA-256

    def test_phone_normalization(self):
        h1 = hash_for_meta("+1-555-123-4567", "PHONE")
        h2 = hash_for_meta("15551234567", "PHONE")
        assert h1 == h2

    def test_name_normalization(self):
        h1 = hash_for_meta("  John  ", "FN")
        h2 = hash_for_meta("john", "FN")
        assert h1 == h2

    def test_gender_normalization(self):
        assert hash_for_meta("male", "GEN") == hash_for_meta("m", "GEN")
        assert hash_for_meta("female", "GEN") == hash_for_meta("f", "GEN")

    def test_extern_id_not_hashed(self):
        result = hash_for_meta("ext_123", "EXTERN_ID")
        assert result == "ext_123"

    def test_madid_not_hashed(self):
        result = hash_for_meta("device_id_456", "MADID")
        assert result == "device_id_456"

    def test_city_normalization(self):
        h = hash_for_meta("New York", "CT")
        expected = hash_for_meta("newyork", "CT")
        assert h == expected

    def test_state_normalization(self):
        h = hash_for_meta("CA", "ST")
        expected = hash_for_meta("ca", "ST")
        assert h == expected

    def test_zip_normalization(self):
        h = hash_for_meta("90210", "ZIP")
        assert len(h) == 64

    def test_country_normalization(self):
        h = hash_for_meta("US", "COUNTRY")
        expected = hash_for_meta("us", "COUNTRY")
        assert h == expected

    def test_empty_value(self):
        result = hash_for_meta("", "EMAIL")
        assert result == ""

    def test_deterministic(self):
        h1 = hash_for_meta("test@example.com", "EMAIL")
        h2 = hash_for_meta("test@example.com", "EMAIL")
        assert h1 == h2


class TestPrepareAudienceData:
    def test_prepare_data(self):
        members = [
            {"email": "test@example.com", "first_name": "John", "last_name": "Doe", "entity_id": "e1"},
        ]
        schema = ["EMAIL", "FN", "LN", "EXTERN_ID"]
        rows = prepare_audience_data(members, schema)
        assert len(rows) == 1
        assert len(rows[0]) == 4
        assert rows[0][3] == "e1"  # EXTERN_ID not hashed

    def test_prepare_data_missing_fields(self):
        members = [{"email": "test@example.com"}]
        schema = ["EMAIL", "FN"]
        rows = prepare_audience_data(members, schema)
        assert len(rows) == 1


class TestLDUPayload:
    def test_build_ldu(self):
        payload = build_ldu_payload([["hash1"]], ["EMAIL"])
        assert payload["data_processing_options"] == ["LDU"]
        assert payload["data_processing_options_country"] == 1
        assert payload["data_processing_options_state"] == 1000


class TestAudienceCRUD:
    @pytest.mark.asyncio
    async def test_create_custom_audience(self):
        client = FakeClient()
        client._request.return_value = {"id": "aud_123", "name": "Test Audience"}
        result = await client.create_custom_audience(name="Test Audience")
        assert result["id"] == "aud_123"

    @pytest.mark.asyncio
    async def test_upload_users(self):
        client = FakeClient()
        client._request.return_value = {
            "audience_id": "aud_123",
            "num_received": 100,
            "num_invalid_entries": 2,
        }
        result = await client.upload_users(
            "aud_123", ["EMAIL"], [["hash1"], ["hash2"]]
        )
        assert result["num_received"] == 100

    @pytest.mark.asyncio
    async def test_remove_users(self):
        client = FakeClient()
        client._request.return_value = {"audience_id": "aud_123"}
        await client.remove_users("aud_123", ["EMAIL"], [["hash1"]])
        assert client._request.call_count == 1

    @pytest.mark.asyncio
    async def test_get_audience(self):
        client = FakeClient()
        client._request.return_value = {
            "id": "aud_123", "name": "Test", "approximate_count": 500
        }
        result = await client.get_audience("aud_123")
        assert result["approximate_count"] == 500

    @pytest.mark.asyncio
    async def test_delete_audience(self):
        client = FakeClient()
        client._request.return_value = {"success": True}
        await client.delete_audience("aud_123")
        assert client._request.call_args[0][0] == "DELETE"

    @pytest.mark.asyncio
    async def test_list_audiences(self):
        client = FakeClient()
        client._paginate.return_value = [{"id": "a1"}, {"id": "a2"}]
        result = await client.list_audiences()
        assert len(result) == 2


class TestLookalikeAudiences:
    @pytest.mark.asyncio
    async def test_create_lookalike(self):
        client = FakeClient()
        client._request.return_value = {"id": "lal_123"}
        result = await client.create_lookalike_audience(
            seed_audience_id="aud_123",
            name="Lookalike 1%",
            ratio=0.01,
        )
        assert result["id"] == "lal_123"
        call_data = client._request.call_args[1]["data"]
        assert call_data["subtype"] == "LOOKALIKE"

    @pytest.mark.asyncio
    async def test_create_multi_country_lookalike(self):
        client = FakeClient()
        client._request.return_value = {"id": "lal_multi"}
        result = await client.create_multi_country_lookalike(
            seed_audience_id="aud_123",
            name="Multi-Country LAL",
            countries=["US", "CA", "GB"],
        )
        assert result["id"] == "lal_multi"

    @pytest.mark.asyncio
    async def test_create_conversion_lookalike(self):
        client = FakeClient()
        client._request.return_value = {"id": "lal_conv"}
        result = await client.create_conversion_lookalike(
            campaign_id="campaign_456",
            name="Conversion LAL",
            ratio=0.05,
        )
        assert result["id"] == "lal_conv"


class TestModels:
    def test_custom_audience_model(self):
        m = MetaCustomAudience(id="123", name="Test")
        assert m.subtype == "CUSTOM"

    def test_upload_result_model(self):
        m = MetaAudienceUploadResult(
            audience_id="123", num_received=100, num_invalid_entries=2
        )
        assert m.num_received == 100
