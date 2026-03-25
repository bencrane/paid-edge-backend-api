"""Tests for Meta Ad Set CRUD + targeting builder (BJC-153)."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.integrations.meta_adsets import MetaAdSet, MetaAdSetCreate, MetaAdSetsMixin
from app.integrations.meta_targeting import (
    MetaTargetingBuilder,
    build_schedule,
    enforce_special_ad_category_restrictions,
)


class FakeClient(MetaAdSetsMixin):
    def __init__(self):
        self.ad_account_id = "act_123"
        self._request = AsyncMock()
        self._paginate = AsyncMock()


class TestMetaTargetingBuilder:
    def test_basic_targeting(self):
        builder = MetaTargetingBuilder()
        spec = (
            builder
            .set_locations(countries=["US"])
            .set_demographics(age_min=25, age_max=55)
            .build()
        )
        assert spec["geo_locations"]["countries"] == ["US"]
        assert spec["age_min"] == 25
        assert spec["age_max"] == 55

    def test_interests_and_behaviors(self):
        builder = MetaTargetingBuilder()
        spec = (
            builder
            .add_interests([{"id": "6003139266461", "name": "Marketing"}])
            .add_behaviors([{"id": "6002714895372", "name": "Small business owners"}])
            .build()
        )
        assert len(spec["flexible_spec"]) == 2
        assert "interests" in spec["flexible_spec"][0]
        assert "behaviors" in spec["flexible_spec"][1]

    def test_custom_audiences(self):
        builder = MetaTargetingBuilder()
        spec = (
            builder
            .set_custom_audiences(["aud_1", "aud_2"], excluded_ids=["aud_3"])
            .build()
        )
        assert len(spec["custom_audiences"]) == 2
        assert spec["custom_audiences"][0]["id"] == "aud_1"
        assert spec["excluded_custom_audiences"][0]["id"] == "aud_3"

    def test_placements(self):
        builder = MetaTargetingBuilder()
        spec = (
            builder
            .set_placements(
                platforms=["facebook", "instagram"],
                positions={"facebook_positions": ["feed", "story"]},
            )
            .build()
        )
        assert "facebook" in spec["publisher_platforms"]
        assert "feed" in spec["facebook_positions"]

    def test_targeting_expansion(self):
        builder = MetaTargetingBuilder()
        spec = builder.enable_targeting_expansion().build()
        assert spec["targeting_expansion"]["expansion"] is True

    def test_exclusions(self):
        builder = MetaTargetingBuilder()
        spec = (
            builder
            .set_exclusions(interests=[{"id": "123", "name": "Excluded"}])
            .build()
        )
        assert "interests" in spec["exclusions"]

    def test_locales(self):
        builder = MetaTargetingBuilder()
        spec = builder.set_locales([6]).build()
        assert spec["locales"] == [6]

    def test_advantage_plus_no_placements(self):
        """Omitting placements enables Advantage+ auto-optimization."""
        builder = MetaTargetingBuilder()
        spec = builder.set_locations(countries=["US"]).build()
        assert "publisher_platforms" not in spec


class TestSpecialAdCategoryRestrictions:
    def test_housing_removes_demographics(self):
        targeting = {"age_min": 25, "age_max": 55, "genders": [1]}
        result = enforce_special_ad_category_restrictions(targeting, ["HOUSING"])
        assert "age_min" not in result
        assert "age_max" not in result
        assert "genders" not in result

    def test_no_restrictions_without_categories(self):
        targeting = {"age_min": 25, "age_max": 55}
        result = enforce_special_ad_category_restrictions(targeting, [])
        assert result["age_min"] == 25

    def test_exclusions_removed_for_credit(self):
        targeting = {"exclusions": {"interests": [{"id": "123"}]}}
        result = enforce_special_ad_category_restrictions(targeting, ["CREDIT"])
        assert "exclusions" not in result


class TestBuildSchedule:
    def test_basic_schedule(self):
        schedule = build_schedule("2026-03-25T00:00:00+0000")
        assert schedule["start_time"] == "2026-03-25T00:00:00+0000"
        assert "end_time" not in schedule

    def test_schedule_with_end(self):
        schedule = build_schedule(
            "2026-03-25T00:00:00+0000", end_time="2026-04-25T00:00:00+0000"
        )
        assert schedule["end_time"] == "2026-04-25T00:00:00+0000"

    def test_dayparting(self):
        schedule = build_schedule(
            "2026-03-25T00:00:00+0000",
            dayparting=[{"start_minute": 0, "end_minute": 480, "days": [1, 2]}],
        )
        assert schedule["pacing_type"] == ["day_parting"]
        assert len(schedule["adset_schedule"]) == 1


class TestAdSetCRUD:
    @pytest.mark.asyncio
    async def test_create_ad_set(self):
        client = FakeClient()
        client._request.return_value = {"id": "adset_123"}
        targeting = {"geo_locations": {"countries": ["US"]}}

        result = await client.create_ad_set(
            campaign_id="campaign_456",
            name="Test Ad Set",
            targeting=targeting,
            optimization_goal="LEAD_GENERATION",
            daily_budget=5000,
        )
        assert result["id"] == "adset_123"

    @pytest.mark.asyncio
    async def test_get_ad_set(self):
        client = FakeClient()
        client._request.return_value = {
            "id": "adset_123", "name": "Test", "effective_status": "ACTIVE"
        }
        result = await client.get_ad_set("adset_123")
        assert result["effective_status"] == "ACTIVE"

    @pytest.mark.asyncio
    async def test_update_ad_set(self):
        client = FakeClient()
        client._request.return_value = {"success": True}
        await client.update_ad_set("adset_123", name="Updated")
        assert client._request.call_count == 1

    @pytest.mark.asyncio
    async def test_list_ad_sets_by_campaign(self):
        client = FakeClient()
        client._paginate.return_value = [{"id": "adset_1"}, {"id": "adset_2"}]
        result = await client.list_ad_sets(campaign_id="campaign_123")
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_delete_ad_set(self):
        client = FakeClient()
        client._request.return_value = {"success": True}
        await client.delete_ad_set("adset_123")
        assert client._request.call_args[0][0] == "DELETE"

    @pytest.mark.asyncio
    async def test_models(self):
        m = MetaAdSetCreate(
            campaign_id="c1", name="AS1", targeting={},
            optimization_goal="LEAD_GENERATION",
        )
        assert m.billing_event == "IMPRESSIONS"

        m2 = MetaAdSet(
            id="as1", name="AS1", campaign_id="c1",
            targeting={}, optimization_goal="LEAD_GENERATION",
        )
        assert m2.status == ""
