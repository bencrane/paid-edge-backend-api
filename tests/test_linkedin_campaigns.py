"""Tests for LinkedIn campaign CRUD + targeting criteria builder (BJC-131)."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.integrations.linkedin import (
    LinkedInAdsClient,
    LinkedInAPIError,
    make_account_urn,
    make_campaign_group_urn,
)
from app.integrations.linkedin_models import LinkedInCampaign
from app.integrations.linkedin_targeting import (
    DEFAULT_LOCALE,
    FACET_EMPLOYERS,
    FACET_INDUSTRIES,
    FACET_INTERFACE_LOCALES,
    FACET_LOCATIONS,
    InvalidCampaignConfigError,
    build_targeting_criteria,
    validate_campaign_config,
)

# --- Targeting criteria builder ---


class TestBuildTargetingCriteria:
    def test_minimal_targeting_includes_locales(self):
        """Even with no facets, interfaceLocales must be present."""
        result = build_targeting_criteria()
        assert "include" in result
        and_clauses = result["include"]["and"]
        assert len(and_clauses) == 1
        assert FACET_INTERFACE_LOCALES in and_clauses[0]["or"]
        assert and_clauses[0]["or"][FACET_INTERFACE_LOCALES] == [
            DEFAULT_LOCALE
        ]

    def test_custom_locales(self):
        """Custom locales should replace the default."""
        result = build_targeting_criteria(
            locales=["urn:li:locale:de_DE", "urn:li:locale:fr_FR"]
        )
        locales_clause = result["include"]["and"][0]
        assert locales_clause["or"][FACET_INTERFACE_LOCALES] == [
            "urn:li:locale:de_DE",
            "urn:li:locale:fr_FR",
        ]

    def test_locations_and_industries(self):
        """Multiple facets should each become an OR clause in AND."""
        result = build_targeting_criteria(
            locations=["urn:li:geo:103644278"],
            industries=["urn:li:industry:4", "urn:li:industry:6"],
        )
        and_clauses = result["include"]["and"]
        # locale + locations + industries = 3 clauses
        assert len(and_clauses) == 3

        facets = [list(c["or"].keys())[0] for c in and_clauses]
        assert FACET_INTERFACE_LOCALES in facets
        assert FACET_LOCATIONS in facets
        assert FACET_INDUSTRIES in facets

    def test_all_facets(self):
        """All supported facets should produce correct AND/OR structure."""
        result = build_targeting_criteria(
            locations=["urn:li:geo:103644278"],
            locales=["urn:li:locale:en_US"],
            industries=["urn:li:industry:4"],
            seniorities=["urn:li:seniority:8"],
            job_functions=["urn:li:function:12"],
            company_sizes=["urn:li:staffCountRange:(51,200)"],
            matched_audiences=["urn:li:adSegment:12345"],
        )
        and_clauses = result["include"]["and"]
        # All 7 facets
        assert len(and_clauses) == 7
        assert "exclude" not in result

    def test_exclusions_in_separate_block(self):
        """Excluded companies should go in the exclude block."""
        result = build_targeting_criteria(
            locations=["urn:li:geo:103644278"],
            exclude_companies=["urn:li:organization:1337"],
        )
        assert "exclude" in result
        assert FACET_EMPLOYERS in result["exclude"]["or"]
        assert result["exclude"]["or"][FACET_EMPLOYERS] == [
            "urn:li:organization:1337"
        ]

    def test_no_exclusion_when_empty(self):
        """No exclude block when no companies excluded."""
        result = build_targeting_criteria(
            locations=["urn:li:geo:103644278"]
        )
        assert "exclude" not in result

    def test_and_or_structure_is_correct(self):
        """Verify the exact structure matches LinkedIn API format."""
        result = build_targeting_criteria(
            locations=["urn:li:geo:103644278"],
            seniorities=[
                "urn:li:seniority:8",
                "urn:li:seniority:9",
            ],
        )
        # Each AND clause has an "or" key
        for clause in result["include"]["and"]:
            assert "or" in clause
            # Each or value is a list
            for facet_values in clause["or"].values():
                assert isinstance(facet_values, list)


# --- Campaign objective validation ---


class TestValidateCampaignConfig:
    def test_valid_brand_awareness(self):
        """BRAND_AWARENESS + SPONSORED_UPDATES + CPM should be valid."""
        validate_campaign_config(
            "BRAND_AWARENESS", "SPONSORED_UPDATES", "CPM"
        )

    def test_valid_website_visits_cpc(self):
        """WEBSITE_VISITS + TEXT_AD + CPC should be valid."""
        validate_campaign_config("WEBSITE_VISITS", "TEXT_AD", "CPC")

    def test_valid_video_views(self):
        """VIDEO_VIEWS + SPONSORED_UPDATES + CPV should be valid."""
        validate_campaign_config("VIDEO_VIEWS", "SPONSORED_UPDATES", "CPV")

    def test_valid_lead_gen_no_offsite(self):
        """LEAD_GENERATION with offsite=False should be valid."""
        validate_campaign_config(
            "LEAD_GENERATION",
            "SPONSORED_UPDATES",
            "CPC",
            offsite_delivery=False,
        )

    def test_invalid_objective(self):
        """Unknown objective should raise."""
        with pytest.raises(
            InvalidCampaignConfigError, match="Unknown objective"
        ):
            validate_campaign_config(
                "FAKE_OBJECTIVE", "SPONSORED_UPDATES", "CPC"
            )

    def test_invalid_type_for_objective(self):
        """VIDEO_VIEWS only supports SPONSORED_UPDATES."""
        with pytest.raises(
            InvalidCampaignConfigError, match="not valid for objective"
        ):
            validate_campaign_config("VIDEO_VIEWS", "TEXT_AD", "CPV")

    def test_invalid_cost_type(self):
        """BRAND_AWARENESS only supports CPM."""
        with pytest.raises(
            InvalidCampaignConfigError, match="Cost type"
        ):
            validate_campaign_config(
                "BRAND_AWARENESS", "SPONSORED_UPDATES", "CPC"
            )

    def test_lead_gen_blocks_offsite(self):
        """LEAD_GENERATION must have offsite disabled."""
        with pytest.raises(
            InvalidCampaignConfigError, match="Offsite delivery"
        ):
            validate_campaign_config(
                "LEAD_GENERATION",
                "SPONSORED_UPDATES",
                "CPC",
                offsite_delivery=True,
            )


# --- Campaign CRUD on LinkedInAdsClient ---


class TestCampaignCreate:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_create_campaign_with_all_fields(self, client):
        """Should POST campaign with correct body structure."""
        api_resp = {"id": "urn:li:sponsoredCampaign:12345"}

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=_mock_resp(200, api_resp),
            ) as mock_req,
        ):
            result = await client.create_campaign(
                account_id=518121035,
                campaign_group_id=635137195,
                name="Q2 Campaign",
                campaign_type="SPONSORED_UPDATES",
                objective="WEBSITE_VISITS",
                targeting={"include": {"and": []}},
                daily_budget={"amount": "50", "currencyCode": "USD"},
                cost_type="CPC",
                unit_cost={"amount": "8", "currencyCode": "USD"},
                run_schedule={"start": 1700000000000},
                offsite_delivery=False,
                status="DRAFT",
            )

        assert result["id"] == "urn:li:sponsoredCampaign:12345"
        body = mock_req.call_args.kwargs["json"]
        assert body["account"] == make_account_urn(518121035)
        assert body["campaignGroup"] == make_campaign_group_urn(635137195)
        assert body["type"] == "SPONSORED_UPDATES"
        assert body["objectiveType"] == "WEBSITE_VISITS"
        assert body["costType"] == "CPC"
        assert body["offsiteDeliveryEnabled"] is False
        assert body["dailyBudget"]["amount"] == "50"
        assert body["unitCost"]["amount"] == "8"
        assert body["runSchedule"]["start"] == 1700000000000
        assert body["targetingCriteria"] == {"include": {"and": []}}
        assert body["status"] == "DRAFT"

    @pytest.mark.asyncio
    async def test_create_validates_config(self, client):
        """Should reject invalid objective/type/cost combos."""
        with pytest.raises(InvalidCampaignConfigError):
            await client.create_campaign(
                account_id=1,
                campaign_group_id=1,
                name="Bad",
                campaign_type="TEXT_AD",
                objective="VIDEO_VIEWS",  # TEXT_AD not valid
                targeting={},
                daily_budget={"amount": "50", "currencyCode": "USD"},
                cost_type="CPV",
            )

    @pytest.mark.asyncio
    async def test_create_without_optional_fields(self, client):
        """Optional fields omitted should not appear in body."""
        api_resp = {"id": "urn:li:sponsoredCampaign:99"}

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=_mock_resp(200, api_resp),
            ) as mock_req,
        ):
            await client.create_campaign(
                account_id=1,
                campaign_group_id=1,
                name="Minimal",
                campaign_type="SPONSORED_UPDATES",
                objective="ENGAGEMENT",
                targeting={},
                daily_budget={"amount": "10", "currencyCode": "USD"},
                cost_type="CPC",
            )

        body = mock_req.call_args.kwargs["json"]
        assert "unitCost" not in body
        assert "runSchedule" not in body


class TestCampaignList:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_get_campaigns_returns_parsed_list(self, client):
        """Should parse campaign elements from API response."""
        api_resp = {
            "elements": [
                {
                    "id": "urn:li:sponsoredCampaign:111",
                    "name": "Active Campaign",
                    "status": "ACTIVE",
                    "type": "SPONSORED_UPDATES",
                    "objectiveType": "WEBSITE_VISITS",
                    "costType": "CPC",
                    "dailyBudget": {
                        "amount": "50",
                        "currencyCode": "USD",
                    },
                    "offsiteDeliveryEnabled": False,
                    "campaignGroup": "urn:li:sponsoredCampaignGroup:1",
                    "account": "urn:li:sponsoredAccount:518121035",
                },
                {
                    "id": "urn:li:sponsoredCampaign:222",
                    "name": "Paused Campaign",
                    "status": "PAUSED",
                    "type": "TEXT_AD",
                    "costType": "CPM",
                    "offsiteDeliveryEnabled": True,
                },
            ]
        }

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=_mock_resp(200, api_resp),
            ),
        ):
            campaigns = await client.get_campaigns(account_id=518121035)

        assert len(campaigns) == 2
        assert isinstance(campaigns[0], LinkedInCampaign)
        assert campaigns[0].id == 111
        assert campaigns[0].name == "Active Campaign"
        assert campaigns[0].status == "ACTIVE"
        assert campaigns[0].objective_type == "WEBSITE_VISITS"
        assert campaigns[1].id == 222
        assert campaigns[1].status == "PAUSED"

    @pytest.mark.asyncio
    async def test_get_campaigns_with_status_filter(self, client):
        """Should pass status filter to API params."""
        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=_mock_resp(200, {"elements": []}),
            ) as mock_req,
        ):
            await client.get_campaigns(
                account_id=1, statuses=["ACTIVE"]
            )

        params = mock_req.call_args.kwargs["params"]
        assert "ACTIVE" in params["search"]


class TestCampaignGet:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_get_single_campaign(self, client):
        """Should fetch campaign by ID."""
        api_resp = {
            "id": "urn:li:sponsoredCampaign:111",
            "name": "Test",
            "status": "ACTIVE",
        }

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=_mock_resp(200, api_resp),
            ) as mock_req,
        ):
            result = await client.get_campaign(
                account_id=518121035, campaign_id=111
            )

        assert result["name"] == "Test"
        url = mock_req.call_args.args[1]
        assert "/adAccounts/518121035/adCampaigns/111" in url


class TestCampaignUpdate:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_update_uses_restli_patch_format(self, client):
        """Updates should use {patch: {$set: {...}}} format."""
        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=_mock_resp(204),
            ) as mock_req,
        ):
            await client.update_campaign(
                account_id=1,
                campaign_id=111,
                updates={
                    "dailyBudget": {
                        "amount": "100",
                        "currencyCode": "USD",
                    }
                },
            )

        body = mock_req.call_args.kwargs["json"]
        assert body == {
            "patch": {
                "$set": {
                    "dailyBudget": {
                        "amount": "100",
                        "currencyCode": "USD",
                    }
                }
            }
        }
        assert mock_req.call_args.args[0] == "PATCH"


class TestCampaignStatusTransition:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_valid_draft_to_active(self, client):
        """DRAFT → ACTIVE should succeed."""
        get_resp = _mock_resp(
            200, {"status": "DRAFT", "name": "Test"}
        )
        patch_resp = _mock_resp(204)
        call_count = 0

        async def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return get_resp if call_count == 1 else patch_resp

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                side_effect=side_effect,
            ),
        ):
            await client.update_campaign_status(
                account_id=1, campaign_id=111, status="ACTIVE"
            )

        assert call_count == 2  # GET + PATCH

    @pytest.mark.asyncio
    async def test_valid_active_to_paused(self, client):
        """ACTIVE → PAUSED should succeed."""
        get_resp = _mock_resp(200, {"status": "ACTIVE"})
        patch_resp = _mock_resp(204)
        responses = iter([get_resp, patch_resp])

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                side_effect=lambda *a, **kw: next(responses),
            ),
        ):
            await client.update_campaign_status(
                account_id=1, campaign_id=111, status="PAUSED"
            )

    @pytest.mark.asyncio
    async def test_valid_paused_to_active(self, client):
        """PAUSED → ACTIVE should succeed."""
        get_resp = _mock_resp(200, {"status": "PAUSED"})
        patch_resp = _mock_resp(204)
        responses = iter([get_resp, patch_resp])

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                side_effect=lambda *a, **kw: next(responses),
            ),
        ):
            await client.update_campaign_status(
                account_id=1, campaign_id=111, status="ACTIVE"
            )

    @pytest.mark.asyncio
    async def test_any_to_archived(self, client):
        """Any status → ARCHIVED should succeed."""
        for current in ["DRAFT", "ACTIVE", "PAUSED", "COMPLETED"]:
            get_resp = _mock_resp(200, {"status": current})
            patch_resp = _mock_resp(204)
            responses = iter([get_resp, patch_resp])

            with (
                patch(
                    "app.integrations.linkedin.get_valid_linkedin_token",
                    new_callable=AsyncMock,
                    return_value="tok",
                ),
                patch.object(
                    client._client,
                    "request",
                    side_effect=lambda *a, **kw: next(responses),
                ),
            ):
                await client.update_campaign_status(
                    account_id=1, campaign_id=111, status="ARCHIVED"
                )

    @pytest.mark.asyncio
    async def test_invalid_draft_to_paused(self, client):
        """DRAFT → PAUSED should raise."""
        get_resp = _mock_resp(200, {"status": "DRAFT"})

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=get_resp,
            ),
        ):
            with pytest.raises(
                LinkedInAPIError, match="Invalid status transition"
            ):
                await client.update_campaign_status(
                    account_id=1, campaign_id=111, status="PAUSED"
                )

    @pytest.mark.asyncio
    async def test_invalid_completed_to_active(self, client):
        """COMPLETED → ACTIVE should raise."""
        get_resp = _mock_resp(200, {"status": "COMPLETED"})

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=get_resp,
            ),
        ):
            with pytest.raises(
                LinkedInAPIError, match="Invalid status transition"
            ):
                await client.update_campaign_status(
                    account_id=1, campaign_id=111, status="ACTIVE"
                )

    @pytest.mark.asyncio
    async def test_invalid_archived_to_anything(self, client):
        """ARCHIVED → anything should raise (no transitions out)."""
        get_resp = _mock_resp(200, {"status": "ARCHIVED"})

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                new_callable=AsyncMock,
                return_value=get_resp,
            ),
        ):
            with pytest.raises(
                LinkedInAPIError, match="Invalid status transition"
            ):
                await client.update_campaign_status(
                    account_id=1, campaign_id=111, status="ACTIVE"
                )


# --- URN helper ---


class TestCampaignGroupURN:
    def test_make_campaign_group_urn(self):
        assert (
            make_campaign_group_urn(635137195)
            == "urn:li:sponsoredCampaignGroup:635137195"
        )


# --- Helpers ---


def _mock_resp(status_code: int, json_data: dict | None = None):
    """Create a mock httpx.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    resp.text = str(json_data)
    return resp
