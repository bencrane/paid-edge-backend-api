"""Tests for LinkedIn creative management + media upload pipelines (BJC-132)."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.integrations.linkedin import (
    LinkedInAdsClient,
    LinkedInAPIError,
    make_account_urn,
    make_campaign_urn,
    make_org_urn,
)
from app.integrations.linkedin_models import LinkedInCreative

# --- Helpers ---


def _mock_resp(
    status_code: int,
    json_data: dict | None = None,
    headers: dict | None = None,
):
    """Create a mock httpx.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    resp.text = str(json_data)
    resp.headers = headers or {}
    return resp


# --- Image upload ---


class TestImageUpload:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_upload_image_calls_init_then_put(self, client):
        """Should POST initializeUpload, then PUT binary data."""
        init_resp = _mock_resp(
            200,
            {
                "value": {
                    "uploadUrl": "https://upload.linkedin.com/img123",
                    "image": "urn:li:image:C4E22AQHabc",
                }
            },
        )
        put_resp = _mock_resp(201)

        call_count = 0

        async def request_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return init_resp

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                side_effect=request_side_effect,
            ),
            patch.object(
                client._client,
                "put",
                new_callable=AsyncMock,
                return_value=put_resp,
            ) as mock_put,
        ):
            urn = await client.upload_image(
                org_id=2414183, image_bytes=b"fake-image-data"
            )

        assert urn == "urn:li:image:C4E22AQHabc"
        mock_put.assert_called_once()
        put_args = mock_put.call_args
        assert put_args.args[0] == "https://upload.linkedin.com/img123"
        assert put_args.kwargs["content"] == b"fake-image-data"

    @pytest.mark.asyncio
    async def test_upload_image_sends_org_urn(self, client):
        """initializeUpload should include org URN as owner."""
        init_resp = _mock_resp(
            200,
            {
                "value": {
                    "uploadUrl": "https://upload.linkedin.com/img",
                    "image": "urn:li:image:abc",
                }
            },
        )
        put_resp = _mock_resp(201)

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
                return_value=init_resp,
            ) as mock_req,
            patch.object(
                client._client,
                "put",
                new_callable=AsyncMock,
                return_value=put_resp,
            ),
        ):
            await client.upload_image(
                org_id=2414183, image_bytes=b"data"
            )

        body = mock_req.call_args.kwargs["json"]
        owner = body["initializeUploadRequest"]["owner"]
        assert owner == make_org_urn(2414183)


# --- Document upload ---


class TestDocumentUpload:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_upload_document_pipeline(self, client):
        """Should init upload, PUT binary, return document URN."""
        init_resp = _mock_resp(
            200,
            {
                "value": {
                    "uploadUrl": "https://upload.linkedin.com/doc456",
                    "document": "urn:li:document:C4D10AQHxyz",
                }
            },
        )
        put_resp = _mock_resp(201)

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
                return_value=init_resp,
            ),
            patch.object(
                client._client,
                "put",
                new_callable=AsyncMock,
                return_value=put_resp,
            ) as mock_put,
        ):
            urn = await client.upload_document(
                org_id=2414183, doc_bytes=b"fake-pdf-data"
            )

        assert urn == "urn:li:document:C4D10AQHxyz"
        mock_put.assert_called_once()
        assert (
            mock_put.call_args.args[0]
            == "https://upload.linkedin.com/doc456"
        )

    @pytest.mark.asyncio
    async def test_upload_document_sends_correct_init(self, client):
        """Init request should POST to /documents?action=initializeUpload."""
        init_resp = _mock_resp(
            200,
            {
                "value": {
                    "uploadUrl": "https://upload.linkedin.com/doc",
                    "document": "urn:li:document:abc",
                }
            },
        )
        put_resp = _mock_resp(201)

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
                return_value=init_resp,
            ) as mock_req,
            patch.object(
                client._client,
                "put",
                new_callable=AsyncMock,
                return_value=put_resp,
            ),
        ):
            await client.upload_document(
                org_id=2414183, doc_bytes=b"pdf"
            )

        url = mock_req.call_args.args[1]
        assert "/documents?action=initializeUpload" in url


# --- Video upload ---


class TestVideoUpload:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_upload_video_single_chunk(self, client):
        """Single-chunk video upload should init, PUT, finalize."""
        video_data = b"fake-video-content"
        init_resp = _mock_resp(
            200,
            {
                "value": {
                    "video": "urn:li:video:C5F10abc",
                    "uploadInstructions": [
                        {
                            "uploadUrl": "https://upload.linkedin.com/v1",
                            "firstByte": 0,
                            "lastByte": len(video_data) - 1,
                        }
                    ],
                }
            },
        )
        finalize_resp = _mock_resp(200, {})
        chunk_put_resp = _mock_resp(200)
        chunk_put_resp.headers = {"etag": "part-1-etag"}

        # Track POST calls: first is init, second is finalize
        post_calls = [init_resp, finalize_resp]
        post_idx = 0

        async def request_side_effect(*args, **kwargs):
            nonlocal post_idx
            result = post_calls[post_idx]
            post_idx += 1
            return result

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                side_effect=request_side_effect,
            ),
            patch.object(
                client._client,
                "put",
                new_callable=AsyncMock,
                return_value=chunk_put_resp,
            ) as mock_put,
        ):
            urn = await client.upload_video(
                org_id=2414183,
                video_bytes=video_data,
                file_size=len(video_data),
            )

        assert urn == "urn:li:video:C5F10abc"
        mock_put.assert_called_once()

    @pytest.mark.asyncio
    async def test_upload_video_multi_chunk(self, client):
        """Multi-chunk upload should PUT each chunk in order."""
        video_data = b"A" * 10 + b"B" * 10
        init_resp = _mock_resp(
            200,
            {
                "value": {
                    "video": "urn:li:video:multi",
                    "uploadInstructions": [
                        {
                            "uploadUrl": "https://upload.linkedin.com/c1",
                            "firstByte": 0,
                            "lastByte": 9,
                        },
                        {
                            "uploadUrl": "https://upload.linkedin.com/c2",
                            "firstByte": 10,
                            "lastByte": 19,
                        },
                    ],
                }
            },
        )
        finalize_resp = _mock_resp(200, {})
        chunk_resp_1 = _mock_resp(200)
        chunk_resp_1.headers = {"etag": "etag-1"}
        chunk_resp_2 = _mock_resp(200)
        chunk_resp_2.headers = {"etag": "etag-2"}

        post_calls = [init_resp, finalize_resp]
        post_idx = 0

        async def request_side_effect(*args, **kwargs):
            nonlocal post_idx
            result = post_calls[post_idx]
            post_idx += 1
            return result

        put_responses = iter([chunk_resp_1, chunk_resp_2])

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                side_effect=request_side_effect,
            ),
            patch.object(
                client._client,
                "put",
                side_effect=lambda *a, **kw: next(put_responses),
            ) as mock_put,
        ):
            urn = await client.upload_video(
                org_id=2414183,
                video_bytes=video_data,
                file_size=len(video_data),
            )

        assert urn == "urn:li:video:multi"
        assert mock_put.call_count == 2

    @pytest.mark.asyncio
    async def test_upload_video_finalize_includes_part_ids(self, client):
        """Finalize request should include uploaded part IDs."""
        video_data = b"data"
        init_resp = _mock_resp(
            200,
            {
                "value": {
                    "video": "urn:li:video:fin",
                    "uploadInstructions": [
                        {
                            "uploadUrl": "https://upload.linkedin.com/c",
                            "firstByte": 0,
                            "lastByte": 3,
                        }
                    ],
                }
            },
        )
        finalize_resp = _mock_resp(200, {})
        chunk_resp = _mock_resp(200)
        chunk_resp.headers = {"etag": "my-etag"}

        post_calls = [init_resp, finalize_resp]
        post_idx = 0

        async def request_side_effect(*args, **kwargs):
            nonlocal post_idx
            result = post_calls[post_idx]
            post_idx += 1
            return result

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "request",
                side_effect=request_side_effect,
            ) as mock_req,
            patch.object(
                client._client,
                "put",
                new_callable=AsyncMock,
                return_value=chunk_resp,
            ),
        ):
            await client.upload_video(
                org_id=2414183,
                video_bytes=video_data,
                file_size=len(video_data),
            )

        # The second POST call is finalize
        finalize_call = mock_req.call_args_list[1]
        body = finalize_call.kwargs["json"]
        assert body["finalizeUploadRequest"]["uploadedPartIds"] == [
            "my-etag"
        ]
        assert body["finalizeUploadRequest"]["video"] == "urn:li:video:fin"


# --- Post creation ---


class TestCreateSponsoredPost:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_creates_post_with_correct_body(self, client):
        """Should create ad-only post with feedDistribution=NONE."""
        resp = _mock_resp(
            201, {}, headers={"x-restli-id": "urn:li:ugcPost:123456"}
        )

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
                return_value=resp,
            ) as mock_req,
        ):
            post_urn = await client.create_sponsored_post(
                org_id=2414183,
                commentary="Check out our update!",
                media_urn="urn:li:image:abc",
                media_title="Q2 Update",
            )

        assert post_urn == "urn:li:ugcPost:123456"
        body = mock_req.call_args.kwargs["json"]
        assert body["author"] == make_org_urn(2414183)
        assert body["commentary"] == "Check out our update!"
        assert body["distribution"]["feedDistribution"] == "NONE"
        assert body["content"]["media"]["id"] == "urn:li:image:abc"
        assert body["content"]["media"]["title"] == "Q2 Update"
        assert body["visibility"] == "PUBLIC"
        assert body["lifecycleState"] == "PUBLISHED"

    @pytest.mark.asyncio
    async def test_returns_urn_from_header(self, client):
        """Post URN should come from x-restli-id header."""
        resp = _mock_resp(
            201,
            {"id": "urn:li:ugcPost:from-body"},
            headers={"x-restli-id": "urn:li:ugcPost:from-header"},
        )

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
                return_value=resp,
            ),
        ):
            post_urn = await client.create_sponsored_post(
                org_id=1,
                commentary="text",
                media_urn="urn:li:image:x",
                media_title="Title",
            )

        # Header takes priority
        assert post_urn == "urn:li:ugcPost:from-header"

    @pytest.mark.asyncio
    async def test_falls_back_to_body_id(self, client):
        """If no x-restli-id header, fall back to body id."""
        resp = _mock_resp(
            200,
            {"id": "urn:li:ugcPost:from-body"},
            headers={},
        )

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
                return_value=resp,
            ),
        ):
            post_urn = await client.create_sponsored_post(
                org_id=1,
                commentary="text",
                media_urn="urn:li:image:x",
                media_title="Title",
            )

        assert post_urn == "urn:li:ugcPost:from-body"


# --- Creative CRUD ---


class TestCreateCreative:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_create_creative_basic(self, client):
        """Should POST creative with campaign URN and post reference."""
        api_resp = {"id": "urn:li:adCreative:99"}

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
            result = await client.create_creative(
                account_id=518121035,
                campaign_id=12345,
                post_urn="urn:li:ugcPost:789",
            )

        assert result["id"] == "urn:li:adCreative:99"
        body = mock_req.call_args.kwargs["json"]
        assert body["campaign"] == make_campaign_urn(12345)
        assert body["content"]["reference"] == "urn:li:ugcPost:789"
        assert body["intendedStatus"] == "ACTIVE"
        assert "leadGenerationContext" not in body["content"]

    @pytest.mark.asyncio
    async def test_create_creative_with_lead_gen_form(self, client):
        """Should attach leadGenerationContext when form URN provided."""
        api_resp = {"id": "urn:li:adCreative:100"}

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
            await client.create_creative(
                account_id=1,
                campaign_id=12345,
                post_urn="urn:li:ugcPost:789",
                lead_gen_form_urn="urn:li:leadGenerationForm:456",
            )

        body = mock_req.call_args.kwargs["json"]
        lgc = body["content"]["leadGenerationContext"]
        assert (
            lgc["leadGenerationFormUrn"]
            == "urn:li:leadGenerationForm:456"
        )

    @pytest.mark.asyncio
    async def test_create_creative_custom_status(self, client):
        """Should allow custom intendedStatus."""
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
                return_value=_mock_resp(200, {"id": "urn:li:adCreative:1"}),
            ) as mock_req,
        ):
            await client.create_creative(
                account_id=1,
                campaign_id=1,
                post_urn="urn:li:ugcPost:1",
                status="PAUSED",
            )

        body = mock_req.call_args.kwargs["json"]
        assert body["intendedStatus"] == "PAUSED"


class TestGetCreatives:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_get_creatives_returns_parsed_list(self, client):
        """Should parse creative elements into LinkedInCreative models."""
        api_resp = {
            "elements": [
                {
                    "id": "urn:li:adCreative:111",
                    "campaign": "urn:li:sponsoredCampaign:100",
                    "content": {
                        "reference": "urn:li:ugcPost:789"
                    },
                    "intendedStatus": "ACTIVE",
                    "reviewStatus": "APPROVED",
                    "servingStatuses": ["RUNNABLE"],
                },
                {
                    "id": "urn:li:adCreative:222",
                    "campaign": "urn:li:sponsoredCampaign:100",
                    "content": {
                        "reference": "urn:li:ugcPost:790"
                    },
                    "intendedStatus": "PAUSED",
                    "reviewStatus": "PENDING_REVIEW",
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
            creatives = await client.get_creatives(
                account_id=518121035
            )

        assert len(creatives) == 2
        assert isinstance(creatives[0], LinkedInCreative)
        assert creatives[0].id == 111
        assert creatives[0].content_reference == "urn:li:ugcPost:789"
        assert creatives[0].review_status == "APPROVED"
        assert creatives[0].serving_statuses == ["RUNNABLE"]
        assert creatives[1].id == 222
        assert creatives[1].intended_status == "PAUSED"

    @pytest.mark.asyncio
    async def test_get_creatives_with_campaign_filter(self, client):
        """Should pass campaign filter in search params."""
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
            await client.get_creatives(
                account_id=1, campaign_id=12345
            )

        params = mock_req.call_args.kwargs["params"]
        assert make_campaign_urn(12345) in params["search"]


class TestUpdateCreativeStatus:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_update_uses_restli_patch_format(self, client):
        """Should use {patch: {$set: {intendedStatus: ...}}} format."""
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
            await client.update_creative_status(
                account_id=1, creative_id=111, status="PAUSED"
            )

        body = mock_req.call_args.kwargs["json"]
        assert body == {
            "patch": {"$set": {"intendedStatus": "PAUSED"}}
        }
        assert mock_req.call_args.args[0] == "PATCH"

    @pytest.mark.asyncio
    async def test_update_creative_to_archived(self, client):
        """Should support ARCHIVED status."""
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
            await client.update_creative_status(
                account_id=1, creative_id=222, status="ARCHIVED"
            )

        body = mock_req.call_args.kwargs["json"]
        assert (
            body["patch"]["$set"]["intendedStatus"] == "ARCHIVED"
        )
        url = mock_req.call_args.args[1]
        assert "/adCreatives/222" in url


# --- InMail content ---


class TestCreateInMailContent:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_creates_inmail_content(self, client):
        """Should POST to /adInMailContents with correct body."""
        resp = _mock_resp(
            201,
            {},
            headers={"x-restli-id": "urn:li:adInMailContent:789"},
        )

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
                return_value=resp,
            ) as mock_req,
        ):
            urn = await client.create_inmail_content(
                account_id=507404993,
                name="Q2 Message",
                subject="Exclusive offer",
                html_body="<p>Hi there</p>",
                sender_urn="urn:li:person:abc123",
                cta_label="Learn More",
                cta_url="https://example.com/offer",
            )

        assert urn == "urn:li:adInMailContent:789"
        body = mock_req.call_args.kwargs["json"]
        assert body["account"] == make_account_urn(507404993)
        assert body["name"] == "Q2 Message"
        assert body["subject"] == "Exclusive offer"
        assert body["htmlBody"] == "<p>Hi there</p>"
        assert body["sender"] == "urn:li:person:abc123"
        assert body["ctaLabel"] == "Learn More"
        assert body["ctaUrl"] == "https://example.com/offer"

    @pytest.mark.asyncio
    async def test_rejects_subject_over_60_chars(self, client):
        """Subject over 60 chars should raise."""
        with pytest.raises(LinkedInAPIError, match="subject exceeds 60"):
            await client.create_inmail_content(
                account_id=1,
                name="test",
                subject="x" * 61,
                html_body="body",
                sender_urn="urn:li:person:1",
                cta_label="CTA",
                cta_url="https://example.com",
            )

    @pytest.mark.asyncio
    async def test_rejects_body_over_1500_chars(self, client):
        """Body over 1500 chars should raise."""
        with pytest.raises(LinkedInAPIError, match="body exceeds 1500"):
            await client.create_inmail_content(
                account_id=1,
                name="test",
                subject="OK subject",
                html_body="x" * 1501,
                sender_urn="urn:li:person:1",
                cta_label="CTA",
                cta_url="https://example.com",
            )

    @pytest.mark.asyncio
    async def test_rejects_cta_over_20_chars(self, client):
        """CTA label over 20 chars should raise."""
        with pytest.raises(
            LinkedInAPIError, match="CTA label exceeds 20"
        ):
            await client.create_inmail_content(
                account_id=1,
                name="test",
                subject="OK",
                html_body="body",
                sender_urn="urn:li:person:1",
                cta_label="x" * 21,
                cta_url="https://example.com",
            )

    @pytest.mark.asyncio
    async def test_accepts_max_length_values(self, client):
        """Exactly at limit should not raise."""
        resp = _mock_resp(
            201, {}, headers={"x-restli-id": "urn:li:adInMailContent:1"}
        )

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
                return_value=resp,
            ),
        ):
            urn = await client.create_inmail_content(
                account_id=1,
                name="test",
                subject="x" * 60,
                html_body="x" * 1500,
                sender_urn="urn:li:person:1",
                cta_label="x" * 20,
                cta_url="https://example.com",
            )

        assert urn == "urn:li:adInMailContent:1"


# --- End-to-end helpers ---


class TestCreateImageAd:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_image_ad_pipeline(self, client):
        """Should chain: upload_image → create_sponsored_post → create_creative."""
        with (
            patch.object(
                client,
                "upload_image",
                new_callable=AsyncMock,
                return_value="urn:li:image:uploaded",
            ) as mock_upload,
            patch.object(
                client,
                "create_sponsored_post",
                new_callable=AsyncMock,
                return_value="urn:li:ugcPost:created",
            ) as mock_post,
            patch.object(
                client,
                "create_creative",
                new_callable=AsyncMock,
                return_value={"id": "urn:li:adCreative:final"},
            ) as mock_creative,
        ):
            result = await client.create_image_ad(
                account_id=518121035,
                campaign_id=12345,
                org_id=2414183,
                image_bytes=b"image-data",
                headline="My Headline",
                intro_text="Check this out!",
            )

        assert result["id"] == "urn:li:adCreative:final"
        mock_upload.assert_called_once_with(2414183, b"image-data")
        mock_post.assert_called_once_with(
            org_id=2414183,
            commentary="Check this out!",
            media_urn="urn:li:image:uploaded",
            media_title="My Headline",
        )
        mock_creative.assert_called_once_with(
            account_id=518121035,
            campaign_id=12345,
            post_urn="urn:li:ugcPost:created",
            lead_gen_form_urn=None,
        )

    @pytest.mark.asyncio
    async def test_image_ad_with_lead_gen(self, client):
        """Should pass lead gen form URN through to create_creative."""
        with (
            patch.object(
                client,
                "upload_image",
                new_callable=AsyncMock,
                return_value="urn:li:image:x",
            ),
            patch.object(
                client,
                "create_sponsored_post",
                new_callable=AsyncMock,
                return_value="urn:li:ugcPost:x",
            ),
            patch.object(
                client,
                "create_creative",
                new_callable=AsyncMock,
                return_value={"id": "urn:li:adCreative:x"},
            ) as mock_creative,
        ):
            await client.create_image_ad(
                account_id=1,
                campaign_id=1,
                org_id=1,
                image_bytes=b"data",
                headline="H",
                intro_text="I",
                lead_gen_form_urn="urn:li:leadGenerationForm:999",
            )

        mock_creative.assert_called_once_with(
            account_id=1,
            campaign_id=1,
            post_urn="urn:li:ugcPost:x",
            lead_gen_form_urn="urn:li:leadGenerationForm:999",
        )


class TestCreateDocumentAd:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_document_ad_pipeline(self, client):
        """Should chain: upload_document → create_sponsored_post → create_creative."""
        with (
            patch.object(
                client,
                "upload_document",
                new_callable=AsyncMock,
                return_value="urn:li:document:uploaded",
            ) as mock_upload,
            patch.object(
                client,
                "create_sponsored_post",
                new_callable=AsyncMock,
                return_value="urn:li:ugcPost:docpost",
            ) as mock_post,
            patch.object(
                client,
                "create_creative",
                new_callable=AsyncMock,
                return_value={"id": "urn:li:adCreative:docad"},
            ) as mock_creative,
        ):
            result = await client.create_document_ad(
                account_id=518121035,
                campaign_id=12345,
                org_id=2414183,
                pdf_bytes=b"pdf-content",
                title="B2B Marketing Guide",
                commentary="Download our guide",
            )

        assert result["id"] == "urn:li:adCreative:docad"
        mock_upload.assert_called_once_with(2414183, b"pdf-content")
        mock_post.assert_called_once_with(
            org_id=2414183,
            commentary="Download our guide",
            media_urn="urn:li:document:uploaded",
            media_title="B2B Marketing Guide",
        )
        mock_creative.assert_called_once_with(
            account_id=518121035,
            campaign_id=12345,
            post_urn="urn:li:ugcPost:docpost",
        )

    @pytest.mark.asyncio
    async def test_document_ad_no_lead_gen(self, client):
        """Document ad should not pass lead gen form."""
        with (
            patch.object(
                client,
                "upload_document",
                new_callable=AsyncMock,
                return_value="urn:li:document:x",
            ),
            patch.object(
                client,
                "create_sponsored_post",
                new_callable=AsyncMock,
                return_value="urn:li:ugcPost:x",
            ),
            patch.object(
                client,
                "create_creative",
                new_callable=AsyncMock,
                return_value={"id": "urn:li:adCreative:x"},
            ) as mock_creative,
        ):
            await client.create_document_ad(
                account_id=1,
                campaign_id=1,
                org_id=1,
                pdf_bytes=b"pdf",
                title="T",
                commentary="C",
            )

        # No lead_gen_form_urn param
        call_kwargs = mock_creative.call_args.kwargs
        assert "lead_gen_form_urn" not in call_kwargs


# --- Binary upload helper ---


class TestUploadBinary:
    @pytest.fixture
    def client(self):
        return LinkedInAdsClient(org_id="org-1", supabase=MagicMock())

    @pytest.mark.asyncio
    async def test_upload_binary_sends_correct_content_type(self, client):
        """Binary upload should set Content-Type to application/octet-stream."""
        put_resp = _mock_resp(200)

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "put",
                new_callable=AsyncMock,
                return_value=put_resp,
            ) as mock_put,
        ):
            await client._upload_binary(
                "https://upload.linkedin.com/test", b"data"
            )

        headers = mock_put.call_args.kwargs["headers"]
        assert headers["Content-Type"] == "application/octet-stream"

    @pytest.mark.asyncio
    async def test_upload_binary_raises_on_error(self, client):
        """Should raise on non-success status."""
        error_resp = _mock_resp(
            400,
            {"status": 400, "message": "Bad upload"},
        )

        with (
            patch(
                "app.integrations.linkedin.get_valid_linkedin_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                client._client,
                "put",
                new_callable=AsyncMock,
                return_value=error_resp,
            ),
        ):
            with pytest.raises(LinkedInAPIError):
                await client._upload_binary(
                    "https://upload.linkedin.com/fail", b"bad"
                )
