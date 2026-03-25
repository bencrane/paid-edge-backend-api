"""Tests for Google Ads audience push — PaidEdge segments to Customer Match (BJC-146)."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.integrations.google_ads import GoogleAdsService
from app.integrations.google_ads_audience_push import (
    GoogleAdsAudiencePushService,
    MIN_TARGETABLE_SIZE,
)


@pytest.fixture
def mock_service():
    service = MagicMock(spec=GoogleAdsService)
    service.customer_id = "1234567890"
    service.enums = MagicMock()
    service._get_type = MagicMock()
    service._get_service = MagicMock()
    service.mutate = AsyncMock()
    service.search_stream = AsyncMock(return_value=[])
    return service


@pytest.fixture
def push_service(mock_service):
    return GoogleAdsAudiencePushService(mock_service)


@pytest.fixture
def sample_segment():
    return {"id": "seg-123", "name": "B2B Prospects"}


@pytest.fixture
def sample_members():
    return [
        {"email": "a@example.com", "first_name": "Alice", "last_name": "A"},
        {"email": "b@example.com", "phone": "+14155551234"},
        {"email": "c@example.com"},
    ]


class TestPushSegment:
    @pytest.mark.asyncio
    async def test_push_segment_creates_list_and_uploads(
        self, push_service, mock_service, sample_segment, sample_members
    ):
        # Mock create_user_list
        mock_response = MagicMock()
        mock_response.results = [
            MagicMock(resource_name="customers/123/userLists/456")
        ]
        mock_service.mutate.return_value = mock_response

        # Mock upload_members via OfflineUserDataJobService
        mock_job_service = MagicMock()
        mock_create_response = MagicMock()
        mock_create_response.resource_name = "customers/123/offlineUserDataJobs/789"
        mock_service._get_service.return_value = mock_job_service

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(side_effect=[
                mock_create_response,
                MagicMock(),
                MagicMock(),
            ])

            result = await push_service.push_segment(sample_segment, sample_members)

        assert result["status"] == "pushed"
        assert result["segment_id"] == "seg-123"
        assert result["provider"] == "google_ads"
        assert result["member_count"] == 3
        assert "pushed_at" in result

    @pytest.mark.asyncio
    async def test_push_segment_reuses_existing_list(
        self, push_service, mock_service, sample_segment, sample_members
    ):
        existing_push = {
            "remote_list_id": "customers/123/userLists/existing",
        }

        mock_job_service = MagicMock()
        mock_create_response = MagicMock()
        mock_create_response.resource_name = "customers/123/offlineUserDataJobs/789"
        mock_service._get_service.return_value = mock_job_service

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(side_effect=[
                mock_create_response,
                MagicMock(),
                MagicMock(),
            ])

            result = await push_service.push_segment(
                sample_segment, sample_members, existing_push=existing_push
            )

        assert result["status"] == "pushed"
        assert result["remote_list_id"] == "customers/123/userLists/existing"
        # Should NOT have called mutate for UserListService (no list creation)
        mock_service.mutate.assert_not_called()

    @pytest.mark.asyncio
    async def test_push_segment_csv_mode(
        self, push_service, sample_segment, sample_members
    ):
        result = await push_service.push_segment(
            sample_segment, sample_members, mode="csv"
        )

        assert result["status"] == "csv_export"
        assert result["row_count"] == 3
        assert result["rows"][0]["Email"] == "a@example.com"

    @pytest.mark.asyncio
    async def test_push_segment_no_valid_members(
        self, push_service, sample_segment
    ):
        members = [{"company": "Acme"}]  # No email/phone/name

        result = await push_service.push_segment(sample_segment, members)

        assert result["status"] == "skipped"
        assert "No members" in result["reason"]

    @pytest.mark.asyncio
    async def test_push_segment_below_min_size(
        self, push_service, mock_service, sample_segment
    ):
        members = [{"email": f"u{i}@example.com"} for i in range(10)]
        assert len(members) < MIN_TARGETABLE_SIZE

        mock_response = MagicMock()
        mock_response.results = [
            MagicMock(resource_name="customers/123/userLists/456")
        ]
        mock_service.mutate.return_value = mock_response

        mock_job_service = MagicMock()
        mock_create_response = MagicMock()
        mock_create_response.resource_name = "customers/123/offlineUserDataJobs/789"
        mock_service._get_service.return_value = mock_job_service

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(side_effect=[
                mock_create_response,
                MagicMock(),
                MagicMock(),
            ])

            result = await push_service.push_segment(sample_segment, members)

        assert result["status"] == "pushed"
        assert result["below_min_size"] is True

    @pytest.mark.asyncio
    async def test_push_segment_incremental_no_new_members(
        self, push_service, sample_segment
    ):
        members = [{"email": "a@example.com"}]
        existing_push = {
            "remote_list_id": "customers/123/userLists/456",
            "last_pushed_at": "2026-01-01T00:00:00",
            "uploaded_emails": ["a@example.com"],
        }

        result = await push_service.push_segment(
            sample_segment, members, existing_push=existing_push
        )

        assert result["status"] == "no_changes"


class TestRemoveStaleMembers:
    @pytest.mark.asyncio
    async def test_remove_stale_members(self, push_service, mock_service):
        mock_job_service = MagicMock()
        mock_create_response = MagicMock()
        mock_create_response.resource_name = "customers/123/offlineUserDataJobs/999"
        mock_service._get_service.return_value = mock_job_service

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(side_effect=[
                mock_create_response,
                MagicMock(),
                MagicMock(),
            ])

            result = await push_service.remove_stale_members(
                "customers/123/userLists/456",
                [{"email": "stale@example.com"}],
            )

        assert result["removed_count"] == 1

    @pytest.mark.asyncio
    async def test_remove_stale_members_empty(self, push_service):
        result = await push_service.remove_stale_members(
            "customers/123/userLists/456", []
        )
        assert result["removed_count"] == 0


class TestCheckPushStatus:
    @pytest.mark.asyncio
    async def test_check_push_status(self, push_service, mock_service):
        mock_row = MagicMock()
        mock_row.offline_user_data_job.status.name = "SUCCESS"
        mock_service.search_stream.return_value = [mock_row]

        result = await push_service.check_push_status(
            "customers/123/offlineUserDataJobs/789"
        )
        assert result["status"] == "SUCCESS"


class TestFindExistingList:
    @pytest.mark.asyncio
    async def test_find_existing_list_found(self, push_service, mock_service):
        mock_row = MagicMock()
        mock_row.user_list.resource_name = "customers/123/userLists/456"
        mock_row.user_list.name = "PaidEdge: B2B Prospects"
        mock_row.user_list.description = ""
        mock_row.user_list.size_for_search = 0
        mock_row.user_list.size_for_display = 0
        mock_service.search_stream.return_value = [mock_row]

        result = await push_service.find_existing_list("B2B Prospects")
        assert result == "customers/123/userLists/456"

    @pytest.mark.asyncio
    async def test_find_existing_list_not_found(self, push_service, mock_service):
        mock_service.search_stream.return_value = []
        result = await push_service.find_existing_list("Unknown Segment")
        assert result is None


class TestFormatMembers:
    def test_format_members(self, push_service):
        members = [
            {"email": "a@example.com", "first_name": "Alice", "last_name": "A"},
            {"email": "b@example.com"},
            {"company": "Acme"},  # no valid identifiers
        ]
        result = push_service._format_members_for_upload(members)
        assert len(result) == 2
        assert result[0]["email"] == "a@example.com"
        assert result[0]["first_name"] == "Alice"

    def test_format_members_empty(self, push_service):
        result = push_service._format_members_for_upload([])
        assert result == []


class TestGetNewMembers:
    def test_get_new_members(self, push_service):
        current = [
            {"email": "a@example.com"},
            {"email": "b@example.com"},
            {"email": "c@example.com"},
        ]
        existing_push = {"uploaded_emails": ["a@example.com", "b@example.com"]}
        result = push_service._get_new_members(current, existing_push)
        assert len(result) == 1
        assert result[0]["email"] == "c@example.com"

    def test_get_new_members_all_new(self, push_service):
        current = [{"email": "x@example.com"}]
        existing_push = {"uploaded_emails": []}
        result = push_service._get_new_members(current, existing_push)
        assert len(result) == 1
