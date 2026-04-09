"""Tests for Google Ads Customer Match audience upload (BJC-145)."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.integrations.google_ads import GoogleAdsService
from app.integrations.google_ads_customer_match import (
    BATCH_SIZE,
    GoogleAdsCustomerMatchService,
    _hash_value,
    _normalize_phone,
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
def cm_service(mock_service):
    return GoogleAdsCustomerMatchService(mock_service)


# --- Hashing helpers ---


class TestHashingHelpers:
    def test_hash_value_sha256(self):
        result = _hash_value("test@example.com")
        assert len(result) == 64  # SHA-256 hex digest
        # Deterministic
        assert result == _hash_value("test@example.com")

    def test_hash_value_different_inputs(self):
        assert _hash_value("a") != _hash_value("b")

    def test_normalize_phone_us_default(self):
        assert _normalize_phone("5551234567") == "+15551234567"

    def test_normalize_phone_with_country_code(self):
        assert _normalize_phone("+14155551234") == "+14155551234"

    def test_normalize_phone_strips_formatting(self):
        assert _normalize_phone("(415) 555-1234") == "+14155551234"

    def test_normalize_phone_international(self):
        assert _normalize_phone("+442071234567") == "+442071234567"


# --- Create user list ---


class TestCreateUserList:
    @pytest.mark.asyncio
    async def test_create_user_list(self, cm_service, mock_service):
        mock_response = MagicMock()
        mock_response.results = [
            MagicMock(resource_name="customers/123/userLists/456")
        ]
        mock_service.mutate.return_value = mock_response

        result = await cm_service.create_user_list("Test List", "A test list")

        assert result == "customers/123/userLists/456"
        mock_service.mutate.assert_called_once()
        call_args = mock_service.mutate.call_args
        assert call_args[0][0] == "UserListService"


# --- Upload members ---


class TestUploadMembers:
    @pytest.mark.asyncio
    async def test_upload_members_email(self, cm_service, mock_service):
        mock_job_service = MagicMock()
        mock_create_response = MagicMock()
        mock_create_response.resource_name = "customers/123/offlineUserDataJobs/789"
        mock_job_service.create_offline_user_data_job.return_value = mock_create_response
        mock_job_service.add_offline_user_data_job_operations.return_value = MagicMock()
        mock_job_service.run_offline_user_data_job.return_value = MagicMock()
        mock_service._get_service.return_value = mock_job_service

        members = [
            {"email": "test@example.com"},
            {"email": "user2@example.com", "phone": "+14155551234"},
        ]

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(side_effect=[
                mock_create_response,  # create job
                MagicMock(),  # add operations
                MagicMock(),  # run job
            ])

            result = await cm_service.upload_members(
                "customers/123/userLists/456", members
            )

        assert result["job_resource_name"] == "customers/123/offlineUserDataJobs/789"
        assert result["user_list_resource_name"] == "customers/123/userLists/456"
        assert result["member_count"] == 2

    @pytest.mark.asyncio
    async def test_upload_members_with_names(self, cm_service, mock_service):
        mock_job_service = MagicMock()
        mock_create_response = MagicMock()
        mock_create_response.resource_name = "customers/123/offlineUserDataJobs/789"
        mock_service._get_service.return_value = mock_job_service

        members = [
            {
                "email": "test@example.com",
                "first_name": "John",
                "last_name": "Doe",
            },
        ]

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(side_effect=[
                mock_create_response,
                MagicMock(),
                MagicMock(),
            ])

            result = await cm_service.upload_members(
                "customers/123/userLists/456", members
            )

        assert result["member_count"] == 1

    @pytest.mark.asyncio
    async def test_upload_members_batching(self, cm_service, mock_service):
        """Test that large member lists are batched."""
        mock_job_service = MagicMock()
        mock_create_response = MagicMock()
        mock_create_response.resource_name = "customers/123/offlineUserDataJobs/789"
        mock_service._get_service.return_value = mock_job_service

        # Create more members than BATCH_SIZE
        members = [{"email": f"user{i}@example.com"} for i in range(BATCH_SIZE + 100)]

        with patch("asyncio.get_event_loop") as mock_loop:
            # create job + 2 batch adds + run job = 4 calls
            mock_loop.return_value.run_in_executor = AsyncMock(side_effect=[
                mock_create_response,
                MagicMock(),  # first batch
                MagicMock(),  # second batch
                MagicMock(),  # run job
            ])

            result = await cm_service.upload_members(
                "customers/123/userLists/456", members
            )

        assert result["member_count"] == BATCH_SIZE + 100
        # 4 executor calls: create + 2 batches + run
        assert mock_loop.return_value.run_in_executor.call_count == 4


# --- Remove members ---


class TestRemoveMembers:
    @pytest.mark.asyncio
    async def test_remove_members(self, cm_service, mock_service):
        mock_job_service = MagicMock()
        mock_create_response = MagicMock()
        mock_create_response.resource_name = "customers/123/offlineUserDataJobs/999"
        mock_service._get_service.return_value = mock_job_service

        members = [{"email": "remove@example.com"}]

        with patch("asyncio.get_event_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock(side_effect=[
                mock_create_response,
                MagicMock(),
                MagicMock(),
            ])

            result = await cm_service.remove_members(
                "customers/123/userLists/456", members
            )

        assert result["job_resource_name"] == "customers/123/offlineUserDataJobs/999"
        assert result["removed_count"] == 1


# --- Job status ---


class TestJobStatus:
    @pytest.mark.asyncio
    async def test_check_job_status_success(self, cm_service, mock_service):
        mock_row = MagicMock()
        mock_row.offline_user_data_job.status.name = "SUCCESS"
        mock_service.search_stream.return_value = [mock_row]

        result = await cm_service.check_job_status(
            "customers/123/offlineUserDataJobs/789"
        )
        assert result == "SUCCESS"

    @pytest.mark.asyncio
    async def test_check_job_status_unknown(self, cm_service, mock_service):
        mock_service.search_stream.return_value = []
        result = await cm_service.check_job_status(
            "customers/123/offlineUserDataJobs/999"
        )
        assert result == "UNKNOWN"

    @pytest.mark.asyncio
    async def test_check_job_status_running(self, cm_service, mock_service):
        mock_row = MagicMock()
        mock_row.offline_user_data_job.status.name = "RUNNING"
        mock_service.search_stream.return_value = [mock_row]

        result = await cm_service.check_job_status(
            "customers/123/offlineUserDataJobs/789"
        )
        assert result == "RUNNING"


# --- User list queries ---


class TestUserListQueries:
    @pytest.mark.asyncio
    async def test_get_user_list_size(self, cm_service, mock_service):
        mock_row = MagicMock()
        mock_row.user_list.name = "Test List"
        mock_row.user_list.size_for_search = 5000
        mock_row.user_list.size_for_display = 4500
        mock_row.user_list.eligible_for_search = True
        mock_row.user_list.eligible_for_display = True
        mock_service.search_stream.return_value = [mock_row]

        result = await cm_service.get_user_list_size(
            "customers/123/userLists/456"
        )
        assert result["name"] == "Test List"
        assert result["size_for_search"] == 5000
        assert result["size_for_display"] == 4500
        assert result["eligible_for_search"] is True

    @pytest.mark.asyncio
    async def test_get_user_list_size_empty(self, cm_service, mock_service):
        mock_service.search_stream.return_value = []
        result = await cm_service.get_user_list_size("customers/123/userLists/999")
        assert result == {}

    @pytest.mark.asyncio
    async def test_get_user_lists(self, cm_service, mock_service):
        mock_row = MagicMock()
        mock_row.user_list.resource_name = "customers/123/userLists/456"
        mock_row.user_list.name = "PaidEdge: B2B Prospects"
        mock_row.user_list.description = "Auto-synced"
        mock_row.user_list.size_for_search = 2000
        mock_row.user_list.size_for_display = 1800
        mock_service.search_stream.return_value = [mock_row]

        result = await cm_service.get_user_lists()
        assert len(result) == 1
        assert result[0]["name"] == "PaidEdge: B2B Prospects"
        assert result[0]["size_for_search"] == 2000

    @pytest.mark.asyncio
    async def test_close_user_list(self, cm_service, mock_service):
        mock_service.mutate.return_value = MagicMock()
        await cm_service.close_user_list("customers/123/userLists/456")
        mock_service.mutate.assert_called_once()


# --- Build member operations ---


class TestBuildMemberOperations:
    def test_build_create_operations_email(self, cm_service, mock_service):
        mock_op = MagicMock()
        mock_service._get_type.return_value = mock_op

        ops = cm_service._build_member_operations(
            [{"email": "test@example.com"}], "create"
        )
        assert len(ops) == 1

    def test_build_create_operations_phone(self, cm_service, mock_service):
        mock_op = MagicMock()
        mock_service._get_type.return_value = mock_op

        ops = cm_service._build_member_operations(
            [{"phone": "+14155551234"}], "create"
        )
        assert len(ops) == 1

    def test_build_remove_operations(self, cm_service, mock_service):
        mock_op = MagicMock()
        mock_service._get_type.return_value = mock_op

        ops = cm_service._build_member_operations(
            [{"email": "test@example.com"}], "remove"
        )
        assert len(ops) == 1

    def test_build_operations_multiple(self, cm_service, mock_service):
        mock_op = MagicMock()
        mock_service._get_type.return_value = mock_op

        members = [
            {"email": f"user{i}@example.com"} for i in range(5)
        ]
        ops = cm_service._build_member_operations(members, "create")
        assert len(ops) == 5
