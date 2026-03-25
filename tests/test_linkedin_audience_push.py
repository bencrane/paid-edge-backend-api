"""Tests for LinkedIn audience push — BJC-135."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.audiences.linkedin_push import (
    _MIN_AUDIENCE_SIZE,
    LinkedInAudiencePushService,
)
from app.integrations.linkedin import LinkedInAdsClient
from app.integrations.linkedin_models import LinkedInDMPSegment

# --- Helpers ---


def _mock_supabase_chain(select_data=None):
    """Create a Supabase mock that supports select + upsert chains."""
    mock_supabase = MagicMock()
    mock_chain = MagicMock()
    mock_chain.eq.return_value = mock_chain
    mock_chain.maybe_single.return_value = mock_chain
    mock_chain.select.return_value = mock_chain
    mock_chain.upsert.return_value = mock_chain
    if select_data is not None:
        mock_chain.execute.return_value = SimpleNamespace(data=select_data)
    else:
        mock_chain.execute.return_value = SimpleNamespace(data=None)
    mock_supabase.table.return_value = mock_chain
    return mock_supabase, mock_chain


def _make_members(count, with_email=True, with_domain=True):
    """Generate mock segment members."""
    members = []
    for i in range(count):
        m = {
            "member_id": f"member-{i}",
            "email": f"user{i}@company{i}.com" if with_email else None,
            "company_domain": f"company{i}.com" if with_domain else None,
            "company_name": f"Company {i}",
            "full_name": f"User {i}",
        }
        members.append(m)
    return members


def _mock_clickhouse_query(members):
    """Create a mock ClickHouse query result."""
    columns = ["member_id", "email", "company_domain", "company_name", "full_name"]
    rows = [
        [m.get(c) for c in columns]
        for m in members
    ]
    mock_result = MagicMock()
    mock_result.column_names = columns
    mock_result.result_rows = rows
    return mock_result


# --- Strategy selection ---


class TestDetermineStrategy:
    def test_auto_selects_contact_when_majority_have_email(self):
        """Should pick contact strategy when >50% have emails."""
        mock_client = AsyncMock(spec=LinkedInAdsClient)
        mock_supabase, _ = _mock_supabase_chain()
        mock_ch = MagicMock()

        service = LinkedInAudiencePushService(
            linkedin_client=mock_client,
            supabase=mock_supabase,
            clickhouse=mock_ch,
        )

        members = _make_members(10, with_email=True)
        assert service._determine_strategy(members, "auto") == "contact"

    def test_auto_selects_company_when_few_have_email(self):
        """Should pick company strategy when <=50% have emails."""
        mock_client = AsyncMock(spec=LinkedInAdsClient)
        mock_supabase, _ = _mock_supabase_chain()
        mock_ch = MagicMock()

        service = LinkedInAudiencePushService(
            linkedin_client=mock_client,
            supabase=mock_supabase,
            clickhouse=mock_ch,
        )

        # 3 with email, 7 without → 30% < 50%
        members = _make_members(3, with_email=True) + _make_members(7, with_email=False)
        assert service._determine_strategy(members, "auto") == "company"

    def test_explicit_strategy_overrides_auto(self):
        """Should use explicit strategy regardless of data."""
        mock_client = AsyncMock(spec=LinkedInAdsClient)
        mock_supabase, _ = _mock_supabase_chain()
        mock_ch = MagicMock()

        service = LinkedInAudiencePushService(
            linkedin_client=mock_client,
            supabase=mock_supabase,
            clickhouse=mock_ch,
        )

        members = _make_members(10, with_email=True)
        assert service._determine_strategy(members, "company") == "company"
        assert service._determine_strategy(members, "contact") == "contact"

    def test_auto_empty_members_defaults_to_company(self):
        """Should default to company for empty segment."""
        mock_client = AsyncMock(spec=LinkedInAdsClient)
        mock_supabase, _ = _mock_supabase_chain()
        mock_ch = MagicMock()

        service = LinkedInAudiencePushService(
            linkedin_client=mock_client,
            supabase=mock_supabase,
            clickhouse=mock_ch,
        )

        assert service._determine_strategy([], "auto") == "company"


# --- Push segment with company strategy ---


class TestPushSegmentCompany:
    @pytest.mark.asyncio
    async def test_push_company_creates_dmp_and_streams(self):
        """Should create COMPANY DMP segment and stream company domains."""
        mock_client = AsyncMock(spec=LinkedInAdsClient)
        mock_client.create_dmp_segment.return_value = {
            "id": "dmp-seg-123"
        }
        mock_client.stream_companies.return_value = {
            "total_sent": 5,
            "batches_completed": 1,
            "errors": [],
        }

        mock_supabase, _ = _mock_supabase_chain(select_data=None)
        members = _make_members(5, with_email=False, with_domain=True)
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_clickhouse_query(members)

        service = LinkedInAudiencePushService(
            linkedin_client=mock_client,
            supabase=mock_supabase,
            clickhouse=mock_ch,
        )

        result = await service.push_segment(
            segment_id="seg-1",
            tenant_id="tenant-1",
            account_id=507404993,
            strategy="company",
        )

        assert result.segment_type == "COMPANY"
        assert result.total_uploaded == 5
        assert result.status == "building"
        mock_client.create_dmp_segment.assert_called_once_with(
            account_id=507404993,
            name="PaidEdge: seg-1",
            segment_type="COMPANY",
        )
        mock_client.stream_companies.assert_called_once()
        companies = mock_client.stream_companies.call_args.kwargs["companies"]
        assert all("companyDomain" in c for c in companies)


# --- Push segment with contact strategy ---


class TestPushSegmentContact:
    @pytest.mark.asyncio
    async def test_push_contact_creates_user_dmp_and_streams(self):
        """Should create USER DMP segment and stream hashed emails."""
        mock_client = AsyncMock(spec=LinkedInAdsClient)
        mock_client.create_dmp_segment.return_value = {
            "id": "dmp-seg-456"
        }
        mock_client.stream_contacts.return_value = {
            "total_sent": 8,
            "batches_completed": 1,
            "errors": [],
        }

        mock_supabase, _ = _mock_supabase_chain(select_data=None)
        members = _make_members(8, with_email=True)
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_clickhouse_query(members)

        service = LinkedInAudiencePushService(
            linkedin_client=mock_client,
            supabase=mock_supabase,
            clickhouse=mock_ch,
        )

        result = await service.push_segment(
            segment_id="seg-2",
            tenant_id="tenant-1",
            account_id=507404993,
            strategy="contact",
        )

        assert result.segment_type == "USER"
        assert result.total_uploaded == 8
        assert result.status == "building"
        mock_client.create_dmp_segment.assert_called_once_with(
            account_id=507404993,
            name="PaidEdge: seg-2",
            segment_type="USER",
        )
        mock_client.stream_contacts.assert_called_once()
        emails = mock_client.stream_contacts.call_args.kwargs["emails"]
        assert len(emails) == 8

    @pytest.mark.asyncio
    async def test_push_contact_skips_members_without_email(self):
        """Should only push members that have emails."""
        mock_client = AsyncMock(spec=LinkedInAdsClient)
        mock_client.create_dmp_segment.return_value = {"id": "dmp-seg-789"}
        mock_client.stream_contacts.return_value = {
            "total_sent": 3,
            "batches_completed": 1,
            "errors": [],
        }

        mock_supabase, _ = _mock_supabase_chain(select_data=None)
        members = _make_members(3, with_email=True) + _make_members(
            2, with_email=False
        )
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_clickhouse_query(members)

        service = LinkedInAudiencePushService(
            linkedin_client=mock_client,
            supabase=mock_supabase,
            clickhouse=mock_ch,
        )

        result = await service.push_segment(
            segment_id="seg-3",
            tenant_id="tenant-1",
            account_id=507404993,
            strategy="contact",
        )

        emails = mock_client.stream_contacts.call_args.kwargs["emails"]
        assert len(emails) == 3
        assert result.total_uploaded == 3


# --- Auto strategy ---


class TestAutoStrategy:
    @pytest.mark.asyncio
    async def test_auto_selects_contact_and_pushes(self):
        """Auto should select contact when >50% have emails."""
        mock_client = AsyncMock(spec=LinkedInAdsClient)
        mock_client.create_dmp_segment.return_value = {"id": "dmp-auto-1"}
        mock_client.stream_contacts.return_value = {
            "total_sent": 8,
            "batches_completed": 1,
            "errors": [],
        }

        mock_supabase, _ = _mock_supabase_chain(select_data=None)
        # 8 with email, 2 without → 80% > 50%
        members = _make_members(8, with_email=True) + _make_members(
            2, with_email=False
        )
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_clickhouse_query(members)

        service = LinkedInAudiencePushService(
            linkedin_client=mock_client,
            supabase=mock_supabase,
            clickhouse=mock_ch,
        )

        result = await service.push_segment(
            segment_id="seg-auto",
            tenant_id="tenant-1",
            account_id=507404993,
        )

        assert result.segment_type == "USER"
        mock_client.stream_contacts.assert_called_once()

    @pytest.mark.asyncio
    async def test_auto_selects_company_and_pushes(self):
        """Auto should select company when <=50% have emails."""
        mock_client = AsyncMock(spec=LinkedInAdsClient)
        mock_client.create_dmp_segment.return_value = {"id": "dmp-auto-2"}
        mock_client.stream_companies.return_value = {
            "total_sent": 4,
            "batches_completed": 1,
            "errors": [],
        }

        mock_supabase, _ = _mock_supabase_chain(select_data=None)
        # 2 with email, 8 without → 20% < 50%
        members = _make_members(2, with_email=True, with_domain=True) + _make_members(
            8, with_email=False, with_domain=True
        )
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_clickhouse_query(members)

        service = LinkedInAudiencePushService(
            linkedin_client=mock_client,
            supabase=mock_supabase,
            clickhouse=mock_ch,
        )

        result = await service.push_segment(
            segment_id="seg-auto-2",
            tenant_id="tenant-1",
            account_id=507404993,
        )

        assert result.segment_type == "COMPANY"
        mock_client.stream_companies.assert_called_once()


# --- Segment mapping persistence ---


class TestSegmentMapping:
    @pytest.mark.asyncio
    async def test_push_persists_mapping(self):
        """Should upsert linkedin_audience_mappings after push."""
        mock_client = AsyncMock(spec=LinkedInAdsClient)
        mock_client.create_dmp_segment.return_value = {"id": "dmp-map-1"}
        mock_client.stream_contacts.return_value = {
            "total_sent": 5,
            "batches_completed": 1,
            "errors": [],
        }

        mock_supabase, mock_chain = _mock_supabase_chain(select_data=None)
        members = _make_members(5, with_email=True)
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_clickhouse_query(members)

        service = LinkedInAudiencePushService(
            linkedin_client=mock_client,
            supabase=mock_supabase,
            clickhouse=mock_ch,
        )

        await service.push_segment(
            segment_id="seg-map",
            tenant_id="tenant-1",
            account_id=507404993,
            strategy="contact",
        )

        # Verify upsert was called
        mock_chain.upsert.assert_called()
        upsert_data = mock_chain.upsert.call_args[0][0]
        assert upsert_data["organization_id"] == "tenant-1"
        assert upsert_data["paidedge_segment_id"] == "seg-map"
        assert upsert_data["linkedin_dmp_segment_id"] == "dmp-map-1"
        assert upsert_data["segment_type"] == "USER"
        assert upsert_data["last_upload_count"] == 5

    @pytest.mark.asyncio
    async def test_reuses_existing_dmp_segment(self):
        """Should not create new DMP segment if mapping already exists."""
        mock_client = AsyncMock(spec=LinkedInAdsClient)
        mock_client.stream_contacts.return_value = {
            "total_sent": 5,
            "batches_completed": 1,
            "errors": [],
        }

        existing_mapping = {
            "linkedin_dmp_segment_id": "existing-dmp-999",
            "segment_type": "USER",
            "last_upload_count": 3,
            "last_synced_at": "2026-03-24T00:00:00Z",
        }
        mock_supabase, _ = _mock_supabase_chain(select_data=existing_mapping)
        members = _make_members(5, with_email=True)
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_clickhouse_query(members)

        service = LinkedInAudiencePushService(
            linkedin_client=mock_client,
            supabase=mock_supabase,
            clickhouse=mock_ch,
        )

        result = await service.push_segment(
            segment_id="seg-existing",
            tenant_id="tenant-1",
            account_id=507404993,
            strategy="contact",
        )

        # Should NOT create a new DMP segment
        mock_client.create_dmp_segment.assert_not_called()
        # Should stream to existing segment
        assert mock_client.stream_contacts.call_args.kwargs["segment_id"] == "existing-dmp-999"
        assert result.segment_id == "existing-dmp-999"


# --- Incremental refresh ---


class TestRefreshSegment:
    @pytest.mark.asyncio
    async def test_refresh_existing_segment(self):
        """Should re-stream data to existing DMP segment."""
        mock_client = AsyncMock(spec=LinkedInAdsClient)
        mock_client.stream_contacts.return_value = {
            "total_sent": 10,
            "batches_completed": 1,
            "errors": [],
        }

        existing_mapping = {
            "linkedin_dmp_segment_id": "dmp-refresh-1",
            "segment_type": "USER",
            "last_upload_count": 8,
            "last_synced_at": "2026-03-24T00:00:00Z",
        }
        mock_supabase, mock_chain = _mock_supabase_chain(
            select_data=existing_mapping
        )
        members = _make_members(10, with_email=True)
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_clickhouse_query(members)

        service = LinkedInAudiencePushService(
            linkedin_client=mock_client,
            supabase=mock_supabase,
            clickhouse=mock_ch,
        )

        result = await service.refresh_segment(
            segment_id="seg-refresh",
            tenant_id="tenant-1",
            account_id=507404993,
        )

        assert result.status == "updating"
        assert result.total_uploaded == 10
        assert result.segment_id == "dmp-refresh-1"
        mock_client.create_dmp_segment.assert_not_called()

    @pytest.mark.asyncio
    async def test_refresh_no_mapping_does_full_push(self):
        """Should fall back to full push if no existing mapping."""
        mock_client = AsyncMock(spec=LinkedInAdsClient)
        mock_client.create_dmp_segment.return_value = {"id": "dmp-new-1"}
        mock_client.stream_companies.return_value = {
            "total_sent": 5,
            "batches_completed": 1,
            "errors": [],
        }

        mock_supabase, _ = _mock_supabase_chain(select_data=None)
        members = _make_members(5, with_email=False, with_domain=True)
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_clickhouse_query(members)

        service = LinkedInAudiencePushService(
            linkedin_client=mock_client,
            supabase=mock_supabase,
            clickhouse=mock_ch,
        )

        result = await service.refresh_segment(
            segment_id="seg-new",
            tenant_id="tenant-1",
            account_id=507404993,
        )

        assert result.status == "building"
        mock_client.create_dmp_segment.assert_called_once()


# --- Sync status ---


class TestGetSyncStatus:
    @pytest.mark.asyncio
    async def test_status_with_existing_mapping(self):
        """Should fetch live status from LinkedIn and update mapping."""
        mock_client = AsyncMock(spec=LinkedInAdsClient)
        mock_client.get_dmp_segment_status.return_value = LinkedInDMPSegment(
            id="dmp-status-1",
            name="PaidEdge: seg-status",
            type="USER",
            status="READY",
            matched_member_count=450,
            destination_segment_id="urn:li:adSegment:99999",
            account_urn="urn:li:sponsoredAccount:507404993",
        )

        existing_mapping = {
            "linkedin_dmp_segment_id": "dmp-status-1",
            "segment_type": "USER",
            "last_upload_count": 500,
            "last_synced_at": "2026-03-24T12:00:00Z",
        }
        mock_supabase, _ = _mock_supabase_chain(
            select_data=existing_mapping
        )

        service = LinkedInAudiencePushService(
            linkedin_client=mock_client,
            supabase=mock_supabase,
            clickhouse=MagicMock(),
        )

        result = await service.get_sync_status(
            segment_id="seg-status",
            tenant_id="tenant-1",
        )

        assert result["status"] == "READY"
        assert result["matched_count"] == 450
        assert result["ad_segment_urn"] == "urn:li:adSegment:99999"
        assert result["linkedin_segment_id"] == "dmp-status-1"

    @pytest.mark.asyncio
    async def test_status_no_mapping(self):
        """Should return not_synced when no mapping exists."""
        mock_client = AsyncMock(spec=LinkedInAdsClient)
        mock_supabase, _ = _mock_supabase_chain(select_data=None)

        service = LinkedInAudiencePushService(
            linkedin_client=mock_client,
            supabase=mock_supabase,
            clickhouse=MagicMock(),
        )

        result = await service.get_sync_status(
            segment_id="seg-none",
            tenant_id="tenant-1",
        )

        assert result["status"] == "not_synced"
        assert result["linkedin_segment_id"] is None
        mock_client.get_dmp_segment_status.assert_not_called()


# --- Small segment warning ---


class TestSmallSegmentWarning:
    @pytest.mark.asyncio
    async def test_small_segment_still_pushes_with_warning(self):
        """Segments with <300 members should still push but log a warning."""
        mock_client = AsyncMock(spec=LinkedInAdsClient)
        mock_client.create_dmp_segment.return_value = {"id": "dmp-small"}
        mock_client.stream_contacts.return_value = {
            "total_sent": 50,
            "batches_completed": 1,
            "errors": [],
        }

        mock_supabase, _ = _mock_supabase_chain(select_data=None)
        members = _make_members(50, with_email=True)
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_clickhouse_query(members)

        service = LinkedInAudiencePushService(
            linkedin_client=mock_client,
            supabase=mock_supabase,
            clickhouse=mock_ch,
        )

        result = await service.push_segment(
            segment_id="seg-small",
            tenant_id="tenant-1",
            account_id=507404993,
            strategy="contact",
        )

        # Should still push despite small size
        assert result.total_uploaded == 50
        assert result.status == "building"
        mock_client.stream_contacts.assert_called_once()

    def test_min_audience_size_constant(self):
        """Minimum audience size should be 300."""
        assert _MIN_AUDIENCE_SIZE == 300


# --- API endpoint tests ---


_FAKE_JWT_PAYLOAD = {
    "sub": "user-1",
    "email": "test@test.com",
    "aud": "authenticated",
}


class TestAPIEndpoints:
    @pytest.mark.asyncio
    async def test_push_endpoint(self):
        """POST /audiences/{id}/push/linkedin should trigger push."""
        from fastapi.testclient import TestClient

        from app.dependencies import (
            get_clickhouse,
            get_current_user,
            get_supabase,
            get_tenant,
        )
        from app.main import app

        mock_result = {
            "segment_id": "dmp-api-1",
            "segment_type": "USER",
            "total_uploaded": 10,
            "batches_completed": 1,
            "status": "building",
            "matched_count": None,
            "ad_segment_urn": None,
        }

        app.dependency_overrides[get_tenant] = lambda: MagicMock(id="tenant-1")
        app.dependency_overrides[get_current_user] = lambda: MagicMock(id="user-1")
        app.dependency_overrides[get_supabase] = lambda: MagicMock()
        app.dependency_overrides[get_clickhouse] = lambda: MagicMock()

        try:
            with (
                patch(
                    "app.auth.middleware.jwt.decode",
                    return_value=_FAKE_JWT_PAYLOAD,
                ),
                patch(
                    "app.audiences.router.LinkedInAdsClient"
                ) as mock_client_cls,
                patch(
                    "app.audiences.router.LinkedInAudiencePushService"
                ) as mock_service_cls,
            ):
                mock_client = AsyncMock()
                mock_client.get_selected_account_id.return_value = 507404993
                mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                mock_client.__aexit__ = AsyncMock(return_value=None)
                mock_client_cls.return_value = mock_client

                mock_service = AsyncMock()
                mock_service.push_segment.return_value = MagicMock(
                    model_dump=MagicMock(return_value=mock_result)
                )
                mock_service_cls.return_value = mock_service

                client = TestClient(app)
                resp = client.post(
                    "/audiences/seg-1/push/linkedin",
                    headers={"Authorization": "Bearer fake-token"},
                )

            assert resp.status_code == 200
            data = resp.json()
            assert data["segment_type"] == "USER"
            assert data["status"] == "building"
        finally:
            app.dependency_overrides.clear()

    @pytest.mark.asyncio
    async def test_status_endpoint(self):
        """GET /audiences/{id}/push/linkedin/status should return status."""
        from fastapi.testclient import TestClient

        from app.dependencies import (
            get_clickhouse,
            get_current_user,
            get_supabase,
            get_tenant,
        )
        from app.main import app

        mock_status = {
            "linkedin_segment_id": "dmp-api-1",
            "status": "READY",
            "matched_count": 450,
            "ad_segment_urn": "urn:li:adSegment:99999",
            "last_synced_at": "2026-03-24T12:00:00Z",
        }

        app.dependency_overrides[get_tenant] = lambda: MagicMock(id="tenant-1")
        app.dependency_overrides[get_current_user] = lambda: MagicMock(id="user-1")
        app.dependency_overrides[get_supabase] = lambda: MagicMock()
        app.dependency_overrides[get_clickhouse] = lambda: MagicMock()

        try:
            with (
                patch(
                    "app.auth.middleware.jwt.decode",
                    return_value=_FAKE_JWT_PAYLOAD,
                ),
                patch(
                    "app.audiences.router.LinkedInAdsClient"
                ) as mock_client_cls,
                patch(
                    "app.audiences.router.LinkedInAudiencePushService"
                ) as mock_service_cls,
            ):
                mock_client = AsyncMock()
                mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                mock_client.__aexit__ = AsyncMock(return_value=None)
                mock_client_cls.return_value = mock_client

                mock_service = AsyncMock()
                mock_service.get_sync_status.return_value = mock_status
                mock_service_cls.return_value = mock_service

                client = TestClient(app)
                resp = client.get(
                    "/audiences/seg-1/push/linkedin/status",
                    headers={"Authorization": "Bearer fake-token"},
                )

            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] == "READY"
            assert data["matched_count"] == 450
        finally:
            app.dependency_overrides.clear()
