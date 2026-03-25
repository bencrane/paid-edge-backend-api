"""Tests for Meta audience push — segment sync bridge (BJC-161)."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.audiences.meta_push import (
    MetaAudienceSyncResult,
    compute_member_diff,
    extract_and_hash_members,
    sync_segment_to_meta,
)


def _mock_clickhouse_query(members):
    result = MagicMock()
    result.result_rows = [
        (m["entity_id"], m.get("email", ""), m.get("full_name", ""))
        for m in members
    ]
    return result


class TestSyncSegmentToMeta:
    @pytest.mark.asyncio
    async def test_first_sync_creates_audience_and_uploads(self):
        mock_supabase = MagicMock()
        # segment query
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.maybe_single.return_value.execute.return_value = MagicMock(
            data={
                "id": "seg-1",
                "name": "Test Segment",
                "organization_id": "org-1",
                "filter_config": {},
            }
        )
        mock_supabase.table.return_value.update.return_value.eq.return_value.execute.return_value = MagicMock()

        mock_clickhouse = MagicMock()
        mock_clickhouse.query.return_value = _mock_clickhouse_query([
            {"entity_id": "e1", "email": "a@b.com", "full_name": "John Doe"},
            {"entity_id": "e2", "email": "c@d.com", "full_name": "Jane Smith"},
        ])

        mock_meta_client = AsyncMock()
        mock_meta_client.create_custom_audience.return_value = {"id": "aud_new"}
        mock_meta_client.replace_users.return_value = {
            "total_received": 2, "total_invalid": 0
        }

        result = await sync_segment_to_meta(
            "seg-1", "org-1", mock_supabase, mock_clickhouse, mock_meta_client
        )
        assert result.audience_id == "aud_new"
        assert result.members_sent == 2
        assert result.status == "success"

    @pytest.mark.asyncio
    async def test_sync_reuses_existing_audience(self):
        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.maybe_single.return_value.execute.return_value = MagicMock(
            data={
                "id": "seg-1",
                "name": "Test",
                "organization_id": "org-1",
                "filter_config": {
                    "meta_push": {
                        "audience_id": "aud_existing",
                        "previous_member_ids": ["e1"],
                    }
                },
            }
        )
        mock_supabase.table.return_value.update.return_value.eq.return_value.execute.return_value = MagicMock()

        mock_clickhouse = MagicMock()
        mock_clickhouse.query.return_value = _mock_clickhouse_query([
            {"entity_id": "e1", "email": "a@b.com", "full_name": "John Doe"},
        ])

        mock_meta_client = AsyncMock()
        mock_meta_client.replace_users.return_value = {
            "total_received": 1, "total_invalid": 0
        }

        result = await sync_segment_to_meta(
            "seg-1", "org-1", mock_supabase, mock_clickhouse, mock_meta_client
        )
        assert result.audience_id == "aud_existing"
        # Should not create new audience
        mock_meta_client.create_custom_audience.assert_not_called()

    @pytest.mark.asyncio
    async def test_sync_empty_segment(self):
        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.maybe_single.return_value.execute.return_value = MagicMock(
            data={"id": "seg-1", "name": "Empty", "organization_id": "org-1", "filter_config": {}}
        )

        mock_clickhouse = MagicMock()
        mock_clickhouse.query.return_value = _mock_clickhouse_query([])

        mock_meta_client = AsyncMock()
        mock_meta_client.create_custom_audience.return_value = {"id": "aud_new"}

        result = await sync_segment_to_meta(
            "seg-1", "org-1", mock_supabase, mock_clickhouse, mock_meta_client
        )
        assert result.status == "skipped"


class TestExtractAndHashMembers:
    @pytest.mark.asyncio
    async def test_extract_and_hash(self):
        members = [
            {"entity_id": "e1", "email": "test@example.com", "full_name": "John Doe"},
        ]
        schema, data = await extract_and_hash_members(members)
        assert schema == ["EMAIL", "FN", "LN", "EXTERN_ID"]
        assert len(data) == 1
        assert len(data[0]) == 4


class TestComputeMemberDiff:
    @pytest.mark.asyncio
    async def test_diff(self):
        current = {"e1", "e2", "e3"}
        previous = {"e2", "e3", "e4"}
        to_add, to_remove = await compute_member_diff(current, previous)
        assert to_add == {"e1"}
        assert to_remove == {"e4"}


class TestSyncResultModel:
    def test_default_values(self):
        r = MetaAudienceSyncResult()
        assert r.status == "success"
        assert r.members_sent == 0
