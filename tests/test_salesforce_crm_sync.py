"""Tests for Salesforce CRM sync Trigger.dev task (BJC-192)."""

from unittest.mock import AsyncMock, MagicMock, patch

from trigger.salesforce_crm_sync import (
    get_salesforce_connected_tenants,
    salesforce_crm_sync_task,
    sync_tenant_salesforce,
)


# --- Fixtures ---


SAMPLE_TENANT = {
    "org_id": "org-1",
    "salesforce_client_id": "cl-1",
    "last_salesforce_sync": "2026-03-20T00:00:00Z",
}

SAMPLE_TENANT_FIRST_SYNC = {
    "org_id": "org-2",
    "salesforce_client_id": "cl-2",
    "last_salesforce_sync": None,
}


def _mock_supabase(data=None):
    mock = MagicMock()
    result = MagicMock()
    result.data = data if data is not None else []

    chain = MagicMock()
    chain.select.return_value = chain
    chain.eq.return_value = chain
    chain.is_.return_value = chain
    chain.execute.return_value = result
    mock.table.return_value = chain

    rpc_chain = MagicMock()
    rpc_chain.execute.return_value = MagicMock()
    mock.rpc.return_value = rpc_chain

    return mock


# --- get_salesforce_connected_tenants ---


class TestGetSalesforceConnectedTenants:
    async def test_returns_connected_tenants(self):
        mock_sb = _mock_supabase(data=[
            {
                "organization_id": "org-1",
                "config": {
                    "status": "connected",
                    "salesforce_client_id": "cl-1",
                    "last_salesforce_sync": "2026-03-20T00:00:00Z",
                },
            },
            {
                "organization_id": "org-2",
                "config": {
                    "status": "disconnected",
                    "salesforce_client_id": "cl-2",
                },
            },
        ])

        tenants = await get_salesforce_connected_tenants(mock_sb)

        assert len(tenants) == 1
        assert tenants[0]["org_id"] == "org-1"
        assert tenants[0]["salesforce_client_id"] == "cl-1"

    async def test_skips_missing_client_id(self):
        mock_sb = _mock_supabase(data=[
            {
                "organization_id": "org-1",
                "config": {"status": "connected"},
            },
        ])

        tenants = await get_salesforce_connected_tenants(mock_sb)

        assert tenants == []

    async def test_empty_when_no_configs(self):
        mock_sb = _mock_supabase(data=[])
        tenants = await get_salesforce_connected_tenants(mock_sb)
        assert tenants == []


# --- sync_tenant_salesforce ---


class TestSyncTenantSalesforce:
    async def test_full_sync_success(self):
        mock_sb = _mock_supabase()
        mock_ch = MagicMock()
        mock_syncer = AsyncMock()

        # Mock syncer responses
        mock_syncer.check_connection.return_value = True
        mock_syncer.pull_contacts.return_value = [MagicMock() for _ in range(5)]
        mock_syncer.pull_opportunities.return_value = [MagicMock() for _ in range(3)]

        with patch("trigger.salesforce_crm_sync.sb_upsert_contacts", return_value=5), \
             patch("trigger.salesforce_crm_sync.sb_upsert_opportunities", return_value=3), \
             patch("trigger.salesforce_crm_sync.ch_insert_contacts", return_value=5), \
             patch("trigger.salesforce_crm_sync.ch_insert_opportunities", return_value=3):

            result = await sync_tenant_salesforce(
                tenant=SAMPLE_TENANT,
                syncer=mock_syncer,
                supabase=mock_sb,
                clickhouse=mock_ch,
            )

        assert result["status"] == "success"
        assert result["contacts_synced"] == 5
        assert result["opportunities_synced"] == 3
        assert result["tenant_id"] == "org-1"
        assert "duration_ms" in result

        # Verify incremental pull (since was passed)
        mock_syncer.pull_contacts.assert_called_once_with(
            "cl-1", since="2026-03-20T00:00:00Z",
        )
        mock_syncer.pull_opportunities.assert_called_once_with(
            "cl-1", since="2026-03-20T00:00:00Z",
        )

        # Verify last_sync_date updated
        mock_sb.rpc.assert_called_once()

    async def test_writes_to_supabase_first(self):
        """Verify Supabase (primary store) is written before ClickHouse."""
        mock_sb = _mock_supabase()
        mock_ch = MagicMock()
        mock_syncer = AsyncMock()
        mock_syncer.check_connection.return_value = True
        mock_syncer.pull_contacts.return_value = [MagicMock()]
        mock_syncer.pull_opportunities.return_value = [MagicMock()]

        call_order = []

        def sb_contacts_side_effect(*a, **kw):
            call_order.append("sb_contacts")
            return 1

        def sb_opps_side_effect(*a, **kw):
            call_order.append("sb_opps")
            return 1

        def ch_contacts_side_effect(*a, **kw):
            call_order.append("ch_contacts")
            return 1

        def ch_opps_side_effect(*a, **kw):
            call_order.append("ch_opps")
            return 1

        with patch("trigger.salesforce_crm_sync.sb_upsert_contacts", side_effect=sb_contacts_side_effect), \
             patch("trigger.salesforce_crm_sync.sb_upsert_opportunities", side_effect=sb_opps_side_effect), \
             patch("trigger.salesforce_crm_sync.ch_insert_contacts", side_effect=ch_contacts_side_effect), \
             patch("trigger.salesforce_crm_sync.ch_insert_opportunities", side_effect=ch_opps_side_effect):

            await sync_tenant_salesforce(
                tenant=SAMPLE_TENANT,
                syncer=mock_syncer,
                supabase=mock_sb,
                clickhouse=mock_ch,
            )

        # Supabase writes must come before ClickHouse writes
        assert call_order.index("sb_contacts") < call_order.index("ch_contacts")
        assert call_order.index("sb_opps") < call_order.index("ch_opps")

    async def test_crm_source_is_salesforce(self):
        """Verify crm_source='salesforce' is passed to writers."""
        mock_sb = _mock_supabase()
        mock_ch = MagicMock()
        mock_syncer = AsyncMock()
        mock_syncer.check_connection.return_value = True
        mock_syncer.pull_contacts.return_value = [MagicMock()]
        mock_syncer.pull_opportunities.return_value = []

        with patch("trigger.salesforce_crm_sync.sb_upsert_contacts", return_value=1) as sb_mock, \
             patch("trigger.salesforce_crm_sync.sb_upsert_opportunities", return_value=0), \
             patch("trigger.salesforce_crm_sync.ch_insert_contacts", return_value=1) as ch_mock, \
             patch("trigger.salesforce_crm_sync.ch_insert_opportunities", return_value=0):

            await sync_tenant_salesforce(
                tenant=SAMPLE_TENANT,
                syncer=mock_syncer,
                supabase=mock_sb,
                clickhouse=mock_ch,
            )

        # Check crm_source arg on Supabase writer
        sb_call_args = sb_mock.call_args
        assert sb_call_args[0][2] == "salesforce"

        # Check crm_source arg on ClickHouse writer
        ch_call_args = ch_mock.call_args
        assert ch_call_args[0][1] == "salesforce"

    async def test_skips_disconnected(self):
        mock_sb = _mock_supabase()
        mock_ch = MagicMock()
        mock_syncer = AsyncMock()
        mock_syncer.check_connection.return_value = False

        result = await sync_tenant_salesforce(
            tenant=SAMPLE_TENANT,
            syncer=mock_syncer,
            supabase=mock_sb,
            clickhouse=mock_ch,
        )

        assert result["status"] == "skipped_disconnected"
        mock_syncer.pull_contacts.assert_not_called()

    async def test_first_sync_no_since(self):
        mock_sb = _mock_supabase()
        mock_ch = MagicMock()
        mock_syncer = AsyncMock()
        mock_syncer.check_connection.return_value = True
        mock_syncer.pull_contacts.return_value = []
        mock_syncer.pull_opportunities.return_value = []

        with patch("trigger.salesforce_crm_sync.sb_upsert_contacts", return_value=0), \
             patch("trigger.salesforce_crm_sync.sb_upsert_opportunities", return_value=0), \
             patch("trigger.salesforce_crm_sync.ch_insert_contacts", return_value=0), \
             patch("trigger.salesforce_crm_sync.ch_insert_opportunities", return_value=0):

            result = await sync_tenant_salesforce(
                tenant=SAMPLE_TENANT_FIRST_SYNC,
                syncer=mock_syncer,
                supabase=mock_sb,
                clickhouse=mock_ch,
            )

        assert result["status"] == "success"
        # since=None for first sync
        mock_syncer.pull_contacts.assert_called_once_with("cl-2", since=None)


# --- salesforce_crm_sync_task ---


class TestSalesforceCrmSyncTask:
    async def test_full_flow(self):
        with (
            patch(
                "trigger.salesforce_crm_sync.get_supabase_client",
                return_value=MagicMock(),
            ),
            patch(
                "trigger.salesforce_crm_sync.get_clickhouse_client",
                return_value=MagicMock(),
            ),
            patch(
                "trigger.salesforce_crm_sync.get_salesforce_connected_tenants",
                new_callable=AsyncMock,
                return_value=[SAMPLE_TENANT, SAMPLE_TENANT_FIRST_SYNC],
            ),
            patch(
                "trigger.salesforce_crm_sync.sync_tenant_salesforce",
                new_callable=AsyncMock,
                return_value={
                    "task": "salesforce_crm_sync",
                    "tenant_id": "org-x",
                    "contacts_synced": 10,
                    "opportunities_synced": 5,
                    "status": "success",
                },
            ) as mock_sync,
            patch(
                "trigger.salesforce_crm_sync.SalesforceEngineClient",
            ) as mock_sfdc_cls,
        ):
            mock_sfdc = AsyncMock()
            mock_sfdc_cls.return_value.__aenter__ = AsyncMock(return_value=mock_sfdc)
            mock_sfdc_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            results = await salesforce_crm_sync_task()

        assert len(results) == 2
        assert mock_sync.call_count == 2

    async def test_per_tenant_error_isolation(self):
        call_count = 0

        async def sync_side_effect(**kwargs):
            nonlocal call_count
            call_count += 1
            if kwargs["tenant"]["org_id"] == "org-1":
                raise RuntimeError("Salesforce API down")
            return {
                "task": "salesforce_crm_sync",
                "tenant_id": "org-2",
                "contacts_synced": 5,
                "opportunities_synced": 2,
                "status": "success",
            }

        with (
            patch(
                "trigger.salesforce_crm_sync.get_supabase_client",
                return_value=MagicMock(),
            ),
            patch(
                "trigger.salesforce_crm_sync.get_clickhouse_client",
                return_value=MagicMock(),
            ),
            patch(
                "trigger.salesforce_crm_sync.get_salesforce_connected_tenants",
                new_callable=AsyncMock,
                return_value=[SAMPLE_TENANT, SAMPLE_TENANT_FIRST_SYNC],
            ),
            patch(
                "trigger.salesforce_crm_sync.sync_tenant_salesforce",
                side_effect=sync_side_effect,
            ),
            patch(
                "trigger.salesforce_crm_sync.SalesforceEngineClient",
            ) as mock_sfdc_cls,
        ):
            mock_sfdc = AsyncMock()
            mock_sfdc_cls.return_value.__aenter__ = AsyncMock(return_value=mock_sfdc)
            mock_sfdc_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            results = await salesforce_crm_sync_task()

        assert len(results) == 2
        assert results[0]["status"] == "error"
        assert results[0]["tenant_id"] == "org-1"
        assert results[1]["status"] == "success"

    async def test_no_tenants(self):
        with (
            patch(
                "trigger.salesforce_crm_sync.get_supabase_client",
                return_value=MagicMock(),
            ),
            patch(
                "trigger.salesforce_crm_sync.get_clickhouse_client",
                return_value=MagicMock(),
            ),
            patch(
                "trigger.salesforce_crm_sync.get_salesforce_connected_tenants",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "trigger.salesforce_crm_sync.SalesforceEngineClient",
            ) as mock_sfdc_cls,
        ):
            mock_sfdc = AsyncMock()
            mock_sfdc_cls.return_value.__aenter__ = AsyncMock(return_value=mock_sfdc)
            mock_sfdc_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            results = await salesforce_crm_sync_task()

        assert results == []
