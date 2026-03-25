"""LinkedIn audience push — wire PaidEdge segments to Matched Audiences (BJC-135)."""

import logging
from datetime import UTC, datetime

from app.integrations.linkedin import LinkedInAdsClient
from app.integrations.linkedin_models import LinkedInAudienceSyncResult

logger = logging.getLogger(__name__)

# Minimum audience size for LinkedIn matching
_MIN_AUDIENCE_SIZE = 300


class LinkedInAudiencePushService:
    """Pushes PaidEdge audience segments to LinkedIn Matched Audiences."""

    def __init__(self, linkedin_client: LinkedInAdsClient, supabase, clickhouse):
        self.linkedin = linkedin_client
        self.supabase = supabase
        self.clickhouse = clickhouse

    def _read_segment_members(
        self, segment_id: str, tenant_id: str
    ) -> list[dict]:
        """Read segment members from ClickHouse."""
        query = (
            "SELECT member_id, email, company_domain, company_name, full_name "
            "FROM paid_edge.audience_segment_members "
            "WHERE segment_id = %(segment_id)s AND tenant_id = %(tenant_id)s"
        )
        result = self.clickhouse.query(
            query,
            parameters={
                "segment_id": segment_id,
                "tenant_id": tenant_id,
            },
        )
        columns = result.column_names
        return [dict(zip(columns, row)) for row in result.result_rows]

    def _determine_strategy(
        self, members: list[dict], strategy: str
    ) -> str:
        """Determine push strategy based on member data.

        Auto: if >50% of members have emails → contact, else → company.
        """
        if strategy != "auto":
            return strategy

        if not members:
            return "company"

        with_email = sum(
            1 for m in members if m.get("email")
        )
        ratio = with_email / len(members)
        return "contact" if ratio > 0.5 else "company"

    async def _get_existing_mapping(
        self, segment_id: str, tenant_id: str
    ) -> dict | None:
        """Look up existing PaidEdge ↔ LinkedIn segment mapping."""
        res = (
            self.supabase.table("linkedin_audience_mappings")
            .select("*")
            .eq("organization_id", tenant_id)
            .eq("paidedge_segment_id", segment_id)
            .maybe_single()
            .execute()
        )
        return res.data

    async def _upsert_mapping(
        self,
        tenant_id: str,
        segment_id: str,
        linkedin_dmp_segment_id: str,
        segment_type: str,
        upload_count: int,
        status: str = "building",
        matched_count: int | None = None,
        ad_segment_urn: str | None = None,
    ) -> None:
        """Upsert the segment mapping in Supabase."""
        self.supabase.table("linkedin_audience_mappings").upsert(
            {
                "organization_id": tenant_id,
                "paidedge_segment_id": segment_id,
                "linkedin_dmp_segment_id": linkedin_dmp_segment_id,
                "linkedin_ad_segment_urn": ad_segment_urn,
                "segment_type": segment_type,
                "last_synced_at": datetime.now(UTC).isoformat(),
                "last_upload_count": upload_count,
                "matched_count": matched_count,
                "status": status,
            },
            on_conflict="organization_id,paidedge_segment_id",
        ).execute()

    async def push_segment(
        self,
        segment_id: str,
        tenant_id: str,
        account_id: int,
        strategy: str = "auto",
    ) -> LinkedInAudienceSyncResult:
        """Push a PaidEdge audience segment to LinkedIn.

        Steps:
        1. Read segment members from ClickHouse
        2. Determine push strategy (company vs contact vs auto)
        3. Create or reuse LinkedIn DMP segment
        4. Stream data to LinkedIn
        5. Return sync result
        """
        members = self._read_segment_members(segment_id, tenant_id)
        resolved_strategy = self._determine_strategy(members, strategy)

        if len(members) < _MIN_AUDIENCE_SIZE:
            logger.warning(
                "Segment %s has only %d members (<%d minimum). "
                "LinkedIn matching may return few or no results.",
                segment_id,
                len(members),
                _MIN_AUDIENCE_SIZE,
            )

        # Check for existing mapping
        mapping = await self._get_existing_mapping(segment_id, tenant_id)

        if mapping:
            dmp_segment_id = mapping["linkedin_dmp_segment_id"]
        else:
            segment_type = (
                "COMPANY" if resolved_strategy == "company" else "USER"
            )
            dmp_result = await self.linkedin.create_dmp_segment(
                account_id=account_id,
                name=f"PaidEdge: {segment_id}",
                segment_type=segment_type,
            )
            dmp_segment_id = dmp_result.get(
                "id", dmp_result.get("urn", "")
            )

        # Stream data
        if resolved_strategy == "company":
            companies = [
                {"companyDomain": m["company_domain"]}
                for m in members
                if m.get("company_domain")
            ]
            upload_result = await self.linkedin.stream_companies(
                segment_id=dmp_segment_id,
                companies=companies,
            )
            upload_count = upload_result["total_sent"]
        else:
            emails = [
                m["email"] for m in members if m.get("email")
            ]
            upload_result = await self.linkedin.stream_contacts(
                segment_id=dmp_segment_id,
                emails=emails,
            )
            upload_count = upload_result["total_sent"]

        segment_type = (
            "COMPANY" if resolved_strategy == "company" else "USER"
        )

        # Persist mapping
        await self._upsert_mapping(
            tenant_id=tenant_id,
            segment_id=segment_id,
            linkedin_dmp_segment_id=dmp_segment_id,
            segment_type=segment_type,
            upload_count=upload_count,
            status="building",
        )

        return LinkedInAudienceSyncResult(
            segment_id=dmp_segment_id,
            segment_type=segment_type,
            total_uploaded=upload_count,
            batches_completed=upload_result["batches_completed"],
            status="building",
        )

    async def refresh_segment(
        self,
        segment_id: str,
        tenant_id: str,
        account_id: int,
    ) -> LinkedInAudienceSyncResult:
        """Incremental refresh: detect adds/removes since last sync, stream deltas."""
        mapping = await self._get_existing_mapping(segment_id, tenant_id)
        if not mapping:
            return await self.push_segment(
                segment_id, tenant_id, account_id
            )

        dmp_segment_id = mapping["linkedin_dmp_segment_id"]
        segment_type = mapping["segment_type"]

        # Read current members
        members = self._read_segment_members(segment_id, tenant_id)

        if segment_type == "COMPANY":
            companies = [
                {"companyDomain": m["company_domain"]}
                for m in members
                if m.get("company_domain")
            ]
            upload_result = await self.linkedin.stream_companies(
                segment_id=dmp_segment_id,
                companies=companies,
            )
            upload_count = upload_result["total_sent"]
        else:
            emails = [
                m["email"] for m in members if m.get("email")
            ]
            upload_result = await self.linkedin.stream_contacts(
                segment_id=dmp_segment_id,
                emails=emails,
            )
            upload_count = upload_result["total_sent"]

        # Update mapping
        await self._upsert_mapping(
            tenant_id=tenant_id,
            segment_id=segment_id,
            linkedin_dmp_segment_id=dmp_segment_id,
            segment_type=segment_type,
            upload_count=upload_count,
            status="updating",
        )

        return LinkedInAudienceSyncResult(
            segment_id=dmp_segment_id,
            segment_type=segment_type,
            total_uploaded=upload_count,
            batches_completed=upload_result["batches_completed"],
            status="updating",
        )

    async def get_sync_status(
        self,
        segment_id: str,
        tenant_id: str,
    ) -> dict:
        """Check LinkedIn segment status for a PaidEdge segment."""
        mapping = await self._get_existing_mapping(segment_id, tenant_id)
        if not mapping:
            return {
                "linkedin_segment_id": None,
                "status": "not_synced",
                "matched_count": None,
                "last_synced_at": None,
            }

        dmp_segment_id = mapping["linkedin_dmp_segment_id"]

        # Fetch live status from LinkedIn
        segment_status = await self.linkedin.get_dmp_segment_status(
            dmp_segment_id
        )

        # Update mapping with latest status
        await self._upsert_mapping(
            tenant_id=tenant_id,
            segment_id=segment_id,
            linkedin_dmp_segment_id=dmp_segment_id,
            segment_type=mapping["segment_type"],
            upload_count=mapping.get("last_upload_count", 0),
            status=segment_status.status.lower(),
            matched_count=segment_status.matched_member_count,
            ad_segment_urn=segment_status.destination_segment_id,
        )

        return {
            "linkedin_segment_id": dmp_segment_id,
            "status": segment_status.status,
            "matched_count": segment_status.matched_member_count,
            "ad_segment_urn": segment_status.destination_segment_id,
            "last_synced_at": mapping.get("last_synced_at"),
        }
