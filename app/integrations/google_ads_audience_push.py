"""Google Ads audience push — wire PaidEdge segments to Customer Match (BJC-146)."""

import logging
from datetime import datetime, timezone

from app.integrations.google_ads import GoogleAdsService
from app.integrations.google_ads_customer_match import GoogleAdsCustomerMatchService

logger = logging.getLogger(__name__)

# Min list size for Google Ads to target
MIN_TARGETABLE_SIZE = 1000


class GoogleAdsAudiencePushService:
    """Pushes PaidEdge audience segments to Google Ads Customer Match lists."""

    def __init__(self, service: GoogleAdsService):
        self.service = service
        self.cm_service = GoogleAdsCustomerMatchService(service)

    async def push_segment(
        self,
        segment: dict,
        members: list[dict],
        existing_push: dict | None = None,
        mode: str = "sync",
    ) -> dict:
        """Push a PaidEdge audience segment to Google Ads.

        Args:
            segment: PaidEdge segment dict with 'id', 'name' keys.
            members: List of member dicts with email/phone/first_name/last_name.
            existing_push: Previous push record if re-pushing (for incremental sync).
            mode: "sync" (direct API) or "csv" (export for manual upload).

        Returns:
            Push result with user_list details and job status.
        """
        if mode == "csv":
            return self._build_csv_export(segment, members)

        # Warn if under min size
        if len(members) < MIN_TARGETABLE_SIZE:
            logger.warning(
                "Segment %s has %d members (min %d for targeting). "
                "Uploading anyway — Google won't target until threshold met.",
                segment["id"], len(members), MIN_TARGETABLE_SIZE,
            )

        # Format members for Customer Match
        formatted = self._format_members_for_upload(members)
        if not formatted:
            return {
                "status": "skipped",
                "reason": "No members with valid identifiers",
                "segment_id": segment["id"],
            }

        # Create or reuse user list
        list_name = f"PaidEdge: {segment['name']}"
        if existing_push and existing_push.get("remote_list_id"):
            user_list_resource = existing_push["remote_list_id"]
            logger.info("Reusing existing user list: %s", user_list_resource)
        else:
            user_list_resource = await self.cm_service.create_user_list(
                list_name=list_name,
                description=f"Auto-synced from PaidEdge segment {segment['id']}",
            )

        # Determine members to upload (incremental vs full)
        if existing_push and existing_push.get("last_pushed_at"):
            members_to_upload = self._get_new_members(formatted, existing_push)
        else:
            members_to_upload = formatted

        if not members_to_upload:
            return {
                "status": "no_changes",
                "user_list_resource_name": user_list_resource,
                "segment_id": segment["id"],
            }

        # Upload members
        result = await self.cm_service.upload_members(
            user_list_resource_name=user_list_resource,
            members=members_to_upload,
        )

        return {
            "status": "pushed",
            "segment_id": segment["id"],
            "provider": "google_ads",
            "remote_list_id": user_list_resource,
            "job_resource_name": result["job_resource_name"],
            "member_count": result["member_count"],
            "pushed_at": datetime.now(timezone.utc).isoformat(),
            "below_min_size": len(members) < MIN_TARGETABLE_SIZE,
        }

    async def remove_stale_members(
        self,
        user_list_resource: str,
        members_to_remove: list[dict],
    ) -> dict:
        """Remove members who left the segment."""
        if not members_to_remove:
            return {"removed_count": 0}

        formatted = self._format_members_for_upload(members_to_remove)
        result = await self.cm_service.remove_members(
            user_list_resource_name=user_list_resource,
            members=formatted,
        )
        return result

    async def check_push_status(self, job_resource_name: str) -> dict:
        """Check the status of an audience push job."""
        status = await self.cm_service.check_job_status(job_resource_name)
        return {"job_resource_name": job_resource_name, "status": status}

    async def get_list_details(self, user_list_resource: str) -> dict:
        """Get size and eligibility for a pushed list."""
        return await self.cm_service.get_user_list_size(user_list_resource)

    async def find_existing_list(self, segment_name: str) -> str | None:
        """Find an existing PaidEdge user list by name."""
        list_name = f"PaidEdge: {segment_name}"
        user_lists = await self.cm_service.get_user_lists()
        for ul in user_lists:
            if ul["name"] == list_name:
                return ul["resource_name"]
        return None

    def _format_members_for_upload(self, members: list[dict]) -> list[dict]:
        """Transform PaidEdge contact records to Customer Match format."""
        formatted = []
        for m in members:
            entry = {}
            if m.get("email"):
                entry["email"] = m["email"]
            if m.get("phone"):
                entry["phone"] = m["phone"]
            if m.get("first_name"):
                entry["first_name"] = m["first_name"]
            if m.get("last_name"):
                entry["last_name"] = m["last_name"]
            if entry:  # at least one identifier
                formatted.append(entry)
        return formatted

    def _get_new_members(
        self, current_members: list[dict], existing_push: dict
    ) -> list[dict]:
        """Get members not in the previous push (by email)."""
        previous_emails = set(existing_push.get("uploaded_emails", []))
        return [
            m for m in current_members
            if m.get("email") and m["email"].lower() not in previous_emails
        ]

    def _build_csv_export(self, segment: dict, members: list[dict]) -> dict:
        """Build CSV export data for manual upload."""
        rows = []
        for m in members:
            row = {}
            if m.get("email"):
                row["Email"] = m["email"]
            if m.get("phone"):
                row["Phone"] = m["phone"]
            if m.get("first_name"):
                row["First Name"] = m["first_name"]
            if m.get("last_name"):
                row["Last Name"] = m["last_name"]
            if row:
                rows.append(row)

        return {
            "status": "csv_export",
            "segment_id": segment["id"],
            "rows": rows,
            "row_count": len(rows),
        }
