"""Google Ads Customer Match — audience upload via OfflineUserDataJobService (BJC-145)."""

import asyncio
import hashlib
import logging
import re
from functools import partial as functools_partial

from app.integrations.google_ads import GoogleAdsService

logger = logging.getLogger(__name__)

BATCH_SIZE = 10_000  # Max 100K per request; 10K balances memory vs call count


class GoogleAdsCustomerMatchService:
    """Manages Customer Match audience lists via OfflineUserDataJobService."""

    def __init__(self, service: GoogleAdsService):
        self.service = service
        self.customer_id = service.customer_id

    async def create_user_list(self, list_name: str, description: str = "") -> str:
        """Create a CRM-based user list. Returns the user_list resource name."""
        operation = self.service._get_type("UserListOperation")
        user_list = operation.create
        user_list.name = list_name
        user_list.description = description
        user_list.crm_based_user_list.upload_key_type = (
            self.service.enums.CustomerMatchUploadKeyTypeEnum.CONTACT_INFO
        )
        user_list.membership_life_span = 10000  # ~27 years

        response = await self.service.mutate("UserListService", [operation])
        resource_name = response.results[0].resource_name
        logger.info("Created Customer Match list: %s", resource_name)
        return resource_name

    async def upload_members(
        self,
        user_list_resource_name: str,
        members: list[dict],
    ) -> dict:
        """Upload hashed member data via OfflineUserDataJobService.

        Each member dict can have: email, phone, first_name, last_name.
        All PII is SHA-256 hashed before upload.
        Returns job resource name and status.
        """
        job_service = self.service._get_service("OfflineUserDataJobService")
        loop = asyncio.get_event_loop()

        # Create the offline user data job
        job = self.service._get_type("OfflineUserDataJob")
        job.type_ = self.service.enums.OfflineUserDataJobTypeEnum.CUSTOMER_MATCH_USER_LIST
        job.customer_match_user_list_metadata.user_list = user_list_resource_name

        create_response = await loop.run_in_executor(
            None,
            functools_partial(
                job_service.create_offline_user_data_job,
                customer_id=self.customer_id,
                job=job,
            ),
        )
        job_resource_name = create_response.resource_name

        # Build operations with hashed PII
        operations = self._build_member_operations(members, "create")

        # Add operations in batches
        for i in range(0, len(operations), BATCH_SIZE):
            batch = operations[i : i + BATCH_SIZE]
            await loop.run_in_executor(
                None,
                functools_partial(
                    job_service.add_offline_user_data_job_operations,
                    resource_name=job_resource_name,
                    operations=batch,
                    enable_partial_failure=True,
                ),
            )
            logger.info(
                "Uploaded batch %d-%d of %d members",
                i, min(i + BATCH_SIZE, len(operations)), len(operations),
            )

        # Run the job (async — Google processes in background)
        await loop.run_in_executor(
            None,
            functools_partial(
                job_service.run_offline_user_data_job,
                resource_name=job_resource_name,
            ),
        )
        logger.info("Started Customer Match job: %s", job_resource_name)

        return {
            "job_resource_name": job_resource_name,
            "user_list_resource_name": user_list_resource_name,
            "member_count": len(members),
        }

    async def remove_members(
        self,
        user_list_resource_name: str,
        members: list[dict],
    ) -> dict:
        """Remove members from a Customer Match list."""
        job_service = self.service._get_service("OfflineUserDataJobService")
        loop = asyncio.get_event_loop()

        job = self.service._get_type("OfflineUserDataJob")
        job.type_ = self.service.enums.OfflineUserDataJobTypeEnum.CUSTOMER_MATCH_USER_LIST
        job.customer_match_user_list_metadata.user_list = user_list_resource_name

        create_response = await loop.run_in_executor(
            None,
            functools_partial(
                job_service.create_offline_user_data_job,
                customer_id=self.customer_id,
                job=job,
            ),
        )
        job_resource_name = create_response.resource_name

        operations = self._build_member_operations(members, "remove")

        for i in range(0, len(operations), BATCH_SIZE):
            batch = operations[i : i + BATCH_SIZE]
            await loop.run_in_executor(
                None,
                functools_partial(
                    job_service.add_offline_user_data_job_operations,
                    resource_name=job_resource_name,
                    operations=batch,
                    enable_partial_failure=True,
                ),
            )

        await loop.run_in_executor(
            None,
            functools_partial(
                job_service.run_offline_user_data_job,
                resource_name=job_resource_name,
            ),
        )

        return {
            "job_resource_name": job_resource_name,
            "removed_count": len(members),
        }

    async def check_job_status(self, job_resource_name: str) -> str:
        """Poll job status. Returns: PENDING, RUNNING, SUCCESS, FAILED, UNKNOWN."""
        query = f"""
            SELECT offline_user_data_job.resource_name,
                   offline_user_data_job.status,
                   offline_user_data_job.failure_reason
            FROM offline_user_data_job
            WHERE offline_user_data_job.resource_name = '{job_resource_name}'
        """
        rows = await self.service.search_stream(query)
        if not rows:
            return "UNKNOWN"
        return rows[0].offline_user_data_job.status.name

    async def get_user_list_size(self, user_list_resource_name: str) -> dict:
        """Query the current size and eligibility of a user list."""
        query = f"""
            SELECT user_list.resource_name,
                   user_list.name,
                   user_list.size_for_search,
                   user_list.size_for_display,
                   user_list.eligible_for_search,
                   user_list.eligible_for_display
            FROM user_list
            WHERE user_list.resource_name = '{user_list_resource_name}'
        """
        rows = await self.service.search_stream(query)
        if not rows:
            return {}
        row = rows[0]
        return {
            "name": row.user_list.name,
            "size_for_search": row.user_list.size_for_search,
            "size_for_display": row.user_list.size_for_display,
            "eligible_for_search": row.user_list.eligible_for_search,
            "eligible_for_display": row.user_list.eligible_for_display,
        }

    async def get_user_lists(self) -> list[dict]:
        """List all CRM-based user lists for the customer."""
        query = """
            SELECT user_list.resource_name,
                   user_list.name,
                   user_list.description,
                   user_list.size_for_search,
                   user_list.size_for_display,
                   user_list.membership_status
            FROM user_list
            WHERE user_list.type = 'CRM_BASED'
            AND user_list.membership_status = 'OPEN'
        """
        rows = await self.service.search_stream(query)
        return [
            {
                "resource_name": row.user_list.resource_name,
                "name": row.user_list.name,
                "description": row.user_list.description,
                "size_for_search": row.user_list.size_for_search,
                "size_for_display": row.user_list.size_for_display,
            }
            for row in rows
        ]

    async def close_user_list(self, user_list_resource_name: str) -> None:
        """Close a user list (cannot be undone)."""
        operation = self.service._get_type("UserListOperation")
        user_list = operation.update
        user_list.resource_name = user_list_resource_name
        user_list.membership_status = (
            self.service.enums.UserListMembershipStatusEnum.CLOSED
        )
        operation.update_mask.paths.append("membership_status")

        await self.service.mutate("UserListService", [operation])
        logger.info("Closed user list: %s", user_list_resource_name)

    def _build_member_operations(self, members: list[dict], action: str) -> list:
        """Build OfflineUserDataJobOperation list for create/remove."""
        operations = []
        for member in members:
            op = self.service._get_type("OfflineUserDataJobOperation")
            target = getattr(op, action)
            user_identifier = target.user_identifiers.add()
            if "email" in member:
                user_identifier.hashed_email = _hash_value(
                    member["email"].strip().lower()
                )
            if "phone" in member:
                user_identifier.hashed_phone_number = _hash_value(
                    _normalize_phone(member["phone"])
                )
            if "first_name" in member and "last_name" in member:
                address_info = user_identifier.address_info
                address_info.hashed_first_name = _hash_value(
                    member["first_name"].strip().lower()
                )
                address_info.hashed_last_name = _hash_value(
                    member["last_name"].strip().lower()
                )
            operations.append(op)
        return operations


def _hash_value(value: str) -> str:
    """SHA-256 hash a value for Customer Match upload."""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _normalize_phone(phone: str) -> str:
    """Strip to E.164 format (digits only with country code)."""
    digits = re.sub(r"[^\d+]", "", phone)
    if not digits.startswith("+"):
        digits = "+1" + digits  # default US
    return digits
