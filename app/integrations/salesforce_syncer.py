"""Salesforce CRM syncer — implements BaseCRMSyncer via SalesforceEngineClient (BJC-189).

Normalizes Salesforce Contact/Opportunity records into canonical
CRMContact/CRMOpportunity models. Uses SOQL queries via sfdc-engine-x's
query proxy with auto-pagination.

Key Salesforce ↔ HubSpot field differences:
  - Salesforce "Opportunity" = HubSpot "Deal"
  - StageName (not dealstage)
  - Amount (not amount string)
  - CloseDate (not closedate)
  - IsClosed / IsWon (booleans, not string props)
  - OpportunityContactRole junction (not deal→contact associations)
"""

import logging
from datetime import date, datetime
from typing import Any

from app.integrations.crm_base import BaseCRMSyncer
from app.integrations.crm_models import (
    CRMContact,
    CRMOpportunity,
    PipelineStage,
)
from app.integrations.salesforce_engine_x import SalesforceEngineClient

logger = logging.getLogger(__name__)

# Salesforce fields to request for each object type.
CONTACT_FIELDS = [
    "Id", "Email", "FirstName", "LastName", "Account.Name", "AccountId",
    "LeadSource", "LifecycleStage__c", "Status__c",
    "Title", "Phone", "NumberOfEmployees__c", "Industry__c",
    "LinkedIn_URL__c", "OwnerId",
    "UTM_Source__c", "UTM_Medium__c", "UTM_Campaign__c",
    "UTM_Term__c", "UTM_Content__c",
    "CreatedDate", "LastModifiedDate",
]

OPPORTUNITY_FIELDS = [
    "Id", "Name", "Amount", "CloseDate", "StageName",
    "ForecastCategoryName", "IsClosed", "IsWon",
    "AccountId", "LeadSource", "OwnerId",
    "UTM_Source__c", "UTM_Medium__c", "UTM_Campaign__c",
    "UTM_Term__c", "UTM_Content__c",
    "CreatedDate", "LastModifiedDate",
]

# Minimal contact fields for fallback if custom fields don't exist.
CONTACT_FIELDS_MINIMAL = [
    "Id", "Email", "FirstName", "LastName", "AccountId",
    "LeadSource", "Title", "Phone", "OwnerId",
    "CreatedDate", "LastModifiedDate",
]

OPPORTUNITY_FIELDS_MINIMAL = [
    "Id", "Name", "Amount", "CloseDate", "StageName",
    "IsClosed", "IsWon", "AccountId", "LeadSource", "OwnerId",
    "CreatedDate", "LastModifiedDate",
]


# --- Salesforce field parsing helpers ---


def parse_sf_float(value: Any) -> float | None:
    """Parse a Salesforce numeric field to float."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def parse_sf_date(value: Any) -> date | None:
    """Parse a Salesforce date string (YYYY-MM-DD) to date."""
    if value is None or value == "":
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    s = str(value)
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S.%fZ"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def parse_sf_datetime(value: Any) -> datetime | None:
    """Parse a Salesforce datetime string to datetime."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    s = str(value)
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def parse_sf_bool(value: Any) -> bool:
    """Parse a Salesforce boolean field."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() == "true"
    return bool(value) if value is not None else False


def parse_sf_int(value: Any) -> int | None:
    """Parse a Salesforce integer field."""
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


class SalesforceSyncer(BaseCRMSyncer):
    """CRM syncer that pulls data from Salesforce via sfdc-engine-x."""

    def __init__(self, engine_client: SalesforceEngineClient):
        self._client = engine_client

    def _normalize_contact(self, record: dict[str, Any]) -> CRMContact:
        """Convert a Salesforce Contact record to canonical CRMContact."""
        # Salesforce returns fields at top level (not nested in 'properties')
        r = record

        # Handle nested Account.Name
        account_name = None
        account = r.get("Account")
        if isinstance(account, dict):
            account_name = account.get("Name")

        return CRMContact(
            crm_contact_id=str(r.get("Id", "")),
            email=r.get("Email", ""),
            first_name=r.get("FirstName"),
            last_name=r.get("LastName"),
            company_name=account_name,
            account_id=r.get("AccountId"),
            lead_source=r.get("LeadSource"),
            lifecycle_stage=r.get("LifecycleStage__c"),
            lead_status=r.get("Status__c"),
            job_title=r.get("Title"),
            phone=r.get("Phone"),
            company_size=parse_sf_int(r.get("NumberOfEmployees__c")),
            industry=r.get("Industry__c"),
            linkedin_url=r.get("LinkedIn_URL__c"),
            owner_id=r.get("OwnerId"),
            utm_source=r.get("UTM_Source__c"),
            utm_medium=r.get("UTM_Medium__c"),
            utm_campaign=r.get("UTM_Campaign__c"),
            utm_term=r.get("UTM_Term__c"),
            utm_content=r.get("UTM_Content__c"),
            created_at=parse_sf_datetime(r.get("CreatedDate")),
            updated_at=parse_sf_datetime(r.get("LastModifiedDate")),
        )

    def _normalize_opportunity(
        self,
        record: dict[str, Any],
        contact_ids: list[str] | None = None,
    ) -> CRMOpportunity:
        """Convert a Salesforce Opportunity record to canonical CRMOpportunity."""
        r = record
        return CRMOpportunity(
            crm_opportunity_id=str(r.get("Id", "")),
            name=r.get("Name", ""),
            amount=parse_sf_float(r.get("Amount")),
            close_date=parse_sf_date(r.get("CloseDate")),
            stage=r.get("StageName", ""),
            pipeline=r.get("ForecastCategoryName"),
            is_closed=parse_sf_bool(r.get("IsClosed")),
            is_won=parse_sf_bool(r.get("IsWon")),
            account_id=r.get("AccountId"),
            lead_source=r.get("LeadSource"),
            contact_ids=contact_ids or [],
            owner_id=r.get("OwnerId"),
            utm_source=r.get("UTM_Source__c"),
            utm_medium=r.get("UTM_Medium__c"),
            utm_campaign=r.get("UTM_Campaign__c"),
            utm_term=r.get("UTM_Term__c"),
            utm_content=r.get("UTM_Content__c"),
            created_at=parse_sf_datetime(r.get("CreatedDate")),
            updated_at=parse_sf_datetime(r.get("LastModifiedDate")),
        )

    def _normalize_pipeline_stage(
        self,
        stage: dict[str, Any],
        display_order: int,
    ) -> PipelineStage:
        """Convert a Salesforce picklist value to canonical PipelineStage."""
        label = stage.get("label", stage.get("value", ""))
        value = stage.get("value", "")
        return PipelineStage(
            stage_id=value,
            label=label,
            display_order=display_order,
            is_closed=parse_sf_bool(stage.get("isClosed")),
            is_won=parse_sf_bool(stage.get("isWon")),
            probability=parse_sf_float(stage.get("defaultProbability")),
        )

    def _build_contact_soql(
        self,
        since: str | None = None,
        fields: list[str] | None = None,
    ) -> str:
        """Build SOQL query for Contact records."""
        field_list = fields or CONTACT_FIELDS_MINIMAL
        soql = f"SELECT {','.join(field_list)} FROM Contact"
        if since:
            soql += f" WHERE LastModifiedDate >= {since}"
        soql += " ORDER BY LastModifiedDate ASC"
        return soql

    def _build_opportunity_soql(
        self,
        since: str | None = None,
        fields: list[str] | None = None,
    ) -> str:
        """Build SOQL query for Opportunity records."""
        field_list = fields or OPPORTUNITY_FIELDS_MINIMAL
        soql = f"SELECT {','.join(field_list)} FROM Opportunity"
        if since:
            soql += f" WHERE LastModifiedDate >= {since}"
        soql += " ORDER BY LastModifiedDate ASC"
        return soql

    # --- BaseCRMSyncer implementation ---

    async def pull_contacts(
        self,
        client_id: str,
        since: str | None = None,
    ) -> list[CRMContact]:
        """Pull contacts modified since timestamp, auto-paginating."""
        soql = self._build_contact_soql(since=since)

        try:
            records = await self._client.query_all(client_id, soql)
        except Exception:
            # Fall back to minimal fields if custom fields don't exist
            logger.warning(
                "Contact query failed for client=%s, retrying with minimal fields",
                client_id,
            )
            soql = self._build_contact_soql(since=since, fields=CONTACT_FIELDS_MINIMAL)
            records = await self._client.query_all(client_id, soql)

        contacts = [self._normalize_contact(r) for r in records]
        logger.info(
            "Pulled %d contacts for client=%s (since=%s)",
            len(contacts), client_id, since,
        )
        return contacts

    async def pull_opportunities(
        self,
        client_id: str,
        since: str | None = None,
    ) -> list[CRMOpportunity]:
        """Pull opportunities modified since timestamp with contact associations."""
        soql = self._build_opportunity_soql(since=since)

        try:
            records = await self._client.query_all(client_id, soql)
        except Exception:
            logger.warning(
                "Opportunity query failed for client=%s, retrying with minimal fields",
                client_id,
            )
            soql = self._build_opportunity_soql(
                since=since, fields=OPPORTUNITY_FIELDS_MINIMAL,
            )
            records = await self._client.query_all(client_id, soql)

        if not records:
            return []

        # Build opp_id → [contact_id] map via OpportunityContactRole
        opp_ids = [str(r.get("Id", "")) for r in records if r.get("Id")]
        contact_role_map: dict[str, list[str]] = {}

        if opp_ids:
            try:
                contact_roles = await self._client.get_contact_roles(
                    client_id, opp_ids,
                )
                for role in contact_roles:
                    opp_id = str(role.get("OpportunityId", ""))
                    contact_id = str(role.get("ContactId", ""))
                    if opp_id and contact_id:
                        contact_role_map.setdefault(opp_id, []).append(contact_id)
            except Exception:
                logger.warning(
                    "OpportunityContactRole query failed for client=%s — "
                    "opportunities will not have contact_ids",
                    client_id,
                )

        opportunities = []
        for record in records:
            opp_id = str(record.get("Id", ""))
            contact_ids = contact_role_map.get(opp_id, [])
            opportunities.append(
                self._normalize_opportunity(record, contact_ids=contact_ids)
            )

        logger.info(
            "Pulled %d opportunities for client=%s (since=%s)",
            len(opportunities), client_id, since,
        )
        return opportunities

    async def pull_pipeline_stages(
        self,
        client_id: str,
    ) -> list[PipelineStage]:
        """Pull all Opportunity pipeline stages (StageName picklist)."""
        data = await self._client.get_pipelines(
            client_id=client_id,
            object_name="Opportunity",
            field_name="StageName",
        )

        raw_stages = data.get("stages", [])
        stages = [
            self._normalize_pipeline_stage(s, i)
            for i, s in enumerate(raw_stages)
        ]

        logger.info("Pulled %d pipeline stages for client=%s", len(stages), client_id)
        return stages

    async def push_lead(
        self,
        client_id: str,
        lead: dict,
        attribution: dict | None = None,
    ) -> str:
        """Push a lead as a Salesforce Contact. Returns created record ID."""
        properties = {
            "Email": lead.get("email", ""),
            "FirstName": lead.get("first_name", ""),
            "LastName": lead.get("last_name", ""),
        }
        if lead.get("company_name"):
            properties["Company"] = lead["company_name"]
        if attribution:
            properties["LeadSource"] = attribution.get("source", "PaidEdge")

        results = await self._client.push_records(
            client_id=client_id,
            object_type="Contact",
            records=[properties],
        )
        if results:
            return str(results[0].get("id", results[0].get("Id", "")))
        return ""

    async def check_connection(
        self,
        client_id: str,
    ) -> bool:
        """Check if Salesforce connection is active."""
        try:
            data = await self._client.get_connection(client_id)
            return data.get("status") == "connected"
        except Exception:
            logger.warning("Connection check failed for client=%s", client_id)
            return False
