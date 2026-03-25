# HubSpot CRM Integration — PaidEdge Design Doc

> **Project:** PaidEdge M4 — Analytics + Attribution
> **Author:** PaidEdge Engineering
> **Date:** 2026-03-25
> **Status:** Draft
> **Depends on:** hubspot-engine-x (Phases 1–5 complete, 35 endpoints)

---

## 1. Overview

PaidEdge needs HubSpot CRM data for the same two purposes as Salesforce:
1. **Read path (attribution):** Pull contacts, deals, pipeline stages, and associations to calculate ROAS.
2. **Write path (enrichment):** Push leads with UTM/campaign attribution, update properties, create associations.

PaidEdge calls `hubspot-engine-x` as a service-to-service consumer — same pattern as `DataEngineXClient`.

### Architecture

```
┌──────────────┐    Bearer token     ┌──────────────────┐    Nango OAuth    ┌──────────────┐
│   PaidEdge   │ ──────────────────→ │  hubspot-engine-x│ ───────────────→ │  HubSpot     │
│   Backend    │                     │  (FastAPI)       │                   │  Portal (per │
│              │ ←────────────────── │  35 endpoints    │ ←──────────────── │  client)     │
└──────────────┘    JSON responses   └──────────────────┘    HubSpot v3 API └──────────────┘
```

### Key Advantage Over Salesforce

hubspot-engine-x already has **full CRM read endpoints** — search, list, get, batch-read, associations, pipelines, lists. There is no blocking read gap. PaidEdge can pull contact/deal data today.

---

## 2. Authentication

### Service-to-Service Auth

Identical pattern to Salesforce. PaidEdge authenticates via **API tokens**.

**Headers on every request:**
```
Authorization: Bearer <HUBSPOT_ENGINE_X_API_TOKEN>
Content-Type: application/json
```

**Doppler secrets to add:**
| Secret | Purpose |
|--------|---------|
| `HUBSPOT_ENGINE_X_BASE_URL` | hubspot-engine-x API URL |
| `HUBSPOT_ENGINE_X_API_TOKEN` | API token from `POST /api/tokens/create` |

**Settings class addition (`app/config.py`):**
```python
HUBSPOT_ENGINE_X_BASE_URL: str = ""
HUBSPOT_ENGINE_X_API_TOKEN: str = ""
```

### Per-Tenant Connection Model

Same as Salesforce: `client_id` in every request body, stored in `provider_configs` (`provider="hubspot_crm"`, `config.hubspot_client_id="<uuid>"`).

### Bootstrapping a New Tenant

1. `POST /api/clients/create` → `{"name": "Acme Corp", "domain": "acme.com"}`
2. `POST /api/connections/create` → returns `connect_link` (Nango HubSpot OAuth widget URL)
3. Customer completes OAuth in browser
4. `POST /api/connections/callback` → connection confirmed with `hub_domain`, `hubspot_portal_id`, `scopes`
5. Store `{"hubspot_client_id": "<uuid>"}` in `provider_configs`

---

## 3. Read Flow — Pulling CRM Data

### Available Endpoints (No Gap)

hubspot-engine-x provides all necessary read endpoints:

| Need | Endpoint | Notes |
|------|----------|-------|
| Search contacts | `POST /api/crm/search` | Filter by email, date, any property |
| List all contacts | `POST /api/crm/list` | Paginated, 100 per page |
| Get single contact | `POST /api/crm/get` | By HubSpot record ID |
| Batch-read contacts | `POST /api/crm/batch-read` | Up to 100 IDs per call |
| Search/list deals | Same endpoints with `object_type: "deals"` | |
| Contact→Deal associations | `POST /api/crm/associations` | Single record |
| Batch associations | `POST /api/crm/associations/batch` | Up to 100 source records |
| Pipeline stages | `POST /api/crm/pipelines` | Full stage list with display order |
| HubSpot lists | `POST /api/crm/lists` | Static + dynamic lists |

### HubSpot Deals vs Salesforce Opportunities

| Concept | Salesforce | HubSpot |
|---------|-----------|---------|
| Deal/Opportunity entity | Opportunity | Deal |
| Amount field | `Amount` (currency) | `amount` (string, all props are strings) |
| Close date | `CloseDate` (date) | `closedate` (string) |
| Stage | `StageName` (picklist) | `dealstage` (internal ID, e.g., `closedwon`) |
| Pipeline | Implicit (one default, or custom) | `pipeline` (explicit ID) |
| Contact linkage | Junction object `OpportunityContactRole` | Direct association (contact→deal) |
| Won/Lost flags | `IsClosed`, `IsWon` (fields on Opportunity) | `hs_is_closed_won` (property), or inferred from stage |
| Stage history | No built-in (requires OpportunityFieldHistory) | `hs_date_entered_{stage}`, `hs_date_exited_{stage}` properties |

### What PaidEdge Pulls

**Contacts (incremental via search):**
```json
POST /api/crm/search
{
  "client_id": "uuid",
  "object_type": "contacts",
  "filter_groups": [{
    "filters": [{
      "propertyName": "lastmodifieddate",
      "operator": "GTE",
      "value": "1711324800000"
    }]
  }],
  "properties": [
    "email", "firstname", "lastname", "company", "jobtitle",
    "lifecyclestage", "hs_lead_status",
    "hs_analytics_source", "hs_analytics_source_data_1",
    "hs_analytics_first_url", "hs_analytics_last_url",
    "createdate", "lastmodifieddate"
  ],
  "sorts": [{"propertyName": "lastmodifieddate", "direction": "ASCENDING"}],
  "limit": 100
}
```

**Deals (incremental via search):**
```json
POST /api/crm/search
{
  "client_id": "uuid",
  "object_type": "deals",
  "filter_groups": [{
    "filters": [{
      "propertyName": "hs_lastmodifieddate",
      "operator": "GTE",
      "value": "1711324800000"
    }]
  }],
  "properties": [
    "dealname", "amount", "closedate", "dealstage", "pipeline",
    "hs_is_closed_won", "hs_deal_stage_probability",
    "createdate", "hs_lastmodifieddate"
  ],
  "limit": 100
}
```

**Contact→Deal Associations (batch):**
```json
POST /api/crm/associations/batch
{
  "client_id": "uuid",
  "from_object_type": "contacts",
  "to_object_type": "deals",
  "object_ids": ["101", "102", "103"]
}
```

**Pipeline Stages (one-time per tenant, cache):**
```json
POST /api/crm/pipelines
{
  "client_id": "uuid",
  "object_type": "deals"
}
```

### HubSpot-Specific: Analytics Properties (Free Attribution Data)

HubSpot stores rich analytics data directly as contact properties, readable without any special API:

| Property | What It Provides |
|----------|------------------|
| `hs_analytics_source` | Original lead source (ORGANIC_SEARCH, PAID_SEARCH, SOCIAL, etc.) |
| `hs_analytics_source_data_1` | Source drill-down (campaign name, referrer domain) |
| `hs_analytics_source_data_2` | Further drill-down |
| `hs_analytics_first_url` | First page visited (landing page with UTMs) |
| `hs_analytics_last_url` | Last page before conversion |
| `hs_analytics_num_page_views` | Total page views |
| `hs_analytics_num_visits` | Total sessions |
| `hs_email_last_open_date` | Last marketing email opened |
| `hs_email_last_click_date` | Last email link clicked |

These are requested in the `properties` array of search/list calls — no additional endpoints needed.

### Deal Stage History (via Properties)

HubSpot automatically creates `hs_date_entered_{stageId}` and `hs_date_exited_{stageId}` properties on deals. To get stage progression timestamps, include these in the properties request. The stage IDs come from the pipelines endpoint.

### Data Mapping to ClickHouse

Uses the same `crm_contacts` and `crm_opportunities` tables as Salesforce, with `crm_source = 'hubspot'`:

**Contact mapping:**
| HubSpot Property | ClickHouse Column |
|------------------|-------------------|
| `hs_object_id` | `crm_contact_id` |
| `email` | `email` |
| `firstname` | `first_name` |
| `lastname` | `last_name` |
| `company` | `company_name` |
| (via association) | `account_id` (company ID) |
| `hs_analytics_source` | `lead_source` |
| `lifecyclestage` | `lifecycle_stage` |
| `createdate` | `created_at` |
| `lastmodifieddate` | `updated_at` |

**Deal mapping:**
| HubSpot Property | ClickHouse Column |
|------------------|-------------------|
| `hs_object_id` | `crm_opportunity_id` |
| `dealname` | `name` |
| `amount` | `amount` (parse string → float) |
| `closedate` | `close_date` (parse string → date) |
| `dealstage` | `stage` |
| (infer from stage) | `is_closed` |
| `hs_is_closed_won` | `is_won` (parse string → bool) |
| (via association) | `account_id` (company ID) |
| `hs_analytics_source` | `lead_source` |
| (via contact associations) | `contact_ids` |
| `createdate` | `created_at` |
| `hs_lastmodifieddate` | `updated_at` |

**Important:** All HubSpot property values are strings. The sync must parse `"50000"` → `50000.0`, `"true"` → `1`, `"2026-06-15"` → `Date`.

---

## 4. Write Flow — Pushing Data to HubSpot

### Available Push Endpoints

| Endpoint | Purpose |
|----------|---------|
| `POST /api/push/records` | Upsert with field mapping resolution |
| `POST /api/push/update` | Update by HubSpot ID (no mapping) |
| `POST /api/push/link` | Create associations |

### Push Use Cases

**1. Push leads with UTM attribution:**
```json
POST /api/push/records
{
  "client_id": "uuid",
  "object_type": "contacts",
  "records": [{
    "properties": {
      "email_address": "jane@acme.com",
      "first_name": "Jane",
      "last_name": "Doe",
      "company_name": "Acme Corp",
      "lead_source": "PaidEdge",
      "pe_utm_source": "linkedin",
      "pe_utm_campaign": "campaign-uuid"
    }
  }],
  "id_property": "email"
}
```

Field mappings resolve canonical names (`email_address` → `email`, `first_name` → `firstname`). Unmapped fields are silently dropped with a warning.

**2. Update deal properties:**
```json
POST /api/push/update
{
  "client_id": "uuid",
  "object_type": "deals",
  "updates": [{
    "id": "501",
    "properties": {
      "pe_attribution_score": "85.5",
      "pe_channel": "Paid"
    }
  }]
}
```

Note: `/push/update` does NOT apply field mappings — uses raw HubSpot property names.

**3. Create contact→deal association:**
```json
POST /api/push/link
{
  "client_id": "uuid",
  "from_object_type": "contacts",
  "to_object_type": "deals",
  "associations": [
    {"from_id": "101", "to_id": "501", "association_type": null}
  ]
}
```

### HubSpot Property vs Salesforce Field Differences

| Concept | Salesforce | HubSpot |
|---------|-----------|---------|
| Custom field deploy | `POST /api/deploy/execute` (via Metadata API) | Not available in hubspot-engine-x. Must create custom properties via HubSpot UI or a future topology/deploy phase. |
| Field name format | `PE_UTM_Source__c` (API name with `__c` suffix) | `pe_utm_source` (lowercase, no suffix) |
| Mapping granularity | One mapping row per canonical_object (JSONB dict) | One mapping row per canonical_field (individual rows) |
| Upsert dedup key | `external_id_field` (any external ID field) | `id_property` (typically `email` for contacts) |

### Field Mapping Setup

Register per-field mappings (HubSpot uses individual rows, not a dict):

```json
// Register each mapping individually
POST /api/field-mappings/set
{"client_id": "uuid", "canonical_object": "contact", "canonical_field": "email_address", "hubspot_object": "contacts", "hubspot_property": "email"}

POST /api/field-mappings/set
{"client_id": "uuid", "canonical_object": "contact", "canonical_field": "first_name", "hubspot_object": "contacts", "hubspot_property": "firstname"}

POST /api/field-mappings/set
{"client_id": "uuid", "canonical_object": "contact", "canonical_field": "last_name", "hubspot_object": "contacts", "hubspot_property": "lastname"}

// ... etc for each field
```

---

## 5. Sync Scheduling

### Trigger.dev Task: `hubspot_crm_sync_task`

File: `trigger/hubspot_crm_sync.py`

**Schedule:** Every 6 hours (`0 */6 * * *`)

**Fan-out pattern** (identical to Salesforce):

```
hubspot_crm_sync_task (scheduled)
  ├── get_hubspot_connected_tenants(supabase)
  │     SELECT org_id, config->>'hubspot_client_id'
  │     FROM provider_configs
  │     WHERE provider = 'hubspot_crm'
  │       AND config->>'status' = 'connected'
  │
  └── for (org_id, hubspot_client_id) in tenants:
        try:
            result = await sync_tenant_hubspot(org_id, hubspot_client_id, hubspot_client)
        except Exception:
            logger.exception(f"HubSpot sync failed for tenant {org_id}")
```

### Per-Tenant Sync Logic

```
sync_tenant_hubspot(org_id, hubspot_client_id, hubspot_client):
  1. Check connection status → skip if expired/revoked
  2. Get last_sync_date from provider_configs
  3. Pull pipeline stages (cache, refresh weekly)
  4. Search contacts modified since last_sync (paginated, 100 per page)
  5. For contact batches: get contact→deal associations (batch, 100 per call)
  6. Search deals modified since last_sync
  7. For deal batches: get deal→company associations
  8. Parse all string property values to typed values
  9. Map to ClickHouse schema (crm_source = 'hubspot')
  10. Insert into crm_contacts and crm_opportunities
  11. Update last_sync_date
  12. Return structured result dict
```

### HubSpot-Specific Pagination

HubSpot uses cursor-based pagination (`after` parameter). The sync must handle:
- Search API: max 100 results per page, 5 requests/second rate limit
- List API: max 100 results per page
- Batch-read: max 100 IDs per call

For large tenants (10K+ contacts), the sync will make many sequential calls. The 6-hour cadence and incremental filtering (`lastmodifieddate >= last_sync`) keep volumes manageable.

### Rate Limit Awareness

hubspot-engine-x handles rate limiting internally (95 req/10s per portal, 429 retry). However, the HubSpot Search API has a stricter 5 req/s limit. PaidEdge should:
- Avoid concurrent search calls for the same portal
- Use batch-read instead of individual get calls where possible
- Be aware of the 500K daily request limit shared across all portals

---

## 6. Attribution Chain

Same conceptual chain as Salesforce, with HubSpot-specific data:

```
Ad Click → UTM-tagged landing page
    ↓
HubSpot tracks via hs_analytics_source, hs_analytics_first_url
    ↓
Form fill → Contact created with attribution properties
    ↓
PaidEdge pushes PE_* custom properties via push/records
    ↓
Deal created, associated to contact (synced via associations endpoint)
    ↓
Deal progresses through stages (hs_date_entered_* timestamps synced)
    ↓
Deal reaches closedwon → amount captured
    ↓
ROAS = revenue / ad spend for campaign
```

### HubSpot Attribution Advantage

HubSpot automatically captures rich analytics data on contacts (`hs_analytics_source`, `hs_analytics_first_url`, etc.) without any custom field deployment. This means PaidEdge gets basic attribution data even before pushing any custom properties — a faster time-to-value than Salesforce.

---

## 7. PaidEdge Client API Surface

### `app/integrations/hubspot_engine_x.py`

```python
class HubSpotEngineClient:
    """Service-to-service client for hubspot-engine-x."""

    MAX_RETRIES = 5
    BACKOFF_BASE = 1
    BACKOFF_CAP = 16
    TIMEOUT = 30.0

    def __init__(self, base_url=None, api_token=None):
        self.base_url = (base_url or settings.HUBSPOT_ENGINE_X_BASE_URL).rstrip("/")
        self.api_token = api_token or settings.HUBSPOT_ENGINE_X_API_TOKEN
        self._client = httpx.AsyncClient(timeout=self.TIMEOUT)

    # Context manager
    async def close(self): ...
    async def __aenter__(self): return self
    async def __aexit__(self, *args): await self.close()

    # --- CRM Reads ---
    async def search(
        self, client_id: str, object_type: str,
        filter_groups: list[dict] = None,
        properties: list[str] = None,
        sorts: list[dict] = None,
        limit: int = 100, after: str = None
    ) -> CrmSearchResponse: ...

    async def search_all(
        self, client_id: str, object_type: str, **kwargs
    ) -> list[CrmRecord]:
        """Auto-paginate through all search results."""

    async def list_records(
        self, client_id: str, object_type: str,
        properties: list[str] = None,
        limit: int = 100, after: str = None
    ) -> CrmListResponse: ...

    async def get_record(
        self, client_id: str, object_type: str, object_id: str
    ) -> CrmRecord: ...

    async def batch_read(
        self, client_id: str, object_type: str,
        ids: list[str], properties: list[str] = None
    ) -> list[CrmRecord]: ...

    # --- Associations ---
    async def get_associations(
        self, client_id: str,
        from_object_type: str, to_object_type: str,
        object_id: str
    ) -> list[AssociationResult]: ...

    async def batch_associations(
        self, client_id: str,
        from_object_type: str, to_object_type: str,
        object_ids: list[str]
    ) -> list[AssociationResult]: ...

    # --- Pipelines ---
    async def get_pipelines(
        self, client_id: str, object_type: str = "deals"
    ) -> list[Pipeline]: ...

    # --- Push ---
    async def push_records(
        self, client_id: str, object_type: str,
        records: list[dict], id_property: str = None
    ) -> PushResponse: ...

    async def update_records(
        self, client_id: str, object_type: str,
        updates: list[dict]
    ) -> PushResponse: ...

    async def link_records(
        self, client_id: str,
        from_object_type: str, to_object_type: str,
        associations: list[dict]
    ) -> PushResponse: ...

    # --- Field Mappings ---
    async def set_mapping(self, client_id: str, mapping: FieldMappingCreate) -> FieldMapping: ...
    async def get_mappings(self, client_id: str, canonical_object: str) -> list[FieldMapping]: ...
    async def list_all_mappings(self, client_id: str) -> list[FieldMapping]: ...

    # --- Connections ---
    async def get_connection(self, client_id: str) -> ConnectionResponse: ...
```

---

## 8. Gaps Compared to sfdc-engine-x

| Capability | sfdc-engine-x | hubspot-engine-x | Impact |
|-----------|--------------|-------------------|--------|
| CRM record reads | NOT available (no search/list/get) | Full (search, list, get, batch-read) | HubSpot ready now; SFDC blocked |
| Association reads | NOT available | Full (single + batch) | HubSpot has direct traversal |
| Pipeline stages | Partial (topology picklist only) | Full (`/api/crm/pipelines`) | HubSpot richer |
| Push/upsert | Full (field mapping + canonical) | Full (field mapping + canonical) | Parity |
| Deploy custom fields | Full (Metadata API) | NOT available (Phase 7) | Must create HubSpot custom props manually or via UI |
| Topology snapshots | Full | NOT available (Phase 6) | Not needed for M4 reads |
| Conflict checks | Full | NOT available | Not needed for M4 |
| Rollback | Full | NOT available | Not needed for M4 |
| Workflows | Full | NOT available | Not needed for M4 |
| HubSpot lists | N/A | Full (lists + members) | Bonus for audience building |
| Association creation | Via push with FK fields | Explicit `POST /api/push/link` | HubSpot more explicit |

### Summary

For M4 (read-heavy analytics/attribution), **hubspot-engine-x is more complete than sfdc-engine-x** because it has CRM read endpoints. For M4 write operations, both are equivalent. sfdc-engine-x has more advanced admin features (deploy, topology, workflows) that aren't needed for M4.

---

## 9. Unified CRM Abstraction Layer

### The Question

Should PaidEdge have a `BaseCRMClient` abstract class with `SalesforceEngineClient` and `HubSpotEngineClient` implementing it, or keep them as separate clients?

### Tradeoff Analysis

**Option A: Unified Abstraction**

Pros:
- Sync tasks, ClickHouse mapping, and ROAS calculation code can be CRM-agnostic
- Adding a third CRM (Pipedrive, Dynamics) follows the same interface
- Single `crm_sync_task` with provider routing instead of two separate tasks
- ClickHouse schema is already unified (`crm_source` column differentiates)

Cons:
- Leaky abstraction: Salesforce SOQL queries vs HubSpot filter_groups are fundamentally different
- Field mapping models differ (Salesforce: one JSONB dict per object; HubSpot: one row per field)
- Association models differ (SFDC: junction objects + FK fields; HubSpot: explicit association API)
- Push semantics differ (`push/records` applies mappings in HubSpot; SFDC has `canonical_object` param)
- Premature generalization risk — only two CRMs now

**Option B: Separate Clients, Shared ClickHouse Layer**

Pros:
- Each client is a faithful wrapper of its engine-x service — no awkward abstraction
- Can leverage CRM-specific features fully (HubSpot lists, SFDC topology)
- Simpler to build, test, and debug
- Can still share ClickHouse tables and downstream code

Cons:
- Duplicate sync task logic
- Each new CRM requires new sync task, new client, new mapping logic
- ROAS calculation must handle both independently

### Recommendation: Hybrid — Shared Protocol, Separate Implementations

```python
# app/integrations/crm_base.py

class CRMSyncResult(BaseModel):
    """Standardized result from any CRM sync."""
    tenant_id: str
    crm_source: str
    contacts: list[CRMContact]
    opportunities: list[CRMOpportunity]
    pipeline_stages: list[PipelineStage]

class CRMContact(BaseModel):
    crm_contact_id: str
    email: str | None
    first_name: str | None
    last_name: str | None
    company_name: str | None
    account_id: str | None
    lead_source: str | None
    lifecycle_stage: str | None
    created_at: datetime | None
    updated_at: datetime | None

class CRMOpportunity(BaseModel):
    crm_opportunity_id: str
    name: str
    amount: float | None
    close_date: date | None
    stage: str
    is_closed: bool
    is_won: bool
    account_id: str | None
    lead_source: str | None
    contact_ids: list[str]
    created_at: datetime | None
    updated_at: datetime | None

class PipelineStage(BaseModel):
    stage_id: str
    label: str
    display_order: int
    is_closed: bool = False
    is_won: bool = False
    probability: float | None = None


class BaseCRMSyncer(ABC):
    """Abstract syncer — each CRM implements its own pull logic."""

    @abstractmethod
    async def pull_contacts(self, client_id: str, since: datetime | None) -> list[CRMContact]: ...

    @abstractmethod
    async def pull_opportunities(self, client_id: str, since: datetime | None) -> list[CRMOpportunity]: ...

    @abstractmethod
    async def pull_pipeline_stages(self, client_id: str) -> list[PipelineStage]: ...

    @abstractmethod
    async def push_lead(self, client_id: str, lead: CRMContact, attribution: dict) -> str: ...

    @abstractmethod
    async def check_connection(self, client_id: str) -> bool: ...
```

**Why hybrid:**
- The HTTP clients (`SalesforceEngineClient`, `HubSpotEngineClient`) stay separate and CRM-specific — no leaky abstraction
- The syncer layer (`SalesforceSyncer`, `HubSpotSyncer`) implements `BaseCRMSyncer` by calling the respective client and normalizing data into `CRMContact`/`CRMOpportunity` models
- The Trigger.dev task, ClickHouse writer, and ROAS calculator work with `CRMSyncResult` only — fully CRM-agnostic
- The push path also uses the shared models but each syncer implements CRM-specific push logic

This avoids abstracting the HTTP clients (where CRM differences are real) while unifying the data layer (where uniformity matters).

---

## 10. Gaps & Risks

| Gap | Severity | Mitigation |
|-----|----------|------------|
| **No custom property deployment in hubspot-engine-x** | Medium | Create PE_* properties via HubSpot UI per client, or build Phase 7 of hubspot-engine-x |
| **Search API 5 req/s limit** | Low | Sequential pagination, no parallel search per portal |
| **All property values are strings** | Low | Type parsing in sync layer (amount → float, dates → datetime, booleans → bool) |
| **No form submissions API** | Low | Use `hs_analytics_first_url` and analytics properties for basic attribution |
| **No engagement/timeline API** | Low | Not needed for M4; defer to M5 |
| **No webhook/incremental sync** | Low | Polling via search with `lastmodifieddate` filter is sufficient at 6h cadence |
| **500K daily API limit (shared across all portals)** | Medium | Monitor usage; at 6h cadence with ~1K records/tenant, well within limits |
