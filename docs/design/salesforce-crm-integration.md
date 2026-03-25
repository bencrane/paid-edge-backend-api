# Salesforce CRM Integration — PaidEdge Design Doc

> **Project:** PaidEdge M4 — Analytics + Attribution
> **Author:** PaidEdge Engineering
> **Date:** 2026-03-25
> **Status:** Draft
> **Depends on:** sfdc-engine-x-api (all phases complete, 38 endpoints)

---

## 1. Overview

PaidEdge needs Salesforce CRM data for two purposes:
1. **Read path (attribution):** Pull contacts, opportunities, pipeline stages, and associations to calculate ROAS by connecting ad spend to deal revenue.
2. **Write path (enrichment):** Push leads with UTM/campaign attribution, update lifecycle stages, and tag deals with PaidEdge campaign data.

PaidEdge does **not** call Salesforce APIs directly. It calls `sfdc-engine-x` as a service-to-service consumer — the same pattern as the existing `DataEngineXClient` in `app/integrations/data_engine_x.py`.

### Architecture

```
┌──────────────┐    Bearer token     ┌──────────────────┐    Nango OAuth    ┌──────────────┐
│   PaidEdge   │ ──────────────────→ │  sfdc-engine-x   │ ───────────────→ │  Salesforce   │
│   Backend    │                     │  (FastAPI)       │                   │  Org (per     │
│              │ ←────────────────── │  38 endpoints    │ ←──────────────── │  client)      │
└──────────────┘    JSON responses   └──────────────────┘    REST API v60   └──────────────┘
```

### Key Constraint

sfdc-engine-x is **write-heavy by design**. It has full push, deploy, topology, and workflow capabilities but **no CRM record read endpoints** — no search, list, get, batch-read, or SOQL query. All record-level reads are a gap that must be addressed before PaidEdge can pull pipeline data.

---

## 2. Authentication

### Service-to-Service Auth

PaidEdge authenticates to sfdc-engine-x via **API tokens** (machine-to-machine). The token encodes PaidEdge's organization identity.

**Headers on every request:**
```
Authorization: Bearer <SFDC_ENGINE_X_API_TOKEN>
Content-Type: application/json
```

**Doppler secrets to add:**
| Secret | Purpose |
|--------|---------|
| `SFDC_ENGINE_X_BASE_URL` | sfdc-engine-x API URL (e.g., `https://sfdc-engine-x.railway.app`) |
| `SFDC_ENGINE_X_API_TOKEN` | API token created via `POST /api/tokens/create` |

**Settings class addition (`app/config.py`):**
```python
SFDC_ENGINE_X_BASE_URL: str = ""
SFDC_ENGINE_X_API_TOKEN: str = ""
```

### Per-Tenant Connection Model

Every CRM-scoped request includes a `client_id` (UUID) in the JSON body. The flow:
1. PaidEdge stores a `client_id` per tenant in its `provider_configs` table (`provider="salesforce_crm"`, `config.sfdc_client_id="<uuid>"`)
2. On each API call, PaidEdge sends `{"client_id": "<uuid>", ...}`
3. sfdc-engine-x validates the client belongs to PaidEdge's org, fetches a fresh Salesforce token from Nango, and executes against that client's Salesforce instance

PaidEdge **never sees or manages Salesforce OAuth tokens**. Token refresh is entirely transparent via Nango.

### Bootstrapping a New Tenant

When a PaidEdge customer connects their Salesforce:
1. PaidEdge calls `POST /api/clients/create` with `{"name": "Acme Corp", "domain": "acme.com"}`
2. PaidEdge calls `POST /api/connections/create` with `{"client_id": "<uuid>"}` → gets a `connect_session.token`
3. The customer completes OAuth in a frontend Nango widget
4. PaidEdge calls `POST /api/connections/callback` → connection is `connected`
5. PaidEdge stores `{"sfdc_client_id": "<uuid>"}` in `provider_configs` for this tenant

---

## 3. Read Flow — Pulling CRM Data

### The Gap: No Record Read Endpoints

sfdc-engine-x currently has no endpoints to search, list, get, or batch-read CRM records. The topology snapshot provides schema metadata (fields, relationships, picklist values) but not record data.

### Recommended Approach: Add SOQL Query Proxy to sfdc-engine-x

The most architecturally consistent solution is to add a SOQL query endpoint to sfdc-engine-x:

```
POST /api/query/execute
{
  "client_id": "uuid",
  "query": "SELECT Id, Email, FirstName, LastName, AccountId, LeadSource FROM Contact WHERE LastModifiedDate > 2026-03-01T00:00:00Z LIMIT 2000",
  "include_deleted": false
}
```

This is a thin proxy over Salesforce's `GET /services/data/v60.0/query/?q=<SOQL>` and keeps all Salesforce API access centralized in sfdc-engine-x.

**Until the query endpoint exists**, PaidEdge cannot pull record data from Salesforce. This is a blocking dependency.

### What PaidEdge Pulls

Once the query endpoint is available, PaidEdge pulls:

**Contacts:**
```sql
SELECT Id, Email, FirstName, LastName, AccountId, LeadSource,
       CreatedDate, LastModifiedDate
FROM Contact
WHERE LastModifiedDate > {last_sync_date}
ORDER BY LastModifiedDate ASC
LIMIT 2000
```

**Opportunities:**
```sql
SELECT Id, Name, Amount, CloseDate, StageName, AccountId,
       LeadSource, CreatedDate, LastModifiedDate, IsClosed, IsWon
FROM Opportunity
WHERE LastModifiedDate > {last_sync_date}
ORDER BY LastModifiedDate ASC
LIMIT 2000
```

**OpportunityContactRole (attribution join):**
```sql
SELECT Id, ContactId, OpportunityId, Role, IsPrimary
FROM OpportunityContactRole
WHERE Opportunity.LastModifiedDate > {last_sync_date}
LIMIT 2000
```

**Pipeline stages** are already available via topology snapshot (`Opportunity.StageName.picklistValues`), but without probability/isClosed/isWon flags. A dedicated stage metadata query would be:
```sql
SELECT MasterLabel, ApiName, IsClosed, IsWon, DefaultProbability, ForecastCategory
FROM OpportunityStage
ORDER BY SortOrder ASC
```

### Data Mapping to ClickHouse

Pulled records are mapped to two new ClickHouse tables:

**`paid_edge.crm_contacts`**
```sql
CREATE TABLE paid_edge.crm_contacts (
    tenant_id       String,
    crm_source      String DEFAULT 'salesforce',
    crm_contact_id  String,       -- Salesforce Contact.Id
    email           String,
    first_name      Nullable(String),
    last_name       Nullable(String),
    company_name    Nullable(String),
    account_id      Nullable(String),  -- Salesforce AccountId
    lead_source     Nullable(String),
    lifecycle_stage Nullable(String),
    created_at      DateTime64(3),
    updated_at      DateTime64(3),
    synced_at       DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(synced_at)
ORDER BY (tenant_id, crm_source, crm_contact_id)
```

**`paid_edge.crm_opportunities`**
```sql
CREATE TABLE paid_edge.crm_opportunities (
    tenant_id           String,
    crm_source          String DEFAULT 'salesforce',
    crm_opportunity_id  String,       -- Salesforce Opportunity.Id
    name                String,
    amount              Nullable(Float64),
    close_date          Nullable(Date),
    stage               String,
    is_closed           UInt8,
    is_won              UInt8,
    account_id          Nullable(String),
    lead_source         Nullable(String),
    contact_ids         Array(String),  -- From OpportunityContactRole
    created_at          DateTime64(3),
    updated_at          DateTime64(3),
    synced_at           DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(synced_at)
ORDER BY (tenant_id, crm_source, crm_opportunity_id)
```

Both tables use `ReplacingMergeTree` with `synced_at` as the version column for deduplication, matching the existing `campaign_metrics` pattern.

---

## 4. Write Flow — Pushing Data to Salesforce

### Available Push Endpoints

sfdc-engine-x provides full push capability today:

| Endpoint | Purpose |
|----------|---------|
| `POST /api/push/records` | Upsert records with field mapping resolution |
| `POST /api/push/validate` | Preflight mapping check |
| `POST /api/push/status` | Check push result |
| `POST /api/deploy/execute` | Deploy custom fields to Opportunity/Contact |

### Push Use Cases

**1. Push leads with UTM attribution:**
When a form fill comes in from a PaidEdge tracked link:
```json
POST /api/push/records
{
  "client_id": "uuid",
  "object_type": "Lead",
  "external_id_field": "Email",
  "canonical_object": "lead",
  "records": [{
    "email": "jane@acme.com",
    "first_name": "Jane",
    "last_name": "Doe",
    "company": "Acme Corp",
    "lead_source": "PaidEdge",
    "pe_utm_source": "linkedin",
    "pe_utm_campaign": "campaign-uuid",
    "pe_attribution_score": 85.5
  }]
}
```

**2. Update lifecycle stages:**
```json
POST /api/push/records
{
  "client_id": "uuid",
  "object_type": "Contact",
  "external_id_field": "Email",
  "records": [{
    "email": "jane@acme.com",
    "pe_lifecycle_stage": "MQL"
  }]
}
```

**3. Tag deals with campaign data:**
```json
POST /api/push/records
{
  "client_id": "uuid",
  "object_type": "Opportunity",
  "external_id_field": "PE_External_Id__c",
  "canonical_object": "opportunity",
  "records": [{
    "pe_external_id": "opp-uuid",
    "pe_channel": "Paid",
    "pe_campaign_id": "campaign-uuid",
    "pe_attribution_score": 92.0
  }]
}
```

### Pre-Deploy Custom Fields

Before pushing PaidEdge-specific data, custom fields must be deployed to each client's Salesforce org:

```json
POST /api/deploy/execute
{
  "client_id": "uuid",
  "plan": {
    "standard_object_fields": [
      {
        "object": "Contact",
        "fields": [
          {"api_name": "PE_UTM_Source__c", "label": "PaidEdge UTM Source", "type": "Text", "length": 255},
          {"api_name": "PE_UTM_Campaign__c", "label": "PaidEdge UTM Campaign", "type": "Text", "length": 255},
          {"api_name": "PE_Attribution_Score__c", "label": "PaidEdge Score", "type": "Number", "precision": 5, "scale": 1},
          {"api_name": "PE_Lifecycle_Stage__c", "label": "PaidEdge Stage", "type": "Text", "length": 50}
        ]
      },
      {
        "object": "Opportunity",
        "fields": [
          {"api_name": "PE_External_Id__c", "label": "PaidEdge ID", "type": "Text", "length": 255},
          {"api_name": "PE_Channel__c", "label": "PaidEdge Channel", "type": "Picklist", "values": ["Organic", "Paid", "Referral", "Direct"]},
          {"api_name": "PE_Campaign_Id__c", "label": "PaidEdge Campaign", "type": "Text", "length": 255},
          {"api_name": "PE_Attribution_Score__c", "label": "PaidEdge Score", "type": "Number", "precision": 5, "scale": 1}
        ]
      }
    ]
  }
}
```

After deploy, identity field mappings are auto-created. PaidEdge should also set up canonical mappings via `POST /api/mappings/create`.

### Field Mapping Setup

Per-client, per-object mappings must be registered:

```json
POST /api/mappings/create
{
  "client_id": "uuid",
  "canonical_object": "contact",
  "sfdc_object": "Contact",
  "field_mappings": {
    "email": "Email",
    "first_name": "FirstName",
    "last_name": "LastName",
    "company": "Company",
    "lead_source": "LeadSource",
    "pe_utm_source": "PE_UTM_Source__c",
    "pe_utm_campaign": "PE_UTM_Campaign__c",
    "pe_attribution_score": "PE_Attribution_Score__c"
  },
  "external_id_field": "Email"
}
```

---

## 5. Sync Scheduling

### Trigger.dev Task: `sfdc_crm_sync_task`

File: `trigger/sfdc_crm_sync.py`

**Schedule:** Every 6 hours (`0 */6 * * *`) — matches LinkedIn/Meta metrics cadence.

**Fan-out pattern** (matching existing tasks):

```
sfdc_crm_sync_task (scheduled)
  ├── get_sfdc_connected_tenants(supabase)
  │     SELECT org_id, config->>'sfdc_client_id'
  │     FROM provider_configs
  │     WHERE provider = 'salesforce_crm'
  │       AND config->>'status' = 'connected'
  │
  └── for (org_id, sfdc_client_id) in tenants:
        try:
            result = await sync_tenant_sfdc(org_id, sfdc_client_id, sfdc_client)
            all_results.append(result)
        except Exception:
            logger.exception(f"SFDC sync failed for tenant {org_id}")
            all_results.append({status: "error", tenant_id: org_id})

  return all_results
```

### Per-Tenant Sync Logic

```
sync_tenant_sfdc(org_id, sfdc_client_id, sfdc_client):
  1. Check connection status: GET /api/connections/get → skip if expired/revoked
  2. Get last_sync_date from Supabase (provider_configs.config.last_sfdc_sync)
  3. Pull contacts modified since last_sync (paginated SOQL, 2000 per page)
  4. Pull opportunities modified since last_sync
  5. Pull OpportunityContactRole for modified opportunities
  6. Map to ClickHouse schema
  7. Insert into crm_contacts and crm_opportunities (ReplacingMergeTree handles dedup)
  8. Update last_sync_date in provider_configs
  9. Return {status, tenant_id, contacts_synced, opportunities_synced, duration_ms}
```

### Failure Handling

- Per-tenant isolation: one tenant's failure doesn't stop others
- Connection errors (502 from expired OAuth): mark connection as needs-reauth, skip tenant
- Rate limit (429): sfdc-engine-x handles internally; PaidEdge retries at HTTP client level
- Partial failures logged with structured result dicts

### Initial Full Sync

First sync for a new tenant uses `last_sync_date = None` (no WHERE clause on LastModifiedDate), pulling all records. Subsequent syncs are incremental. For very large orgs, pagination via `LIMIT 2000` with `ORDER BY LastModifiedDate ASC` ensures progress.

---

## 6. Attribution Chain

The full attribution chain PaidEdge needs to calculate ROAS:

```
Ad Click (LinkedIn/Meta/Google)
    ↓
Landing Page Visit (tracked via dub.co UTM: utm_source=paidedge, utm_campaign={id})
    ↓
Form Fill (captured by PaidEdge or client's form tool)
    ↓
Lead Created in Salesforce (pushed via sfdc-engine-x with PE_UTM_* fields)
    ↓
Lead Converted → Contact + Opportunity created (tracked via ConvertedContactId)
    ↓
Opportunity moves through pipeline stages (synced to ClickHouse every 6h)
    ↓
Opportunity reaches Closed Won (is_won=true, amount captured)
    ↓
ROAS Calculated: revenue from won opportunities / ad spend for that campaign
```

### ROAS Calculation Query

```sql
SELECT
    cm.campaign_id,
    cm.platform,
    SUM(cm.spend) AS total_spend,
    SUM(co.amount) AS total_revenue,
    IF(SUM(cm.spend) > 0, SUM(co.amount) / SUM(cm.spend), 0) AS roas
FROM paid_edge.campaign_metrics cm
LEFT JOIN paid_edge.crm_opportunities co
    ON co.tenant_id = cm.tenant_id
    AND co.is_won = 1
    AND co.lead_source = 'PaidEdge'
    -- Join via contact → opportunity → campaign mapping
WHERE cm.tenant_id = {tenant_id}
GROUP BY cm.campaign_id, cm.platform
```

The exact join depends on how campaign IDs flow through the CRM. The `PE_Campaign_Id__c` custom field on Opportunity enables direct campaign attribution.

### Firing Conversion Events

When the sync detects a new `opportunity_created` or `closed_won` event (by comparing current vs previous stage), PaidEdge fires:
- **LinkedIn CAPI:** `opportunity_created` → OTHER, `closed_won` → PURCHASE (via existing `linkedin_conversions.py`)
- **Meta CAPI:** `closed_won` → Purchase event with `value = amount` (via existing `meta_conversions.py`)

---

## 7. PaidEdge Client API Surface

### `app/integrations/sfdc_engine_x.py`

```python
class SalesforceEngineClient:
    """Service-to-service client for sfdc-engine-x."""

    MAX_RETRIES = 5
    BACKOFF_BASE = 1
    BACKOFF_CAP = 16
    TIMEOUT = 30.0

    def __init__(self, base_url=None, api_token=None):
        self.base_url = (base_url or settings.SFDC_ENGINE_X_BASE_URL).rstrip("/")
        self.api_token = api_token or settings.SFDC_ENGINE_X_API_TOKEN
        self._client = httpx.AsyncClient(timeout=self.TIMEOUT)

    # Context manager
    async def close(self): ...
    async def __aenter__(self): return self
    async def __aexit__(self, *args): await self.close()

    # Core request with retry
    async def _request(self, method: str, path: str, json: dict = None) -> dict: ...

    # --- Connection management ---
    async def get_connection(self, client_id: str) -> ConnectionResponse: ...
    async def list_connections(self, client_id: str = None) -> list[ConnectionResponse]: ...

    # --- Record reads (requires SOQL query endpoint) ---
    async def query(self, client_id: str, soql: str) -> list[dict]: ...
    async def query_all(self, client_id: str, soql: str) -> list[dict]:
        """Auto-paginate through all results."""

    # --- Record writes ---
    async def push_records(
        self, client_id: str, object_type: str,
        records: list[dict], external_id_field: str,
        canonical_object: str = None
    ) -> PushResponse: ...

    async def validate_push(
        self, client_id: str, canonical_object: str, field_names: list[str]
    ) -> ValidateResponse: ...

    # --- Field mappings ---
    async def get_mapping(self, client_id: str, canonical_object: str) -> MappingResponse: ...
    async def set_mapping(self, client_id: str, mapping: MappingCreate) -> MappingResponse: ...

    # --- Topology ---
    async def get_topology(self, client_id: str) -> TopologyResponse: ...

    # --- Deploy ---
    async def deploy_fields(self, client_id: str, plan: DeployPlan) -> DeployResponse: ...
    async def check_conflicts(self, client_id: str, plan: DeployPlan) -> ConflictResponse: ...
```

### Pydantic Response Models

```python
# app/integrations/sfdc_models.py

class ConnectionResponse(BaseModel):
    id: str
    client_id: str
    status: str  # connected, expired, revoked, error
    instance_url: str | None = None

class PushResponse(BaseModel):
    id: str
    status: str
    records_total: int
    records_succeeded: int
    records_failed: int

class MappingResponse(BaseModel):
    id: str
    canonical_object: str
    sfdc_object: str
    field_mappings: dict[str, str]
    external_id_field: str | None
    mapping_version: int

class SoqlQueryResponse(BaseModel):
    records: list[dict]
    total_size: int
    done: bool
    next_records_url: str | None = None
```

---

## 8. Gaps & Risks

| Gap | Severity | Mitigation |
|-----|----------|------------|
| **No CRM record read endpoints in sfdc-engine-x** | Blocking | Must add SOQL query proxy endpoint before read path works |
| **No pipeline stage metadata** (isClosed, isWon, probability) | Medium | Can parse topology snapshot picklistValues; add OpportunityStage query for full metadata |
| **No CampaignMember read** | Medium | Defer to M5; use LeadSource field for basic attribution |
| **No SOQL query rate tracking** | Low | sfdc-engine-x should count API calls per client |
| **Salesforce daily API limits** (100K+) | Low | 6-hour cadence with incremental sync keeps volume manageable |
| **Full sync for large orgs** | Low | Paginate with LIMIT 2000; consider async job pattern for 100K+ records |

---

## 9. Unified CRM Abstraction Layer

See HubSpot design doc section on this topic. **Recommendation: Yes, implement a `BaseCRMClient` abstraction** with `SalesforceEngineClient` and `HubSpotEngineClient` implementing it. The tradeoff analysis and recommended interface are documented there.
