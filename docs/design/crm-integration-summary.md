# PaidEdge CRM Integration — Summary & Build Plan

## Deliverables

### Design Docs (PR #14)
- [PR #14 — CRM Integration Design Docs](https://github.com/bencrane/paid-edge-backend-api/pull/14) on branch `docs/crm-integration-design`
  - `docs/design/salesforce-crm-integration.md`
  - `docs/design/hubspot-crm-integration.md`

### Linear Issues — PaidEdge M4: Analytics + Attribution

| Issue | Title | Estimate | Priority | Labels |
|-------|-------|----------|----------|--------|
| [BJC-187](https://linear.app/bjc/issue/BJC-187) | Shared CRM abstraction layer — BaseCRMSyncer + canonical models | 3 pts | High | backend, integration, Feature, M4 |
| [BJC-188](https://linear.app/bjc/issue/BJC-188) | HubSpot engine-x service client — HubSpotEngineClient + HubSpotSyncer | 5 pts | High | backend, integration, Feature, M4 |
| [BJC-189](https://linear.app/bjc/issue/BJC-189) | Salesforce engine-x service client — SalesforceEngineClient + SalesforceSyncer | 5 pts | High | backend, integration, Feature, M4 |
| [BJC-190](https://linear.app/bjc/issue/BJC-190) | ClickHouse CRM tables — crm_contacts + crm_opportunities schema and writer | 3 pts | High | backend, infrastructure, Feature, M4 |
| [BJC-191](https://linear.app/bjc/issue/BJC-191) | CRM sync Trigger.dev task — HubSpot (6-hour cadence, per-tenant fan-out) | 5 pts | High | backend, integration, Feature, M4 |
| [BJC-192](https://linear.app/bjc/issue/BJC-192) | CRM sync Trigger.dev task — Salesforce (6-hour cadence, per-tenant fan-out) | 5 pts | High | backend, integration, Feature, M4 |

**Total: 26 story points across 6 issues.**

### BJC-82 Superseded
[Comment added to BJC-82](https://linear.app/bjc/issue/BJC-82/crm-ingestion-salesforce-hubspot-clients-sync-task#comment-d089d762) noting it has been decomposed into BJC-187 through BJC-192. Ready to close/archive.

---

## Architectural Decisions

### Hybrid CRM Abstraction
- Separate HTTP clients per CRM provider (`HubSpotEngineClient`, `SalesforceEngineClient`)
- Shared `BaseCRMSyncer` protocol defining the sync contract
- Canonical Pydantic models (`CRMContact`, `CRMOpportunity`) normalize data across providers
- ClickHouse tables unified with a `crm_source` discriminator column (`'hubspot'` | `'salesforce'`)

### Service-to-Service Architecture
PaidEdge does **not** call Salesforce or HubSpot APIs directly. It calls the engine-x services as service-to-service consumers:
- `hubspot-engine-x` — full CRM read endpoints available (search, list, get contacts/companies/deals)
- `sfdc-engine-x-api` — **write-only today** (38 endpoints, all push-oriented). No CRM read/search/query endpoints exist.

### ROAS = 0.0 Gap
`campaign_metrics` in ClickHouse currently has `roas = 0.0` for all rows. CRM integration fills this gap by connecting ad spend data to deal revenue from Salesforce/HubSpot opportunities.

---

## Dependency Graph

```
BJC-187  Shared CRM abstraction layer
  ├── BJC-188  HubSpot service client        ──┐
  ├── BJC-189  Salesforce service client*     ──┤
  └── BJC-190  ClickHouse CRM tables         ──┤
                                                │
        BJC-188 + BJC-190 ──► BJC-191  HubSpot sync task
        BJC-189 + BJC-190 ──► BJC-192  Salesforce sync task*

  * BJC-189 and BJC-192 are also blocked on sfdc-engine-x
    getting a SOQL query proxy endpoint (external dependency)
```

---

## Recommended Build Order

### Phase 1 — Foundation (parallel, ~1 week)
- **BJC-187** — Shared abstraction layer (`BaseCRMSyncer` protocol, `CRMContact`/`CRMOpportunity` Pydantic models). No external dependencies; unblocks everything else.
- **BJC-190** — ClickHouse schema (`crm_contacts`, `crm_opportunities` with `crm_source` discriminator column). Can proceed in parallel with BJC-187.

### Phase 2 — CRM Clients (parallel, ~1.5 weeks)
- **BJC-188** — HubSpot client. Depends on BJC-187. HubSpot engine-x already has full read endpoints — no external blockers.
- **BJC-189** — Salesforce client. Depends on BJC-187. **Blocked** until someone adds a SOQL query proxy endpoint to `sfdc-engine-x-api` (currently write-only, zero CRM read endpoints). This is the critical-path risk.

### Phase 3 — Sync Tasks (sequential after their clients)
- **BJC-191** — HubSpot sync task. Depends on BJC-188 + BJC-190. Can ship as soon as Phase 1 + HubSpot client are done — **this is the fastest path to closing the ROAS = 0.0 gap** for HubSpot tenants.
- **BJC-192** — Salesforce sync task. Depends on BJC-189 + BJC-190. Blocked behind the sfdc-engine-x SOQL endpoint.

---

## Critical Path Risk

The single blocker for the Salesforce path is the missing SOQL query proxy in `sfdc-engine-x-api`. That repo currently has 38 endpoints but all are write-heavy (push contacts, push opportunities, manage field mappings). Zero read/search/query endpoints exist. At minimum, a `POST /api/salesforce/soql/query` endpoint must be added before BJC-189 can be completed.

**HubSpot has no such blocker and can go end-to-end immediately.**

---

## Repository Investigation Summaries

### sfdc-engine-x-api
- **38 endpoints**, all write-heavy / push-oriented
- Handles: contact sync, opportunity sync, field mappings, campaign member sync, OAuth flows
- **No CRM read endpoints** — no search, list, get, or SOQL query proxy
- Auth: per-tenant OAuth2 with token refresh, stored in Supabase
- Stack: Express.js + TypeScript, jsforce library

### hubspot-engine-x
- **35 endpoints**, full CRUD including reads
- Handles: contacts, companies, deals, lists, search, properties, associations, workflows
- **Full CRM read path available** — `GET /contacts/search`, `GET /contacts/list`, `GET /contacts/:id`, same for companies and deals
- Auth: per-tenant OAuth2 with token refresh, stored in Supabase
- Stack: Express.js + TypeScript, @hubspot/api-client

### paid-edge-backend-api
- Existing `DataEngineXClient` pattern for service-to-service calls (reference implementation)
- ClickHouse tables: `campaign_metrics`, `ad_performance`, `attribution_events` — CRM tables will follow same patterns
- Trigger.dev fan-out pattern: `syncAdMetrics` task iterates tenants, spawns per-tenant sub-tasks
- Multi-tenant: tenant isolation via `tenant_id` columns, per-tenant credentials in Supabase
