# PaidEdge — Agent Operations Guide

**Last updated:** March 24, 2026
**Purpose:** Canonical reference for any AI agent working on PaidEdge. Read this before doing anything.

---

## 1. What PaidEdge Is

Multi-tenant B2B SaaS platform for heads of demand generation. Unifies audience building (from real signals), AI-generated campaign assets, campaign launch to ad platforms, cross-platform analytics, CRM revenue attribution, and competitor ad monitoring.

**Canonical specs (all three are fully merged and up to date — no separate updates doc needed):**
- `PaidEdge_PRD.md` (v3.0) — product vision, core loop, business model, milestones, design direction
- `PaidEdge_Backend_Spec.md` (v2.0) — FastAPI app: every API endpoint, Supabase DDL, ClickHouse DDL, integration contracts, auth middleware
- `PaidEdge_Frontend_Spec.md` (v2.0) — Next.js app: every route, page spec, component breakdown, data fetching, auth flow

---

## 2. Infrastructure Access

### ClickHouse Cloud
- **Host:** `gf9xtjjqyl.us-east-1.aws.clickhouse.cloud`
- **Port:** 8443 (HTTPS)
- **User:** `default`
- **Database:** `paid_edge` (PaidEdge data) — separate from DemandEdge's `raw`/`raw_crm`/`core`
- **Query via curl:**
  ```bash
  curl -s "https://gf9xtjjqyl.us-east-1.aws.clickhouse.cloud:8443" \
    --user "default:$CLICKHOUSE_PASSWORD" \
    --data "SELECT * FROM paid_edge.campaign_metrics LIMIT 10"
  ```
- **Rule:** All queries MUST filter by `tenant_id`. No exceptions.
- **Rule:** Do NOT touch DemandEdge databases (`raw`, `raw_crm`, `core`).

### Supabase (paid-edge project)
- **Direct host:** `db.mkxlkcudcrrzfuqvdeev.supabase.co` (IPv6 only — may not work from all environments)
- **Pooler host (use this):** `aws-0-us-west-2.pooler.supabase.com:6543`
- **Pooler user:** `postgres.mkxlkcudcrrzfuqvdeev`
- **Database:** `postgres`
- **Connection via Python:**
  ```python
  import psycopg2
  conn = psycopg2.connect(
      host='aws-0-us-west-2.pooler.supabase.com',
      port=6543,
      dbname='postgres',
      user='postgres.mkxlkcudcrrzfuqvdeev',
      password='$SUPABASE_DB_PASSWORD',
      sslmode='require'
  )
  ```
- **Also available** via Perplexity Computer's PostgreSQL connector (source_id: `postgresql__pipedream`). Use `postgresql-execute-custom-query` tool.
- **Rule:** All tables with `organization_id` have RLS policies. Respect tenant isolation.

### GitHub Repos
- **Backend:** `bencrane/paid-edge-backend-api` — FastAPI (Python 3.12+)
- **Frontend:** `bencrane/paid-edge-frontend-app` — Next.js 14+ (TypeScript)
- **Access:** Via `gh` CLI with `api_credentials=["github"]`
  ```bash
  gh repo clone bencrane/paid-edge-backend-api
  gh repo clone bencrane/paid-edge-frontend-app
  ```
- **Ignore:** `bencrane/paid-edge-master-app` (deprecated monorepo concept)

### Railway
- Both apps deploy to Railway on push to `main`
- Backend: FastAPI on port 8000, health check at `/health`
- Frontend: Next.js on port 3000, health check at `/`

### Doppler
- **Project:** `paid-edge`
- **Configs:** `dev`, `stg`, `prd`
- All secrets (Supabase, ClickHouse, RudderStack, API keys) live in Doppler. Never hardcode credentials.

### RudderStack
- **Data plane:** `https://substratevyaxk.dataplane.rudderstack.com`
- Existing DemandEdge sources (`demand_gen_web`, `demand_gen_crm`) — do not touch
- PaidEdge sources being added alongside

### Linear
- **Connector:** `linear_alt` (via Perplexity Computer external tools)
- **Team:** BJC (`2ba6bb7a-8f07-4eb0-b355-1c0bcb5ad9d9`)
- **Project:** PaidEdge (`c4d50fe4-2c7e-4636-816c-93c3ca394339`)
- All IDs (states, labels, etc.) in `/home/user/workspace/linear_ids.json`

---

## 3. Linear Project Management

### How We Track Work

All work is tracked in the **PaidEdge** project in Linear. The project contains 46 issues organized across 6 milestones.

### IDs Reference

Full ID reference file: `/home/user/workspace/linear_ids.json`

**Workflow states:**
| State | ID | When to use |
|-------|----|-------------|
| Backlog | `8804f7a5-858c-451c-9c9a-dd7d9e6020ac` | Future milestone work, not yet scheduled |
| Todo | `e2afa297-b85f-4dde-9752-0af95e46f566` | Current milestone, ready to start |
| In Progress | `89834386-5382-4851-99f4-e82f37ba0278` | Actively being worked on |
| In Review | `b3ea90da-6c7e-4f47-afb5-4f297a310c29` | Code complete, reviewing |
| Done | `096ad9c8-cbc4-4e93-9174-17a0cc2c3a7e` | Complete and verified |
| Canceled | `51891af7-fb8d-4ec1-92b8-0740b3f39b97` | Won't do |

**Milestone labels:**
| Label | ID | Color |
|-------|----|-------|
| M1: Infrastructure | `3cd39273-bad8-42fa-a86b-34dd8a05fdd7` | #ef4444 |
| M2: Audience Engine | `2b1a89ad-a744-4cd5-a068-13280ee1b906` | #f97316 |
| M3: Campaigns + Assets | `b5292a02-6e1b-4b47-880e-f2c3a222c1ab` | #eab308 |
| M4: Analytics + Attribution | `f2c500a6-5d17-4630-8cfc-d0c7c9d67896` | #22c55e |
| M5: AI + Polish | `1bb37825-8704-47ea-984e-c491cffe8bd7` | #3b82f6 |
| M6: First Customer | `9812c1b3-0d49-4d4b-b6ce-abb64f7d2b4b` | #8b5cf6 |

**Type labels:**
| Label | ID | Use when |
|-------|----|----------|
| Feature | `743f9144-fa49-49d3-b97d-9f73ca841e7b` | New functionality |
| Bug | `de8e84ed-0e74-4536-987c-84598559ed91` | Fixing broken behavior |
| Improvement | `97489e15-3471-4664-8f70-7f3be4b38f63` | Improving without changing behavior |

**Domain labels:**
| Label | ID | Technical area |
|-------|----|---------------|
| backend | `7d630ede-2d10-4bc0-88f9-3421077c73b4` | APIs, server, database |
| frontend | `a1e32ed8-f5ae-4151-b408-15c8997174fa` | UI, React, styling |
| infrastructure | `abd316b5-2140-4bd4-8731-b5c35e1053ca` | CI/CD, deployment, databases |
| integration | `f06525bf-fca9-4959-9da4-3d00e8d3b422` | Third-party services |

### Issue Map by Milestone

**M1: Infrastructure Foundation** (7 issues, Priority: High, State: Todo)
| ID | Title |
|----|-------|
| BJC-48 | Create paid_edge database in ClickHouse with all tables |
| BJC-50 | Deploy Supabase multi-tenant schema with RLS policies |
| BJC-54 | FastAPI backend skeleton — auth, tenant resolution, health checks |
| BJC-58 | Next.js frontend skeleton — App Router, auth, sidebar, org switcher |
| BJC-60 | Railway deployment pipeline for backend and frontend |
| BJC-63 | Configure Doppler secrets for both apps |
| BJC-66 | Configure RudderStack sources for PaidEdge |

**M2: Audience Engine** (10 issues, Priority: Medium, State: Backlog)
| ID | Title |
|----|-------|
| BJC-68 | BlitzAPI integration client |
| BJC-71 | Signal provider framework (pluggable module system) |
| BJC-74 | Signal providers: BlitzAPI signals (new_in_role, exec_departed, promoted, raised_money) |
| BJC-77 | Signal providers: lookalike, page_visitor, linkedin_engager, form_fill |
| BJC-79 | Trigify + Clay integration clients |
| BJC-81 | Audience CRUD API endpoints |
| BJC-83 | Chat-driven audience builder (Claude API) |
| BJC-85 | Trigger.dev tasks: audience refresh (daily + hourly) |
| BJC-87 | Frontend: Dashboard — signal cards + KPI row |
| BJC-88 | Frontend: Audiences pages (list, create, detail) |

**M3: Campaigns + Assets** (8 issues, Priority: Medium, State: Backlog)
| ID | Title |
|----|-------|
| BJC-46 | Campaign CRUD API endpoints |
| BJC-52 | Claude API asset generation service (8 asset types) |
| BJC-55 | Asset API endpoints — generate, preview, approve |
| BJC-57 | Landing page hosting via FastAPI + form handling |
| BJC-61 | Audience push: CSV export per ad platform format + Prospeo email resolution |
| BJC-64 | dub.co tracked link integration |
| BJC-69 | Frontend: Chat-driven campaign builder |
| BJC-72 | Frontend: Campaign list and detail pages |

**M4: Analytics + Attribution** (8 issues, Priority: Medium, State: Backlog)
| ID | Title |
|----|-------|
| BJC-76 | Ad platform API clients: LinkedIn, Meta, Google Ads |
| BJC-80 | Trigger.dev task: ad platform metrics sync (every 6 hours) |
| BJC-82 | CRM ingestion: Salesforce + HubSpot clients + sync task |
| BJC-84 | Attribution matching service + daily task |
| BJC-86 | Analytics ClickHouse queries + API endpoints |
| BJC-89 | Attribution API endpoints |
| BJC-90 | Frontend: Analytics dashboard |
| BJC-91 | Frontend: Attribution pages + campaign attribution tab |

**M5: AI + Polish** (7 issues, Priority: Low, State: Backlog)
| ID | Title |
|----|-------|
| BJC-47 | Claude API performance analysis + recommendation generation |
| BJC-49 | Recommendation API endpoints + approve/dismiss |
| BJC-51 | Competitor ad monitoring: Adyntel integration + API endpoints |
| BJC-53 | Campaign health scoring |
| BJC-56 | Frontend: Recommendation cards + Competitor Ads page |
| BJC-59 | Frontend: AI chat sidebar (persistent, context-aware) |
| BJC-62 | UI polish: loading states, error handling, empty states, responsive |

**M6: First Customer Live** (6 issues, Priority: Low, State: Backlog)
| ID | Title |
|----|-------|
| BJC-65 | Onboarding flow: CRM → ad platforms → site snippet → brand context |
| BJC-67 | Settings: Integrations page (OAuth, webhooks, API keys) |
| BJC-70 | Settings: Brand & Content + Organization + Team pages |
| BJC-73 | Webhook handlers: RudderStack, BYO visitor ID, ad platforms |
| BJC-75 | Stripe billing integration |
| BJC-78 | End-to-end testing with real ad platform accounts |

### Linear Operations — How To

**Read an issue:**
```
call_external_tool(tool_name="linear__get_issue", source_id="linear_alt", arguments={"input": {"id": "BJC-48"}})
```

**Update issue status (when starting work):**
```
call_external_tool(tool_name="linear__update_issue", source_id="linear_alt", arguments={
  "id": "BJC-48",
  "stateId": "89834386-5382-4851-99f4-e82f37ba0278"  // In Progress
})
```

**Update issue status (when done):**
```
call_external_tool(tool_name="linear__update_issue", source_id="linear_alt", arguments={
  "id": "BJC-48",
  "stateId": "096ad9c8-cbc4-4e93-9174-17a0cc2c3a7e"  // Done
})
```

**Add a comment to an issue:**
```
call_external_tool(tool_name="linear__create_comment", source_id="linear_alt", arguments={
  "issueId": "BJC-48",
  "body": "## Progress Update\n\nClickHouse tables created. Verified with DESCRIBE on all 6 tables."
})
```

**Create a new issue:**
```
call_external_tool(tool_name="linear__create_issue", source_id="linear_alt", arguments={"input": {
  "teamId": "2ba6bb7a-8f07-4eb0-b355-1c0bcb5ad9d9",
  "projectId": "c4d50fe4-2c7e-4636-816c-93c3ca394339",
  "title": "Issue title",
  "description": "Markdown description with acceptance criteria",
  "priority": 3,
  "stateId": "8804f7a5-858c-451c-9c9a-dd7d9e6020ac",
  "labelIds": ["743f9144-fa49-49d3-b97d-9f73ca841e7b", "7d630ede-2d10-4bc0-88f9-3421077c73b4"],
  "assigneeId": null, "estimate": null, "cycleId": null, "parentId": null, "dueDate": null,
  "subscriberIds": null, "sortOrder": null, "displayIconUrl": null, "subIssueSortOrder": null,
  "createAsUser": null, "createdAt": null, "custom_fields": null, "projectMilestoneId": null
}})
```

**List issues for the project:**
```
call_external_tool(tool_name="linear__get_issues", source_id="linear_alt", arguments={
  "filter": {
    "project": {"id": "c4d50fe4-2c7e-4636-816c-93c3ca394339"},
    "id": null, "assignee": null, "creator": null, "createdAt": null, "updatedAt": null,
    "completedAt": null, "status": null, "team": null, "labels": null, "priority": null,
    "searchableContent": null
  },
  "forward_pagination": {"limit": 20, "cursor": null},
  "backward_pagination": null
})
```

**IMPORTANT:** The `linear_alt` connector does NOT support parallel calls. Always make Linear API calls sequentially, one at a time. Never issue two Linear calls in the same function_calls block.

### When to Update Linear

| Trigger | Action |
|---------|--------|
| Starting work on an issue | Set state to "In Progress" |
| Code complete, ready for review | Set state to "In Review" |
| Issue is fully done and verified | Set state to "Done" |
| Discovered something unexpected | Add a comment explaining the finding |
| Scope changed during implementation | Add a comment with the updated scope |
| Created a sub-task not in the plan | Create a new issue with proper labels, link as child |
| Starting a new milestone | Move that milestone's issues from Backlog → Todo |
| Blocked on something | Add a comment explaining the blocker |

### Label Rules

Every issue gets:
1. **Exactly one milestone label** (M1–M6)
2. **Exactly one type label** (Feature, Bug, or Improvement)
3. **1–2 domain labels** (backend, frontend, infrastructure, integration)

---

## 4. Build Order and Dependencies

Milestones are sequential: M1 → M2 → M3 → M4 → M5 → M6.

### M1 Dependency Graph
```
BJC-48 (ClickHouse) ──┐
BJC-50 (Supabase)  ───┤
BJC-63 (Doppler)   ───┼──→ BJC-54 (FastAPI skeleton) ──→ BJC-60 (Railway deploy)
BJC-66 (RudderStack) ─┘    BJC-58 (Next.js skeleton) ──→ BJC-60 (Railway deploy)
```
Start with BJC-48, BJC-50, BJC-63, BJC-66 in parallel. Then BJC-54 + BJC-58. Then BJC-60.

### M2 Dependency Graph
```
BJC-68 (BlitzAPI client) ──→ BJC-74 (BlitzAPI signals) ──→ BJC-85 (Trigger.dev refresh)
BJC-79 (Trigify+Clay)    ──→ BJC-77 (other signals)    ──→ BJC-85
BJC-71 (signal framework) ──→ BJC-74, BJC-77
BJC-83 (chat builder)     ──→ BJC-81 (audience CRUD) requires Claude client
BJC-81 (audience CRUD)    ──→ BJC-87 (dashboard), BJC-88 (audiences pages)
```

### Cross-Milestone Dependencies
- M2 requires M1 complete (needs working API, auth, databases)
- M3 requires M2 audience CRUD (campaigns reference audience segments)
- M4 requires M3 campaigns (analytics report on campaign data)
- M5 requires M4 analytics (recommendations analyze performance data)
- M6 requires M5 (polish before customer launch)

---

## 5. Constraints — Always Follow These

1. **All ClickHouse queries MUST filter by tenant_id.** No exceptions. Even admin queries should scope to a tenant.
2. **All Supabase tables with tenant data MUST have RLS policies.** Enforce tenant isolation via `organization_id IN (SELECT organization_id FROM memberships WHERE user_id = auth.uid())`.
3. **Frontend talks to backend only.** Never query ClickHouse or external APIs directly from the Next.js app.
4. **Two separate repos.** `paid-edge-backend-api` (FastAPI/Python) and `paid-edge-frontend-app` (Next.js/TypeScript). Keep them cleanly separated.
5. **Doppler for all secrets.** Never hardcode credentials, API keys, or connection strings in code.
6. **Don't touch DemandEdge infrastructure.** Existing `raw`/`raw_crm`/`core` databases, existing RudderStack sources — leave them alone.

---

## 6. Key Product Decisions (from Spec Updates)

These override the original specs where they conflict:

1. **Campaign builder is chat-driven, not a wizard.** Single route `/campaigns/new` with split-panel: chat left, assembly blocks right. No sub-routes per step.
2. **8 asset types** (not 5). Added: `document_ad` (LinkedIn carousel PDF), `video_script` (production brief), `case_study_page` (narrative landing page).
3. **Competitor ad monitoring** via Adyntel. New `competitor_configs` table, weekly sync task, `/competitors` page.
4. **Tracked links** via dub.co. Every campaign gets a short link for independent attribution.
5. **No global asset library.** Assets are campaign-ephemeral. No "Assets" sidebar item.
6. **Sidebar nav:** Overview (Dashboard, Attribution) → Campaigns (Active Campaigns, Campaign Builder, Audiences) → Intelligence (Recommendations, Competitor Ads) → Settings.
7. **Design direction:** Dark mode default (#09090b), green accents (#00e87b), Instrument Sans body, JetBrains Mono numbers, lucide-react icons, no emojis.

---

## 7. Working with the Perplexity Computer Connector Ecosystem

### Available Connectors
| Connector | source_id | Use for |
|-----------|-----------|---------|
| Linear | `linear_alt` | Project management, issue tracking |
| PostgreSQL (Supabase) | `postgresql__pipedream` | Direct Supabase queries |
| Google Ads | `google_ads__pipedream` | Ad platform integration (later milestones) |
| GitHub | `github_mcp_direct` | Code via `gh` CLI with `api_credentials=["github"]` |

### Tool Calling Pattern
1. `list_external_tools` — discover available connectors
2. `describe_external_tools` — get input schemas (MUST do before calling)
3. `call_external_tool` — execute the tool

### GitHub Operations
```bash
# Clone a repo
gh repo clone bencrane/paid-edge-backend-api

# Push changes
cd /home/user/workspace/paid-edge-backend-api
git add -A && git commit -m "feat: implement audience CRUD endpoints" && git push origin main
```
Always use `api_credentials=["github"]` when running `gh` or `git` commands.

---

## 8. File Map — What Lives Where

```
/home/user/workspace/
├── PAIDEDGE_AGENT_OPS.md           ← YOU ARE HERE. Read this first.
├── linear_ids.json                  ← All Linear UUIDs (team, project, states, labels)
├── PaidEdge_PRD.md                  ← Canonical PRD v3.0 (fully merged, single source of truth)
├── PaidEdge_Backend_Spec.md         ← Canonical backend spec v2.0 (fully merged)
├── PaidEdge_Frontend_Spec.md        ← Canonical frontend spec v2.0 (fully merged)
├── created_issues_m1_m2.txt         ← Issue IDs and URLs for M1+M2
├── created_issues_m3_m4.txt         ← Issue IDs and URLs for M3+M4
├── created_issues_m5_m6.txt         ← Issue IDs and URLs for M5+M6
│
├── [ARCHIVED — superseded by canonical versions above]
├── PaidEdge_PRD_v2-3.md             ← Original PRD (pre-merge)
├── PaidEdge_Backend_Spec-2.md       ← Original backend spec (pre-merge)
├── PaidEdge_Spec_Updates.md         ← Updates doc (now merged into canonical specs)
└── paidedge_plan.md                 ← Pre-creation issue draft (now in Linear)
```

---

## 9. Onboarding Checklist for New Agent Sessions

When you start a new session working on PaidEdge:

1. **Read this file** (`PAIDEDGE_AGENT_OPS.md`)
2. **Check Linear** for current issue states — what's Done, what's In Progress, what's next
3. **Check the repos** — `gh repo view bencrane/paid-edge-backend-api` and `paid-edge-frontend-app` to see latest commits
4. **Verify infrastructure access** — ClickHouse (curl test), Supabase (psycopg2 test)
5. **Ask Benjamin** if anything is unclear about priority or scope
6. **Update Linear** as you work — move issues to In Progress when starting, Done when complete, add comments for notable findings

---

## 10. Communication Style

- Benjamin is the product owner. He makes all product decisions.
- When you find ambiguity in the specs, check this ops guide first, then the Spec Updates doc, then ask Benjamin.
- Keep Linear comments concise and factual: what was done, what was found, what's blocked.
- When completing an issue, list what was built and confirm each acceptance criterion was met.
- Don't ask for permission to proceed through planned work. Build it, update Linear, move on. Only stop for blockers or scope questions.
