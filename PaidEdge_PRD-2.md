# PaidEdge — Product Requirements Document

**Version:** 3.0  
**Date:** March 24, 2026  
**Author:** Benjamin, Outbound Solutions  
**Status:** Ready for build (canonical)  
**Companion specs:** `PaidEdge_Backend_Spec.md`, `PaidEdge_Frontend_Spec.md`

> This is the single canonical PRD for PaidEdge. It supersedes `PaidEdge_PRD_v2.md` and incorporates all decisions from `PaidEdge_Spec_Updates.md`. Someone reading only this file should understand the full product.

---

## 1. What PaidEdge Is

PaidEdge is a multi-tenant SaaS platform for B2B heads of demand generation. It unifies audience building, campaign asset generation, campaign launch, cross-platform performance analytics, competitor ad monitoring, and CRM revenue attribution into one product. It replaces the 5–10 tool patchwork that demand gen teams currently use to run paid advertising.

**One-sentence pitch:** "Build smarter audiences from real signals, launch campaigns with AI-generated assets, monitor competitor ads, and see exactly which campaigns generated revenue — all in one place."

---

## 2. Target Users

**Primary:** Head of Demand Generation at a B2B technology company (Series A–C, $5–50M ARR). This person tests paid growth strategies, scales what works, and proves ROI to leadership. They currently juggle Clay, LinkedIn Campaign Manager, Meta Ads Manager, Google Ads, Canva, Salesforce reports, and spreadsheets.

**Secondary:** Paid media agencies running B2B campaigns for multiple clients. Each client is a tenant in PaidEdge's multi-tenant architecture.

---

## 3. The Problem

No B2B demand gen leader has all of these unified today:

- **Audience discovery** — finding targetable segments based on real buying signals, not just static lists
- **Asset creation** — generating lead magnets, landing pages, document ads, video scripts, case studies, and ad creative tailored to each audience and angle
- **Campaign launch** — pushing audiences and creative to ad platforms from one interface
- **Performance tracking** — seeing cross-platform campaign metrics in a unified view with tracked links for independent attribution
- **Revenue attribution** — tying campaign spend to actual CRM pipeline and closed-won revenue
- **Competitive intelligence** — seeing what competitors are running across ad platforms

They have fragments across many tools. The result is fragmented data, slow execution, no unified view, and an inability to answer "what's actually working and why."

---

## 4. The Core Loop

PaidEdge delivers a closed loop. Each stage feeds the next.

### 4.1 Audience Building — Signal Cards

On login, the user sees actionable audience signal cards. Each represents a targetable segment backed by real data, ready to be activated into a campaign.

**V1 signals (in product at launch):**

| Signal | Source | Cost |
|--------|--------|------|
| New in role | BlitzAPI delta detection | Near-zero |
| Senior exec departed | BlitzAPI delta detection | Near-zero |
| Promoted | BlitzAPI delta detection | Near-zero |
| Company raised money | BlitzAPI / public data | Near-zero |
| Lookalikes of closed-won | CRM + BlitzAPI matching | Near-zero |
| Closed-lost exclusions | CRM pull | Zero |
| LinkedIn post engagers | Trigify API ($0.012/credit) or Clay | Low |
| Companies visiting key pages | Clay web intent (~4 credits/lookup, ~$0.20/resolution) on RudderStack events. Company-level only. PaidEdge absorbs cost. | Low (~$40-100/mo per tenant) |
| Lead magnet downloaders | Self-identified via form fill | Zero |
| Custom segments via chat | Claude API → enrichment queries | Near-zero |
| Deep research signals | Parallel.ai / OpenClaw | TBD |

**Coming later (post-V1):**

| Signal | Source | Blocker |
|--------|--------|---------|
| "Companies searching for [category]" | TrustRadius direct API or via Clay managed integration | Annual contract cost (direct) or Clay Growth plan. Will pursue when revenue justifies. |
| Topic surge intent | Bombora or via Propensity | Requires $2K+/mo direct deal |

The signal provider system is pluggable — each signal type is a module implementing a common interface. New signals can be added without touching core logic.

### 4.2 Campaign Asset Generation

Once an audience is selected, PaidEdge generates campaign-specific assets using AI (Claude API).

**Core principle:** Assets are generated per-campaign, not pre-created globally. There is NO global asset library. Assets are campaign-ephemeral. The marginal cost of AI-generated content is effectively zero, so every campaign gets bespoke assets.

**Where assets appear:**
- Within the campaign builder (generated during campaign creation)
- Within the campaign detail view (assets tab showing what was generated for this campaign with performance data)

**Asset types (8 total):**

| Asset Type | Key | Format | Description |
|------------|-----|--------|-------------|
| Lead magnet | `lead_magnet` | PDF | Downloadable guides, whitepapers, checklists |
| Landing page | `landing_page` | HTML (PaidEdge-hosted) | Conversion-optimized pages with RudderStack instrumentation |
| Document ad | `document_ad` | PDF (multi-slide carousel) | LinkedIn Document Ads. 5–8 slides, swipeable in-feed. Highest-performing LinkedIn format (22%+ completion rates). Claude generates slide content; PDF rendered via python-pptx or reportlab. Stored in Supabase Storage. |
| Ad copy | `ad_copy` | Text (per platform) | Platform-specific ad headlines and body copy |
| Video script | `video_script` | Structured text | 30-second talking-head video script with hook, body, CTA, shot direction, caption overlay text. Human records, AI writes. Not a video file — a production brief. |
| Case study page | `case_study_page` | HTML (landing page variant) | Conversion-optimized case study page generated from tenant context (customer win stories). Narrative structure: situation → challenge → solution → results. Can auto-generate a companion 5-slide Document Ad carousel from the same inputs. |
| Email copy | `email_copy` | Text | Nurture email sequences |
| Image brief | `image_brief` | Text | Creative direction for visual assets |

**DB constraint:**
```sql
CHECK (asset_type IN ('lead_magnet', 'landing_page', 'document_ad', 'ad_copy', 'video_script', 'case_study_page', 'email_copy', 'image_brief'))
```

**Client onboarding inputs** serve as persistent raw material the AI draws from: customer lists, recorded testimonials (transcribed), case study narratives, brand guidelines, product positioning, competitor differentiators, ICP definition. These are NOT finished assets — they're context that makes generated assets sharp and relevant.

### 4.3 Campaign Builder — Chat-Driven Assembly

The campaign builder is a **chat-driven interface with a split-panel layout**, not a step-by-step wizard.

**Layout:**

- **Left panel — Chat interface.** The AI is the primary interaction surface. User describes what they want ("compliance angle, target CISOs, LinkedIn + Meta, $3K budget"), and the AI builds the campaign.
- **Right panel — Campaign assembly view.** Blocks populate and update in real-time as the AI works through each stage. Blocks: Research, Audience, Assets, Config, Pre-Launch Checks.
- **Bottom bar:** Persistent summary (total budget, audience size, asset count, check status) + "Approve & Launch" button.
- **Helper buttons** below chat input for common adjustments ("Tighten audience", "Add Google search", "More aggressive CTA", "Increase budget").

**Behavior:**

- The AI may initiate the conversation by surfacing an intent signal or recommendation ("12 accounts are actively researching endpoint security this week — want me to build a campaign?").
- User can intervene at any point via chat to adjust any block (tighten audience, change budget, swap angle, regenerate an asset).
- The chat uses competitor ad intelligence as context when available ("CrowdStrike has been running this angle for 6 weeks — validates durability").

**Route:** Single route `/campaigns/new` with the split-panel layout. No sub-routes per step.

**State:** Campaign assembly state lives in a Zustand store. Chat messages are component state (not persisted for V1). Each campaign block on the right panel reads from the store and updates reactively.

### 4.4 Campaign Launch

User describes campaign via chat → reviews AI-assembled blocks → adjusts as needed → approves and launches.

**Audience push to ad platforms — V1 approach:** CSV export formatted per platform (manual upload). Direct API integrations (LinkedIn Marketing API, Meta Custom Audiences, Google Customer Match) built over time.

**V2 approach:** Explore Clay Ads for managed audience sync to LinkedIn/Meta. This would replace the need to build direct ad platform integrations for audience push. The cost is Clay credits rather than engineering time.

**Email resolution for audience push:** Prospeo API (Enrich Person endpoint, `company_website` preferred) or Clay credits. Match rates will be "good enough" — ad platform matching is secondary to the audience building and analytics value.

**Tracked links:** Every campaign auto-generates a tracked short link via dub.co (e.g., `pe.link/cmmc-q1`) pointing to the campaign's landing page or destination URL. The short link is used in ad copy and shown in the campaign config. Click analytics from dub.co supplement ad platform metrics, providing an independent attribution layer outside of ad platform click IDs.

**Optional:** Sendoso integration for incentive-attached offers (e.g., "book a meeting, get a $50 gift card").

### 4.5 Performance Analytics

Campaign metrics pulled from ad platform APIs (LinkedIn, Meta, Google Ads) via RudderStack into ClickHouse.

**Dashboard views:**
- Campaign overview — all campaigns, all platforms, sortable by any metric
- Single campaign detail — platform breakdown, budget pacing, time series charts, tracked link click analytics
- Cross-platform comparison — same segment on LinkedIn vs Meta vs Google, side by side
- AI recommendations — Claude API analyzes campaign data and generates actionable cards ("Increase Meta retargeting +15%") with projected impact and confidence
- Campaign health scoring — composite metric

### 4.6 Revenue Attribution

**This is the key differentiator.** CRM data (Salesforce or HubSpot) pulled into ClickHouse via RudderStack.

**Attribution chain:** Ad click → tracked link (dub.co) → PaidEdge landing page (UTM tagged, RudderStack instrumented) → form fill (person self-identifies) → CRM lead/contact → opportunity → closed-won.

**What PaidEdge shows that no ad platform can:**
- Cost-per-opportunity (campaign spend / opportunities created)
- Cost-per-closed-won (campaign spend / deals won)
- Pipeline $ influenced by campaigns
- Closed-won firmographic profile → fed back into audience building as lookalike seed

### 4.7 Competitor Ad Monitoring

PaidEdge tracks competitor ad activity across LinkedIn, Meta, and Google via Adyntel.

**Setup:** Users configure competitors to track (domain + name + platforms) in Settings. Tracking runs weekly via a Trigger.dev scheduled task.

**What users see:**
- Per-competitor ad listings: platform badge, ad format, headline/body preview, duration running, status (active/new/stopped)
- Filterable by competitor and platform
- Changes flagged: new ads, stopped ads, long-running ads

**Integration with the rest of the product:**
- Competitor intel feeds into AI recommendations and campaign builder chat context
- The AI references competitor activity when suggesting campaign angles

---

## 5. Sidebar Navigation

```
Overview
  ├── Dashboard
  └── Attribution
Campaigns
  ├── Active Campaigns
  ├── Campaign Builder
  └── Audiences
Intelligence
  ├── Recommendations
  └── Competitor Ads
Settings
```

There is no "Assets" page in the sidebar. Assets exist only within campaign context.

---

## 6. What's NOT in the Product

| Exclusion | Reasoning |
|-----------|-----------|
| Person-level visitor deanonymization | Not core. HockeyStack (closest comp) doesn't do this natively either — they say "BYO Vector." If customer wants RB2B/Vector, they install it themselves. PaidEdge does company-level only via Clay web intent. |
| Intent data (TrustRadius, Bombora) | Too expensive for V1. "Coming soon" on roadmap. V2 may access TrustRadius via Clay managed integration. |
| Outbound email sequencing | PaidEdge is a paid advertising platform. Hands off leads to Outreach/Salesloft/Apollo. |
| CRM replacement | Reads from Salesforce/HubSpot. Does not replace them. |
| Global asset library | Assets are campaign-ephemeral. There is no "Assets" page, no global asset browser. Client onboarding inputs (tenant context) are the raw material, not finished assets. |
| Video/motion graphic generation | Out for V1. Video scripts (production briefs) are in scope; actual video rendering is not. |

---

## 7. BYO Integrations (Supported, Not Provided)

Customers can optionally connect their own tools. Nothing breaks without them:

| Integration | What It Adds |
|-------------|-------------|
| RB2B / Vector / Warmly | "Identified Visitors" breakout view within campaigns — who clicked but didn't convert. Webhook ingestion. |
| 6sense / Demandbase | Account-level intent enrichment overlay. |
| Sendoso | Incentive-attached campaign offers. |

---

## 8. Data Architecture

### 8.1 Stack

| Component | Role |
|-----------|------|
| **ClickHouse Cloud** | Metrics warehouse — campaign metrics, CRM data, behavioral events, audience members, web intent results. Existing instance (`gf9xtjjqyl.us-east-1.aws.clickhouse.cloud`), new `paid_edge` database. |
| **Supabase** | Entity DB + auth + multi-tenancy. New project (separate from data-engine-x). Organizations, users, memberships, segments, campaigns, assets, competitor configs, tenant context, provider configs. |
| **RudderStack** | Event ingestion. Existing account (`substratevyaxk.dataplane.rudderstack.com`), new sources for PaidEdge. Shared source with `tenant_id` property on all events (injected via transformation). JS SDK on customer sites + PaidEdge landing pages. Cloud Sources for ad platform and CRM data. |
| **FastAPI** | Backend API. All business logic, integration orchestration, webhook handling. |
| **Next.js** | Frontend. App Router, React 18, TypeScript, Tailwind, Supabase Auth. |
| **BlitzAPI** | Unlimited enrichment — company/person data, firmographics, lookalikes, job title detection. |
| **Clay** | Web intent deanonymization (company-level, ~4 credits/lookup). LinkedIn engager extraction (fallback to Trigify). V1 uses Clay for web intent + LinkedIn engagers. V2 explores Clay Ads for managed audience sync to LinkedIn/Meta. |
| **Trigify** | LinkedIn post engager extraction. PAYG $0.012/credit. Open API. Primary tool for this workflow. |
| **Claude API** | AI layer — asset generation (8 types), recommendations, chat-driven campaign builder, performance analysis. |
| **Trigger.dev** | Async job orchestration — scheduled audience refreshes, ad platform metric pulls, CRM syncs, recommendation generation, competitor ad sync. |
| **Railway** | Hosting for both frontend and backend services. |
| **Doppler** | Secret management. Project: `paid-edge`, configs: `dev`/`stg`/`prd`. |
| **Adyntel** | Competitor ad monitoring. Pulls active competitor ads across LinkedIn, Meta, and Google. Weekly sync via Trigger.dev. |
| **dub.co** | Tracked short links. Every campaign gets a short link (e.g., `pe.link/cmmc-q1`) for independent click attribution outside ad platform click IDs. |

### 8.2 Multi-Tenancy

- Each customer/agency-client is an **organization** in Supabase
- Users belong to one or more organizations via **memberships** (admin/member/viewer roles)
- Per-org **provider_configs** store encrypted API keys and OAuth tokens for ad platforms, CRM, BYO integrations
- All ClickHouse queries filtered by **tenant_id**
- Supabase RLS policies enforce tenant isolation
- RudderStack: shared source with `tenant_id` property on all events (injected via transformation)

### 8.3 Existing Infrastructure (from DemandEdge)

The following already exists and will be reused/extended:

- **ClickHouse Cloud instance** — deployed, connected, operational. 3 existing databases (`raw`, `raw_crm`, `core`). PaidEdge creates new `paid_edge` database alongside.
- **RudderStack account** — configured with data plane, 2 existing sources, 2 ClickHouse destinations. PaidEdge creates new sources.
- **Materialized view patterns** — 6 MVs from DemandEdge (raw→core for page views, form fills, content downloads, demo requests, CRM deal stages, CRM leads). These patterns are proven and will be referenced when building PaidEdge's ClickHouse architecture.
- **Perplexity Computer connections** — already connected to ClickHouse and Supabase.

**Do NOT touch:** Existing DemandEdge databases, tables, sources, or MVs.

Full schema definitions, API endpoint contracts, and integration specs are in the companion documents:
- **`PaidEdge_Backend_Spec.md`** — Supabase DDL, ClickHouse DDL, all API endpoints, integration clients, Trigger.dev tasks, auth middleware
- **`PaidEdge_Frontend_Spec.md`** — Next.js route structure, page specs, component breakdown, data fetching patterns, state management, auth flow

---

## 9. Frontend Design Direction

- **Dark mode default.** Background: `#09090b`. Green accents: `#00e87b`.
- **Typography.** Instrument Sans for body text. JetBrains Mono for numbers and metrics.
- **Color semantics.** Green for positive/active states. Red for negative/alerts. Orange for warnings/medium confidence.
- **Icons.** `lucide-react`. No emojis in the production UI.
- **Density.** Compact information density. Data-dense, professional aesthetic.
- **Design philosophy.** The AI building the frontend should make design decisions that serve the data and the workflow. Reference mockups from product exploration are directional, not prescriptive.

---

## 10. Competitive Landscape

| Competitor | What They Do | Price | PaidEdge Differentiator |
|-----------|-------------|-------|------------------------|
| **HockeyStack** | Attribution + journey analytics + account scoring. No audience building, no asset gen, no campaign launch. BYO Vector for person-level ID. | ~$28K/yr median | Full loop — audience building through revenue attribution. Not just analytics. |
| **Propensity** | ABM platform with Bombora intent, omnichannel campaigns, contact-level attribution. Walled garden. | $1K–$4K/mo | Composable — sits on customer's own ad accounts, open data sources, more control. |
| **Metadata.io** | B2B demand gen platform. Campaign execution + audiences. Acquired by Demandbase. | Enterprise | AI-native asset generation + CRM revenue attribution. Independent. |
| **Clay** | Data orchestration and enrichment. Spreadsheet UI, not a campaign platform. Expensive at scale. | $185–$2,975/mo | Purpose-built demand gen UI. User never sees Clay — it's infrastructure. |

---

## 11. Clay Strategic Positioning

Clay is infrastructure, not a competitor. PaidEdge uses Clay under the hood; the user never interacts with Clay directly.

**V1 (launch):** Use Clay for web intent lookups (company-level visitor identification) and LinkedIn engager extraction. CSV export for audience push to ad platforms. BlitzAPI handles the bulk of enrichment. Clay is a supplementary data source.

**V2 (when revenue justifies):** Explore Clay Ads for managed audience sync to LinkedIn/Meta. This replaces the need to build direct LinkedIn Marketing API and Meta Custom Audiences integrations. The cost is Clay credits rather than engineering time.

**Post-V2:** If PaidEdge has enough customers to justify it, negotiate a direct TrustRadius deal or continue through Clay's managed integration. Add TrustRadius intent signals as a premium signal source.

Clay is not a hard dependency — PaidEdge works without it (BlitzAPI for enrichment, CSV export for audience push, RudderStack for site events). Clay is an accelerator that reduces build time and unlocks capabilities (TrustRadius, personal email resolution) that would be expensive or impossible to access directly at PaidEdge's current scale.

---

## 12. Business Model

**Price:** $25,000/year (~$2,083/mo). Will underprice initially for first customers.

**Absorbed data costs per customer:** <$500/mo (BlitzAPI, Clay web intent, Trigify, Claude API, Adyntel, dub.co, ClickHouse compute).

**Gross margin:** ~75%+ on data costs at $2,083/mo revenue.

**NOT absorbed:** TrustRadius intent (future premium tier), person-level visitor ID (BYO), full Clay Ads sync (requires customer's Clay Growth plan or PaidEdge builds direct API integrations).

---

## 13. Build Plan

### 13.1 Build Approach

| Role | Tool |
|------|------|
| Primary builder (planning + building + Linear tracking) | Perplexity Computer |
| Audit, refinement, debugging | Claude Code |
| Deep research enrichment workflows | Parallel.ai / OpenClaw |
| Project state | Linear |

### 13.2 Perplexity Computer Budget

- **March 2026:** 50,000 credits (bonus month) — front-load heaviest build work
- **April 2026+:** 15,000 credits/mo — iteration, new features, fixes

### 13.3 Milestones

**M1 — Infrastructure Foundation**
- New Supabase project with full multi-tenant schema (organizations, users, memberships, provider_configs, audience_segments, campaigns, generated_assets, competitor_configs, tenant_context)
- RLS policies on all tables
- New `paid_edge` database in ClickHouse with all tables (campaign_metrics, crm_opportunities, crm_contacts, behavioral_events, audience_segment_members, web_intent_results)
- New RudderStack sources for PaidEdge (JS SDK source, ad platform Cloud Sources, CRM Cloud Sources). Shared source with `tenant_id` property on all events.
- FastAPI skeleton (auth middleware, tenant resolution, health checks, CORS, dependency injection)
- Next.js skeleton (App Router, Supabase Auth, sidebar layout per section 5, org switcher, protected routes)
- Railway deployment pipeline for both apps
- Doppler secret management configured

**M2 — Audience Engine**
- BlitzAPI integration (company enrichment, person search, firmographic matching)
- Signal provider framework (pluggable module system)
- Individual signal providers: new_in_role, exec_departed, promoted, raised_money, lookalike, page_visitor (Clay web intent), linkedin_engager (Trigify), form_fill
- Trigger.dev scheduled tasks for audience refresh (daily/hourly deltas)
- Chat-driven audience builder (Claude API → structured segment definition)
- Dashboard: signal card grid + KPI row
- Audiences pages: list, create (structured + chat), detail with member table

**M3 — Campaign Builder + Asset Generation**
- Chat-driven campaign builder: split-panel layout (chat left, assembly blocks right), single route `/campaigns/new`
- Zustand store for campaign assembly state
- Claude API integration for all 8 asset types (lead_magnet, landing_page, document_ad, ad_copy, video_script, case_study_page, email_copy, image_brief)
- Document Ad PDF generation pipeline (Claude → python-pptx/reportlab → Supabase Storage)
- Case study page generation from structured tenant context inputs
- Landing page hosting via FastAPI (`/lp/:slug` serving generated HTML with RudderStack)
- Form submission handling on landing pages
- Tracked link creation via dub.co for every campaign
- Audience push (V1: CSV export per platform format)
- Campaign list and detail pages (including assets tab with per-asset performance data)

**M4 — Analytics + Attribution**
- Ad platform API ingestion: LinkedIn Ads, Meta Marketing, Google Ads → RudderStack → ClickHouse
- Trigger.dev scheduled tasks for metric sync (every 6 hours)
- CRM ingestion: Salesforce, HubSpot → RudderStack → ClickHouse
- Attribution matching: CRM contacts ↔ behavioral events via email + UTM + tracked link clicks
- Analytics dashboard: campaign overview, time series, platform comparison, tracked link analytics
- Attribution pages: funnel visualization, cost-per-opp, cost-per-closed-won, pipeline influenced

**M5 — Intelligence Layer (Recommendations + Competitor Monitoring)**
- Claude API performance analysis → recommendation cards
- Budget optimization suggestions
- Audience refinement recommendations
- Campaign health scoring
- Approve/dismiss recommendation flow
- Adyntel integration for competitor ad monitoring
- Competitor config management (add/remove competitors, select platforms)
- Competitor Ads page (`/competitors`): ad listings with filters by competitor and platform
- Trigger.dev weekly task (`competitor_ad_sync`) for pulling competitor ad data
- Competitor intel surfaced in campaign builder chat context and recommendation cards
- UI polish: loading states, error handling, empty states, responsive design

**M6 — First Customer Live**
- End-to-end testing with real ad platform accounts
- Onboarding flow (connect CRM → connect ad platforms → install site snippet → upload brand context → configure competitors)
- Settings pages (integrations, brand & content, team management, competitor tracking)
- Stripe billing integration
- First customer deployment

---

## 14. Open Questions

1. **Ad platform audience push sequencing:** Direct API integrations (requires OAuth partner approval) vs Clay Ads V2 (requires Clay Growth plan) vs CSV export only for V1. CSV ships first; timing of API or Clay Ads path depends on customer demand and revenue.
2. **Trigify vs Clay for LinkedIn engagers:** Trigify cheaper, open API. Clay already in stack. May use both — Trigify as primary, Clay as fallback.
3. **TrustRadius access:** Direct deal ($30K+/yr) vs Clay managed integration. When revenue justifies. Price and timeline unknown.
4. **Landing page hosting:** PaidEdge subdomain vs customer CNAME vs integrate with Unbounce/Webflow.
5. **Adyntel pricing and rate limits:** Need to confirm API cost structure and weekly pull limits before M5.
6. **Perplexity Computer sprint planning:** 50K credits in March, 15K/mo after. Optimal sequencing TBD.

---

## 15. Success Metrics

| Metric | Target |
|--------|--------|
| Login to campaign launch | <30 minutes |
| Signal cards on dashboard | 5+ per tenant |
| Campaign → revenue attribution | 100% of PaidEdge campaigns trackable to CRM outcomes |
| Data cost per tenant | <$500/mo absorbed |
| First paying customer | Within 60 days of M6 |
| ACV | $25K minimum |
| Asset types available | 8 (all types generating successfully) |
| Competitor ads tracked per tenant | 3+ competitors configured |

---

*This is the canonical product definition for PaidEdge. For implementation details, see:*
- *`PaidEdge_Backend_Spec.md` — API endpoints, database schemas, integration contracts, Trigger.dev tasks*
- *`PaidEdge_Frontend_Spec.md` — routes, pages, components, data fetching, auth flow, state management*
