# PaidEdge Backend Spec — `paid-edge-backend-app`

**Version:** 2.0  
**Date:** March 24, 2026  
**Companion docs:** PaidEdge_PRD.md (product vision), PaidEdge_Frontend_Spec.md (frontend)  
**Stack:** FastAPI (Python 3.12+), Supabase (Postgres 17), ClickHouse Cloud, RudderStack, Trigger.dev  
**Hosting:** Railway  
**Secrets:** Doppler (project: `paid-edge`, configs: `dev`, `stg`, `prd`)

---

## 1. Project Structure

```
paid-edge-backend-app/
├── app/
│   ├── main.py                    # FastAPI app init, middleware, CORS
│   ├── config.py                  # Settings via Doppler/env vars
│   ├── dependencies.py            # Shared deps (get_db, get_clickhouse, get_current_user, get_tenant)
│   │
│   ├── auth/
│   │   ├── router.py              # Auth endpoints (delegates to Supabase Auth)
│   │   ├── middleware.py           # JWT validation, tenant resolution
│   │   └── models.py              # User, session models
│   │
│   ├── tenants/
│   │   ├── router.py              # Org CRUD, provider config management
│   │   ├── models.py              # Organization, Membership, ProviderConfig
│   │   └── service.py             # Tenant resolution, config retrieval
│   │
│   ├── audiences/
│   │   ├── router.py              # Segment CRUD, member listing, chat builder
│   │   ├── models.py              # AudienceSegment, SegmentMember
│   │   ├── service.py             # Segment refresh orchestration, signal resolution
│   │   ├── signals/
│   │   │   ├── base.py            # BaseSignalProvider interface
│   │   │   ├── new_in_role.py     # BlitzAPI delta detection
│   │   │   ├── exec_departed.py   # BlitzAPI delta detection
│   │   │   ├── promoted.py        # BlitzAPI delta detection
│   │   │   ├── raised_money.py    # BlitzAPI funding data
│   │   │   ├── lookalike.py       # CRM closed-won → BlitzAPI matching
│   │   │   ├── page_visitor.py    # Clay web intent on RudderStack events
│   │   │   ├── linkedin_engager.py # Trigify API extraction
│   │   │   ├── form_fill.py       # Self-identified from landing page forms
│   │   │   └── deep_research.py   # Parallel.ai / OpenClaw queries
│   │   └── chat_builder.py        # Claude API natural language → segment definition
│   │
│   ├── campaigns/
│   │   ├── router.py              # Campaign CRUD, launch, status
│   │   ├── models.py              # Campaign, CampaignAsset, CampaignPlatform
│   │   ├── service.py             # Launch orchestration, platform push
│   │   └── platforms/
│   │       ├── base.py            # BasePlatformAdapter interface
│   │       ├── linkedin.py        # LinkedIn Marketing API
│   │       ├── meta.py            # Meta Marketing API
│   │       └── google.py          # Google Ads API
│   │
│   ├── competitors/
│   │   ├── router.py              # Competitor config CRUD, tracked ad listing
│   │   ├── models.py              # CompetitorConfig, CompetitorAd
│   │   └── service.py             # Competitor ad sync orchestration
│   │
│   ├── assets/
│   │   ├── router.py              # Asset generation, preview, download
│   │   ├── models.py              # AssetType enum, GeneratedAsset
│   │   ├── service.py             # Claude API orchestration for generation
│   │   ├── generators/
│   │   │   ├── lead_magnet.py     # PDF generation
│   │   │   ├── landing_page.py    # HTML generation + hosting
│   │   │   ├── document_ad.py     # LinkedIn Document Ad PDF (multi-slide carousel)
│   │   │   ├── video_script.py    # Structured video script + production brief
│   │   │   ├── case_study_page.py # Conversion-optimized case study HTML page
│   │   │   ├── ad_copy.py         # Per-platform ad copy
│   │   │   └── email_copy.py      # Nurture sequence copy
│   │   └── templates/             # Base templates for landing pages
│   │
│   ├── analytics/
│   │   ├── router.py              # Performance dashboards, cross-platform views
│   │   ├── models.py              # MetricsSummary, TimeSeriesPoint, PlatformBreakdown
│   │   ├── service.py             # ClickHouse query builder
│   │   └── queries/               # Named ClickHouse queries as .sql files
│   │       ├── campaign_overview.sql
│   │       ├── campaign_detail.sql
│   │       ├── platform_comparison.sql
│   │       └── budget_pacing.sql
│   │
│   ├── attribution/
│   │   ├── router.py              # Revenue attribution views, funnel data
│   │   ├── models.py              # AttributionResult, FunnelStage, CostMetrics
│   │   ├── service.py             # UTM → CRM matching, attribution calculation
│   │   └── queries/
│   │       ├── cost_per_opportunity.sql
│   │       ├── cost_per_closed_won.sql
│   │       ├── pipeline_influenced.sql
│   │       └── funnel_stages.sql
│   │
│   ├── recommendations/
│   │   ├── router.py              # AI recommendations endpoints
│   │   ├── models.py              # Recommendation, ConfidenceLevel
│   │   └── service.py             # Claude API analysis of ClickHouse data
│   │
│   ├── webhooks/
│   │   ├── router.py              # Inbound webhook handlers
│   │   ├── rudderstack.py         # RudderStack event webhook (page visits, form fills)
│   │   ├── byo_integrations.py    # RB2B, Vector, etc. webhook ingestion
│   │   └── ad_platforms.py        # Ad platform conversion webhooks
│   │
│   ├── integrations/
│   │   ├── blitzapi.py            # BlitzAPI client
│   │   ├── clay.py                # Clay API client (web intent, engager extraction)
│   │   ├── trigify.py             # Trigify API client
│   │   ├── claude_ai.py           # Claude API client (asset gen, recommendations, chat)
│   │   ├── rudderstack.py         # RudderStack server-side identify/track calls
│   │   ├── prospeo.py             # Prospeo email resolution
│   │   ├── sendoso.py             # Sendoso incentive API
│   │   ├── salesforce.py          # Salesforce REST API client
│   │   ├── hubspot.py             # HubSpot API client
│   │   ├── adyntel.py             # Adyntel competitor ad monitoring
│   │   └── dubco.py               # dub.co tracked short links
│   │
│   ├── db/
│   │   ├── supabase.py            # Supabase client init
│   │   └── clickhouse.py          # ClickHouse client init (clickhouse-connect)
│   │
│   └── shared/
│       ├── models.py              # Shared Pydantic base models
│       ├── pagination.py          # Cursor/offset pagination
│       └── errors.py              # Error handling, exception classes
│
├── trigger/                       # Trigger.dev task definitions
│   ├── audience_refresh.py        # Scheduled: delta detection per signal type
│   ├── ad_platform_sync.py        # Scheduled: pull metrics from ad platform APIs
│   ├── crm_sync.py                # Scheduled: pull CRM data
│   ├── recommendation_gen.py      # Scheduled: AI recommendation generation
│   ├── web_intent_lookup.py       # Event-driven: Clay lookup on page visit
│   └── competitor_ad_sync.py      # Scheduled: weekly competitor ad pull via Adyntel
│
├── migrations/                    # Supabase SQL migrations
│   ├── 001_organizations.sql
│   ├── 002_users_memberships.sql
│   ├── 003_provider_configs.sql
│   ├── 004_audience_segments.sql
│   ├── 005_campaigns.sql
│   ├── 006_tenant_context.sql
│   ├── 007_generated_assets.sql
│   ├── 008_rls_policies.sql
│   └── 009_competitor_configs.sql
│
├── clickhouse/                    # ClickHouse DDL
│   ├── 001_campaign_metrics.sql
│   ├── 002_crm_opportunities.sql
│   ├── 003_crm_contacts.sql
│   ├── 004_behavioral_events.sql
│   ├── 005_audience_segment_members.sql
│   ├── 006_web_intent_results.sql
│   └── 007_materialized_views.sql
│
├── tests/
├── Dockerfile
├── railway.toml
├── pyproject.toml
└── README.md
```

---

## 2. Supabase Schema

### 2.1 organizations

```sql
CREATE TABLE public.organizations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    slug TEXT UNIQUE NOT NULL,
    domain TEXT,                          -- company domain (e.g., acme.com)
    logo_url TEXT,
    plan TEXT DEFAULT 'starter',         -- starter, pro, enterprise
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);
```

### 2.2 users + memberships

```sql
-- Users are managed by Supabase Auth. This table extends auth.users.
CREATE TABLE public.user_profiles (
    id UUID PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
    full_name TEXT,
    avatar_url TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE public.memberships (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    organization_id UUID NOT NULL REFERENCES public.organizations(id) ON DELETE CASCADE,
    role TEXT NOT NULL DEFAULT 'member' CHECK (role IN ('admin', 'member', 'viewer')),
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(user_id, organization_id)
);
```

### 2.3 provider_configs

Per-tenant encrypted credentials for external services.

```sql
CREATE TABLE public.provider_configs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES public.organizations(id) ON DELETE CASCADE,
    provider TEXT NOT NULL,              -- 'linkedin_ads', 'meta_ads', 'google_ads', 'salesforce', 'hubspot', 'rb2b', 'vector', 'sendoso'
    config JSONB NOT NULL DEFAULT '{}',  -- encrypted at rest. Contains: api_key, oauth_tokens, account_ids, etc.
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(organization_id, provider)
);
```

### 2.4 audience_segments

```sql
CREATE TABLE public.audience_segments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES public.organizations(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    description TEXT,
    segment_type TEXT NOT NULL,          -- 'new_in_role', 'exec_departed', 'promoted', 'raised_money', 'lookalike', 'page_visitor', 'linkedin_engager', 'form_fill', 'custom', 'deep_research'
    filter_config JSONB NOT NULL DEFAULT '{}',  -- signal-type-specific parameters
    status TEXT DEFAULT 'active' CHECK (status IN ('active', 'paused', 'archived')),
    refresh_schedule TEXT DEFAULT 'daily', -- 'hourly', 'daily', 'weekly', 'manual'
    last_refreshed_at TIMESTAMPTZ,
    member_count INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);
```

### 2.5 campaigns

```sql
CREATE TABLE public.campaigns (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES public.organizations(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    description TEXT,
    status TEXT DEFAULT 'draft' CHECK (status IN ('draft', 'review', 'active', 'paused', 'completed', 'archived')),
    audience_segment_id UUID REFERENCES public.audience_segments(id),
    
    -- Platform configs (which platforms, budgets, schedules per platform)
    platforms JSONB DEFAULT '[]',        -- [{platform: 'linkedin', budget_daily: 100, platform_campaign_id: null, ...}]
    
    -- Campaign schedule
    start_date DATE,
    end_date DATE,
    
    -- Incentive config (Sendoso)
    incentive_config JSONB,              -- {enabled: bool, type: 'gift_card', value: 50, trigger: 'meeting_booked'}
    
    -- Tracked link (dub.co)
    tracked_link_url TEXT,               -- dub.co short link for independent attribution tracking
    
    -- Tracking
    launched_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);
```

### 2.6 generated_assets

```sql
CREATE TABLE public.generated_assets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES public.organizations(id) ON DELETE CASCADE,
    campaign_id UUID REFERENCES public.campaigns(id) ON DELETE SET NULL,
    asset_type TEXT NOT NULL CHECK (asset_type IN ('lead_magnet', 'landing_page', 'document_ad', 'ad_copy', 'video_script', 'case_study_page', 'email_copy', 'image_brief')),
    title TEXT NOT NULL,
    content JSONB NOT NULL,              -- type-specific: {html: ...} for pages, {headline: ..., body: ..., cta: ...} for ad copy, {pdf_url: ...} for lead magnets
    platform TEXT,                       -- 'linkedin', 'meta', 'google', null (for non-platform-specific)
    status TEXT DEFAULT 'draft' CHECK (status IN ('draft', 'approved', 'active', 'archived')),
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);
```

### 2.7 tenant_context

Client onboarding inputs — persistent raw material for AI asset generation.

```sql
CREATE TABLE public.tenant_context (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES public.organizations(id) ON DELETE CASCADE,
    context_type TEXT NOT NULL,          -- 'customers', 'testimonial', 'case_study', 'brand_guidelines', 'positioning', 'competitors', 'icp_definition'
    title TEXT NOT NULL,
    content TEXT,                        -- text content or transcript
    file_url TEXT,                       -- URL to stored file (audio, video, PDF) in Supabase Storage
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);
```

### 2.8 competitor_configs

Per-tenant competitor tracking configuration for ad monitoring.

```sql
CREATE TABLE public.competitor_configs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES public.organizations(id) ON DELETE CASCADE,
    competitor_domain TEXT NOT NULL,
    competitor_name TEXT NOT NULL,
    platforms TEXT[] DEFAULT '{}',        -- which platforms to track: ['linkedin', 'meta', 'google']
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT now()
);
```

### 2.9 RLS Policies

Every table gets tenant isolation:

```sql
-- Pattern applied to ALL tables with organization_id:
-- organizations, memberships, provider_configs, audience_segments,
-- campaigns, generated_assets, tenant_context, competitor_configs
ALTER TABLE public.{table} ENABLE ROW LEVEL SECURITY;

CREATE POLICY "tenant_isolation" ON public.{table}
    FOR ALL
    USING (
        organization_id IN (
            SELECT organization_id FROM public.memberships
            WHERE user_id = auth.uid()
        )
    );
```

---

## 3. ClickHouse Schema

Existing ClickHouse instance: `gf9xtjjqyl.us-east-1.aws.clickhouse.cloud`  
New database: `paid_edge` (separate from DemandEdge's `raw`/`raw_crm`/`core`)

### 3.1 campaign_metrics

```sql
CREATE TABLE paid_edge.campaign_metrics (
    tenant_id UUID,
    campaign_id UUID,                    -- PaidEdge campaign ID
    platform LowCardinality(String),     -- 'linkedin', 'meta', 'google'
    platform_campaign_id String,         -- native platform campaign ID
    platform_ad_group_id String DEFAULT '',
    platform_ad_id String DEFAULT '',
    date Date,
    spend Decimal(12, 2),
    impressions UInt64,
    clicks UInt64,
    conversions UInt32,
    leads UInt32 DEFAULT 0,
    ctr Float32,
    cpc Decimal(8, 2),
    cpm Decimal(8, 2),
    roas Float32 DEFAULT 0,
    ingested_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (tenant_id, campaign_id, platform, platform_campaign_id, date)
PARTITION BY toYYYYMM(date);
```

### 3.2 crm_opportunities

```sql
CREATE TABLE paid_edge.crm_opportunities (
    tenant_id UUID,
    opportunity_id String,
    opportunity_name String DEFAULT '',
    contact_email String,
    contact_name String DEFAULT '',
    company_domain String,
    company_name String DEFAULT '',
    amount Decimal(12, 2),
    stage LowCardinality(String),
    is_won UInt8 DEFAULT 0,
    is_lost UInt8 DEFAULT 0,
    close_date Nullable(Date),
    created_at DateTime,
    updated_at DateTime DEFAULT now(),
    source_campaign_id Nullable(UUID),   -- matched PaidEdge campaign
    source_utm_source String DEFAULT '',
    source_utm_medium String DEFAULT '',
    source_utm_campaign String DEFAULT '',
    source_utm_content String DEFAULT '',
    source_click_id String DEFAULT '',   -- gclid, fbclid, li_fat_id
    crm_source LowCardinality(String) DEFAULT '', -- 'salesforce', 'hubspot'
    ingested_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (tenant_id, opportunity_id);
```

### 3.3 crm_contacts

```sql
CREATE TABLE paid_edge.crm_contacts (
    tenant_id UUID,
    contact_id String,
    email String,
    first_name String DEFAULT '',
    last_name String DEFAULT '',
    title String DEFAULT '',
    company_domain String DEFAULT '',
    company_name String DEFAULT '',
    lead_source String DEFAULT '',
    utm_source String DEFAULT '',
    utm_medium String DEFAULT '',
    utm_campaign String DEFAULT '',
    utm_content String DEFAULT '',
    click_id String DEFAULT '',
    crm_source LowCardinality(String) DEFAULT '',
    created_at DateTime,
    ingested_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (tenant_id, contact_id);
```

### 3.4 behavioral_events

Site events from RudderStack (customer site + PaidEdge landing pages).

```sql
CREATE TABLE paid_edge.behavioral_events (
    tenant_id UUID,
    anonymous_id String,
    user_id String DEFAULT '',           -- populated after identify()
    event_type LowCardinality(String),   -- 'page', 'track'
    event_name String DEFAULT '',        -- 'page_view', 'form_submitted', 'cta_clicked', 'content_downloaded'
    page_url String DEFAULT '',
    page_path String DEFAULT '',
    page_title String DEFAULT '',
    referrer String DEFAULT '',
    utm_source String DEFAULT '',
    utm_medium String DEFAULT '',
    utm_campaign String DEFAULT '',
    utm_content String DEFAULT '',
    click_id String DEFAULT '',
    properties String DEFAULT '',        -- JSON string of additional properties
    timestamp DateTime,
    ingested_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (tenant_id, timestamp, anonymous_id)
PARTITION BY toYYYYMM(timestamp);
```

### 3.5 audience_segment_members

Materialized audience members after segment refresh.

```sql
CREATE TABLE paid_edge.audience_segment_members (
    tenant_id UUID,
    segment_id UUID,
    entity_type LowCardinality(String),  -- 'person', 'company'
    entity_id String,                    -- person or company identifier
    email String DEFAULT '',
    full_name String DEFAULT '',
    title String DEFAULT '',
    company_domain String DEFAULT '',
    company_name String DEFAULT '',
    signal_strength Float32 DEFAULT 0,   -- 0-1 confidence/relevance score
    signal_details String DEFAULT '',    -- JSON: signal-specific metadata
    matched_at DateTime DEFAULT now(),
    ingested_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (tenant_id, segment_id, entity_type, entity_id);
```

### 3.6 web_intent_results

Clay web intent deanonymization results.

```sql
CREATE TABLE paid_edge.web_intent_results (
    tenant_id UUID,
    visitor_ip String,                   -- hashed
    resolved_company_domain String DEFAULT '',
    resolved_company_name String DEFAULT '',
    resolved_company_industry String DEFAULT '',
    resolved_company_size String DEFAULT '',
    page_url String,
    page_path String,
    provider LowCardinality(String),     -- which provider in Clay waterfall resolved it
    resolved_at DateTime DEFAULT now(),
    ingested_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (tenant_id, visitor_ip, resolved_at);
```

---

## 4. API Endpoints

All endpoints require `Authorization: Bearer <supabase_jwt>` header.  
Tenant is resolved from JWT → memberships → active organization (set via `X-Organization-Id` header or default org).

### 4.1 Auth

| Method | Path | Description |
|--------|------|-------------|
| POST | `/auth/signup` | Proxy to Supabase Auth signup |
| POST | `/auth/login` | Proxy to Supabase Auth login |
| POST | `/auth/logout` | Proxy to Supabase Auth logout |
| GET | `/auth/me` | Current user profile + org memberships |
| POST | `/auth/refresh` | Refresh JWT |

### 4.2 Tenants / Organizations

| Method | Path | Description |
|--------|------|-------------|
| GET | `/orgs` | List orgs current user belongs to |
| POST | `/orgs` | Create new org |
| GET | `/orgs/:id` | Get org details |
| PATCH | `/orgs/:id` | Update org |
| POST | `/orgs/:id/members` | Invite member |
| DELETE | `/orgs/:id/members/:user_id` | Remove member |
| GET | `/orgs/:id/providers` | List configured providers |
| PUT | `/orgs/:id/providers/:provider` | Set/update provider config (encrypted) |
| DELETE | `/orgs/:id/providers/:provider` | Remove provider config |

### 4.3 Audiences

| Method | Path | Description |
|--------|------|-------------|
| GET | `/audiences` | List segments for current tenant |
| POST | `/audiences` | Create segment (structured filter_config) |
| POST | `/audiences/chat` | Create segment via natural language (Claude API) |
| GET | `/audiences/:id` | Get segment detail + member count |
| PATCH | `/audiences/:id` | Update segment |
| DELETE | `/audiences/:id` | Archive segment |
| GET | `/audiences/:id/members` | List resolved members (paginated, from ClickHouse) |
| POST | `/audiences/:id/refresh` | Trigger manual refresh |
| POST | `/audiences/:id/export` | Export members as CSV (formatted for ad platform upload) |
| GET | `/audiences/signals` | Get available signal cards for dashboard (all active segments with latest counts) |

### 4.4 Campaigns

| Method | Path | Description |
|--------|------|-------------|
| GET | `/campaigns` | List campaigns for current tenant |
| POST | `/campaigns` | Create campaign (draft) |
| GET | `/campaigns/:id` | Get campaign detail |
| PATCH | `/campaigns/:id` | Update campaign |
| POST | `/campaigns/:id/launch` | Launch campaign (push audiences to platforms, activate) |
| POST | `/campaigns/:id/pause` | Pause campaign |
| POST | `/campaigns/:id/complete` | Mark complete |
| DELETE | `/campaigns/:id` | Archive campaign |
| GET | `/campaigns/:id/metrics` | Campaign performance metrics (from ClickHouse) |
| GET | `/campaigns/:id/metrics/timeseries` | Time series data for charts |
| GET | `/campaigns/:id/metrics/platforms` | Per-platform breakdown |
| GET | `/campaigns/:id/attribution` | Revenue attribution for this campaign |
| GET | `/campaigns/:id/identified-visitors` | BYO visitor ID data (if connected) |

### 4.5 Assets

| Method | Path | Description |
|--------|------|-------------|
| POST | `/assets/generate` | Generate assets for a campaign (specify types) |
| GET | `/assets/:id` | Get asset detail |
| PATCH | `/assets/:id` | Update asset (edit generated content) |
| POST | `/assets/:id/approve` | Mark asset as approved |
| GET | `/campaigns/:id/assets` | List assets for a campaign |

**Generate request body:**
```json
{
    "campaign_id": "uuid",
    "asset_types": ["lead_magnet", "landing_page", "document_ad", "video_script", "case_study_page", "ad_copy", "email_copy", "image_brief"],
    "platforms": ["linkedin", "meta"],
    "angle": "SOC 2 compliance readiness for healthtech",
    "tone": "authoritative but approachable",
    "cta": "Download the guide"
}
```

### 4.6 Analytics

| Method | Path | Description |
|--------|------|-------------|
| GET | `/analytics/overview` | KPI summary (total spend, avg CAC, total conversions, pipeline influenced) |
| GET | `/analytics/campaigns` | All campaigns performance table |
| GET | `/analytics/platforms` | Cross-platform comparison |
| GET | `/analytics/timeseries` | Aggregate time series (spend, clicks, conversions over time) |

Query params for all: `?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD&platform=linkedin&campaign_id=uuid`

### 4.7 Attribution

| Method | Path | Description |
|--------|------|-------------|
| GET | `/attribution/funnel` | Funnel data: campaigns → leads → opportunities → closed-won |
| GET | `/attribution/cost-per-opportunity` | Cost-per-opportunity by campaign |
| GET | `/attribution/cost-per-closed-won` | Cost-per-closed-won by campaign |
| GET | `/attribution/pipeline-influenced` | Pipeline $ influenced by campaigns |
| GET | `/attribution/lookalike-profile` | Firmographic profile of closed-won for audience building feedback |

### 4.8 Recommendations

| Method | Path | Description |
|--------|------|-------------|
| GET | `/recommendations` | List active recommendations for current tenant |
| POST | `/recommendations/:id/approve` | Approve recommendation |
| POST | `/recommendations/:id/dismiss` | Dismiss recommendation |

### 4.9 Webhooks (Inbound)

| Method | Path | Description |
|--------|------|-------------|
| POST | `/webhooks/rudderstack` | RudderStack event webhook (page views, form fills, identify calls) |
| POST | `/webhooks/visitor-id` | BYO visitor identification webhook (RB2B, Vector, etc.) |
| POST | `/webhooks/ad-platforms/:platform` | Ad platform conversion/event webhooks |

### 4.10 Competitors

| Method | Path | Description |
|--------|------|-------------|
| GET | `/competitors` | List configured competitors for tenant |
| POST | `/competitors` | Add competitor to track |
| DELETE | `/competitors/:id` | Stop tracking competitor |
| GET | `/competitors/ads` | Get all tracked competitor ads (filterable by competitor, platform, status) |

### 4.11 Tenant Context (Onboarding Inputs)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/context` | List all context items for tenant |
| POST | `/context` | Add context item (text or file upload to Supabase Storage) |
| GET | `/context/:id` | Get context detail |
| PATCH | `/context/:id` | Update context |
| DELETE | `/context/:id` | Delete context |

### 4.12 Landing Pages

| Method | Path | Description |
|--------|------|-------------|
| GET | `/lp/:slug` | Serve hosted landing page (public, no auth) |
| POST | `/lp/:slug/submit` | Handle form submission on landing page (public, no auth) |

---

## 5. Integration Contracts

### 5.1 BlitzAPI

**Used for:** Company enrichment, person search, firmographic matching, job title resolution, lookalike building, delta detection.

```python
# app/integrations/blitzapi.py

class BlitzAPIClient:
    BASE_URL = "https://api.blitzapi.com/v1"
    
    async def enrich_company(self, domain: str) -> CompanyProfile
    async def search_people(self, filters: PeopleSearchFilters) -> list[PersonProfile]
    async def enrich_person(self, email: str = None, linkedin_url: str = None) -> PersonProfile
    async def find_lookalikes(self, seed_companies: list[str], filters: dict) -> list[CompanyProfile]
```

**Delta detection pattern:** Trigger.dev task stores previous snapshot of enriched people per segment. On next run, enriches again and compares title, company, seniority. Differences = signals (new_in_role, promoted, departed).

### 5.2 Clay

**Used for:** Web intent deanonymization (company-level), LinkedIn post engager extraction (fallback to Trigify).

```python
# app/integrations/clay.py

class ClayClient:
    BASE_URL = "https://api.clay.com/v1"
    
    async def web_intent_lookup(self, ip_address: str) -> WebIntentResult | None
    # Returns: company_domain, company_name, industry, size, provider_used
    # Cost: ~4 credits per successful resolution
    
    async def extract_linkedin_engagers(self, post_url: str) -> list[LinkedInEngager]
    # Returns: list of {name, title, company, linkedin_url}
    # Cost: 1 action per post
```

### 5.3 Trigify

**Used for:** LinkedIn post engager extraction (primary tool for this workflow).

```python
# app/integrations/trigify.py

class TrigifyClient:
    BASE_URL = "https://api.trigify.io/v1"
    
    async def get_post_engagers(self, post_url: str, engagement_type: str = "all") -> list[Engager]
    # engagement_type: 'likes', 'comments', 'all'
    # Cost: $0.012/credit via PAYG
    # Returns: list of {name, title, company, linkedin_url, engagement_type}
```

### 5.4 Claude API

**Used for:** Asset generation, campaign recommendations, chat-driven audience builder, performance analysis.

```python
# app/integrations/claude_ai.py

class ClaudeClient:
    MODEL_FAST = "claude-sonnet-4-20250514"       # Speed-sensitive: ad copy, chat, recommendations
    MODEL_QUALITY = "claude-opus-4-6"  # Quality-sensitive: lead magnets, landing pages, case study pages, document ads
    
    async def generate_asset(self, asset_type: str, context: AssetContext) -> GeneratedContent
    async def chat_audience_builder(self, user_message: str, tenant_context: TenantContext) -> SegmentDefinition
    async def analyze_campaign(self, metrics: CampaignMetrics, history: list) -> list[Recommendation]
    async def generate_ad_copy(self, platform: str, audience: AudienceProfile, angle: str) -> AdCopyVariants
```

**Model selection guidance:** Use `MODEL_QUALITY` (Opus) for quality-sensitive generation where depth and nuance matter — lead magnets, landing pages, case study pages, and document ads. Use `MODEL_FAST` (Sonnet) for speed-sensitive tasks where latency matters more than prose quality — ad copy generation, chat interactions, and recommendation analysis.

### 5.5 Prospeo

**Used for:** Email resolution for audience push to ad platforms.

```python
# app/integrations/prospeo.py

class ProspeoClient:
    BASE_URL = "https://api.prospeo.io/v1"
    
    async def find_email(self, full_name: str, company_website: str) -> EmailResult | None
    # Enrich Person endpoint, company_website preferred over company_name
```

### 5.6 RudderStack (Server-Side)

**Used for:** Server-side identify() calls to merge anonymous → known identity, track() for server-side events.

```python
# app/integrations/rudderstack.py

class RudderStackClient:
    DATA_PLANE_URL = "https://substratevyaxk.dataplane.rudderstack.com"
    
    async def identify(self, user_id: str, anonymous_id: str, traits: dict)
    async def track(self, user_id: str, event: str, properties: dict)
```

### 5.7 Ad Platform APIs

```python
# app/campaigns/platforms/linkedin.py
class LinkedInAdsClient:
    async def create_matched_audience(self, emails: list[str], name: str) -> str  # audience_id
    async def create_campaign(self, config: LinkedInCampaignConfig) -> str  # campaign_id
    async def get_campaign_metrics(self, campaign_id: str, date_range: DateRange) -> list[DailyMetrics]
    async def pause_campaign(self, campaign_id: str)

# app/campaigns/platforms/meta.py
class MetaAdsClient:
    async def create_custom_audience(self, hashed_emails: list[str], name: str) -> str
    async def create_campaign(self, config: MetaCampaignConfig) -> str
    async def get_campaign_insights(self, campaign_id: str, date_range: DateRange) -> list[DailyMetrics]

# app/campaigns/platforms/google.py
class GoogleAdsClient:
    async def create_customer_match_list(self, hashed_emails: list[str], name: str) -> str
    async def create_campaign(self, config: GoogleCampaignConfig) -> str
    async def get_campaign_metrics(self, campaign_id: str, date_range: DateRange) -> list[DailyMetrics]
```

### 5.8 Adyntel (Competitor Ad Monitoring)

**Used for:** Tracking competitor ad creatives and activity across LinkedIn, Meta, and Google. Powers the Competitor Ads intelligence feature.

```python
# app/integrations/adyntel.py

class AdyntelClient:
    """Competitor ad monitoring via Adyntel API."""
    
    async def get_competitor_ads(
        self, 
        competitor_domains: list[str], 
        platforms: list[str],            # ['linkedin', 'meta', 'google']
        date_range: DateRange
    ) -> list[CompetitorAd]
    
    # CompetitorAd fields:
    #   competitor_domain: str           — domain of the competitor
    #   competitor_name: str             — display name
    #   platform: str                    — 'linkedin', 'meta', 'google'
    #   ad_format: str                   — 'image', 'video', 'carousel', 'document', 'text'
    #   headline: str                    — ad headline text
    #   body_text: str                   — ad body/description text
    #   cta: str                         — call-to-action text
    #   landing_page_url: str            — destination URL
    #   first_seen: datetime             — when the ad was first detected
    #   last_seen: datetime              — most recent detection
    #   is_active: bool                  — currently running
    #   estimated_duration_days: int     — how long the ad has been running
    #   targeting_hints: dict | None     — targeting info if available
```

### 5.9 dub.co (Tracked Links)

**Used for:** Creating short tracked links for every campaign, providing an independent attribution layer outside of ad platform click IDs.

```python
# app/integrations/dubco.py

class DubCoClient:
    """Short link creation and analytics via dub.co API."""
    
    async def create_link(
        self,
        destination_url: str,
        slug: str,                       # e.g., "cmmc-q1"
        tags: dict,                      # campaign_id, tenant_id, platform
    ) -> TrackedLink
    # TrackedLink: {short_url, destination_url, slug, click_count}
    
    async def get_link_analytics(self, link_id: str) -> LinkAnalytics
```

**Flow:** When a campaign is created, PaidEdge auto-generates a dub.co short link (e.g., `pe.link/cmmc-q1`) pointing to the campaign's landing page or destination URL. The short link is used in ad copy and shown in the campaign config. Click analytics from dub.co supplement ad platform metrics.

---

## 6. Trigger.dev Tasks

### 6.1 Scheduled Tasks

| Task | Schedule | Description |
|------|----------|-------------|
| `audience_refresh_daily` | Daily 6am UTC | For each active segment with `refresh_schedule='daily'`: run the appropriate signal provider, materialize results to ClickHouse `audience_segment_members`, update `member_count` in Supabase |
| `audience_refresh_hourly` | Hourly | Same but for `refresh_schedule='hourly'` segments (page_visitor, form_fill) |
| `ad_platform_metrics_sync` | Every 6 hours | For each tenant with connected ad platforms: pull campaign metrics via API, write to ClickHouse `campaign_metrics` |
| `crm_sync` | Every 6 hours | For each tenant with connected CRM: pull opportunities + contacts, write to ClickHouse `crm_opportunities` + `crm_contacts` |
| `recommendation_gen` | Daily 8am UTC | For each tenant with active campaigns: query ClickHouse for campaign performance, send to Claude API for analysis, write recommendations to Supabase |
| `attribution_match` | Daily 7am UTC | For each tenant: match CRM contacts to behavioral events via email + UTM params, update `source_campaign_id` on crm_opportunities |
| `competitor_ad_sync` | Weekly (Sunday night) | For each tenant with configured competitors: pull active ads via Adyntel, store/update in Supabase, flag changes (new ads, stopped ads, long-running ads) |

### 6.2 Event-Driven Tasks

| Task | Trigger | Description |
|------|---------|-------------|
| `web_intent_lookup` | RudderStack webhook for designated pages | When a page visit event comes in for a high-intent page (pricing, case study, demo): fire Clay web intent lookup, write result to ClickHouse `web_intent_results` |
| `form_fill_process` | RudderStack webhook for form submissions | When a form is submitted on a PaidEdge landing page: call RudderStack `identify()` to merge anonymous history, write lead to Supabase, optionally push to CRM |

---

## 7. Auth + Middleware

### 7.1 Request Flow

```
Request → CORS middleware → JWT validation → Tenant resolution → Route handler
```

### 7.2 JWT Validation

```python
# Extract JWT from Authorization header
# Validate against Supabase JWT secret
# Extract user_id from JWT claims
```

### 7.3 Tenant Resolution

```python
# Check X-Organization-Id header
# If present: verify user has membership in that org
# If absent: use user's default/first org
# Inject tenant_id into request state for all downstream queries
```

### 7.4 Dependency Injection

```python
async def get_current_user(request: Request) -> UserProfile:
    # From validated JWT

async def get_tenant(request: Request, user: UserProfile) -> Organization:
    # From X-Organization-Id header or default

async def get_clickhouse(tenant: Organization) -> ClickHouseClient:
    # Client with tenant_id pre-bound for query filtering

async def get_supabase(tenant: Organization) -> SupabaseClient:
    # Client with RLS context set
```

---

## 8. Environment Variables (Doppler)

```
# Supabase
SUPABASE_URL=
SUPABASE_ANON_KEY=
SUPABASE_SERVICE_ROLE_KEY=
SUPABASE_JWT_SECRET=

# ClickHouse
CLICKHOUSE_HOST=gf9xtjjqyl.us-east-1.aws.clickhouse.cloud
CLICKHOUSE_PORT=8443
CLICKHOUSE_USER=
CLICKHOUSE_PASSWORD=
CLICKHOUSE_DATABASE=paid_edge

# RudderStack
RUDDERSTACK_DATA_PLANE_URL=https://substratevyaxk.dataplane.rudderstack.com
RUDDERSTACK_WRITE_KEY=

# Integrations
BLITZAPI_API_KEY=
CLAY_API_KEY=
TRIGIFY_API_KEY=
PROSPEO_API_KEY=
ANTHROPIC_API_KEY=
ADYNTEL_API_KEY=
DUBCO_API_KEY=

# Trigger.dev
TRIGGER_API_KEY=
TRIGGER_API_URL=

# App
APP_ENV=development
APP_URL=https://api.paidedge.com
FRONTEND_URL=https://app.paidedge.com
CORS_ORIGINS=["https://app.paidedge.com"]
```

---

## 9. Key Technical Decisions

1. **Separate Supabase project from data-engine-x.** PaidEdge is a standalone product, not an extension of data-engine-x. Clean separation of concerns.

2. **Same ClickHouse instance, new database.** `paid_edge` database lives alongside DemandEdge's `raw`/`raw_crm`/`core`. Shares compute, separate data.

3. **RudderStack shared source with tenant_id property.** Single JS SDK source, `tenant_id` injected via RudderStack transformation based on write key or source config. Simpler than per-tenant sources for now.

4. **Signal providers are pluggable modules.** Each signal type (`new_in_role`, `page_visitor`, etc.) is a class implementing `BaseSignalProvider`. New signals can be added without touching core logic.

5. **ClickHouse for read-heavy analytics, Supabase for CRUD.** Campaign metadata, segment definitions, user/org data lives in Supabase. Time-series metrics, behavioral events, materialized audience members live in ClickHouse. Frontend queries hit the FastAPI backend which routes to the appropriate database.

6. **Landing pages served by the backend.** `/lp/:slug` serves generated HTML directly. No separate landing page hosting needed for V1. RudderStack JS SDK is injected into every served page.

---

*This document defines the complete backend architecture for PaidEdge. It should be used alongside PaidEdge_PRD.md for context and PaidEdge_Frontend_Spec.md for the frontend contract.*
