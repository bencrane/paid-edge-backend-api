# PaidEdge Frontend Spec — `paid-edge-frontend-app`

**Version:** 2.0
**Date:** March 24, 2026
**Companion docs:** PaidEdge_PRD.md (product vision), paid_engine_x_Backend_Spec.md (API contract), PaidEdge_Spec_Updates.md (change log)
**Stack:** Next.js 14+ (App Router), React 18, TypeScript, Tailwind CSS, Supabase Auth
**Hosting:** Railway
**Secrets:** Doppler (project: `paid-edge-frontend-app`, config: `dev`/`stg`/`prd`)

---

## 1. Project Structure

```
paid-edge-frontend-app/
├── src/
│   ├── app/                           # Next.js App Router
│   │   ├── layout.tsx                 # Root layout (auth provider, sidebar, Supabase client)
│   │   ├── page.tsx                   # Redirect → /dashboard
│   │   │
│   │   ├── (auth)/                    # Auth routes (no sidebar)
│   │   │   ├── login/page.tsx
│   │   │   ├── signup/page.tsx
│   │   │   └── layout.tsx             # Minimal auth layout
│   │   │
│   │   ├── (app)/                     # Authenticated routes (with sidebar)
│   │   │   ├── layout.tsx             # App layout: sidebar + header + org switcher
│   │   │   ├── dashboard/
│   │   │   │   └── page.tsx           # Signal cards + KPI overview
│   │   │   │
│   │   │   ├── audiences/
│   │   │   │   ├── page.tsx           # Segment list
│   │   │   │   ├── new/page.tsx       # Create segment (structured + chat)
│   │   │   │   └── [id]/page.tsx      # Segment detail + member list
│   │   │   │
│   │   │   ├── campaigns/
│   │   │   │   ├── page.tsx           # Campaign list
│   │   │   │   ├── new/
│   │   │   │   │   └── page.tsx       # Chat-driven campaign builder (single route)
│   │   │   │   └── [id]/
│   │   │   │       ├── page.tsx       # Campaign detail (overview tab)
│   │   │   │       ├── metrics/page.tsx     # Performance metrics tab
│   │   │   │       ├── assets/page.tsx      # Campaign assets tab
│   │   │   │       └── attribution/page.tsx # Revenue attribution tab
│   │   │   │
│   │   │   ├── competitors/
│   │   │   │   └── page.tsx           # Competitor ad monitoring
│   │   │   │
│   │   │   ├── analytics/
│   │   │   │   └── page.tsx           # Cross-campaign analytics dashboard
│   │   │   │
│   │   │   ├── attribution/
│   │   │   │   └── page.tsx           # Revenue attribution overview
│   │   │   │
│   │   │   └── settings/
│   │   │       ├── page.tsx           # Settings overview / redirect
│   │   │       ├── organization/page.tsx   # Org name, logo, plan
│   │   │       ├── integrations/page.tsx   # Connect CRM, ad platforms, BYO
│   │   │       ├── brand/page.tsx          # Brand & content (tenant context)
│   │   │       └── team/page.tsx           # Team members, roles
│   │   │
│   │   └── api/                       # Next.js API routes (minimal — proxy to backend)
│   │       └── [...proxy]/route.ts    # Optional: proxy to FastAPI if needed
│   │
│   ├── components/
│   │   ├── layout/
│   │   │   ├── Sidebar.tsx            # App sidebar navigation
│   │   │   ├── Header.tsx             # Top bar with org switcher, user menu
│   │   │   ├── OrgSwitcher.tsx        # Organization dropdown
│   │   │   └── MobileNav.tsx          # Mobile responsive nav
│   │   │
│   │   ├── dashboard/
│   │   │   ├── SignalCard.tsx          # Individual signal card component
│   │   │   ├── SignalCardGrid.tsx      # Grid/list of signal cards
│   │   │   ├── KPICard.tsx            # Single KPI metric card
│   │   │   └── KPIRow.tsx             # Row of KPI cards
│   │   │
│   │   ├── audiences/
│   │   │   ├── SegmentList.tsx        # Table of saved segments
│   │   │   ├── SegmentDetail.tsx      # Segment info + member preview
│   │   │   ├── MemberTable.tsx        # Paginated member list (companies/people)
│   │   │   ├── FilterBuilder.tsx      # Structured filter UI for segment creation
│   │   │   ├── ChatBuilder.tsx        # Natural language audience builder
│   │   │   └── ExportButton.tsx       # CSV export for ad platform upload
│   │   │
│   │   ├── campaigns/
│   │   │   ├── CampaignList.tsx       # Table of campaigns with status badges
│   │   │   ├── CampaignChat.tsx       # Chat interface for campaign builder (left panel)
│   │   │   ├── CampaignAssemblyPanel.tsx  # Assembly view with blocks (right panel)
│   │   │   ├── AssemblyBlock.tsx      # Individual assembly block (Research, Audience, Assets, Config, Pre-Launch Checks)
│   │   │   ├── CampaignDetail.tsx     # Campaign overview with tabs
│   │   │   ├── AssetPreview.tsx       # Preview generated asset (all 8 types)
│   │   │   └── StatusBadge.tsx        # Campaign status pill
│   │   │
│   │   ├── competitors/
│   │   │   ├── CompetitorAdCard.tsx   # Individual competitor ad display
│   │   │   ├── CompetitorAdList.tsx   # Filterable list of tracked competitor ads
│   │   │   └── AddCompetitorDialog.tsx # Dialog to add a new competitor to track
│   │   │
│   │   ├── analytics/
│   │   │   ├── CampaignTable.tsx      # Sortable performance table
│   │   │   ├── TimeSeriesChart.tsx    # Line/area chart for metrics over time
│   │   │   ├── PlatformComparison.tsx # Side-by-side platform bars
│   │   │   ├── BudgetPacing.tsx       # Progress bars — spend vs target
│   │   │   └── MetricCard.tsx         # Individual metric with trend indicator
│   │   │
│   │   ├── attribution/
│   │   │   ├── FunnelChart.tsx        # Visual funnel: campaigns → leads → opps → revenue
│   │   │   ├── CostMetricsTable.tsx   # Cost-per-opp, cost-per-closed-won by campaign
│   │   │   └── PipelineInfluenced.tsx # Pipeline $ influenced summary
│   │   │
│   │   ├── recommendations/
│   │   │   ├── RecommendationCard.tsx # Individual recommendation with approve/dismiss
│   │   │   └── RecommendationPanel.tsx # Sidebar or section of recommendation cards
│   │   │
│   │   ├── chat/
│   │   │   ├── ChatInterface.tsx      # Persistent chat sidebar for general AI interaction
│   │   │   ├── ChatMessage.tsx        # Individual message bubble
│   │   │   └── ChatInput.tsx          # Text input with send
│   │   │
│   │   ├── settings/
│   │   │   ├── IntegrationCard.tsx    # Card per integration (CRM, ad platform, BYO)
│   │   │   ├── OAuthConnectButton.tsx # OAuth flow trigger for Salesforce, LinkedIn, etc.
│   │   │   ├── ContextUploader.tsx    # Upload tenant context (text, file, recording)
│   │   │   ├── ContextList.tsx        # List of uploaded context items
│   │   │   └── TeamTable.tsx          # Team member list with role management
│   │   │
│   │   └── shared/
│   │       ├── DataTable.tsx          # Reusable sortable, paginated table
│   │       ├── EmptyState.tsx         # Empty state illustrations + CTAs
│   │       ├── LoadingState.tsx       # Skeleton loaders
│   │       ├── ErrorBoundary.tsx      # Error boundary with retry
│   │       ├── Modal.tsx              # Reusable modal
│   │       ├── ConfirmDialog.tsx      # Confirmation dialog
│   │       ├── DateRangePicker.tsx    # Date range selector for analytics
│   │       └── PlatformIcon.tsx       # LinkedIn/Meta/Google icons
│   │
│   ├── lib/
│   │   ├── api.ts                     # API client (fetch wrapper hitting FastAPI backend)
│   │   ├── supabase-browser.ts        # Supabase client for browser
│   │   ├── supabase-server.ts         # Supabase client for server components
│   │   ├── auth.ts                    # Auth utilities (session, redirect)
│   │   ├── hooks/
│   │   │   ├── useAuth.ts             # Auth state hook
│   │   │   ├── useOrg.ts             # Current org context hook
│   │   │   ├── useAudiences.ts        # Audience data hooks (React Query)
│   │   │   ├── useCampaigns.ts        # Campaign data hooks
│   │   │   ├── useAnalytics.ts        # Analytics data hooks
│   │   │   ├── useAttribution.ts      # Attribution data hooks
│   │   │   ├── useCompetitors.ts      # Competitor config + ad data hooks
│   │   │   └── useChat.ts             # Chat state + streaming hook
│   │   ├── stores/
│   │   │   ├── org-store.ts           # Zustand: active org, org list
│   │   │   └── campaign-wizard-store.ts # Zustand: campaign assembly state
│   │   └── utils/
│   │       ├── format.ts              # Number, currency, date formatting
│   │       ├── metrics.ts             # Metric calculation helpers (CTR, CPC, etc.)
│   │       └── platform.ts            # Platform-specific utilities
│   │
│   └── styles/
│       └── globals.css                # Tailwind base + custom CSS variables
│
├── public/
│   ├── icons/                         # Platform icons, signal type icons
│   └── images/
│
├── next.config.ts
├── tailwind.config.ts
├── tsconfig.json
├── package.json
├── Dockerfile
├── railway.toml
└── README.md
```

---

## 2. Route Map → Backend API

| Frontend Route | Page | Primary API Calls |
|---------------|------|-------------------|
| `/dashboard` | Signal cards + KPIs | `GET /audiences/signals`, `GET /analytics/overview` |
| `/audiences` | Segment list | `GET /audiences` |
| `/audiences/new` | Create segment | `POST /audiences` or `POST /audiences/chat` |
| `/audiences/[id]` | Segment detail | `GET /audiences/:id`, `GET /audiences/:id/members` |
| `/campaigns` | Campaign list | `GET /campaigns` |
| `/campaigns/new` | Chat-driven campaign builder | `GET /audiences`, `POST /assets/generate`, `POST /campaigns`, `POST /campaigns/:id/launch`, chat endpoint (streaming) |
| `/campaigns/[id]` | Campaign detail | `GET /campaigns/:id` |
| `/campaigns/[id]/metrics` | Campaign metrics | `GET /campaigns/:id/metrics`, `GET /campaigns/:id/metrics/timeseries`, `GET /campaigns/:id/metrics/platforms` |
| `/campaigns/[id]/assets` | Campaign assets | `GET /campaigns/:id/assets` |
| `/campaigns/[id]/attribution` | Campaign attribution | `GET /campaigns/:id/attribution` |
| `/competitors` | Competitor ad monitoring | `GET /competitors`, `GET /competitors/ads` |
| `/analytics` | Cross-campaign analytics | `GET /analytics/campaigns`, `GET /analytics/platforms`, `GET /analytics/timeseries` |
| `/attribution` | Revenue attribution | `GET /attribution/funnel`, `GET /attribution/cost-per-opportunity`, `GET /attribution/cost-per-closed-won`, `GET /attribution/pipeline-influenced` |
| `/settings/organization` | Org settings | `GET /orgs/:id`, `PATCH /orgs/:id` |
| `/settings/integrations` | Provider configs | `GET /orgs/:id/providers`, `PUT /orgs/:id/providers/:provider` |
| `/settings/brand` | Tenant context | `GET /context`, `POST /context`, `DELETE /context/:id` |
| `/settings/team` | Team management | `GET /orgs/:id` (with members), `POST /orgs/:id/members`, `DELETE /orgs/:id/members/:user_id` |

---

## 3. Page Specifications

### 3.1 Dashboard (`/dashboard`)

**Purpose:** First thing the user sees. Actionable signal cards, high-level KPIs, and AI-driven recommendations.

**Data & behavior:**

- **KPI row:** Total Spend, Avg CAC, Pipeline $, Closed-Won $. Each card shows value, trend (percentage change vs prior period), and trend period label. Rendered via the `KPIRow` / `KPICard` components.
- **Signal cards grid:** Each card represents a live intent signal segment (e.g., "34 VPs of Engineering new in role", "18 companies raised funding"). Cards show segment type icon, title, count, trend vs last refresh, and an "Activate" action that navigates to `/campaigns/new` with the segment pre-selected.
- **Active campaigns summary:** Compact table of running campaigns with key metrics (spend, leads, CPL, ROAS). Clicking a row navigates to the campaign detail page.
- **Recommendation cards:** AI-generated recommendation cards may surface here, including competitor intelligence insights (e.g., "Competitor X has been running endpoint security ads for 6 weeks — consider a counter-campaign"). These cards link into the campaign builder with pre-populated context.

**Signal Card component props:**
```typescript
interface SignalCardProps {
  segmentId: string;
  segmentType: SignalType;
  title: string;          // "34 VPs of Engineering new in role"
  count: number;
  lastRefreshed: string;  // ISO date
  icon: string;           // signal type icon
  trend?: number;         // +/- vs last refresh
  onActivate: () => void; // → navigate to /campaigns/new with segment pre-selected
}
```

**KPI Card props:**
```typescript
interface KPICardProps {
  label: string;          // "Total Spend"
  value: string;          // "$12.4K"
  trend: number;          // percentage change
  trendPeriod: string;    // "vs last 30 days"
}
```

### 3.2 Audiences — Segment List (`/audiences`)

**Layout:** Table with columns: Name, Type, Members, Last Refreshed, Status, Actions (view, edit, refresh, export, archive). "Create Segment" button top right.

**Filters:** Status (active/paused/archived), Type (dropdown of signal types).

### 3.3 Audiences — Create Segment (`/audiences/new`)

**Two modes:**

1. **Structured builder** — select signal type from dropdown, configure parameters per type (e.g., for `new_in_role`: target titles, company size, industry, time window). FilterBuilder component.

2. **Chat builder** — natural language input. "Show me VPs of IT at healthcare companies with 200-500 employees who changed jobs in the last 30 days." Backend calls Claude API, returns structured segment definition, user reviews and confirms.

Both modes produce the same output: a segment definition (`segment_type` + `filter_config`) that gets saved via `POST /audiences`.

### 3.4 Audiences — Segment Detail (`/audiences/[id]`)

**Layout:** Segment info header (name, type, member count, last refreshed, refresh button). Member table below (paginated, from ClickHouse). Columns: Name, Title, Company, Domain, Signal Strength, Matched At. Export button. "Activate → Campaign" button.

### 3.5 Campaigns — Campaign List (`/campaigns`)

**Layout:** Table with columns: Name, Status (badge), Audience, Platforms (icons), Spend, Leads, CPL, ROAS, Created. Filterable by status. "Create Campaign" button.

### 3.6 Campaigns — Chat-Driven Campaign Builder (`/campaigns/new`)

**Purpose:** The primary campaign creation experience. A single route with a split-panel layout where AI builds the campaign through conversation while the user reviews and adjusts.

**Why not a wizard:** The traditional 4-step wizard forces linear progression through stages (audience → assets → platforms → review) that should happen fluidly. The chat-driven approach lets the AI do the heavy lifting while the user reviews, adjusts, and approves. The backend is the same — only the interaction model changes.

#### Layout: Split-Panel

**Left panel — Chat interface:**
- The AI is the primary interaction surface. The user describes what they want: "compliance angle, target CISOs, LinkedIn + Meta, $3K budget."
- The AI may initiate the conversation by surfacing an intent signal or recommendation: "12 accounts are actively researching endpoint security this week — want me to build a campaign?"
- The user can intervene at any point via chat to adjust any block: tighten audience, change budget, swap angle, regenerate an asset.
- **Helper buttons** below chat input for common adjustments: "Tighten audience", "Add Google search", "More aggressive CTA", "Increase budget". These inject the corresponding message into the chat.
- Chat messages are stored in component state (not persisted to backend for V1).

**Right panel — Campaign Assembly view:**
- Blocks populate and update in real-time as the AI works through each stage. Each block is rendered by the `AssemblyBlock` component.
- **Block types:**
  - **Research** — market context, competitor intel, signal data informing the campaign angle.
  - **Audience** — selected segment(s), member count, preview of top accounts.
  - **Assets** — generated assets with inline previews (ad copy, landing page, document ad, etc.). All 8 asset types supported.
  - **Config** — platform selection, budgets, schedule, tracked link (dub.co short URL), incentive config (Sendoso).
  - **Pre-Launch Checks** — validation checklist: audience synced, assets approved, budget set, platforms connected, schedule confirmed.
- Each block has a status indicator (empty → in-progress → complete) and can be expanded/collapsed.

**Bottom bar:**
- Persistent summary: total budget, audience size, asset count, check status (e.g., "4/5 checks passed").
- **"Approve & Launch"** button — enabled only when all pre-launch checks pass. Calls `POST /campaigns` then `POST /campaigns/:id/launch`.

#### Campaign Assembly State (Zustand)

```typescript
interface CampaignAssemblyState {
  // Assembly blocks
  research: ResearchBlock | null;
  audienceSegmentId: string | null;
  assets: GeneratedAsset[];
  config: {
    platforms: PlatformConfig[];
    schedule: { startDate: string; endDate: string };
    trackedLinkUrl: string | null;
    incentiveConfig: IncentiveConfig | null;
  };
  preLaunchChecks: Record<string, boolean>;

  // Metadata
  campaignName: string;

  // Actions
  setResearch: (research: ResearchBlock) => void;
  setAudience: (segmentId: string) => void;
  addAsset: (asset: GeneratedAsset) => void;
  updateAsset: (assetId: string, updates: Partial<GeneratedAsset>) => void;
  removeAsset: (assetId: string) => void;
  updateConfig: (config: Partial<CampaignAssemblyState['config']>) => void;
  setCheck: (key: string, passed: boolean) => void;
  reset: () => void;
}
```

The Zustand store (`campaign-wizard-store.ts`) drives the assembly panel. Each `AssemblyBlock` subscribes to its relevant slice of state and re-renders when the AI updates it via chat actions.

### 3.7 Campaigns — Campaign Detail (`/campaigns/[id]`)

**Tab navigation:** Overview | Metrics | Assets | Attribution

**Overview tab:**
- Status badge, audience name (linked), platform icons, date range.
- **Tracked link:** dub.co short URL displayed prominently (e.g., `pe.link/cmmc-q1`) with click count if available.
- KPI row: spend, impressions, clicks, CTR, conversions, CPL.
- AI recommendation cards (if any exist for this campaign).

**Metrics tab:**
- Time series chart (togglable: spend, clicks, conversions, CPC, CTR).
- Platform breakdown table (if multi-platform).
- Budget pacing progress bars.

**Assets tab:**
- List of generated assets for this campaign with inline previews:
  - **ad_copy** — text preview with platform badge.
  - **landing_page** — HTML preview (iframe or screenshot).
  - **lead_magnet** — PDF preview / download link.
  - **document_ad** — PDF carousel viewer (multi-slide preview, swipeable).
  - **video_script** — structured text preview (hook, body, CTA, shot direction, caption overlay).
  - **case_study_page** — HTML preview (narrative structure: situation → challenge → solution → results).
  - **email_copy** — text preview with subject line.
  - **image_brief** — structured text preview with visual direction.
- Each asset shows performance data when available (impressions, clicks, conversions attributed to that specific asset).

**Attribution tab:**
- Leads generated (table: name, company, date, source).
- Opportunities created from those leads.
- Revenue attributed (closed-won from this campaign).
- Cost-per-opportunity, cost-per-closed-won for this campaign.

### 3.8 Analytics (`/analytics`)

**Cross-campaign analytics dashboard:**
- Date range picker (top).
- KPI row: total spend, total leads, avg CPL, total pipeline, total closed-won.
- Campaign performance table: all campaigns, sortable by any metric.
- Platform comparison: bar chart or table showing LinkedIn vs Meta vs Google across metrics.
- Time series: aggregate spend, leads, conversions over time.

### 3.9 Attribution (`/attribution`)

**Revenue attribution overview:**
- Funnel visualization: total campaigns → total leads → total opportunities → pipeline $ → closed-won $.
- Cost-per-opportunity table by campaign.
- Cost-per-closed-won table by campaign.
- Pipeline influenced summary.
- Closed-won lookalike profile (firmographic breakdown for audience building feedback).

### 3.10 Settings — Integrations (`/settings/integrations`)

**Integration cards grid:**

Each card shows: provider name, icon, connection status (connected/disconnected), last synced, configure button.

**Categories:**

- **CRM:** Salesforce, HubSpot. OAuth connect flow.
- **Ad Platforms:** LinkedIn Ads, Meta Ads, Google Ads. OAuth connect flow.
- **Visitor Identification (BYO):** RB2B, Vector, Warmly. Webhook URL display + toggle.
- **Incentives:** Sendoso. API key input.

**OAuth flow:** Click "Connect" → redirect to provider OAuth → callback to `/api/oauth/callback/:provider` → store tokens in provider_configs via backend.

### 3.11 Settings — Brand & Content (`/settings/brand`)

**Context uploader:** Add customer lists, testimonials (text/audio/video), case studies, brand guidelines, positioning docs, competitor info, ICP definition.

Each item: type selector, title, text area or file upload (to Supabase Storage), save. List of existing items with edit/delete.

### 3.12 Competitor Ads (`/competitors`)

**Purpose:** Monitor competitor advertising activity across platforms. Feeds into AI recommendations and campaign builder context.

**Sidebar location:** Under the Intelligence section.

**Data & behavior:**

- **Competitor ad list:** Each row/card displays:
  - Competitor name
  - Platform badge (LinkedIn, Meta, Google)
  - Ad format (e.g., single image, document ad, video, carousel)
  - Headline and body preview (truncated)
  - Duration running (e.g., "Active 6 weeks")
  - Status badge: Active, New (first seen this sync), Stopped
- **Filters:** By competitor (dropdown of configured competitors) and by platform.
- **"Add Competitor" button:** Opens `AddCompetitorDialog` — user enters competitor domain and name, selects platforms to track. Calls `POST /competitors`.
- **Remove competitor:** Action in the competitor config to stop tracking. Calls `DELETE /competitors/:id`.

**Integration with other features:**
- Competitor intel is available in the campaign builder chat context. When the AI recommends a campaign angle, it can reference competitor activity ("CrowdStrike has been running this angle for 6 weeks — validates durability").
- Competitor insights may surface as recommendation cards on the dashboard.
- Data sourced from Adyntel integration, synced weekly via Trigger.dev task.

### 3.13 Chat Interface (Persistent Sidebar)

**Available on every page** as a collapsible right sidebar. Context-aware — knows which page the user is on.

**Note:** This is separate from the campaign builder chat (Section 3.6). The campaign builder has its own dedicated chat panel for campaign assembly. This persistent sidebar chat is for general AI assistance across all pages.

**Capabilities:**
- On dashboard: "What should I focus on this week?"
- On audiences: "Build me a segment of DevOps managers at fintech companies"
- On campaigns: "How is my LinkedIn campaign performing vs Meta?"
- On analytics: "Why did my CPL increase last week?"
- On competitors: "What angles are competitors running on LinkedIn?"

**Implementation:** Streaming responses from `POST /assets/generate` or dedicated chat endpoint. Messages stored in component state (not persisted to DB for V1).

---

## 4. Sidebar Navigation

The sidebar organizes the application into four sections:

**Overview**
- Dashboard
- Attribution

**Campaigns**
- Active Campaigns (`/campaigns`)
- Campaign Builder (`/campaigns/new`)
- Audiences (`/audiences`)

**Intelligence**
- Recommendations
- Competitor Ads (`/competitors`)

**Settings**
- Organization (`/settings/organization`)
- Integrations (`/settings/integrations`)
- Brand & Content (`/settings/brand`)
- Team (`/settings/team`)

There is no "Assets" entry in the sidebar. Assets are campaign-ephemeral — they are generated within the campaign builder and viewed within the campaign detail. There is no global asset library.

---

## 5. Data Fetching

### 5.1 Pattern: React Query

All data fetching uses React Query (TanStack Query) for caching, background refetching, optimistic updates.

```typescript
// lib/hooks/useAudiences.ts
export function useAudiences() {
  return useQuery({
    queryKey: ['audiences'],
    queryFn: () => api.get('/audiences'),
  });
}

export function useAudienceMembers(id: string, page: number) {
  return useQuery({
    queryKey: ['audiences', id, 'members', page],
    queryFn: () => api.get(`/audiences/${id}/members?page=${page}`),
  });
}

export function useCreateAudience() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (data: CreateAudienceInput) => api.post('/audiences', data),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['audiences'] }),
  });
}
```

```typescript
// lib/hooks/useCompetitors.ts
export function useCompetitors() {
  return useQuery({
    queryKey: ['competitors'],
    queryFn: () => api.get('/competitors'),
  });
}

export function useCompetitorAds(filters?: { competitor?: string; platform?: string; status?: string }) {
  return useQuery({
    queryKey: ['competitors', 'ads', filters],
    queryFn: () => api.get('/competitors/ads', { params: filters }),
  });
}

export function useAddCompetitor() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (data: { competitor_domain: string; competitor_name: string; platforms: string[] }) =>
      api.post('/competitors', data),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['competitors'] }),
  });
}

export function useRemoveCompetitor() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => api.delete(`/competitors/${id}`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['competitors'] }),
  });
}
```

### 5.2 API Client

```typescript
// lib/api.ts
class APIClient {
  private baseUrl: string;

  constructor() {
    this.baseUrl = process.env.NEXT_PUBLIC_API_URL!; // FastAPI backend URL
  }

  private async getHeaders(): Promise<Headers> {
    const session = await supabase.auth.getSession();
    return {
      'Authorization': `Bearer ${session.data.session?.access_token}`,
      'Content-Type': 'application/json',
    };
  }

  async get(path: string, options?: { params?: Record<string, string> }) { /* ... */ }
  async post(path: string, body?: any) { /* ... */ }
  async patch(path: string, body: any) { /* ... */ }
  async delete(path: string) { /* ... */ }
}

export const api = new APIClient();
```

---

## 6. Auth Flow

### 6.1 Login/Signup

Supabase Auth handles login/signup. Email + password, Google OAuth.

```typescript
// On login success:
// 1. Get user's org memberships from backend: GET /orgs
// 2. Set active org in Zustand store
// 3. Redirect to /dashboard
```

### 6.2 Org Switching

OrgSwitcher dropdown in header. On switch:
1. Update `activeOrgId` in Zustand store.
2. Invalidate all React Query caches.
3. Refetch current page data.

### 6.3 Protected Routes

All `(app)/*` routes wrapped in auth check. If no session → redirect to `/login`. Middleware in `src/middleware.ts` handles this server-side.

---

## 7. State Management

| State Type | Solution | Where |
|-----------|---------|-------|
| Server state (API data) | React Query | Per-component via hooks |
| Auth state | Supabase Auth + React context | AuthProvider in root layout |
| Active org | Zustand | `org-store.ts` |
| Campaign assembly | Zustand | `campaign-wizard-store.ts` — drives the assembly panel blocks (research, audience, assets, config, pre-launch checks) |
| Chat messages (campaign builder) | Component state (useState) | CampaignChat component |
| Chat messages (global sidebar) | Component state (useState) | ChatInterface component |
| UI state (modals, sidebars) | Component state | Individual components |

---

## 8. Charts

**Library:** Recharts (React-native, composable, good with Tailwind).

**Standard chart configs:**

- **Time series:** `<AreaChart>` or `<LineChart>` with date X axis, metric Y axis. Tooltip on hover. Toggle between metrics.
- **Platform comparison:** `<BarChart>` grouped by platform, metric as bars.
- **Funnel:** Custom SVG or `<BarChart>` with decreasing values, horizontal orientation.
- **Budget pacing:** Custom `<Progress>` component with target line.

**Color palette for platforms:**
```typescript
const PLATFORM_COLORS = {
  linkedin: '#0A66C2',
  meta: '#1877F2',
  google: '#4285F4',
};
```

---

## 9. Environment Variables

```
# Public (exposed to browser)
NEXT_PUBLIC_API_URL=https://api.paidedge.com      # FastAPI backend
NEXT_PUBLIC_SUPABASE_URL=https://xxx.supabase.co
NEXT_PUBLIC_SUPABASE_ANON_KEY=xxx
NEXT_PUBLIC_RUDDERSTACK_WRITE_KEY=xxx              # For PaidEdge's own analytics
NEXT_PUBLIC_RUDDERSTACK_DATA_PLANE_URL=xxx

# Server-only
SUPABASE_SERVICE_ROLE_KEY=xxx
```

---

## 10. Deployment

**Railway:**
- Dockerfile with multi-stage build (install → build → serve)
- `next start` on port 3000
- Health check: `GET /` returns 200
- Custom domain: `app.paidedge.com`

**Build command:** `npm run build`
**Start command:** `npm start`

---

## 11. Design Principles

1. **Dark mode default.** Background: `#09090b`. Green accents: `#00e87b` for positive/active states. Red for negative/alerts. Orange for warnings/medium confidence. Light mode available as an option.

2. **Typography.** Instrument Sans for body text. JetBrains Mono for numbers, metrics, and data values. This creates a clear visual hierarchy between narrative and quantitative content.

3. **Icons, not emojis.** Use `lucide-react` for all icons. No emojis in the production UI.

4. **Data density over whitespace.** This is a tool for demand gen professionals. They want to see numbers, not illustrations. Compact tables, small charts, information-dense dashboards.

5. **Speed of action.** Signal card → campaign launch should be achievable in <30 minutes. Minimize clicks, maximize defaults, pre-fill intelligently. The AI handles the heavy lifting — the user reviews and approves.

6. **Platform-aware.** Always show which platform (LinkedIn, Meta, Google) data comes from. Platform icons everywhere. Color-coded consistently per the platform color palette.

7. **AI is embedded, not a separate product.** Chat sidebar is always available on every page. Recommendations appear inline on the dashboard and within campaigns. Asset generation happens within the campaign builder flow. The campaign builder itself is AI-first — conversation, not forms. AI is a feature of PaidEdge, not a bolt-on.

8. **Progressive disclosure.** Dashboard shows signal cards (simple). Click through to segment detail (more data). Click through to campaign (full detail). Don't overwhelm on the first screen.

9. **Don't over-specify component layouts.** The agent building the frontend should make design decisions that serve the data and the workflow. Reference designs (HTML mockups) are directional, not prescriptive. Design for the information architecture, not for pixel-perfect wireframe replication.

10. **Professional aesthetic.** Dark, data-dense, precise. Inspired by professional-grade intelligence dashboards. Every element should earn its screen real estate.

---

*This document is the single source of truth for the PaidEdge frontend architecture (v2.0). It should be used alongside PaidEdge_PRD.md for product context and paid_engine_x_Backend_Spec.md for the API contract. Where this document conflicts with earlier versions or companion docs, this document takes precedence.*
