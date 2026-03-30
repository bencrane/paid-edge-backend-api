# Creative Engine X — Extraction Audit

**Source Repo:** `bencrane/paid-engine-x-api`
**Date:** 2026-03-26
**Purpose:** Comprehensive migration/extraction document for building `creative-engine-x` — a standalone multi-tenant API service that generates marketing assets for any consuming application.

---

## Section 1: Repository Structure

```
paid-engine-x-api/
├── app/
│   ├── __init__.py
│   ├── main.py                              # FastAPI app init, middleware, router registration
│   ├── config.py                            # Settings (env vars via pydantic-settings)
│   ├── dependencies.py                      # DI: get_current_user, get_tenant, get_claude, get_supabase
│   │
│   ├── assets/                              # ★ ASSET GENERATION SYSTEM (moves to creative-engine-x)
│   │   ├── __init__.py
│   │   ├── context.py                       # ★ Brand context loading (AssetContext, build_asset_context)
│   │   ├── service.py                       # ★ AssetGenerationService orchestrator
│   │   ├── router.py                        # ★ Asset CRUD API (/assets/generate, /assets/{id}, etc.)
│   │   ├── generation_router.py             # ★ Rendering endpoints (/render/landing-page, /render/lead-magnet, /render/document-ad)
│   │   ├── models.py                        # ★ Pydantic input models (BrandingConfig, LandingPageInput, etc.)
│   │   ├── storage.py                       # ★ Supabase Storage upload utility
│   │   ├── validators.py                    # ★ Post-generation output validation
│   │   ├── generators/                      # ★ 8 generator modules
│   │   │   ├── __init__.py
│   │   │   ├── lead_magnet.py               # BJC-169: 5 PDF formats
│   │   │   ├── landing_page.py              # BJC-170: 4 template types
│   │   │   ├── ad_copy.py                   # BJC-171: LinkedIn, Meta, Google RSA
│   │   │   ├── email_copy.py                # BJC-172: 5-email nurture sequences
│   │   │   ├── image_brief.py               # BJC-173: Platform-specific image briefs
│   │   │   ├── document_ad.py               # BJC-174: LinkedIn carousel (3 patterns)
│   │   │   ├── video_script.py              # BJC-175: 30s/60s scripts
│   │   │   └── case_study_page.py           # BJC-176: Case study narratives
│   │   ├── prompts/                         # ★ Prompt template system
│   │   │   ├── __init__.py
│   │   │   ├── base.py                      # PromptTemplate ABC, generator registry
│   │   │   └── schemas.py                   # Generation output Pydantic schemas
│   │   ├── renderers/                       # ★ PDF rendering engines
│   │   │   ├── __init__.py
│   │   │   ├── lead_magnet_pdf.py           # ReportLab multi-page PDF
│   │   │   └── document_ad_pdf.py           # ReportLab slide-per-page PDF
│   │   ├── templates/                       # ★ Jinja2 HTML landing page templates
│   │   │   ├── lead_magnet_download.html
│   │   │   ├── case_study.html
│   │   │   ├── webinar.html
│   │   │   └── demo_request.html
│   │   ├── test_content.py
│   │   └── test_render.py
│   │
│   ├── landing_pages/                       # ★ Landing page hosting (moves to creative-engine-x)
│   │   ├── __init__.py
│   │   └── router.py                        # GET /lp/{slug}, POST /lp/{slug}/submit + RudderStack
│   │
│   ├── integrations/                        # Mixed: some move, some stay
│   │   ├── __init__.py
│   │   ├── claude_ai.py                     # ★ Claude API client (moves)
│   │   ├── data_engine_x.py                 # Stays (entity enrichment for audiences)
│   │   ├── dubco.py                         # Stays (tracked short links for campaigns)
│   │   ├── crm_base.py                      # Stays (CRM sync protocol)
│   │   ├── crm_models.py                    # Stays
│   │   ├── hubspot_engine_x.py              # Stays
│   │   ├── hubspot_syncer.py                # Stays
│   │   ├── salesforce_engine_x.py           # Stays
│   │   ├── salesforce_syncer.py             # Stays
│   │   ├── linkedin.py                      # Stays (ad platform)
│   │   ├── linkedin_auth.py                 # Stays
│   │   ├── linkedin_conversions.py          # Stays
│   │   ├── linkedin_leads.py               # Stays
│   │   ├── linkedin_metrics.py              # Stays
│   │   ├── linkedin_models.py               # Stays
│   │   ├── linkedin_targeting.py            # Stays
│   │   ├── meta_adsets.py                   # Stays
│   │   ├── meta_audiences.py                # Stays
│   │   ├── meta_auth.py                     # Stays
│   │   ├── meta_campaigns.py                # Stays
│   │   ├── meta_client.py                   # Stays
│   │   ├── meta_conversions.py              # Stays
│   │   ├── meta_creatives.py                # Stays
│   │   ├── meta_leads.py                    # Stays
│   │   ├── meta_media.py                    # Stays
│   │   ├── meta_metrics.py                  # Stays
│   │   └── meta_targeting.py                # Stays
│   │
│   ├── auth/                                # Stays (but patterns replicated in creative-engine-x)
│   │   ├── __init__.py
│   │   ├── middleware.py                     # JWTAuthMiddleware
│   │   ├── models.py                        # UserProfile, AuthTokens
│   │   ├── router.py                        # Auth endpoints
│   │   ├── linkedin.py                      # LinkedIn OAuth
│   │   └── meta.py                          # Meta OAuth
│   │
│   ├── audiences/                           # Stays in paid-engine-x
│   │   ├── __init__.py
│   │   ├── export.py
│   │   ├── linkedin_push.py
│   │   ├── meta_push.py
│   │   ├── models.py
│   │   ├── router.py
│   │   └── service.py
│   │
│   ├── campaigns/                           # Stays in paid-engine-x
│   │   ├── __init__.py
│   │   ├── models.py
│   │   ├── platforms/
│   │   │   ├── __init__.py
│   │   │   ├── linkedin.py
│   │   │   └── meta.py
│   │   └── router.py
│   │
│   ├── tenants/                             # Stays (but patterns replicated)
│   │   ├── __init__.py
│   │   ├── models.py                        # Organization, Membership, ProviderConfig
│   │   ├── router.py
│   │   └── service.py                       # resolve_tenant, require_admin
│   │
│   ├── services/                            # Stays (CRM data services)
│   │   ├── __init__.py
│   │   ├── crm_clickhouse.py
│   │   └── crm_supabase.py
│   │
│   ├── shared/                              # Replicate in creative-engine-x
│   │   ├── __init__.py
│   │   ├── errors.py                        # NotFoundError, ForbiddenError, etc.
│   │   ├── models.py                        # HealthResponse
│   │   └── pagination.py
│   │
│   └── db/                                  # Replicate connection patterns
│       ├── __init__.py
│       ├── supabase.py                      # Singleton Supabase client
│       └── clickhouse.py                    # Singleton ClickHouse client (stays)
│
├── trigger/                                 # Stays (Trigger.dev scheduled tasks)
│   ├── __init__.py
│   ├── audience_refresh.py
│   ├── hubspot_crm_sync.py
│   ├── linkedin_lead_sync.py
│   ├── linkedin_metrics_sync.py
│   ├── meta_metrics_sync.py
│   └── salesforce_crm_sync.py
│
├── migrations/
│   └── 003_crm_tables.sql                   # ClickHouse CRM tables (stays)
│
├── docs/                                    # Stays (ad platform API references)
│   ├── DUBCO_API_REFERENCE.md
│   ├── GOOGLE_ADS_API_REFERENCE.md
│   ├── LINKEDIN_MARKETING_API_REFERENCE.md
│   ├── META_MARKETING_API_REFERENCE.md
│   └── ...
│
├── tests/                                   # Relevant asset tests move
├── Dockerfile                               # Python 3.12-slim + Doppler CLI
├── pyproject.toml                           # Python 3.12+, FastAPI, Pydantic, etc.
├── railway.toml                             # Railway deployment config
└── README.md
```

---

## Section 2: Asset Generation System — Complete Inventory

### 2.1 Lead Magnet PDF (BJC-169)

| Field | Value |
|-------|-------|
| **Generator file** | `app/assets/generators/lead_magnet.py` |
| **Generator class** | `LeadMagnetGenerator(PromptTemplate)` |
| **Convenience function** | `generate_lead_magnet(claude, ctx, format)` |
| **Claude model** | `MODEL_QUALITY` = `claude-opus-4-20250514` |
| **Why Opus** | Long-form content (2K-10K words), needs deep expertise and coherence |
| **Output schema** | `LeadMagnetOutput` (title, subtitle, sections: `LeadMagnetSectionOutput[]`) |
| **Output format** | Structured JSON → mapped to `LeadMagnetPDFInput` → rendered to PDF via ReportLab |
| **Template file** | N/A (programmatic PDF via `app/assets/renderers/lead_magnet_pdf.py`) |
| **Prompt file** | Inline in `lead_magnet.py` — 5 format-specific instruction builders |

**Input model (from generation orchestrator):**
- `format`: one of `checklist`, `ultimate_guide`, `benchmark_report`, `template_toolkit`, `state_of_industry`
- `AssetContext` (brand, ICP, campaign, social proof)

**5 format subtypes:**

| Format | Word Range | Temperature | Two-Pass |
|--------|-----------|-------------|----------|
| `checklist` | 2,000–4,000 | 0.3 | No |
| `ultimate_guide` | 5,500–8,500 | 0.7 | Yes |
| `benchmark_report` | 4,000–8,000 | 0.5 | No |
| `template_toolkit` | 3,000–6,000 | 0.4 | No |
| `state_of_industry` | 6,000–10,000 | 0.6 | Yes |

**Two-pass generation** (for `ultimate_guide`, `state_of_industry`):
- Pass 1: Generate outline (section titles + summaries) at lower temperature
- Pass 2: Expand each section fully using outline as context

**Industry vertical guidance** (6 verticals with custom tone, language, CTA recommendations):
- SaaS, Healthcare, Financial Services, Manufacturing (+ generic fallback)

**Dependencies:** `app.assets.context`, `app.assets.models`, `app.assets.prompts.base`, `app.assets.prompts.schemas`, `app.integrations.claude_ai`

---

### 2.2 Landing Page HTML (BJC-170)

| Field | Value |
|-------|-------|
| **Generator file** | `app/assets/generators/landing_page.py` |
| **Generator class** | `LandingPageGenerator(PromptTemplate)` |
| **Convenience function** | `generate_landing_page(claude, ctx, template_type, event_date, speakers)` |
| **Claude model** | `MODEL_QUALITY` = `claude-opus-4-20250514` |
| **Why Opus** | High-converting copy requires nuanced persuasion and brand consistency |
| **Output schemas** | Per template: `LeadMagnetPageOutput`, `CaseStudyPageOutput`, `WebinarPageOutput`, `DemoRequestPageOutput` |
| **Output format** | Structured JSON → mapped to `LandingPageInput` → rendered to HTML via Jinja2 |
| **Template files** | `app/assets/templates/{lead_magnet_download,case_study,webinar,demo_request}.html` |
| **Prompt file** | Inline in `landing_page.py` — 4 template-specific instruction builders |

**4 template types:**

| Template | Output Schema | Default Form Fields |
|----------|--------------|-------------------|
| `lead_magnet_download` | `LeadMagnetPageOutput` | first_name, email, company, title |
| `case_study` | `CaseStudyPageOutput` | (none — read-only page) |
| `webinar` | `WebinarPageOutput` | first_name, last_name, email, company, title |
| `demo_request` | `DemoRequestPageOutput` | first_name, last_name, email, company, title, phone (optional) |

**Template selection logic** (`select_landing_page_template`): keyword-based heuristic on `ctx.objective` and `ctx.angle` — webinar → demo_request → case_study → lead_magnet_download (default).

**Dependencies:** Same as lead magnet + `app.assets.models` (full landing page input models)

---

### 2.3 Ad Copy (BJC-171)

| Field | Value |
|-------|-------|
| **Generator file** | `app/assets/generators/ad_copy.py` |
| **Generator class** | `AdCopyGenerator(PromptTemplate)` |
| **Convenience function** | `generate_ad_copy(claude, ctx, platforms) → dict[str, BaseModel]` |
| **Claude model** | `MODEL_FAST` = `claude-sonnet-4-20250514` |
| **Why Sonnet** | Short-form with strict character limits; fast iteration over 3 platforms |
| **Output schemas** | `LinkedInAdCopyOutput`, `MetaAdCopyOutput`, `GoogleRSACopyOutput` |
| **Output format** | JSON only (no rendering) |
| **Template file** | N/A |

**Platform-specific character limits:**

| Platform | Field | Hard Limit | Recommended |
|----------|-------|-----------|-------------|
| LinkedIn | introductory_text | 600 | 150 (fold) |
| LinkedIn | headline | 200 | 70 |
| LinkedIn | description | 100 | — |
| Meta | primary_text | — | 125 |
| Meta | headline | — | 40 |
| Meta | description | — | 30 |
| Google RSA | headlines (each) | 30 | — |
| Google RSA | descriptions (each) | 90 | — |
| Google RSA | path1/path2 | 15 | — |

**Multi-platform:** Runs platform generations in parallel via `asyncio.gather`. Returns `dict[str, BaseModel]` mapping platform → output.

**Post-generation:** `validate_ad_copy_limits()` auto-truncates at word boundaries for overflows.

---

### 2.4 Email Nurture Sequence (BJC-172)

| Field | Value |
|-------|-------|
| **Generator file** | `app/assets/generators/email_copy.py` |
| **Generator class** | `EmailCopyGenerator(PromptTemplate)` |
| **Convenience function** | `generate_email_sequence(claude, ctx, trigger)` |
| **Claude model** | `MODEL_FAST` = `claude-sonnet-4-20250514` |
| **Output schema** | `EmailSequenceOutput` (sequence_name, trigger, emails: `NurtureEmail[]`) |
| **Output format** | JSON only |

**3 trigger types** with context-specific instructions:
- `lead_magnet_download` — top-of-funnel, educational to soft pitch
- `webinar_registration` — mid-funnel, pre/post-webinar cadence
- `demo_request` — bottom-of-funnel, accelerated cadence

**5-email progression:**
1. Day 0 — Value Delivery (`purpose: value_delivery`)
2. Day 2 — Education (`purpose: education`)
3. Day 5 — Social Proof (`purpose: social_proof`)
4. Day 8 — Soft Pitch (`purpose: soft_pitch`)
5. Day 12 — Direct CTA (`purpose: direct_cta`)

**Email schema:** subject_line (≤60 chars), preview_text (≤90 chars), body_html, send_delay_days, purpose

---

### 2.5 Document Ad / LinkedIn Carousel (BJC-174)

| Field | Value |
|-------|-------|
| **Generator file** | `app/assets/generators/document_ad.py` |
| **Generator class** | `DocumentAdGenerator(PromptTemplate)` |
| **Convenience function** | `generate_document_ad(claude, ctx, pattern)` |
| **Claude model** | `MODEL_QUALITY` = `claude-opus-4-20250514` |
| **Output schema** | `DocumentAdOutput` (slides: `SlideOutput[]`, aspect_ratio) |
| **Output format** | JSON → mapped to `DocumentAdInput` → rendered to PDF via ReportLab |
| **Renderer** | `app/assets/renderers/document_ad_pdf.py` |

**3 narrative patterns:**
- `problem_solution` — Hook → Problem → Solution → Proof → CTA
- `listicle` — Title → N signs/items → Summary → CTA
- `data_story` — Big stat hook → Supporting data → Implications → CTA

**Slide constraints:** 5-8 slides, headline ≤50 chars, body ≤120 chars, last slide must be CTA. Two aspect ratios: `1:1` (1080×1080) and `4:5` (1080×1350).

---

### 2.6 Video Script (BJC-175)

| Field | Value |
|-------|-------|
| **Generator file** | `app/assets/generators/video_script.py` |
| **Generator class** | `VideoScriptGenerator(PromptTemplate)` |
| **Convenience function** | `generate_video_script(claude, ctx, duration, platform)` |
| **Claude model** | `MODEL_FAST` = `claude-sonnet-4-20250514` |
| **Output schema** | `VideoScriptOutput` (hook, body, cta: `ScriptSegment`, total_word_count, music_direction) |
| **Output format** | JSON only |

**Duration configs:**
- `30s` — ~75 words: Hook (3s) → Problem (7s) → Solution (10s) → CTA (10s)
- `60s` — ~150 words: Hook (3s) → Problem (12s) → Solution (20s) → Proof (15s) → CTA (10s)

**Platform guidance** (linkedin, meta, youtube): aspect ratio, tone, caption requirements, visual direction style.

---

### 2.7 Case Study Page (BJC-176)

| Field | Value |
|-------|-------|
| **Generator file** | `app/assets/generators/case_study_page.py` |
| **Generator class** | `CaseStudyPageGenerator(PromptTemplate)` |
| **Convenience function** | `generate_case_study_page(claude, ctx, case_study_index)` |
| **Claude model** | `MODEL_QUALITY` = `claude-opus-4-20250514` |
| **Output schema** | `CaseStudyContentOutput` (headline, sections, metrics, quote, cta_text) |
| **Output format** | JSON → mapped to `CaseStudyPageInput` → rendered via case_study.html |

**4 narrative sections** (fixed order): Situation → Challenge → Solution → Results (200-400 words each).

**Metrics:** 2-4 metric callouts with value + label (e.g., `{value: "3x", label: "ROI increase"}`).

---

### 2.8 Image Concept Brief (BJC-173)

| Field | Value |
|-------|-------|
| **Generator file** | `app/assets/generators/image_brief.py` |
| **Generator class** | `ImageBriefGenerator(PromptTemplate)` |
| **Convenience function** | `generate_image_briefs(claude, ctx, platforms)` |
| **Claude model** | `MODEL_FAST` = `claude-sonnet-4-20250514` |
| **Output schema** | `ImageBriefSetOutput` (briefs: `ImageBriefOutput[]`) |
| **Output format** | JSON only |

**5 platform/format dimensions:**

| Platform | Dimensions | Aspect Ratio |
|----------|-----------|-------------|
| `linkedin_sponsored` | 1200×628 | 1.91:1 |
| `linkedin_carousel` | 1080×1080 | 1:1 |
| `meta_feed` | 1080×1080 | 1:1 |
| `meta_story` | 1080×1920 | 9:16 |
| `landing_page_hero` | 1920×1080 | 16:9 |

**Brief fields:** concept_name, intended_use, dimensions, visual_description, text_overlay, color_palette (hex), mood, style_reference, do_not_include (anti-cliché list).

---

## Section 3: Rendering Infrastructure

### 3.1 PDF Rendering — Lead Magnets

**File:** `app/assets/renderers/lead_magnet_pdf.py`
**Library:** ReportLab (`reportlab >= 4.1`)
**Entry point:** `render_lead_magnet_pdf(input_data: LeadMagnetPDFInput) -> bytes`

**PDF structure:**
1. **Cover page** — Dark background (secondary_color), white title/subtitle, accent bar (primary_color), company name top-left
2. **Table of Contents** — Numbered section entries
3. **Content sections** — Each on its own page:
   - Section heading (bold, secondary_color)
   - Body text (11pt, #333)
   - Bullet list (indented, bull characters)
   - Callout box (tinted table cell with primary_color border and 8% alpha background)

**Typography:** Helvetica family, 28pt title, 14pt subtitle, 20pt section headings, 11pt body, 10.5pt bullets/callouts.

**Branding:** Colors from `BrandingConfig.primary_color` and `secondary_color`. Company name in header/footer.

### 3.2 PDF Rendering — Document Ads

**File:** `app/assets/renderers/document_ad_pdf.py`
**Library:** ReportLab
**Entry point:** `render_document_ad_pdf(input_data: DocumentAdInput) -> bytes`

**PDF structure:** Each slide is one PDF page. Two page sizes:
- `1:1` → 540×540 points (from 1080×1080 pixels)
- `4:5` → 540×675 points (from 1080×1350 pixels)

**Content slides:** Dark background (secondary_color), primary_color accent bar at top. Stat callout (72pt, primary_color), headline (32pt, white), body (16pt, #ddd). Company name bottom-right, slide counter bottom-left.

**CTA slides:** Same dark background, large translucent accent circle (primary_color at 12% alpha), centered headline (36pt, white), CTA text (20pt, secondary_color).

### 3.3 HTML Rendering — Landing Pages

**Library:** Jinja2 (`jinja2 >= 3.1`)
**Template directory:** `app/assets/templates/`

**4 templates:**

| Template | File | Key Layout Features |
|----------|------|-------------------|
| Lead Magnet Download | `lead_magnet_download.html` | 2-column: value props (left) + form (right), hero, social proof |
| Case Study | `case_study.html` | Hero + customer badge, metrics bar, narrative sections, quote block |
| Webinar | `webinar.html` | 2-column: speakers + agenda (left) + sticky form (right) |
| Demo Request | `demo_request.html` | 3-column benefits grid, trust signals, form section |

**CSS:** Inline minified styles. Uses branding colors via Jinja2 variables (`{{ branding.primary_color }}`, `{{ branding.secondary_color }}`, `{{ branding.font_family }}`). Mobile responsive via `@media(max-width:768px)`.

**RudderStack JS SDK injection:** Conditionally loaded when `tracking.rudderstack_write_key` and `tracking.rudderstack_data_plane_url` are set. Fires `rudderanalytics.page()` on load. Form submission triggers client-side `identify()` and `track("form_submitted")`.

**UTM capture:** Hidden form fields (`utm_source`, `utm_medium`, `utm_campaign`) populated from URL query params via JavaScript.

### 3.4 Storage — Supabase

**File:** `app/assets/storage.py`

**Bucket:** `assets` (public, auto-created if missing)

**Upload function:**
```python
async def upload_asset(file_bytes: bytes, filename: str, content_type: str) -> str
```

**URL pattern:** `{SUPABASE_URL}/storage/v1/object/public/assets/{filename}`

**File paths by asset type:**
- `lead-magnets/{asset_id}.pdf`
- `document-ads/{asset_id}.pdf`
- `landing-pages/{asset_id}.html`

**Auth:** Service role key (`SUPABASE_SERVICE_ROLE_KEY`) in Bearer header. Uses REST API directly via httpx (POST to create, PUT to overwrite).

### 3.5 Render Endpoints

**File:** `app/assets/generation_router.py`
**Prefix:** `/render`

| Endpoint | Input | Output | Description |
|----------|-------|--------|-------------|
| `POST /render/landing-page` | `LandingPageInput` | `{asset_id, content_url, template_used, slug}` | Render Jinja2 → HTML → upload → persist |
| `POST /render/lead-magnet` | `LeadMagnetPDFInput` | `{asset_id, content_url}` | Render ReportLab → PDF → upload → persist |
| `POST /render/document-ad` | `DocumentAdInput` | `{asset_id, content_url}` | Render ReportLab → PDF → upload → persist |

All endpoints require JWT auth and resolve tenant from verified JWT claims (`org_id`).

---

## Section 4: Landing Page Hosting System

**File:** `app/landing_pages/router.py`
**Prefix:** `/lp`
**Auth:** Public (no JWT required — explicitly in `PUBLIC_PREFIXES`)

### 4.1 `GET /lp/{slug}` — Serve Landing Page

**Slug resolution:**
1. Query `generated_assets` table where `slug = :slug`
2. If `content_url` exists → fetch rendered HTML from Supabase Storage via httpx
3. Fallback → render from `input_data` JSON using Jinja2 template (identified by `template_used` column)
4. Return `HTMLResponse` (200) or raise 404

### 4.2 `POST /lp/{slug}/submit` — Form Submission

**Flow:**
1. Look up asset by slug in `generated_assets`
2. Parse JSON body — extract `email`, `anonymous_id`, UTM params
3. Insert into `landing_page_submissions` table:
   ```json
   {
     "asset_id": "...",
     "slug": "...",
     "form_data": { /* full form body */ },
     "utm_params": { "utm_source": "...", "utm_medium": "...", ... },
     "organization_id": "...",
     "campaign_id": "...",
     "submitted_at": "2026-03-26T..."
   }
   ```
4. Fire RudderStack `identify()` — merge anonymous visitor → known identity (email)
5. Fire RudderStack `track("form_submitted")` — event with slug, template, campaign_id, org_id, UTMs
6. Return `{"status": "ok"}`

### 4.3 RudderStack Integration

**Server-side calls** (in addition to client-side JS SDK in templates):

```python
# Identify — merge anonymous → known
POST {RUDDERSTACK_DATA_PLANE_URL}/v1/identify
Headers: Content-Type: application/json, Authorization: Basic {RUDDERSTACK_WRITE_KEY}
Body: { "anonymousId": "...", "userId": "email", "traits": { ...form_data } }

# Track — fire event
POST {RUDDERSTACK_DATA_PLANE_URL}/v1/track
Headers: same
Body: { "anonymousId": "...", "userId": "email", "event": "form_submitted", "properties": { slug, template, campaign_id, org_id, ...utms } }
```

### 4.4 URL Structure

Slugs are 12-character hex strings generated by `uuid.uuid4().hex[:12]`. The slug is stored in the `generated_assets` table alongside the asset. There is no separate mapping table — the slug is a column on `generated_assets`.

---

## Section 5: Asset Orchestration Layer

**File:** `app/assets/service.py`
**Class:** `AssetGenerationService`

### 5.1 `POST /assets/generate` — Full Request/Response

**Request (`GenerateRequest`):**
```python
class GenerateRequest(BaseModel):
    campaign_id: str
    asset_types: list[str]             # e.g., ["lead_magnet", "ad_copy", "email_copy"]
    platforms: list[str] | None        # For ad_copy, image_brief
    angle: str | None                  # Override campaign angle
    tone: str | None                   # Override brand voice
    cta: str | None                    # Override CTA text
    lead_magnet_format: str | None     # checklist, ultimate_guide, etc.
    landing_page_template: str | None  # lead_magnet_download, case_study, etc.
    document_ad_pattern: str | None    # problem_solution, listicle, data_story
    video_duration: str | None         # 30s, 60s
    email_trigger: str | None          # lead_magnet_download, webinar_registration, demo_request
```

**Response (`list[GeneratedAssetResponse]`):**
```python
class GeneratedAssetResponse(BaseModel):
    id: str
    asset_type: str
    status: str                        # "draft" or "failed"
    content_url: str | None            # Supabase Storage URL (renderable types)
    content_preview: dict | None       # Summary of JSON content (text-only types)
    template_used: str | None
    error: str | None
```

### 5.2 Preview + Revision Flow

**Preview:** After generation, renderable assets get a `content_url` pointing to the uploaded PDF/HTML. Text-only assets include a `content_preview` (first 500 chars of serialized JSON).

**Revision:** `POST /assets/{asset_id}/revise` with `{"revision_instructions": "..."}`:
1. Fetch existing asset row
2. Rebuild `AssetContext` from `org_id` + `campaign_id`
3. Append revision instructions to `ctx.angle`: `"REVISION INSTRUCTIONS: {instructions}"`
4. Re-dispatch to same generator
5. Re-render and re-upload (if renderable)
6. Update DB row with new content, status → `draft`

### 5.3 Generator Selection

The `_dispatch_generator` method routes by `asset_type` string:

```python
"lead_magnet"      → generate_lead_magnet(claude, ctx, format=...)
"landing_page"     → generate_landing_page(claude, ctx, template_type=...)
"document_ad"      → generate_document_ad(claude, ctx, pattern=...)
"case_study_page"  → generate_case_study_page(claude, ctx)
"ad_copy"          → generate_ad_copy(claude, ctx, platforms=...)
"email_copy"       → generate_email_sequence(claude, ctx, trigger=...)
"video_script"     → generate_video_script(claude, ctx, duration=..., platform=...)
"image_brief"      → generate_image_briefs(claude, ctx, platforms=...)
```

### 5.4 Tenant Context Loading

Before generation, `build_asset_context(org_id, campaign_id, supabase)` loads all context. See Section 6 for full details.

### 5.5 Result Storage

**Status transitions:** `generating` → `draft` (success) or `failed` (error)

**Renderable types** (`lead_magnet`, `document_ad`, `landing_page`, `case_study_page`):
- Render to file (PDF or HTML)
- Upload to Supabase Storage
- Store `content_url` in `generated_assets`

**Text-only types** (`ad_copy`, `email_copy`, `video_script`, `image_brief`):
- Serialize output to JSON
- Store in `content_json` column of `generated_assets`

**Parallelism:** All asset types in a single request are generated concurrently via `asyncio.gather(*tasks, return_exceptions=True)`. Failures for individual types don't block others.

---

## Section 6: Brand Context / Tenant Context System

### 6.1 `tenant_context` Table

**Table:** `tenant_context` (Supabase PostgreSQL)
**Queried by:** `organization_id`

| context_type | context_data (JSON) |
|-------------|-------------------|
| `brand_guidelines` | `{voice, tone, company_name, messaging_pillars, dos, donts, key_messages}` |
| `positioning` | `{value_proposition, company_name}` |
| `icp_definition` | `{job_titles, company_size, industry, pain_points, goals, decision_criteria, seniority, buying_triggers, objections}` |
| `case_study` | `{customer_name, customer_industry, problem, solution, results: {}, quote: {text, author, title}}` |
| `testimonial` | `{quote, author, title, company}` |
| `customers` | `{logos: [url, ...]}` or `{logo_url: "..."}` |
| `competitors` | `{differentiators: ["...", ...]}` |

### 6.2 Context Loading Service

**File:** `app/assets/context.py`
**Function:** `async build_asset_context(org_id, campaign_id, supabase) -> AssetContext`

**Full `AssetContext` model:**
```python
class AssetContext(BaseModel):
    organization_id: str
    campaign_id: str | None = None
    # Brand
    company_name: str = ""
    brand_voice: str = ""
    brand_guidelines: dict | None = None
    value_proposition: str = ""
    # ICP
    icp_definition: dict | None = None
    target_persona: str = ""
    # Content inputs
    case_studies: list[dict] = []
    testimonials: list[dict] = []
    customer_logos: list[str] = []
    competitor_differentiators: list[str] = []
    # Campaign-specific
    angle: str | None = None
    objective: str | None = None
    platforms: list[str] = []
    industry: str | None = None
```

**Loading flow:**
1. Query all `tenant_context` rows for `organization_id`
2. Group by `context_type`
3. Map each type to `AssetContext` fields (see Section 6.1)
4. If `campaign_id` provided: load campaign data (angle, objective, platforms) + audience segment description
5. Fallback: if `company_name` missing, load from `organizations` table

### 6.3 Personalization Drivers

What makes output client-specific (not generic):

| Data Point | Source | Impact on Output |
|-----------|--------|-----------------|
| Company name | `brand_guidelines` or `organizations` | Referenced throughout all content |
| Brand voice/tone | `brand_guidelines.voice` | Controls language style, formality |
| Value proposition | `positioning.value_proposition` | Core messaging in headlines, CTAs |
| ICP definition | `icp_definition` | Persona-specific pain points, language, examples |
| Case studies | `case_study` rows | Real customer results, quotes, metrics |
| Testimonials | `testimonial` rows | Social proof in landing pages, carousels |
| Competitor differentiators | `competitors.differentiators` | Positioning against alternatives |
| Campaign angle | `campaigns.angle` | Specific messaging hook/theme |
| Industry | `icp_definition.industry` | Industry-specific language, compliance, examples |

**Prompt formatting utilities:**
- `format_brand_context_block(ctx)` → System prompt (company, value prop, voice, guidelines, differentiators)
- `format_persona_block(ctx)` → User prompt (ICP details, buying triggers, objections)
- `format_social_proof_block(ctx)` → User prompt (case studies ×3 max, testimonials ×3 max, logo count)

**Token budget:** ~32K chars (~8K tokens) total context. Truncation via `_truncate_block()` at 10K chars per block.

---

## Section 7: Claude API Client

### 7.1 Client File

**File:** `app/integrations/claude_ai.py`

### 7.2 Configuration

```python
MODEL_FAST = "claude-sonnet-4-20250514"     # Ad copy, email, image briefs, video scripts
MODEL_QUALITY = "claude-opus-4-20250514"    # Lead magnets, landing pages, case studies, carousels

# Timeouts
MODEL_QUALITY: 120.0s
MODEL_FAST: 30.0s

# Retry
MAX_RETRIES = 3
RETRY_BASE_DELAY = 1.0s  # Exponential: 1s, 2s, 4s
RETRYABLE_STATUS_CODES = {429, 500, 529}
```

**Client initialization:**
```python
class ClaudeClient:
    def __init__(self, api_key: str | None = None):
        key = api_key or settings.ANTHROPIC_API_KEY
        self._client = anthropic.Anthropic(api_key=key)
```

### 7.3 Structured Output

**Method:** `generate_structured(model, system_prompt, user_prompt, output_schema, temperature, max_tokens, asset_type) -> T`

**Flow:**
1. Append output enforcement to system prompt:
   - "Return your response inside `<output>` XML tags"
   - Include full JSON schema of `output_schema`
2. Call Claude API
3. Parse response: try XML `<output>` tags → markdown fences → raw JSON extraction
4. Validate against Pydantic schema
5. On parse failure: retry once with explicit "return only valid JSON" instruction at lower temperature

### 7.4 Error Handling / Retry

```
Attempt 1 → fail (429/500/529/timeout) → sleep 1s →
Attempt 2 → fail → sleep 2s →
Attempt 3 → fail → raise RuntimeError
```

Retries on: `RateLimitError`, `InternalServerError`, `APIStatusError` (429/500/529), `APITimeoutError`.
Non-retryable errors (400, 401, etc.) raise immediately.

### 7.5 Token Tracking

Every API call logs: model, asset_type, input_tokens, output_tokens, elapsed_seconds, attempt number.

### 7.6 Tools/Function Calling

Not used. All structured output is via XML tag prompting (`<output>` tags) with JSON schema enforcement.

---

## Section 8: Database Schema (Asset-Related)

### 8.1 `generated_assets` (Supabase PostgreSQL)

| Column | Type | Constraints | Notes |
|--------|------|------------|-------|
| `id` | uuid | PK | Generated by orchestrator |
| `organization_id` | uuid | NOT NULL | FK to organizations |
| `campaign_id` | uuid | nullable | FK to campaigns |
| `asset_type` | text | NOT NULL | lead_magnet, landing_page, ad_copy, etc. |
| `status` | text | NOT NULL | generating, draft, approved, failed |
| `content_url` | text | nullable | Supabase Storage public URL (renderable) |
| `content_json` | jsonb | nullable | Serialized output (text-only) |
| `template_used` | text | nullable | Template/generator identifier |
| `input_data` | jsonb | nullable | Full rendering input (for re-rendering) |
| `slug` | text | nullable | 12-char hex (landing pages) |
| `error_message` | text | nullable | Error detail on failure |
| `created_at` | timestamptz | default now() | |
| `updated_at` | timestamptz | | |

### 8.2 `tenant_context` (Supabase PostgreSQL)

| Column | Type | Constraints | Notes |
|--------|------|------------|-------|
| `id` | uuid | PK | |
| `organization_id` | uuid | NOT NULL | FK to organizations |
| `context_type` | text | NOT NULL | brand_guidelines, positioning, icp_definition, case_study, testimonial, customers, competitors |
| `context_data` | jsonb | NOT NULL | Arbitrary JSON per type |
| `created_at` | timestamptz | | |
| `updated_at` | timestamptz | | |

### 8.3 `landing_page_submissions` (Supabase PostgreSQL)

| Column | Type | Constraints | Notes |
|--------|------|------------|-------|
| `id` | uuid | PK | Auto-generated |
| `asset_id` | uuid | NOT NULL | FK to generated_assets |
| `slug` | text | NOT NULL | Landing page slug |
| `form_data` | jsonb | NOT NULL | Full form submission body |
| `utm_params` | jsonb | nullable | Extracted UTM parameters |
| `organization_id` | uuid | nullable | From asset |
| `campaign_id` | uuid | nullable | From asset |
| `submitted_at` | timestamptz | NOT NULL | |

### 8.4 Related Tables (Read by Asset System)

**`campaigns`:** id, organization_id, angle, objective, platforms (text[]), audience_segment_id
**`audience_segments`:** id, organization_id, name, description
**`organizations`:** id, name, slug, domain, logo_url, plan, created_at, updated_at
**`user_profiles`:** id, full_name, avatar_url, created_at, updated_at
**`memberships`:** id, user_id, organization_id, role, created_at

### 8.5 Supabase Storage

**Bucket:** `assets`
**Visibility:** Public
**Access:** Service role key (bypasses RLS)
**Structure:**
```
assets/
├── lead-magnets/{asset_id}.pdf
├── document-ads/{asset_id}.pdf
└── landing-pages/{asset_id}.html
```

---

## Section 9: Multi-Tenancy Pattern

### 9.1 Tenant Resolution

**Tenant source:** JWT `org_id` claim

**Flow** (`app/tenants/service.py`):
1. Read `org_id` from verified JWT claims
2. Verify user membership via `memberships` table → load org
3. Raises `ForbiddenError` if not a member, `NotFoundError` if org context is missing

### 9.2 Auth Middleware

**File:** `app/auth/middleware.py`
**Class:** `JWTAuthMiddleware(BaseHTTPMiddleware)`

```python
# Public paths (no auth):
PUBLIC_PATHS = {"/health", "/docs", "/openapi.json", "/redoc"}
PUBLIC_PREFIXES = ("/auth/signup", "/auth/login", "/auth/refresh",
                   "/auth/linkedin/callback", "/auth/meta/callback", "/lp/")

# JWT validation:
# Better Auth EdDSA via JWKS (no HS256 fallback)
request.state.user_id = payload["sub"]
request.state.jwt_payload = payload
```

### 9.3 Dependency Injection

**File:** `app/dependencies.py`

```python
async def get_supabase() -> SupabaseClient:
    return get_supabase_client()              # Singleton, service role key

async def get_current_user(request, supabase) -> UserProfile:
    user_id = request.state.user_id           # Set by JWT middleware
    # Fetch from user_profiles table, merge email from JWT payload

async def get_claude() -> ClaudeClient:
    return ClaudeClient()                     # Uses ANTHROPIC_API_KEY from settings

async def get_tenant(request, user, supabase) -> Organization:
    org_id = getattr(request.state, "org_id", None)
    return await resolve_tenant(user.id, org_id, supabase)
```

### 9.4 RLS Policies

The application uses **service role key** to bypass RLS. All tenant scoping is done at the application level by filtering queries with `organization_id`.

### 9.5 Provider Configs

Per-tenant API keys/credentials are stored in `provider_configs` table:

```python
class ProviderConfig(BaseModel):
    id: str
    organization_id: str
    provider: str           # e.g., "linkedin", "meta", "hubspot", "salesforce"
    config: dict            # Encrypted/stored credentials
    is_active: bool
    created_at: datetime
    updated_at: datetime | None
```

---

## Section 10: External Dependencies

### 10.1 Anthropic / Claude

- **SDK:** `anthropic >= 0.40`
- **Models:** `claude-opus-4-20250514` (quality), `claude-sonnet-4-20250514` (fast)
- **Endpoints:** `messages.create` (synchronous SDK, wrapped in async)
- **Auth:** `ANTHROPIC_API_KEY` via Doppler
- **Features used:** Structured output via XML tag prompting, temperature control, max_tokens

### 10.2 Supabase

- **SDK:** `supabase >= 2.13`
- **Services used:**
  - **Database (PostgreSQL):** All tables — generated_assets, tenant_context, landing_page_submissions, campaigns, etc.
  - **Storage:** `assets` bucket for PDFs and HTML files
  - **Auth:** Better Auth EdDSA/JWKS token validation
- **Auth pattern:** Service role key (bypasses RLS for all operations)
- **Env vars:** `SUPABASE_URL`, `SUPABASE_ANON_KEY`, `SUPABASE_SERVICE_ROLE_KEY`

### 10.3 RudderStack

- **Used in:** Landing page form submissions (`app/landing_pages/router.py`) + client-side JS SDK in templates
- **Server-side endpoints:** `/v1/identify`, `/v1/track`
- **Auth:** Basic auth with write key
- **Events fired:** `form_submitted` (with slug, template, campaign_id, org_id, UTMs)
- **Env vars:** `RUDDERSTACK_DATA_PLANE_URL`, `RUDDERSTACK_WRITE_KEY`

### 10.4 Other Services

| Service | Purpose | Env Vars | Moves to creative-engine-x? |
|---------|---------|----------|---------------------------|
| **Doppler** | Secrets management (runtime injection) | `DOPPLER_TOKEN` (in Dockerfile) | Yes — own project needed |
| **dub.co** | Tracked short links for campaigns | `DUBCO_API_KEY` | No — stays in paid-engine-x |
| **data-engine-x** | Entity enrichment | `DATA_ENGINE_X_BASE_URL`, `DATA_ENGINE_X_API_TOKEN` | No |
| **hubspot-engine-x** | HubSpot CRM sync | `HUBSPOT_ENGINE_X_BASE_URL`, `HUBSPOT_ENGINE_X_API_TOKEN` | No |
| **sfdc-engine-x** | Salesforce CRM sync | `SFDC_ENGINE_X_BASE_URL`, `SFDC_ENGINE_X_API_TOKEN` | No |
| **ClickHouse** | Analytics/metrics warehouse | `CLICKHOUSE_HOST`, etc. | No |

---

## Section 11: What Would Move to creative-engine-x

### 11.1 Files/Modules That Move As-Is

| Module | Files | Notes |
|--------|-------|-------|
| **Generators** | `app/assets/generators/*.py` (all 8) | Core asset generation logic |
| **Prompt system** | `app/assets/prompts/base.py`, `app/assets/prompts/schemas.py` | PromptTemplate ABC + output schemas |
| **Renderers** | `app/assets/renderers/lead_magnet_pdf.py`, `document_ad_pdf.py` | ReportLab PDF engines |
| **HTML templates** | `app/assets/templates/*.html` (all 4) | Jinja2 landing page templates |
| **Models** | `app/assets/models.py` | Input Pydantic models (BrandingConfig, LandingPageInput, etc.) |
| **Context** | `app/assets/context.py` | AssetContext + prompt formatting utilities |
| **Validators** | `app/assets/validators.py` | Post-generation output validation |
| **Service** | `app/assets/service.py` | AssetGenerationService orchestrator |
| **Storage** | `app/assets/storage.py` | Supabase Storage upload utility |
| **Claude client** | `app/integrations/claude_ai.py` | Full client with retry, structured output, token tracking |
| **Landing pages** | `app/landing_pages/router.py` | Slug resolution, form handling, RudderStack |

### 11.2 Files/Modules That Stay in paid-engine-x

| Module | Files | Notes |
|--------|-------|-------|
| **Audiences** | `app/audiences/*` | Audience management + platform push |
| **Campaigns** | `app/campaigns/*` | Campaign orchestration + platform-specific |
| **Ad platform integrations** | `app/integrations/linkedin*.py`, `meta*.py` (20+ files) | LinkedIn/Meta/Google Ads APIs |
| **CRM integrations** | `app/integrations/crm_*.py`, `hubspot_*.py`, `salesforce_*.py` | CRM sync |
| **Data enrichment** | `app/integrations/data_engine_x.py` | Entity enrichment |
| **Short links** | `app/integrations/dubco.py` | dub.co integration |
| **Trigger.dev tasks** | `trigger/*` | Scheduled jobs (CRM sync, metrics, etc.) |
| **ClickHouse** | `app/db/clickhouse.py`, `migrations/*` | Analytics warehouse |
| **CRM services** | `app/services/crm_*.py` | CRM data services |

### 11.3 Shared Patterns to Replicate

| Pattern | Source | What to Replicate |
|---------|--------|------------------|
| **JWT auth middleware** | `app/auth/middleware.py` | Same pattern, own JWT secret |
| **Tenant resolution** | `app/tenants/service.py` | Same pattern: header → membership → org |
| **Dependency injection** | `app/dependencies.py` | get_current_user, get_tenant, get_claude, get_supabase |
| **Error handling** | `app/shared/errors.py` | NotFoundError, ForbiddenError, etc. |
| **Supabase client** | `app/db/supabase.py` | Singleton pattern with service role key |
| **Config management** | `app/config.py` | pydantic-settings with Doppler injection |
| **FastAPI app setup** | `app/main.py` | Middleware stack, router registration, health check |

### 11.4 New Things creative-engine-x Needs

| Need | Description |
|------|-------------|
| **Own API surface** | New FastAPI app with routes: `/assets/generate`, `/assets/{id}`, `/render/*`, `/lp/*` |
| **Own Supabase project** | Separate PostgreSQL database + Storage bucket. Tables: `generated_assets`, `tenant_context`, `landing_page_submissions`, `organizations`, `memberships`, `user_profiles` |
| **Own Doppler project** | `creative-engine-x` project with configs: dev, stg, prd. Env vars: SUPABASE_*, ANTHROPIC_API_KEY, RUDDERSTACK_*, APP_* |
| **Service-to-service auth** | API key or JWT-based auth for consumers (paid-engine-x, OEX-API, Money Machine) to call creative-engine-x. Pattern: `Authorization: Bearer <service_api_key>` with per-consumer API keys |
| **Tenant provisioning API** | Endpoints for consumers to register tenants, upload brand context, manage tenant_context rows |
| **Webhook/callback system** | Notify consumers when async generation completes (currently synchronous return) |
| **Rate limiting** | Per-tenant rate limits to prevent Claude API abuse |
| **Usage tracking / billing** | Track generation counts, token usage, storage consumption per tenant |
| **Own Dockerfile + deployment** | Separate Railway/Fly.io service with Doppler integration |

---

## Section 12: Key Code Snippets

### 12.1 Asset Orchestrator — Main `generate` Function

```python
# app/assets/service.py

class AssetGenerationService:
    def __init__(self, claude: ClaudeClient, supabase: SupabaseClient):
        self.claude = claude
        self.supabase = supabase

    async def generate(
        self,
        org_id: str,
        campaign_id: str,
        asset_types: list[str],
        *,
        platforms: list[str] | None = None,
        angle: str | None = None,
        tone: str | None = None,
        cta: str | None = None,
        lead_magnet_format: str | None = None,
        landing_page_template: str | None = None,
        document_ad_pattern: str | None = None,
        video_duration: str | None = None,
        email_trigger: str | None = None,
    ) -> list[dict]:
        """Orchestrate generation for multiple asset types in parallel."""
        for at in asset_types:
            if at not in VALID_ASSET_TYPES:
                raise BadRequestError(
                    detail=f"Unknown asset type '{at}'. Valid: {sorted(VALID_ASSET_TYPES)}"
                )

        ctx = await build_asset_context(org_id, campaign_id, self.supabase)

        if angle:
            ctx.angle = angle
        if tone:
            ctx.brand_voice = tone
        if platforms:
            ctx.platforms = platforms

        tasks = []
        for at in asset_types:
            tasks.append(
                self._generate_single(
                    ctx=ctx, org_id=org_id, campaign_id=campaign_id, asset_type=at,
                    platforms=platforms, cta=cta, lead_magnet_format=lead_magnet_format,
                    landing_page_template=landing_page_template,
                    document_ad_pattern=document_ad_pattern,
                    video_duration=video_duration, email_trigger=email_trigger,
                )
            )

        results = await asyncio.gather(*tasks, return_exceptions=True)

        final: list[dict] = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error("Generation failed for %s: %s", asset_types[i], result)
                final.append({
                    "id": str(uuid.uuid4()), "asset_type": asset_types[i],
                    "status": "failed", "content_url": None,
                    "content_preview": None, "template_used": None, "error": str(result),
                })
            else:
                final.append(result)
        return final
```

### 12.2 Lead Magnet Generator (Complete)

```python
# app/assets/generators/lead_magnet.py

class LeadMagnetGenerator(PromptTemplate):
    asset_type = "lead_magnet"
    model = ClaudeClient.MODEL_QUALITY
    output_schema = LeadMagnetOutput
    temperature = 0.5

    def build_asset_specific_instructions(self, ctx: AssetContext, **kwargs: Any) -> str:
        fmt = kwargs.get("format", "checklist")
        if fmt not in _FORMAT_BUILDERS:
            raise ValueError(f"Unknown lead magnet format '{fmt}'. Valid: {sorted(VALID_FORMATS)}")
        parts: list[str] = []
        builder = _FORMAT_BUILDERS[fmt]
        parts.append(builder(ctx))
        industry = ctx.industry or ""
        if industry in INDUSTRY_GUIDANCE:
            parts.append(INDUSTRY_GUIDANCE[industry])
        elif industry:
            parts.append(f"INDUSTRY CONTEXT: {industry} — tailor examples and language accordingly.")
        return "\n\n".join(parts)

    async def generate(self, claude: ClaudeClient, ctx: AssetContext, **kwargs: Any) -> LeadMagnetOutput:
        fmt = kwargs.get("format", "checklist")
        if fmt not in LEAD_MAGNET_FORMATS:
            raise ValueError(f"Unknown lead magnet format '{fmt}'. Valid: {sorted(VALID_FORMATS)}")
        fmt_config = LEAD_MAGNET_FORMATS[fmt]
        original_temp = self.temperature
        self.temperature = fmt_config["temperature"]
        try:
            if fmt_config["two_pass"]:
                return await self._two_pass_generate(claude, ctx, fmt=fmt)
            else:
                return await super().generate(claude, ctx, **kwargs)
        finally:
            self.temperature = original_temp

    async def _two_pass_generate(self, claude, ctx, fmt):
        system_prompt = self.build_system_prompt(ctx)
        outline_prompt = (
            self.build_user_prompt(ctx, format=fmt) + "\n\n"
            "IMPORTANT: For this first pass, generate ONLY an outline.\n"
            "Return a JSON object matching the output schema, but with abbreviated content:\n"
            "- title: The full title\n- subtitle: The full subtitle\n"
            "- sections: For each section, include:\n"
            "  - heading: The section/chapter title\n"
            "  - body: A 2–3 sentence summary\n  - bullets: 3–5 key points\n  - callout_box: null\n"
        )
        outline = await claude.generate_structured(
            model=self.model, system_prompt=system_prompt, user_prompt=outline_prompt,
            output_schema=self.output_schema, temperature=max(self.temperature - 0.1, 0.0),
            asset_type=self.asset_type,
        )
        outline_summary = "\n".join(f"- {s.heading}: {s.body}" for s in outline.sections)
        expand_prompt = (
            self.build_user_prompt(ctx, format=fmt) + "\n\n"
            f"OUTLINE (expand each section fully):\n{outline_summary}\n\n"
            "Now generate the COMPLETE content. Write the full prose, detailed bullets, "
            "and callout boxes. Do not abbreviate."
        )
        return await claude.generate_structured(
            model=self.model, system_prompt=system_prompt, user_prompt=expand_prompt,
            output_schema=self.output_schema, temperature=self.temperature,
            asset_type=self.asset_type,
        )


async def generate_lead_magnet(claude, ctx, format):
    generator = LeadMagnetGenerator()
    output = await generator.generate(claude, ctx, format=format)
    return map_output_to_pdf_input(output, ctx)
```

### 12.3 Claude API Client — Initialization and Call Pattern

```python
# app/integrations/claude_ai.py

MODEL_FAST = "claude-sonnet-4-20250514"
MODEL_QUALITY = "claude-opus-4-20250514"

class ClaudeClient:
    MODEL_FAST = MODEL_FAST
    MODEL_QUALITY = MODEL_QUALITY

    def __init__(self, api_key: str | None = None):
        key = api_key or settings.ANTHROPIC_API_KEY
        self._client = anthropic.Anthropic(api_key=key)

    async def generate_structured(
        self, model, system_prompt, user_prompt, output_schema: type[T],
        temperature=0.3, max_tokens=4096, asset_type="unknown",
    ) -> T:
        schema_json = json.dumps(output_schema.model_json_schema(), indent=2)
        full_system = (
            f"{system_prompt}\n\nOUTPUT RULES:\n"
            "- Return your response inside <output> XML tags\n"
            "- The content inside <output> must be valid JSON matching the schema below\n"
            "- Do not include any text outside the <output> tags\n\n"
            f"OUTPUT SCHEMA:\n```json\n{schema_json}\n```"
        )
        raw = await self._call_api(model=model, system_prompt=full_system,
            user_prompt=user_prompt, temperature=temperature,
            max_tokens=max_tokens, asset_type=asset_type)
        try:
            data = parse_json_from_response(raw)
            return validate_against_schema(data, output_schema)
        except (json.JSONDecodeError, ValueError):
            # Retry with explicit JSON instruction
            retry_prompt = (f"{user_prompt}\n\nIMPORTANT: Return ONLY valid JSON "
                          "inside <output></output> tags.")
            raw = await self._call_api(model=model, system_prompt=full_system,
                user_prompt=retry_prompt, temperature=max(temperature - 0.1, 0.0),
                max_tokens=max_tokens, asset_type=asset_type)
            data = parse_json_from_response(raw)
            return validate_against_schema(data, output_schema)

    async def _call_api(self, model, system_prompt, user_prompt, temperature, max_tokens, asset_type):
        timeout = _TIMEOUTS.get(model, 60.0)
        for attempt in range(_MAX_RETRIES):
            try:
                start = time.monotonic()
                response = self._client.messages.create(
                    model=model, max_tokens=max_tokens, temperature=temperature,
                    system=system_prompt, messages=[{"role": "user", "content": user_prompt}],
                    timeout=timeout,
                )
                elapsed = time.monotonic() - start
                logger.info("claude_api_call", extra={
                    "model": model, "asset_type": asset_type,
                    "input_tokens": response.usage.input_tokens,
                    "output_tokens": response.usage.output_tokens,
                    "elapsed_seconds": round(elapsed, 2), "attempt": attempt + 1,
                })
                return "\n".join(b.text for b in response.content if b.type == "text")
            except anthropic.RateLimitError:
                time.sleep(_RETRY_BASE_DELAY * (2 ** attempt))
            except anthropic.InternalServerError as exc:
                time.sleep(_RETRY_BASE_DELAY * (2 ** attempt))
            except anthropic.APIStatusError as exc:
                if exc.status_code in _RETRYABLE_STATUS_CODES:
                    time.sleep(_RETRY_BASE_DELAY * (2 ** attempt))
                else:
                    raise
            except anthropic.APITimeoutError:
                if attempt < _MAX_RETRIES - 1:
                    time.sleep(_RETRY_BASE_DELAY)
        raise RuntimeError(f"Claude API call failed after {_MAX_RETRIES} attempts")
```

### 12.4 Tenant Context Loading Function

```python
# app/assets/context.py

async def build_asset_context(org_id: str, campaign_id: str | None, supabase: Client) -> AssetContext:
    ctx = AssetContext(organization_id=org_id)

    # Load tenant_context rows
    res = supabase.table("tenant_context").select("*").eq("organization_id", org_id).execute()
    rows = res.data or []

    grouped: dict[str, list[dict]] = {}
    for row in rows:
        ct = row.get("context_type", "unknown")
        grouped.setdefault(ct, []).append(row)

    # Brand guidelines
    brand_rows = grouped.get("brand_guidelines", [])
    if brand_rows:
        data = brand_rows[0].get("context_data", {})
        ctx.brand_guidelines = data
        ctx.brand_voice = data.get("voice", data.get("tone", ""))
        ctx.company_name = data.get("company_name", "")

    # Positioning
    positioning_rows = grouped.get("positioning", [])
    if positioning_rows:
        data = positioning_rows[0].get("context_data", {})
        ctx.value_proposition = data.get("value_proposition", "")
        if not ctx.company_name:
            ctx.company_name = data.get("company_name", "")

    # ICP Definition
    icp_rows = grouped.get("icp_definition", [])
    if icp_rows:
        data = icp_rows[0].get("context_data", {})
        ctx.icp_definition = data
        ctx.target_persona = _format_icp_summary(data)
        ctx.industry = data.get("industry", None)

    # Case studies, testimonials, customers, competitors
    for row in grouped.get("case_study", []):
        ctx.case_studies.append(row.get("context_data", {}))
    for row in grouped.get("testimonial", []):
        ctx.testimonials.append(row.get("context_data", {}))
    for row in grouped.get("customers", []):
        data = row.get("context_data", {})
        logos = data.get("logos", [])
        if isinstance(logos, list):
            ctx.customer_logos.extend(logos)
        elif data.get("logo_url"):
            ctx.customer_logos.append(data["logo_url"])
    for row in grouped.get("competitors", []):
        data = row.get("context_data", {})
        diffs = data.get("differentiators", [])
        if isinstance(diffs, list):
            ctx.competitor_differentiators.extend(diffs)

    # Campaign data
    if campaign_id:
        camp_res = supabase.table("campaigns").select("*").eq("id", campaign_id).eq("organization_id", org_id).maybe_single().execute()
        if camp_res.data:
            camp = camp_res.data
            ctx.campaign_id = campaign_id
            ctx.angle = camp.get("angle")
            ctx.objective = camp.get("objective")
            ctx.platforms = camp.get("platforms", [])
            segment_id = camp.get("audience_segment_id")
            if segment_id:
                seg_res = supabase.table("audience_segments").select("*").eq("id", segment_id).maybe_single().execute()
                if seg_res.data:
                    ctx.target_persona += f"\n\nAudience Segment: {seg_res.data.get('name', '')}\n{seg_res.data.get('description', '')}"

    # Fallback: company name from organizations table
    if not ctx.company_name:
        org_res = supabase.table("organizations").select("name").eq("id", org_id).maybe_single().execute()
        if org_res.data:
            ctx.company_name = org_res.data.get("name", "")

    return ctx
```

### 12.5 Supabase Storage Upload Utility

```python
# app/assets/storage.py

import httpx
from app.config import settings

_STORAGE_BASE = f"{settings.SUPABASE_URL}/storage/v1"
_BUCKET = "assets"

async def _ensure_bucket():
    headers = {
        "Authorization": f"Bearer {settings.SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
    }
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{_STORAGE_BASE}/bucket", headers=headers,
            json={"id": _BUCKET, "name": _BUCKET, "public": True},
        )
        if resp.status_code not in (200, 201, 409):
            resp.raise_for_status()

async def upload_asset(file_bytes: bytes, filename: str, content_type: str) -> str:
    await _ensure_bucket()
    headers = {
        "Authorization": f"Bearer {settings.SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": content_type,
    }
    url = f"{_STORAGE_BASE}/object/{_BUCKET}/{filename}"
    async with httpx.AsyncClient() as client:
        resp = await client.post(url, headers=headers, content=file_bytes)
        if resp.status_code == 400:
            resp = await client.put(url, headers=headers, content=file_bytes)
        resp.raise_for_status()
    return f"{settings.SUPABASE_URL}/storage/v1/object/public/{_BUCKET}/{filename}"
```

### 12.6 Landing Page Serve Route

```python
# app/landing_pages/router.py

@router.get("/{slug}", response_class=HTMLResponse)
async def serve_landing_page(slug: str, request: Request):
    asset = _get_asset_by_slug(slug)
    content_url = asset.get("content_url")

    if content_url:
        async with httpx.AsyncClient() as client:
            resp = await client.get(content_url)
            resp.raise_for_status()
        return HTMLResponse(content=resp.text, status_code=200)

    input_data = asset.get("input_data")
    template_used = asset.get("template_used")
    if input_data and template_used:
        from jinja2 import Environment, FileSystemLoader
        from pathlib import Path
        templates_dir = Path(__file__).parent.parent / "assets" / "templates"
        env = Environment(loader=FileSystemLoader(str(templates_dir)), autoescape=False)
        template = env.get_template(f"{template_used}.html")
        html = template.render(slug=slug, **input_data)
        return HTMLResponse(content=html, status_code=200)

    raise NotFoundError(detail="Landing page content not available")
```

### 12.7 Landing Page Form Handler

```python
# app/landing_pages/router.py

@router.post("/{slug}/submit")
async def submit_landing_page_form(slug: str, request: Request):
    asset = _get_asset_by_slug(slug)
    body = await request.json()

    email = body.get("email", "")
    anonymous_id = body.get("anonymous_id", "") or body.get("anonymousId", "")

    utm_params = {
        k: body.get(k, "")
        for k in ("utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content")
        if body.get(k)
    }

    submission_record = {
        "asset_id": asset["id"], "slug": slug, "form_data": body,
        "utm_params": utm_params, "organization_id": asset.get("organization_id"),
        "campaign_id": asset.get("campaign_id"),
        "submitted_at": datetime.now(UTC).isoformat(),
    }

    supabase = get_supabase_client()
    supabase.table("landing_page_submissions").insert(submission_record).execute()

    if email:
        traits = {k: v for k, v in body.items() if k not in ("anonymous_id", "anonymousId")}
        traits["email"] = email
        await _fire_rudderstack_identify(
            anonymous_id=anonymous_id or email, user_id=email, traits=traits,
        )

    await _fire_rudderstack_track(
        anonymous_id=anonymous_id or email or "unknown", user_id=email or "",
        event="form_submitted",
        properties={"slug": slug, "template": asset.get("template_used", ""),
                    "campaign_id": asset.get("campaign_id", ""),
                    "organization_id": asset.get("organization_id", ""), **utm_params},
    )

    return JSONResponse(content={"status": "ok", "message": "Form submitted successfully"}, status_code=200)
```

### 12.8 Jinja2 Landing Page Template — Lead Magnet Download

```html
<!-- app/assets/templates/lead_magnet_download.html -->
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{{ headline }} | {{ branding.company_name }}</title>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:{{ branding.font_family }};color:#1a1a2e;background:#fafafa;line-height:1.6;-webkit-font-smoothing:antialiased}
.page-wrap{max-width:1080px;margin:0 auto;min-height:100vh;display:flex;flex-direction:column}
.hero{background:{{ branding.secondary_color }};color:#fff;padding:64px 40px 56px;text-align:center}
.hero-logo{max-height:40px;margin-bottom:32px}
.hero h1{font-size:2.25rem;font-weight:700;line-height:1.2;max-width:720px;margin:0 auto 16px}
.hero p{font-size:1.125rem;opacity:.85;max-width:560px;margin:0 auto}
.main{display:grid;grid-template-columns:1fr 1fr;gap:48px;padding:56px 40px;flex:1}
.value-col h2{font-size:1.35rem;font-weight:600;margin-bottom:24px;color:{{ branding.secondary_color }}}
.value-list{list-style:none;padding:0}
.value-list li{position:relative;padding:12px 0 12px 32px;border-bottom:1px solid #eee;font-size:1rem}
.value-list li::before{content:"";position:absolute;left:0;top:18px;width:16px;height:16px;border-radius:50%;background:{{ branding.primary_color }}}
.social-proof{margin-top:40px;padding:24px;background:#f4f4f5;border-radius:8px}
.form-col{background:#fff;border:1px solid #e4e4e7;border-radius:12px;padding:36px;box-shadow:0 1px 3px rgba(0,0,0,.06)}
.cta-btn{width:100%;padding:14px;background:{{ branding.primary_color }};color:{{ branding.secondary_color }};font-size:1rem;font-weight:600;border:none;border-radius:8px;cursor:pointer}
@media(max-width:768px){
  .main{grid-template-columns:1fr;gap:32px;padding:32px 20px}
}
</style>
</head>
<body>
<div class="page-wrap">
  <header class="hero">
    {% if branding.logo_url %}<img src="{{ branding.logo_url }}" alt="{{ branding.company_name }}" class="hero-logo">{% endif %}
    <h1>{{ headline }}</h1>
    <p>{{ subhead }}</p>
  </header>
  <div class="main" id="main-content">
    <div class="value-col">
      <h2>What You'll Get</h2>
      <ul class="value-list">
        {% for prop in value_props %}<li>{{ prop }}</li>{% endfor %}
      </ul>
      {% if social_proof %}
      <div class="social-proof">
        {% if social_proof.type == "quote" and social_proof.quote_text %}
          <p class="quote">"{{ social_proof.quote_text }}"</p>
          {% if social_proof.quote_author %}<p class="quote-attr">— {{ social_proof.quote_author }}{% if social_proof.quote_title %}, {{ social_proof.quote_title }}{% endif %}</p>{% endif %}
        {% elif social_proof.type == "stats" and social_proof.stats %}
          <div class="stats-row">
            {% for stat in social_proof.stats %}
            <div class="stat-item"><div class="val">{{ stat.value }}</div><div class="lbl">{{ stat.label }}</div></div>
            {% endfor %}
          </div>
        {% elif social_proof.type == "logos" and social_proof.logos %}
          <p>Trusted by leading companies</p>
        {% endif %}
      </div>
      {% endif %}
    </div>
    <div class="form-col" id="form-wrapper">
      <h3>Get Your Free Copy</h3>
      <form id="lp-form" method="POST" action="/lp/{{ slug }}/submit">
        {% for field in form_fields %}
        <div class="field">
          <label for="{{ field.name }}">{{ field.label }}</label>
          <input type="{{ field.type }}" id="{{ field.name }}" name="{{ field.name }}" {% if field.required %}required{% endif %}>
        </div>
        {% endfor %}
        <input type="hidden" name="utm_source" id="utm_source">
        <input type="hidden" name="utm_medium" id="utm_medium">
        <input type="hidden" name="utm_campaign" id="utm_campaign">
        <button type="submit" class="cta-btn">{{ cta_text }}</button>
      </form>
    </div>
    <div class="thank-you" id="thank-you" style="display:none">
      <h3>Thank You!</h3>
      <p>Your download is on its way. Check your inbox shortly.</p>
    </div>
  </div>
  <footer>&copy; {{ branding.company_name }}</footer>
</div>
{% if tracking.rudderstack_write_key and tracking.rudderstack_data_plane_url %}
<script>/* RudderStack JS SDK loader */
!function(){var e=window.rudderanalytics=[];/* ... minified SDK ... */
e.load("{{ tracking.rudderstack_write_key }}","{{ tracking.rudderstack_data_plane_url }}"),e.page()}();
</script>
{% endif %}
<script>
(function(){
  var params=new URLSearchParams(window.location.search);
  ["utm_source","utm_medium","utm_campaign"].forEach(function(k){
    var v=params.get(k);if(v){var el=document.getElementById(k);if(el)el.value=v;}
  });
  var form=document.getElementById("lp-form");
  form.addEventListener("submit",function(e){
    e.preventDefault();
    var fd=new FormData(form);
    var data={};fd.forEach(function(v,k){data[k]=v;});
    {% if tracking.rudderstack_write_key %}
    if(window.rudderanalytics){
      rudderanalytics.identify(data.email||"",data);
      rudderanalytics.track("form_submitted",{slug:"{{ slug }}",template:"lead_magnet_download"});
    }
    {% endif %}
    fetch(form.action,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(data)})
      .then(function(){
        document.getElementById("form-wrapper").style.display="none";
        document.getElementById("thank-you").style.display="block";
      });
  });
})();
</script>
</body>
</html>
```

### 12.9 Pydantic Models — Asset Generation Input/Output

**Input Models** (`app/assets/models.py`):

```python
class BrandingConfig(BaseModel):
    logo_url: str | None = None
    primary_color: str = "#00e87b"
    secondary_color: str = "#09090b"
    font_family: str = "Inter, sans-serif"
    company_name: str = ""

class TrackingConfig(BaseModel):
    rudderstack_write_key: str | None = None
    rudderstack_data_plane_url: str | None = None
    utm_source: str | None = None
    utm_medium: str | None = None
    utm_campaign: str | None = None

class FormField(BaseModel):
    name: str
    label: str
    type: str = "text"
    required: bool = True

class Section(BaseModel):
    heading: str
    body: str
    bullets: list[str] | None = None
    callout: str | None = None

class MetricCallout(BaseModel):
    value: str
    label: str

class SocialProofConfig(BaseModel):
    type: Literal["logos", "quote", "stats"]
    logos: list[str] | None = None
    quote_text: str | None = None
    quote_author: str | None = None
    quote_title: str | None = None
    stats: list[MetricCallout] | None = None

class LeadMagnetPageInput(BaseModel):
    template: Literal["lead_magnet_download"] = "lead_magnet_download"
    headline: str
    subhead: str
    value_props: list[str]
    form_fields: list[FormField]
    cta_text: str = "Download Now"
    branding: BrandingConfig
    tracking: TrackingConfig = TrackingConfig()
    social_proof: SocialProofConfig | None = None
    hero_image_url: str | None = None

class CaseStudyPageInput(BaseModel):
    template: Literal["case_study"] = "case_study"
    customer_name: str
    customer_logo_url: str | None = None
    headline: str
    sections: list[Section]
    metrics: list[MetricCallout]
    quote_text: str | None = None
    quote_author: str | None = None
    quote_title: str | None = None
    cta_text: str = "Get Similar Results"
    form_fields: list[FormField] = []
    branding: BrandingConfig
    tracking: TrackingConfig = TrackingConfig()

class WebinarPageInput(BaseModel):
    template: Literal["webinar"] = "webinar"
    event_name: str
    event_date: str
    headline: str
    speakers: list[dict]
    agenda: list[str]
    form_fields: list[FormField]
    cta_text: str = "Register Now"
    branding: BrandingConfig
    tracking: TrackingConfig = TrackingConfig()

class DemoRequestPageInput(BaseModel):
    template: Literal["demo_request"] = "demo_request"
    headline: str
    subhead: str
    benefits: list[Section]
    trust_signals: SocialProofConfig | None = None
    form_fields: list[FormField]
    cta_text: str = "Request Demo"
    branding: BrandingConfig
    tracking: TrackingConfig = TrackingConfig()

LandingPageInput = Union[LeadMagnetPageInput, CaseStudyPageInput, WebinarPageInput, DemoRequestPageInput]

class PDFSection(BaseModel):
    heading: str
    body: str
    bullets: list[str] | None = None
    callout_box: str | None = None

class LeadMagnetPDFInput(BaseModel):
    title: str
    subtitle: str | None = None
    sections: list[PDFSection]
    branding: BrandingConfig

class Slide(BaseModel):
    headline: str
    body: str | None = None
    stat_callout: str | None = None
    stat_label: str | None = None
    is_cta_slide: bool = False
    cta_text: str | None = None

class DocumentAdInput(BaseModel):
    slides: list[Slide]
    branding: BrandingConfig
    aspect_ratio: Literal["1:1", "4:5"] = "1:1"
```

**Output Schemas** (`app/assets/prompts/schemas.py`): See Section 2 for each asset type's output schema. Key schemas include `LeadMagnetOutput`, `LinkedInAdCopyOutput`, `MetaAdCopyOutput`, `GoogleRSACopyOutput`, `EmailSequenceOutput`, `DocumentAdOutput`, `VideoScriptOutput`, `ImageBriefSetOutput`, `CaseStudyContentOutput`.

---

## Section 13: Configuration & Environment

All environment variables are defined in `app/config.py` and injected via Doppler at runtime (`doppler run --`).

| Variable | Purpose | Doppler Project |
|----------|---------|----------------|
| **`SUPABASE_URL`** | Supabase project URL | paid-engine-x-api |
| **`SUPABASE_ANON_KEY`** | Public anon key (for client auth) | paid-engine-x-api |
| **`SUPABASE_SERVICE_ROLE_KEY`** | Service role key (bypasses RLS) | paid-engine-x-api |
| **`ANTHROPIC_API_KEY`** | Claude API authentication | paid-engine-x-api |
| **`RUDDERSTACK_DATA_PLANE_URL`** | RudderStack data plane endpoint | paid-engine-x-api |
| **`RUDDERSTACK_WRITE_KEY`** | RudderStack write key (Basic auth) | paid-engine-x-api |
| `CLICKHOUSE_HOST` | ClickHouse cloud host | paid-engine-x-api |
| `CLICKHOUSE_PORT` | ClickHouse port (default 8443) | paid-engine-x-api |
| `CLICKHOUSE_USER` | ClickHouse user (default "default") | paid-engine-x-api |
| `CLICKHOUSE_PASSWORD` | ClickHouse password | paid-engine-x-api |
| `CLICKHOUSE_DATABASE` | ClickHouse database (paid_engine_x_api) | paid-engine-x-api |
| `LINKEDIN_CLIENT_ID` | LinkedIn OAuth client ID | paid-engine-x-api |
| `LINKEDIN_CLIENT_SECRET` | LinkedIn OAuth client secret | paid-engine-x-api |
| `LINKEDIN_REDIRECT_URI` | LinkedIn OAuth redirect URI | paid-engine-x-api |
| `META_APP_ID` | Meta OAuth app ID | paid-engine-x-api |
| `META_APP_SECRET` | Meta OAuth app secret | paid-engine-x-api |
| `META_REDIRECT_URI` | Meta OAuth redirect URI | paid-engine-x-api |
| `META_SYSTEM_USER_ID` | Meta system user ID | paid-engine-x-api |
| `DATA_ENGINE_X_BASE_URL` | data-engine-x API base URL | paid-engine-x-api |
| `DATA_ENGINE_X_API_TOKEN` | data-engine-x Bearer token | paid-engine-x-api |
| `HUBSPOT_ENGINE_X_BASE_URL` | hubspot-engine-x API base URL | paid-engine-x-api |
| `HUBSPOT_ENGINE_X_API_TOKEN` | hubspot-engine-x Bearer token | paid-engine-x-api |
| `SFDC_ENGINE_X_BASE_URL` | sfdc-engine-x API base URL | paid-engine-x-api |
| `SFDC_ENGINE_X_API_TOKEN` | sfdc-engine-x Bearer token | paid-engine-x-api |
| `DUBCO_API_KEY` | dub.co API key (tracked links) | paid-engine-x-api |
| **`APP_ENV`** | Environment (development/staging/production) | paid-engine-x-api |
| **`APP_URL`** | Backend URL (default http://localhost:8000) | paid-engine-x-api |
| **`FRONTEND_URL`** | Frontend URL (default http://localhost:3000) | paid-engine-x-api |
| **`CORS_ORIGINS`** | Allowed CORS origins | paid-engine-x-api |

**Bold = needed by creative-engine-x.** The rest stay in paid-engine-x.

**creative-engine-x will need its own Doppler project** with:
- `SUPABASE_URL`, `SUPABASE_ANON_KEY`, `SUPABASE_SERVICE_ROLE_KEY` (own Supabase project)
- `ANTHROPIC_API_KEY` (shared or own key)
- `RUDDERSTACK_DATA_PLANE_URL`, `RUDDERSTACK_WRITE_KEY` (own source in RudderStack)
- `APP_ENV`, `APP_URL`, `CORS_ORIGINS`
- New: `SERVICE_API_KEYS` (for consumer authentication)
