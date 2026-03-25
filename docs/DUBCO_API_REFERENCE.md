# dub.co API Reference for PaidEdge

> Comprehensive reference for building a dub.co tracked link integration for PaidEdge — a multi-tenant B2B SaaS platform where every campaign gets a short link for independent attribution. Click analytics supplement ad platform metrics.
>
> Target ticket: **BJC-64**

---

## Table of Contents

1. [Authentication](#1-authentication)
2. [Link Management — CRUD](#2-link-management--crud)
3. [Custom Domains](#3-custom-domains)
4. [Click Analytics](#4-click-analytics)
5. [QR Codes](#5-qr-codes)
6. [Workspace / Project Management](#6-workspace--project-management)
7. [Webhooks](#7-webhooks)
8. [Pricing & Limits](#8-pricing--limits)
9. [SDKs & Libraries](#9-sdks--libraries)
10. [Integration Patterns for Multi-Tenant SaaS](#10-integration-patterns-for-multi-tenant-saas)
11. [Common Gotchas](#11-common-gotchas)

---

## 1. Authentication

**Base URL:** `https://api.dub.co`

### API Key Types

| Type | Format | Purpose |
|------|--------|---------|
| API Key | `dub_xxxxxxxx` | Server-to-server REST API access. Must be kept secret. |
| Publishable Key | `dub_pk_xxxxxxxx` | Client-side conversion tracking only. Safe for frontend. |

### Header Format

```
Authorization: Bearer dub_xxxxxxxx
```

### Creating API Keys

1. Navigate to **Settings → API Keys** in the workspace dashboard.
2. Click "Create" and select permissions.
3. Choose association: **"You"** (tied to user account) or **"Machine"** (machine user).
4. Copy the key immediately — it cannot be retrieved later.

### Permission Levels

- **All permissions** — Full resource access
- **Read only** — All resources in read-only mode
- **Restricted** — Granular: links, analytics, domains, tags

### Workspace Scoping & Multi-Tenant Key Management

Each API key is tied to a **specific workspace**. One key cannot manage links across multiple workspaces. For PaidEdge's multi-tenant model, the recommended approach is a **single workspace** with `tenantId` per customer (see [Section 10](#10-integration-patterns-for-multi-tenant-saas)), not one workspace per tenant.

### Machine Users

Machine users enable API key creation without tying credentials to individual employees:
- Have **owner role** permissions
- Appear in the People tab without counting toward user limits
- Deleted automatically when their API key is deleted

### Rate Limits (Per Minute)

| Plan | Limit |
|------|-------|
| Free | 60 req/min |
| Pro | 600 req/min |
| Business | 1,200 req/min |
| Advanced | 3,000 req/min |
| Enterprise | Custom |

### Rate Limit Headers (IETF standard)

- `X-RateLimit-Limit` — Max requests per hour
- `X-RateLimit-Remaining` — Remaining in current window
- `X-RateLimit-Reset` — UTC epoch seconds when window resets
- `Retry-After` — Seconds to wait before retrying

Exceeding limits returns **429 Too Many Requests**.

### Error Response Format

All endpoints return errors in this shape:

```json
{
  "error": {
    "code": "not_found",
    "message": "The requested resource was not found.",
    "doc_url": "https://dub.co/docs/api-reference/errors#not-found"
  }
}
```

Error codes: `bad_request` (400), `unauthorized` (401), `forbidden` (403), `not_found` (404), `conflict` (409), `invite_expired` (410), `unprocessable_entity` (422), `rate_limit_exceeded` (429), `internal_server_error` (500).

---

## 2. Link Management — CRUD

### Create a Link

**`POST /links`**

**Required:**
- `url` (string, max 32,000 chars) — destination URL

**Key optional params:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `domain` | string (max 190) | Custom domain. Defaults to workspace primary or `dub.sh` |
| `key` | string (max 190) | Custom slug (e.g., `cmmc-q1`). Random 7-char if omitted |
| `keyLength` | number (3–190) | Length of auto-generated slug. Default 7 |
| `prefix` | string | Prefix for random slugs (e.g., `/c/`) |
| `externalId` | string (1–255) | Your database ID. **Must be unique per workspace** |
| `tenantId` | string (max 255) | Tenant identifier for multi-tenant grouping |
| `tagIds` | string \| string[] | Tag IDs to assign |
| `tagNames` | string \| string[] | Tag names (case insensitive, created if missing) |
| `folderId` | string | Folder assignment |
| `expiresAt` | string | ISO-8601 expiration datetime |
| `expiredUrl` | string (max 32,000) | Redirect URL after expiration |
| `password` | string | Password protection |
| `trackConversion` | boolean | Enable conversion tracking |
| `webhookIds` | string[] | Webhook IDs triggered on click |
| `comments` | string | Internal comments |
| `proxy` | boolean | Enable custom link previews |
| `title` | string | og:title for link preview |
| `description` | string | og:description |
| `image` | string | og:image (base64 or URI) |
| `rewrite` | boolean | Link cloaking |
| `doIndex` | boolean | Allow search engine indexing |
| `ios` | string (max 32,000) | iOS-specific redirect |
| `android` | string (max 32,000) | Android-specific redirect |
| `geo` | object | Geo targeting: `{ "US": "https://...", "GB": "https://..." }` |
| `utm_source` | string | UTM source parameter |
| `utm_medium` | string | UTM medium parameter |
| `utm_campaign` | string | UTM campaign parameter |
| `utm_term` | string | UTM term parameter |
| `utm_content` | string | UTM content parameter |
| `testVariants` | object[] (2–4) | A/B testing: `[{ url, percentage }]` |

#### curl Example

```bash
curl -X POST https://api.dub.co/links \
  -H "Authorization: Bearer dub_xxxxxxxx" \
  -H "Content-Type: application/json" \
  -d '{
    "url": "https://landing.paidedge.com/nexus-q1-offer",
    "domain": "go.nexussecurity.com",
    "key": "cmmc-q1",
    "externalId": "campaign_abc123",
    "tenantId": "tenant_nexus",
    "tagNames": ["q1-2026", "cmmc"],
    "trackConversion": true,
    "utm_source": "email",
    "utm_medium": "campaign",
    "utm_campaign": "cmmc-q1-2026"
  }'
```

#### Python (httpx) Example

```python
import httpx

response = httpx.post(
    "https://api.dub.co/links",
    headers={"Authorization": "Bearer dub_xxxxxxxx"},
    json={
        "url": "https://landing.paidedge.com/nexus-q1-offer",
        "domain": "go.nexussecurity.com",
        "key": "cmmc-q1",
        "externalId": "campaign_abc123",
        "tenantId": "tenant_nexus",
        "tagNames": ["q1-2026", "cmmc"],
        "trackConversion": True,
        "utm_source": "email",
        "utm_medium": "campaign",
        "utm_campaign": "cmmc-q1-2026",
    },
)
link = response.json()
# link["shortLink"] => "https://go.nexussecurity.com/cmmc-q1"
```

#### Response (200)

```json
{
  "id": "clx1234abcdef",
  "domain": "go.nexussecurity.com",
  "key": "cmmc-q1",
  "url": "https://landing.paidedge.com/nexus-q1-offer",
  "shortLink": "https://go.nexussecurity.com/cmmc-q1",
  "qrCode": "https://api.dub.co/qr?url=https://go.nexussecurity.com/cmmc-q1",
  "trackConversion": true,
  "externalId": "campaign_abc123",
  "tenantId": "tenant_nexus",
  "archived": false,
  "expiresAt": null,
  "expiredUrl": null,
  "password": null,
  "proxy": false,
  "title": null,
  "description": null,
  "image": null,
  "rewrite": false,
  "doIndex": false,
  "ios": null,
  "android": null,
  "geo": {},
  "tags": [
    { "id": "tag_abc", "name": "q1-2026", "color": "blue" },
    { "id": "tag_def", "name": "cmmc", "color": "green" }
  ],
  "folderId": null,
  "webhookIds": [],
  "comments": null,
  "utm_source": "email",
  "utm_medium": "campaign",
  "utm_campaign": "cmmc-q1-2026",
  "utm_term": null,
  "utm_content": null,
  "userId": "user_xxx",
  "workspaceId": "ws_xxx",
  "clicks": 0,
  "leads": 0,
  "conversions": 0,
  "sales": 0,
  "saleAmount": 0,
  "lastClicked": null,
  "createdAt": "2026-03-25T00:00:00.000Z",
  "updatedAt": "2026-03-25T00:00:00.000Z"
}
```

### Retrieve a Link

**`GET /links/info`**

Use ONE of these query param combos:
- `domain` + `key` — e.g., `?domain=go.nexussecurity.com&key=cmmc-q1`
- `linkId` — the Dub link ID
- `externalId` — your external ID, **prefixed with `ext_`**: `?externalId=ext_campaign_abc123`

#### curl

```bash
curl "https://api.dub.co/links/info?externalId=ext_campaign_abc123" \
  -H "Authorization: Bearer dub_xxxxxxxx"
```

#### Python

```python
response = httpx.get(
    "https://api.dub.co/links/info",
    headers={"Authorization": "Bearer dub_xxxxxxxx"},
    params={"externalId": "ext_campaign_abc123"},
)
```

### List Links

**`GET /links`**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `domain` | string | — | Filter by domain |
| `tagIds` | string \| string[] | — | Filter by tag IDs |
| `tagNames` | string \| string[] | — | Filter by tag names (case insensitive) |
| `folderId` | string | — | Filter by folder |
| `search` | string | — | Match against slug and destination URL |
| `userId` | string | — | Filter by user |
| `tenantId` | string | — | Filter by tenant |
| `showArchived` | boolean | false | Include archived links |
| `sortBy` | string | `createdAt` | `createdAt`, `clicks`, `saleAmount`, `lastClicked` |
| `sortOrder` | string | `desc` | `asc` or `desc` |
| `page` | number (min 1) | 1 | Page number |
| `pageSize` | number (max 100) | 100 | Items per page |

**Response:** Array of link objects.

#### curl — List Links by Tag

```bash
curl "https://api.dub.co/links?tagNames=cmmc&tenantId=tenant_nexus&sortBy=clicks&sortOrder=desc" \
  -H "Authorization: Bearer dub_xxxxxxxx"
```

#### Python — List Links by Tag

```python
response = httpx.get(
    "https://api.dub.co/links",
    headers={"Authorization": "Bearer dub_xxxxxxxx"},
    params={
        "tagNames": "cmmc",
        "tenantId": "tenant_nexus",
        "sortBy": "clicks",
        "sortOrder": "desc",
    },
)
links = response.json()
```

### Count Links

**`GET /links/count`**

Same filtering params as list, plus:
- `groupBy` — `domain`, `tagId`, `userId`, or `folderId`

**Response:** A number (or array of counts if `groupBy` is set).

### Update a Link

**`PATCH /links/{linkId}`**

- `linkId` accepts either the Dub link ID or `ext_`-prefixed externalId
- Body: any optional fields from create

```bash
curl -X PATCH "https://api.dub.co/links/ext_campaign_abc123" \
  -H "Authorization: Bearer dub_xxxxxxxx" \
  -H "Content-Type: application/json" \
  -d '{ "url": "https://landing.paidedge.com/nexus-q1-offer-v2" }'
```

### Delete a Link

**`DELETE /links/{linkId}`**

```bash
curl -X DELETE "https://api.dub.co/links/ext_campaign_abc123" \
  -H "Authorization: Bearer dub_xxxxxxxx"
```

**Response:** `{ "id": "clx1234abcdef" }`

### Upsert a Link

**`PUT /links/upsert`**

Same body as create. If a link with the same URL already exists in the workspace, it is returned (or updated). Otherwise, a new link is created.

### Bulk Create Links

**`POST /links/bulk`**

Body: Array of up to **100** link objects.

**Response:** Array of link objects or error objects:
```json
{ "link": {...}, "error": "message", "code": "bad_request" }
```

> **Warning:** Bulk operations do NOT trigger webhook events.

### Bulk Update Links

**`PATCH /links/bulk`**

```json
{
  "linkIds": ["id1", "id2"],
  "data": { "tagNames": ["updated-tag"] }
}
```

Up to 100 links. Cannot modify `domain` or `key`. Webhooks NOT triggered.

### Bulk Delete Links

**`DELETE /links/bulk?linkIds=id1,id2,id3`**

Up to 100 link IDs, comma-separated. **Irreversible.** Webhooks NOT triggered.

**Response:** `{ "deletedCount": 3 }`

### Link Expiration

- Set `expiresAt` to an ISO-8601 datetime
- Set `expiredUrl` to redirect users after expiration
- Time-based only — no click-based expiration

### Password Protection

- Set `password` on create/update
- Users will be prompted for the password before being redirected
- Relevant for gated content landing pages

### Tags

- Tags are many-to-many labels on links
- Create via `POST /tags` with `name` (required, 1–50 chars) and `color` (optional: `red`, `yellow`, `green`, `blue`, `purple`, `brown`, `gray`, `pink`)
- Assign on link create/update via `tagIds` or `tagNames`
- Filter links by tag: `GET /links?tagNames=cmmc`
- CRUD endpoints: `POST /tags`, `GET /tags`, `PATCH /tags/{id}`, `DELETE /tags/{id}`
- Tag limits: Unlimited on Free, **25 on Pro**, Unlimited on Business+

### External ID

- Attach your own identifier via `externalId` (e.g., `campaign_abc123`)
- **Must be unique per workspace** — duplicate returns `409 Conflict`
- Reference in API calls with `ext_` prefix: `ext_campaign_abc123`
- Use for easy lookup from your campaign/asset records

### Tenant ID

- `tenantId` is the primary isolation mechanism for multi-tenant platforms
- One-to-many: a tenant can have many links
- Filter links and analytics by tenant: `?tenantId=tenant_nexus`

---

## 3. Custom Domains

### Setup Process

1. Add the domain in the Dub dashboard or via `POST /domains`
2. Configure DNS:
   - **Subdomain** (e.g., `go.nexussecurity.com`): CNAME → `cname.dub.co` (TTL 86400)
   - **Apex domain** (e.g., `nexussecurity.com`): A record → `76.76.21.21` (TTL 86400)
3. `www.` is **not supported**. Use `go.`, `try.`, `links.`, or `l.` instead.
4. SSL is provisioned automatically by Dub.

### Verification

- TXT record verification is **only required** for domains currently on Vercel
- TXT record: Name `_vercel`, Value provided by Dub dashboard
- **Warning**: Setting the TXT record will transfer domain ownership away from any existing Vercel project using that domain
- DNS propagation takes **1 to 24 hours**

### Per-Tenant Custom Domains

Yes — different PaidEdge tenants can have different short link domains. Each domain is registered in the workspace, and links specify which domain to use via the `domain` parameter.

**Example:** Tenant Nexus Security uses `go.nexussecurity.com`, Tenant Acme Corp uses `links.acmecorp.com` — both within the same PaidEdge workspace.

### Domain Limits by Plan

| Plan | Custom Domains |
|------|---------------|
| Free | 3 |
| Pro | 10 |
| Business | 100 |
| Advanced | 250 |
| Enterprise | Unlimited |

### Cloudflare-Specific Notes

- **Strongly recommended**: DNS-only mode (disable Cloudflare proxy / "orange cloud")
- If proxy mode is required: Set SSL/TLS to **Custom SSL/TLS → Full**
- **Warning**: Cloudflare proxy causes **inaccurate geolocation data** and records proxy IP instead of user IP

### Domain API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/domains` | Create a domain. Required: `slug` (1–190 chars) |
| `GET` | `/domains` | List all domains |
| `PATCH` | `/domains/{slug}` | Update a domain |
| `DELETE` | `/domains/{slug}` | Delete a domain (**irreversible — deletes ALL associated links**) |

Optional domain params: `expiredUrl`, `notFoundUrl`, `archived`, `placeholder`, `logo` (for QR codes), `assetLinks` (Android deep links), `appleAppSiteAssociation` (iOS deep links).

---

## 4. Click Analytics

> **Requires Pro plan or higher.** Events endpoint requires **Business plan or higher.**

### Retrieve Analytics

**`GET /analytics`**

#### Key Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `event` | string | `clicks` | `clicks`, `leads`, `sales`, `composite` |
| `groupBy` | string | `count` | Dimension to group by (see below) |
| `interval` | string | `24h` | `24h`, `7d`, `30d`, `90d`, `1y`, `mtd`, `qtd`, `ytd`, `all` |
| `start` | string | — | ISO datetime, overrides `interval` |
| `end` | string | now | ISO datetime |
| `timezone` | string | `UTC` | IANA timezone (e.g., `America/New_York`) |

#### Available Dimensions (`groupBy`)

| Category | Values |
|----------|--------|
| Aggregate | `count` |
| Time series | `timeseries` (auto-adjusts granularity: hourly/daily/weekly/monthly) |
| Geography | `continents`, `countries`, `regions`, `cities` |
| Device | `devices`, `browsers`, `os` |
| Traffic source | `referers`, `referer_urls`, `trigger` / `triggers` |
| UTM | `utm_sources`, `utm_mediums`, `utm_campaigns`, `utm_terms`, `utm_contents` |
| Top content | `top_links`, `top_urls`, `top_base_urls`, `top_folders`, `top_link_tags`, `top_domains` |

#### Filter Parameters

- **Link:** `domain`, `key`, `linkId`, `externalId` (prefix `ext_`), `url`
- **Tenant:** `tenantId`, `tagId`, `folderId`
- **Geo:** `country` (ISO 3166-1), `region` (ISO 3166-2), `city`, `continent`
- **Device:** `device`, `browser`, `os`
- **Referrer:** `referer`, `refererUrl`
- **UTM:** `utm_source`, `utm_medium`, `utm_campaign`, `utm_term`, `utm_content`
- **Trigger:** `trigger` (`qr`, `link`, `pageview`, `deeplink`)

All filter params support comma-separated multiple values and `-` prefix for exclusion.

#### Response Shapes

**`groupBy=count`:**
```json
{ "clicks": 1234, "leads": 56, "sales": 12, "saleAmount": 4800 }
```

**`groupBy=timeseries`:**
```json
[
  { "start": "2026-03-01T00:00:00Z", "clicks": 150, "leads": 3, "sales": 1, "saleAmount": 400 },
  { "start": "2026-03-02T00:00:00Z", "clicks": 203, "leads": 5, "sales": 2, "saleAmount": 800 }
]
```

**`groupBy=countries`:**
```json
[
  { "country": "US", "clicks": 890, "leads": 30, "sales": 8, "saleAmount": 3200 },
  { "country": "GB", "clicks": 234, "leads": 10, "sales": 3, "saleAmount": 1200 }
]
```

#### curl — Get Link Analytics

```bash
curl "https://api.dub.co/analytics?externalId=ext_campaign_abc123&groupBy=timeseries&interval=30d" \
  -H "Authorization: Bearer dub_xxxxxxxx"
```

#### Python — Get Link Analytics

```python
response = httpx.get(
    "https://api.dub.co/analytics",
    headers={"Authorization": "Bearer dub_xxxxxxxx"},
    params={
        "externalId": "ext_campaign_abc123",
        "groupBy": "timeseries",
        "interval": "30d",
    },
)
timeseries = response.json()
```

#### Python — Aggregate Analytics by Tenant

```python
response = httpx.get(
    "https://api.dub.co/analytics",
    headers={"Authorization": "Bearer dub_xxxxxxxx"},
    params={
        "tenantId": "tenant_nexus",
        "groupBy": "countries",
        "interval": "90d",
    },
)
by_country = response.json()
```

### Analytics Rate Limits (Per Second)

| Plan | Limit |
|------|-------|
| Free | N/A (not available) |
| Pro | 2 req/sec |
| Business | 4 req/sec |
| Advanced | 8 req/sec |
| Enterprise | Custom |

### Real-Time vs Delayed

Dub advertises "real-time click and conversion data." No specific latency numbers are documented, but the implication is near-real-time (seconds, not minutes).

### List Events (Raw Click Stream)

**`GET /events`** — **Requires Business plan or higher**

Same filter params as `/analytics`, plus pagination:

| Parameter | Default | Max |
|-----------|---------|-----|
| `page` | 1 | — |
| `limit` | 100 | 1,000 |
| `sortBy` | `timestamp` | — |
| `sortOrder` | `desc` | — |

**Click event response:**
```json
{
  "event": "click",
  "timestamp": "2026-03-25T14:30:00.000Z",
  "click": {
    "id": "click_xxx",
    "timestamp": "2026-03-25T14:30:00.000Z",
    "url": "https://landing.paidedge.com/nexus-q1-offer",
    "country": "US",
    "city": "San Francisco",
    "region": "CA",
    "continent": "NA",
    "device": "Desktop",
    "browser": "Chrome",
    "os": "Mac OS",
    "trigger": "link",
    "referer": "https://google.com",
    "refererUrl": "https://google.com/search?q=...",
    "qr": false,
    "ip": "203.0.113.1"
  },
  "link": { "...full link object..." }
}
```

### UTM Parameter Tracking

Dub captures UTM parameters that you set **on the link itself** (`utm_source`, `utm_medium`, etc.). These are appended to the destination URL on redirect and tracked in analytics. You can filter analytics by UTM dimensions.

### Conversion Tracking

Dub supports conversion events beyond clicks:
- **Leads**: Track with `dub.track.lead()` (client SDK) or `POST /track/lead`
- **Sales**: Track with `dub.track.sale()` (client SDK) or `POST /track/sale`
- Must enable `trackConversion: true` on the link
- Attribution window: 90 days (configurable via `expiresInDays`)

---

## 5. QR Codes

### Retrieve QR Code

**`GET /qr`**

Returns `image/png` binary.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `url` | string (required) | — | Target URL to encode |
| `size` | number | 600 | QR code dimensions in pixels |
| `level` | string | `L` | Error correction: `L`, `M`, `Q`, `H` |
| `fgColor` | hex string | `#000000` | Foreground color |
| `bgColor` | hex string | `#FFFFFF` | Background color |
| `logo` | string | — | Logo URL to embed (**paid plans only**) |
| `hideLogo` | boolean | false | Hide the logo (**paid plans only**) |
| `margin` | number | 2 | Margin size around QR code |

#### curl

```bash
curl "https://api.dub.co/qr?url=https://go.nexussecurity.com/cmmc-q1&size=800&fgColor=%23003366&level=H" \
  -H "Authorization: Bearer dub_xxxxxxxx" \
  --output qr_code.png
```

#### Python

```python
response = httpx.get(
    "https://api.dub.co/qr",
    headers={"Authorization": "Bearer dub_xxxxxxxx"},
    params={
        "url": "https://go.nexussecurity.com/cmmc-q1",
        "size": 800,
        "fgColor": "#003366",
        "level": "H",
    },
)
with open("qr_code.png", "wb") as f:
    f.write(response.content)
```

### Custom Domain QR Logos

On Pro+ plans, set a custom QR code logo per domain. QR codes for that domain's links automatically include the logo. The `hideLogo` param can suppress it per-request.

### Analytics Attribution

QR code scans are tracked as `trigger: "qr"` in analytics. The `link.clicked` webhook payload includes `"qr": true` for QR scans. Relevant for direct mail pieces that link to landing pages.

---

## 6. Workspace / Project Management

### What is a Workspace

A workspace is the top-level container in dub.co — equivalent to a "Team" or "Organization." It holds domains, links, tags, folders, and members. Each workspace is **billed independently**.

### Multi-Workspace Access

- Each API key is scoped to **exactly one workspace** (no cross-workspace API calls)
- Users can belong to multiple workspaces
- Free plan: max **2 workspaces**. Pro+: unlimited workspaces

### For PaidEdge: Single Workspace, Not One Per Tenant

Use a **single PaidEdge workspace** with `tenantId` for customer isolation. One workspace per tenant would require separate API keys and billing per tenant — operationally impractical.

### Member Roles

| Role | Available On | Permissions |
|------|-------------|-------------|
| Owner | All plans | Full access: domains, teammates, billing, webhooks, settings |
| Member | All plans | Create/edit links, folders, tags. No domains, billing, webhooks |
| Viewer | Business+ | Read-only access to links, analytics |
| Billing | Advanced+ | Confirming payouts, managing billing only |

### User Limits by Plan

| Plan | Users |
|------|-------|
| Free | 1 |
| Pro | 3 |
| Business | 10 |
| Advanced | 20 |
| Enterprise | Unlimited |

---

## 7. Webhooks

### Setup

Webhooks are configured via the **dashboard only** at `https://app.dub.co/webhooks`. No API endpoint for managing webhooks programmatically.

### Available Events

#### Link Events

| Event | Description |
|-------|-------------|
| `link.created` | New link created in workspace |
| `link.updated` | Link updated |
| `link.deleted` | Link deleted |
| `link.clicked` | User clicks a link (**must be scoped to specific links**) |

#### Partner Events

| Event | Description |
|-------|-------------|
| `partner.enrolled` | Partner enrolled in program |
| `partner.application_submitted` | Partner submits application |
| `lead.created` | New lead tracked |
| `sale.created` | New sale tracked |
| `commission.created` | New commission generated |

> **Important:** Bulk operations (bulk create, update, delete) do NOT trigger webhook events.

### Payload Format

All webhooks share this top-level structure:

```json
{
  "id": "evt_abc123",
  "event": "link.clicked",
  "createdAt": "2026-03-25T14:30:00.000Z",
  "data": { "..." }
}
```

#### `link.clicked` Payload

```json
{
  "id": "evt_abc123",
  "event": "link.clicked",
  "createdAt": "2026-03-25T14:30:00.000Z",
  "data": {
    "click": {
      "id": "click_xxx",
      "timestamp": "2026-03-25T14:30:00.000Z",
      "url": "https://landing.paidedge.com/nexus-q1-offer",
      "ip": "203.0.113.1",
      "continent": "NA",
      "country": "US",
      "city": "San Francisco",
      "device": "Desktop",
      "browser": "Chrome",
      "os": "Mac OS",
      "ua": "Mozilla/5.0...",
      "bot": false,
      "qr": false,
      "referer": "https://google.com"
    },
    "link": {
      "id": "clx1234abcdef",
      "domain": "go.nexussecurity.com",
      "key": "cmmc-q1",
      "shortLink": "https://go.nexussecurity.com/cmmc-q1",
      "url": "https://landing.paidedge.com/nexus-q1-offer",
      "externalId": "campaign_abc123",
      "tenantId": "tenant_nexus",
      "clicks": 1234,
      "leads": 56,
      "sales": 12,
      "saleAmount": 4800,
      "utm_source": "email",
      "utm_medium": "campaign",
      "utm_campaign": "cmmc-q1-2026"
    }
  }
}
```

#### `link.created` / `link.updated` / `link.deleted` Payload

`data` contains the full link object directly.

### Webhook Security

- **Signature header:** `Dub-Signature`
- **Algorithm:** HMAC-SHA256
- **Verification:** Compute `HMAC-SHA256(raw_body, signing_secret)` and compare hex digest to header value

#### Python Verification Example

```python
import hashlib
import hmac

def verify_webhook(raw_body: bytes, signature: str, secret: str) -> bool:
    expected = hmac.new(
        secret.encode(), raw_body, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature)
```

#### curl — Set Up Webhook (Dashboard)

Webhooks must be set up via the Dub dashboard. Steps:
1. Go to `https://app.dub.co/webhooks`
2. Click "Create Webhook"
3. Enter your endpoint URL (e.g., `https://api.paidedge.com/webhooks/dub`)
4. Select events (`link.clicked` for attribution)
5. Scope to specific links if using `link.clicked`
6. Copy the signing secret → store as `DUB_WEBHOOK_SECRET`

### Retry Policy (Exponential Backoff)

| Attempt | Delay |
|---------|-------|
| 1 | 12 seconds |
| 2 | ~2.5 minutes |
| 3 | ~30 minutes |
| 4 | ~6 hours |
| 5+ | 24 hours (capped) |

- Requires 2XX response
- Notifications sent to workspace owners at 5, 10, 15 consecutive failures
- **Auto-disabled at 20 consecutive failures** — must re-enable from dashboard

### PaidEdge Click Event Flow

1. User clicks short link (`go.nexussecurity.com/cmmc-q1`)
2. Dub records click with full attribution data
3. Dub redirects user to destination URL
4. `link.clicked` webhook fires to `https://api.paidedge.com/webhooks/dub`
5. PaidEdge backend validates signature, extracts `tenantId` + `externalId`
6. Writes click event to ClickHouse for attribution analytics

---

## 8. Pricing & Limits

### Plan Comparison

| | Free | Pro | Business | Advanced | Enterprise |
|---|---|---|---|---|---|
| **Price** | $0 | $25/mo | $75/mo | $250/mo | Custom |
| **Tracked Events/mo** | 1K | 50K | 250K | 1M | Unlimited |
| **New Links/mo** | 25 | 1K | 10K | 50K | Unlimited |
| **Analytics Retention** | 30 days | 1 year | 3 years | 5 years | Unlimited |
| **Custom Domains** | 3 | 10 | 100 | 250 | Unlimited |
| **Users** | 1 | 3 | 10 | 20 | Unlimited |
| **Tags** | Unlimited | 25 | Unlimited | Unlimited | Unlimited |
| **Folders** | — | 3 | 20 | — | — |
| **API Rate Limit** | 60/min | 600/min | 1,200/min | 3,000/min | Custom |
| **Analytics Rate Limit** | N/A | 2/sec | 4/sec | 8/sec | Custom |
| **Events Endpoint** | No | No | Yes | Yes | Yes |
| **Viewer Role** | No | No | Yes | Yes | Yes |

### Cost Model

- **Flat monthly fee** per workspace — no per-link or per-click charges
- Link creation and tracked events are **independent quotas**
- Yearly billing saves ~17%
- Pro plan includes a **free `.link` domain** for 1 year
- Overage pricing is not publicly documented — requires sales contact

### PaidEdge Recommendation

For a platform creating thousands of links across multiple tenants:
- **Business ($75/mo)** as a starting point: 10K links/mo, 250K events/mo, 100 custom domains, Events endpoint access
- **Advanced ($250/mo)** if you need 50K links/mo or 1M events/mo
- **Enterprise** for unlimited links/events and custom rate limits — contact sales

---

## 9. SDKs & Libraries

### Official Python SDK

**Package:** `dub` on PyPI

```bash
pip install dub
```

Supports both sync and async patterns:

```python
from dub import Dub

client = Dub(token="dub_xxxxxxxx")

# Create link
link = client.links.create(request={
    "url": "https://landing.paidedge.com/nexus-q1-offer",
    "domain": "go.nexussecurity.com",
    "key": "cmmc-q1",
    "externalId": "campaign_abc123",
    "tenantId": "tenant_nexus",
    "tagNames": ["q1-2026", "cmmc"],
    "trackConversion": True,
})

# List links by tenant
links = client.links.list(request={"tenantId": "tenant_nexus"})

# Get analytics
analytics = client.analytics.retrieve(request={
    "external_id": "ext_campaign_abc123",
    "group_by": "timeseries",
    "interval": "30d",
})

# Bulk create
results = client.links.create_many(request=[
    {"url": "https://example.com/a", "tenantId": "tenant_nexus"},
    {"url": "https://example.com/b", "tenantId": "tenant_nexus"},
])

# Update
client.links.update(link_id="ext_campaign_abc123", request_body={
    "url": "https://landing.paidedge.com/nexus-q1-offer-v2"
})

# Delete
client.links.delete(link_id="ext_campaign_abc123")
```

**Full method surface:**
- `client.links` — `create`, `get`, `list`, `count`, `update`, `delete`, `upsert`, `create_many`, `update_many`, `delete_many`
- `client.domains` — `create`, `list`, `update`, `delete`
- `client.tags` — `create`, `list`, `update`, `delete`
- `client.analytics` — `retrieve`
- `client.events` — `list`
- `client.track` — `lead`, `sale`
- `client.qrcodes` — `get`

### Official TypeScript SDK

**Package:** `dub` on npm

```bash
npm install dub
```

```typescript
import { Dub } from "dub";

const dub = new Dub({ token: process.env.DUB_API_KEY });

const link = await dub.links.create({
  url: "https://landing.paidedge.com/nexus-q1-offer",
  externalId: "campaign_abc123",
  tenantId: "tenant_nexus",
});
```

### Other Official SDKs

- **Go:** `github.com/dubinc/dub-go`
- **Ruby:** `dub` gem
- **PHP:** `dub/dub-php`

### Client-Side SDKs (for conversion tracking)

- **Web:** `@dub/analytics` npm package
- **iOS:** `dub-ios` Swift package
- **React Native:** `@dub/react-native` npm package

### For PaidEdge (FastAPI Backend)

Use the **official Python SDK** (`pip install dub`). It wraps the REST API with typed methods. No need for raw httpx calls unless you need fine-grained control over retries or connection pooling.

---

## 10. Integration Patterns for Multi-Tenant SaaS

### Recommended Architecture

**Single dub.co workspace** for all of PaidEdge, using these organization methods:

| Method | Cardinality | PaidEdge Use Case |
|--------|-------------|-------------------|
| `tenantId` | One-to-many | Group all links for a PaidEdge customer |
| `externalId` | One-to-one | Map a Dub link to a PaidEdge campaign/asset record |
| Tags | Many-to-many | Campaign type, quarter, asset type labels |
| Custom domains | Per-tenant | `go.nexussecurity.com`, `links.acmecorp.com` |

### Link Organization by Tenant + Campaign + Asset Type

```python
# Create a campaign link with full organization
link = client.links.create(request={
    "url": "https://landing.paidedge.com/nexus-cmmc-whitepaper",
    "domain": "go.nexussecurity.com",
    "key": "cmmc-wp",
    "tenantId": "tenant_nexus",                    # tenant isolation
    "externalId": "campaign_abc123_asset_wp01",    # maps to your DB
    "tagNames": ["q1-2026", "cmmc", "whitepaper"], # campaign + asset type
    "trackConversion": True,
    "utm_source": "linkedin",
    "utm_medium": "paid-social",
    "utm_campaign": "cmmc-q1-2026",
})
```

### Click Event Flow: Dub → PaidEdge → ClickHouse

```
[User clicks go.nexussecurity.com/cmmc-wp]
        ↓
[Dub records click + redirects user]
        ↓
[Dub fires link.clicked webhook]
        ↓
[PaidEdge webhook endpoint receives event]
  - Validates Dub-Signature (HMAC-SHA256)
  - Extracts: tenantId, externalId, click data (geo, device, referrer, UTM)
        ↓
[PaidEdge writes to ClickHouse]
  - click_id, timestamp, campaign_id, tenant_id
  - country, city, device, browser, os
  - referrer, utm_source, utm_medium, utm_campaign
  - qr_scan (boolean)
        ↓
[PaidEdge attribution dashboard]
  - Joins click data with ad platform metrics
  - Independent attribution per campaign
```

### Custom Domain Per Tenant

Feasible and recommended for branded experiences:

1. Tenant provides their desired subdomain (e.g., `go.nexussecurity.com`)
2. Tenant adds CNAME record: `go.nexussecurity.com` → `cname.dub.co`
3. PaidEdge calls `POST /domains` with `slug: "go.nexussecurity.com"`
4. Dub auto-provisions SSL
5. All campaign links for that tenant use `domain: "go.nexussecurity.com"`

Domain limits: 100 on Business, 250 on Advanced, Unlimited on Enterprise.

### Alternative: Poll Instead of Webhooks

If webhooks are problematic (e.g., `link.clicked` must be scoped per-link), you can poll:

```python
# Poll click events for a tenant (requires Business plan)
events = client.events.list(request={
    "tenantId": "tenant_nexus",
    "event": "clicks",
    "sortBy": "timestamp",
    "sortOrder": "desc",
    "limit": 1000,
})
```

---

## 11. Common Gotchas

### Rate Limits
- Free tier (60 req/min) is insufficient for any production use
- Analytics endpoints have **separate, stricter per-second limits** (2/sec on Pro)
- Use bulk operations (up to 100 links) to conserve rate limit budget

### Webhooks
- **Bulk operations do NOT trigger webhooks** — if you bulk-create links, you won't get `link.created` events
- `link.clicked` webhooks must be **scoped to specific links**, not workspace-wide — this is a significant limitation for a platform with thousands of links
- After **20 consecutive failures**, webhooks auto-disable and must be manually re-enabled
- No API for managing webhooks — dashboard only

### Custom Domains
- **Deleting a domain deletes ALL associated links** — irreversible
- DNS propagation can take up to **24 hours**
- Cloudflare proxy mode causes **inaccurate analytics** (wrong geo, proxy IPs)
- `www.` subdomain is not supported
- Vercel domain ownership transfer warning with TXT record

### Analytics
- **Events endpoint requires Business plan** ($75/mo)
- Analytics retention is tier-limited: 30 days (Free), 1 year (Pro), 3 years (Business)
- `interval: "all"` is constrained by your plan's retention period

### Link Management
- `externalId` must be **unique per workspace** — duplicate returns `409 Conflict`
- Use `ext_` prefix when referencing externalId in update/retrieve/analytics
- Pagination max is **100 items per page** for links, **1,000 for events**
- Free plan limited to **25 new links/month** — unusable for production
- Tag limit on Pro is **25** — may need Business for extensive tagging

### Data Portability
- No documented way to **export or migrate links** between workspaces or out of Dub
- No bulk export endpoint
- Links are essentially locked into the platform once created

### Poorly Documented
- Exact overage pricing — must contact sales
- SSL provisioning details (automatic but no timing/provider docs)
- `registeredDomain` response field suggests Dub can register domains, but docs are sparse
- Webhook payload schemas are incomplete in public docs

---

*Generated for BJC-64. Sources: [dub.co/docs](https://dub.co/docs), [dub.co/pricing](https://dub.co/pricing), [dub.co/docs/api-reference](https://dub.co/docs/api-reference).*
