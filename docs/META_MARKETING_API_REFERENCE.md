# Meta (Facebook) Marketing API — Comprehensive Reference

> **Purpose:** Everything needed to build a Meta/Facebook Ads integration for PaidEdge, a multi-tenant B2B SaaS platform managing paid advertising campaigns on behalf of multiple client organizations.
>
> **API Version:** v25.0 (current as of March 2026)
>
> **Base URL:** `https://graph.facebook.com/v25.0`

---

## Table of Contents

1. [Authentication & OAuth 2.0](#1-authentication--oauth-20)
2. [Account Structure](#2-account-structure)
3. [Custom Audiences](#3-custom-audiences)
4. [Programmatic Ad Creation & Upload](#4-programmatic-ad-creation--upload)
5. [Campaign Management](#5-campaign-management)
6. [Lead Ads](#6-lead-ads)
7. [Conversions API (CAPI)](#7-conversions-api-capi)
8. [Reporting & Insights](#8-reporting--insights)
9. [Rate Limits](#9-rate-limits)
10. [Python SDK](#10-python-sdk)
11. [Common Gotchas](#11-common-gotchas)

---

## 1. Authentication & OAuth 2.0

### App Setup

1. Register at [developers.facebook.com](https://developers.facebook.com)
2. Create a new app → select **Business** type
3. Add the **Marketing API** product
4. Configure Basic Settings: App ID, App Secret, privacy policy URL, terms of service URL
5. Set up OAuth redirect URIs

### Business Manager Requirement

Business Manager is **mandatory** for multi-tenant SaaS platforms. It provides:
- Centralized management of ad accounts, pages, and pixels
- System user creation for server-to-server API calls
- Asset sharing between businesses (your platform ↔ client ad accounts)
- Separation of personal Facebook accounts from business assets

Every client's ad account must be accessible through Business Manager (either owned or shared).

### Token Types

| Token Type | Expiry | Best For |
|------------|--------|----------|
| **Short-lived user token** | ~1-2 hours | Initial OAuth exchange |
| **Long-lived user token** | ~60 days | Extended user sessions |
| **System user token (non-expiring)** | Never | Production server-to-server calls |
| **System user token (expiring)** | 60 days | Security-conscious production use |
| **Page token (from long-lived user token)** | Never | Page-specific operations |

### Recommended for Multi-Tenant SaaS: System User Tokens

For PaidEdge, use **system users** (not user tokens) because:
- They don't expire (or have controlled 60-day expiry)
- They aren't tied to a personal Facebook account
- They survive employee departures
- They can be scoped to specific ad accounts and permissions

**Admin system users** have broader permissions; **regular system users** are scoped to assigned assets.

### OAuth 2.0 Flow (Client Onboarding)

When a PaidEdge client connects their Meta ad account:

**Step 1 — Redirect to Meta authorization:**
```
https://www.facebook.com/v25.0/dialog/oauth?
  client_id={APP_ID}
  &redirect_uri={YOUR_REDIRECT_URI}
  &scope=ads_management,ads_read,business_management,leads_retrieval
  &state={CSRF_TOKEN}
```

**Step 2 — Exchange authorization code for short-lived token:**
```bash
curl -X GET "https://graph.facebook.com/v25.0/oauth/access_token?\
  client_id={APP_ID}&\
  redirect_uri={REDIRECT_URI}&\
  client_secret={APP_SECRET}&\
  code={AUTHORIZATION_CODE}"
```

**Response:**
```json
{
  "access_token": "{SHORT_LIVED_TOKEN}",
  "token_type": "bearer",
  "expires_in": 5183944
}
```

**Step 3 — Exchange for long-lived token:**
```bash
curl -X GET "https://graph.facebook.com/v25.0/oauth/access_token?\
  grant_type=fb_exchange_token&\
  client_id={APP_ID}&\
  client_secret={APP_SECRET}&\
  fb_exchange_token={SHORT_LIVED_TOKEN}"
```

### System User Token Generation

After a client grants access, generate a system user token for ongoing API calls:

**Install app on system user:**
```bash
curl -X POST "https://graph.facebook.com/v25.0/{SYSTEM_USER_ID}/applications" \
  -F "business_app={APP_ID}" \
  -F "access_token={ADMIN_TOKEN}"
```

**Generate system user token:**
```bash
curl -X POST "https://graph.facebook.com/v25.0/{SYSTEM_USER_ID}/access_tokens" \
  -F "business_app={APP_ID}" \
  -F "appsecret_proof={HMAC_SHA256_HASH}" \
  -F "scope=ads_management,ads_read,business_management,leads_retrieval" \
  -F "set_token_expires_in_60_days=true" \
  -F "access_token={ADMIN_TOKEN}"
```

**Response:**
```json
{
  "access_token": "{SYSTEM_USER_TOKEN}"
}
```

The `appsecret_proof` is computed as:
```python
import hmac, hashlib
appsecret_proof = hmac.new(
    app_secret.encode('utf-8'),
    access_token.encode('utf-8'),
    hashlib.sha256
).hexdigest()
```

### Token Refresh (Expiring System User Tokens)

```bash
curl -X GET "https://graph.facebook.com/v25.0/oauth/access_token?\
  grant_type=fb_exchange_token&\
  client_id={APP_ID}&\
  client_secret={APP_SECRET}&\
  set_token_expires_in_60_days=true&\
  fb_exchange_token={CURRENT_TOKEN}"
```

**Important:** The old token remains valid until its original expiry. Revoke it after deploying the new one:

```bash
curl -X GET "https://graph.facebook.com/v25.0/oauth/revoke?\
  client_id={APP_ID}&\
  client_secret={APP_SECRET}&\
  revoke_token={OLD_TOKEN}&\
  access_token={ADMIN_TOKEN}"
```

### Per-Tenant Token Management

For a multi-tenant platform like PaidEdge:

1. **Store per-organization:** Each client org gets its own system user token stored in your `provider_configs` table (encrypted `config` JSONB field)
2. **Track expiry:** Store `token_expires_at` alongside the token; schedule refresh jobs 7+ days before expiry
3. **Scope minimally:** Request only the permissions each client needs
4. **Monitor validity:** Check token debug endpoint periodically:
   ```bash
   curl "https://graph.facebook.com/debug_token?\
     input_token={CLIENT_TOKEN}&\
     access_token={APP_ID}|{APP_SECRET}"
   ```

### App Review & Required Permissions

| Permission | Purpose | Triggers Review |
|------------|---------|-----------------|
| `ads_management` | Create/edit campaigns, ad sets, ads | Yes |
| `ads_read` | Read campaign data and insights | Yes |
| `business_management` | Manage Business Manager assets | Yes |
| `leads_retrieval` | Read lead form submissions | Yes |
| `pages_manage_metadata` | Subscribe to lead webhooks | Yes |
| `pages_read_engagement` | Read page engagement data | Yes |
| `pages_show_list` | List pages user manages | Yes |

**App review process:**
- Submit via App Dashboard → App Review → Permissions and Features
- Provide detailed use case descriptions, screencasts demonstrating the flow
- Timeline: typically 5-10 business days
- Common rejection reasons: insufficient screencasts, vague use case descriptions, requesting unnecessary permissions
- Apps must have a valid privacy policy and terms of service

---

## 2. Account Structure

### Hierarchy

```
Business Manager (your platform)
├── System Users (for API calls)
├── Apps (your PaidEdge app)
└── Client Access
    └── Client's Business Manager
        └── Ad Account (act_XXXXX)
            ├── Campaign (objective-level)
            │   ├── Ad Set (targeting, budget, schedule)
            │   │   ├── Ad (creative + placement)
            │   │   └── Ad
            │   └── Ad Set
            └── Campaign
```

### Business Manager → Ad Account Relationship

- A Business Manager can **own** ad accounts (created within it) or have **agency access** to client-owned accounts
- For SaaS platforms: clients share their ad account with your Business Manager via agency access
- Each ad account has a unique ID prefixed with `act_` (e.g., `act_123456789`)

### Client Onboarding Flow for SaaS

1. Client authorizes your app via OAuth (Section 1)
2. Your app requests agency access to their ad account
3. Client approves the access request in their Business Manager
4. You generate a system user token scoped to their ad account
5. Store the token + ad account ID in your provider config

### Managing Ad Accounts via API

**List owned ad accounts:**
```bash
curl -X GET "https://graph.facebook.com/v25.0/{BUSINESS_ID}/owned_ad_accounts?\
  access_token={TOKEN}"
```

**Claim an existing ad account:**
```bash
curl -X POST "https://graph.facebook.com/v25.0/{BUSINESS_ID}/owned_ad_accounts" \
  -F "adaccount_id=act_123456" \
  -F "access_token={TOKEN}"
```

Response: `access_status: CONFIRMED` (if you're admin) or `access_status: PENDING` (requires approval).

**Create a new ad account** (limited to 5 via API):
```bash
curl -X POST "https://graph.facebook.com/v25.0/{BUSINESS_ID}/adaccount" \
  -F "name=Client X Ad Account" \
  -F "currency=USD" \
  -F "timezone_id=1" \
  -F "end_advertiser={CLIENT_BUSINESS_ID}" \
  -F "media_agency=NONE" \
  -F "partner=NONE" \
  -F "access_token={TOKEN}"
```

**Remove ad account access:**
```bash
curl -X DELETE "https://graph.facebook.com/v25.0/{BUSINESS_ID}/ad_accounts" \
  -F "adaccount_id=act_123456" \
  -F "access_token={TOKEN}"
```

Note: Cannot remove `CONFIRMED` owner accounts — only `PENDING` or `AGENCY` access.

### User Permissions on Ad Accounts

| Role | Tasks Array | Capabilities |
|------|-------------|--------------|
| Reporting Only | `['ANALYZE']` | View performance data |
| General User | `['ADVERTISE', 'ANALYZE']` | Create/edit ads + view data |
| Admin | `['MANAGE', 'ADVERTISE', 'ANALYZE']` | Full control including permissions |

**Assign user to ad account:**
```bash
curl -X POST "https://graph.facebook.com/v25.0/act_{AD_ACCOUNT_ID}/assigned_users" \
  -F "user={BUSINESS_SCOPED_USER_ID}" \
  -F "tasks=['MANAGE', 'ADVERTISE', 'ANALYZE']" \
  -F "access_token={TOKEN}"
```

**List users on an ad account:**
```bash
curl -X GET "https://graph.facebook.com/v25.0/act_{AD_ACCOUNT_ID}/assigned_users?\
  access_token={TOKEN}"
```

**Remove user from ad account:**
```bash
curl -X DELETE "https://graph.facebook.com/v25.0/act_{AD_ACCOUNT_ID}/assigned_users" \
  -F "user={BUSINESS_SCOPED_USER_ID}" \
  -F "access_token={TOKEN}"
```

---

## 3. Custom Audiences

### Overview

Custom audiences let you target specific groups of people based on customer data, website visitors, app activity, or engagement. Key limits per ad account:

| Audience Type | Max Count |
|---------------|-----------|
| Customer File (Standard) | 500 |
| Website Custom Audiences | 10,000 |
| Mobile App Audiences | 200 |
| Lookalike Audiences | 500 |

### Creating a Customer List Audience

**Step 1 — Create empty audience:**
```bash
curl -X POST "https://graph.facebook.com/v25.0/act_{AD_ACCOUNT_ID}/customaudiences" \
  -F "name=PaidEdge - Client X Email List" \
  -F "subtype=CUSTOM" \
  -F "customer_file_source=USER_PROVIDED_ONLY" \
  -F "access_token={TOKEN}"
```

`customer_file_source` options: `USER_PROVIDED_ONLY`, `PARTNER_PROVIDED_ONLY`, `BOTH_USER_AND_PARTNER_PROVIDED`

**Step 2 — Add users (max 10,000 per request):**
```bash
curl -X POST "https://graph.facebook.com/v25.0/{AUDIENCE_ID}/users" \
  -F 'payload={
    "schema": ["EMAIL", "FN", "LN"],
    "data": [
      ["f1904cf1a9d73a55fa5de0ac823c4403ded71afd4c3248d00bdcd0866552bb79", "abc123...", "def456..."],
      ["a2b3c4d5...", "ghi789...", "jkl012..."]
    ]
  }' \
  -F 'session={
    "session_id": 12345,
    "batch_seq": 1,
    "last_batch_flag": true,
    "estimated_num_total": 2
  }' \
  -F "access_token={TOKEN}"
```

**Response:**
```json
{
  "audience_id": "12345678",
  "session_id": 12345,
  "num_received": 2,
  "num_invalid_entries": 0,
  "invalid_entry_samples": []
}
```

### Hashing Requirements (SHA-256)

All PII fields must be **normalized then hashed with SHA-256** (hex format, lowercase a-f).

| Field | Schema Key | Normalization Rules |
|-------|-----------|---------------------|
| Email | `EMAIL` | Trim whitespace, lowercase |
| Phone | `PHONE` | Remove symbols/letters, prefix country code |
| First Name | `FN` | Lowercase, a-z only, no punctuation (UTF-8 special chars OK) |
| Last Name | `LN` | Lowercase, a-z only, no punctuation |
| First Initial | `FI` | Single char, lowercase |
| Gender | `GEN` | `m` or `f` |
| Date of Birth (Year) | `DOBY` | `YYYY` format |
| Date of Birth (Month) | `DOBM` | `MM` format |
| Date of Birth (Day) | `DOBD` | `DD` format |
| City | `CT` | Lowercase, no special chars |
| State | `ST` | 2-char abbreviation, lowercase |
| Zip Code | `ZIP` | Lowercase, no whitespace (5 digits for US) |
| Country | `COUNTRY` | ISO 3166-1 alpha-2, lowercase |

**Do NOT hash:**
- `MADID` (mobile advertiser ID) — send raw
- `EXTERN_ID` (custom identifiers like loyalty IDs) — send raw

**Python hashing example:**
```python
import hashlib

def hash_for_meta(value: str) -> str:
    """Normalize and SHA-256 hash a value for Meta Custom Audiences."""
    normalized = value.strip().lower()
    return hashlib.sha256(normalized.encode('utf-8')).hexdigest()

# Example
hash_for_meta("mary@example.com")
# → "f1904cf1a9d73a55fa5de0ac823c4403ded71afd4c3248d00bdcd0866552bb79"
```

### Multi-Key Matching

Provide multiple identifiers per user for better match rates:
```json
{
  "schema": ["EMAIL", "FN", "LN", "PHONE", "COUNTRY"],
  "data": [
    ["<hashed_email>", "<hashed_fn>", "<hashed_ln>", "<hashed_phone>", "<hashed_country>"]
  ]
}
```

### Website Custom Audiences (Pixel-Based)

```bash
curl -X POST "https://graph.facebook.com/v25.0/act_{AD_ACCOUNT_ID}/customaudiences" \
  -F "name=Website Visitors - Last 30 Days" \
  -F "subtype=WEBSITE" \
  -F 'rule={"inclusions":{"operator":"or","rules":[{"event_sources":[{"id":"{PIXEL_ID}","type":"pixel"}],"retention_seconds":2592000}]}}' \
  -F "access_token={TOKEN}"
```

### Lookalike Audiences

**From custom audience seed:**
```bash
curl -X POST "https://graph.facebook.com/v25.0/act_{AD_ACCOUNT_ID}/customaudiences" \
  -F "name=Lookalike - US 1%" \
  -F "subtype=LOOKALIKE" \
  -F "origin_audience_id={SEED_AUDIENCE_ID}" \
  -F 'lookalike_spec={"type":"similarity","country":"US","ratio":0.01}' \
  -F "access_token={TOKEN}"
```

**Key parameters:**
- `ratio`: `0.01` to `0.20` (1% to 20% of country population) — in 1% increments
- `type`: `similarity` (top 1%, most precise) or `reach` (top 5%, broader)
- Source audience must have **at least 100 people**
- Takes **1-6 hours** to populate; you can target it immediately though

**Multi-country lookalike:**
```bash
curl -X POST "https://graph.facebook.com/v25.0/act_{AD_ACCOUNT_ID}/customaudiences" \
  -F "subtype=LOOKALIKE" \
  -F "origin_audience_id={SEED_AUDIENCE_ID}" \
  -F 'lookalike_spec={
    "ratio": 0.02,
    "location_spec": {
      "geo_locations": {
        "countries": ["US", "CA", "GB"]
      }
    }
  }' \
  -F "access_token={TOKEN}"
```

**From campaign conversions:**
```bash
curl -X POST "https://graph.facebook.com/v25.0/act_{AD_ACCOUNT_ID}/customaudiences" \
  -F "subtype=LOOKALIKE" \
  -F 'lookalike_spec={
    "origin_ids": ["{CAMPAIGN_ID}"],
    "ratio": 0.05,
    "conversion_type": "campaign_conversions",
    "country": "US"
  }' \
  -F "access_token={TOKEN}"
```

Requires at least **100 unique conversions** (200+ recommended).

### Update/Refresh Audiences

**Add users:**
```bash
curl -X POST "https://graph.facebook.com/v25.0/{AUDIENCE_ID}/users" \
  -F 'payload={"schema":"EMAIL","data":[["<hashed_email>"]]}' \
  -F 'session={"session_id":123,"batch_seq":1,"last_batch_flag":true}' \
  -F "access_token={TOKEN}"
```

**Remove users:**
```bash
curl -X DELETE "https://graph.facebook.com/v25.0/{AUDIENCE_ID}/users" \
  -F 'payload={"schema":"EMAIL","data":[["<hashed_email>"]]}' \
  -F "access_token={TOKEN}"
```

**Replace entire audience** (without resetting learning phase):
```bash
curl -X POST "https://graph.facebook.com/v25.0/{AUDIENCE_ID}/usersreplace" \
  -F 'payload={"schema":"EMAIL","data":[...]}' \
  -F 'session={"session_id":456,"batch_seq":1,"last_batch_flag":true}' \
  -F "access_token={TOKEN}"
```

Requirements for replace: audience under 100M users, subtype `CUSTOM`, complete within 90-minute window.

### Delete Audience

```bash
curl -X DELETE "https://graph.facebook.com/v25.0/{AUDIENCE_ID}?\
  access_token={TOKEN}"
```

### Retention & Expiry

- **EXTERN_ID data:** 90-day retention
- **Inactive audiences:** Unused in ad sets for 2+ years → flagged for deletion (90-day countdown)
- **Lookalike audiences:** Refreshed every 3 days if used in active ad sets; inactive 90+ days → `approximate_count` returns `-1`
- **Audience integrity:** As of Sept 2, 2025, audiences suggesting restricted topics (health, financial status) are flagged with `operation_status: 471` and blocked from campaigns

### Limited Data Use (CCPA)

For California residents, include in your user data upload:
```json
{
  "data_processing_options": ["LDU"],
  "data_processing_options_country": 1,
  "data_processing_options_state": 1000
}
```

---

## 4. Programmatic Ad Creation & Upload

### Ad Creation Flow

```
1. Upload media (image/video) → get hash/ID
2. Create Ad Creative → get creative_id
3. Create Ad (links creative to ad set) → get ad_id
```

### Image Upload

**Endpoint:** `POST /act_{AD_ACCOUNT_ID}/adimages`

```bash
# Upload via base64
curl -X POST "https://graph.facebook.com/v25.0/act_{AD_ACCOUNT_ID}/adimages" \
  -F "bytes={BASE64_ENCODED_IMAGE}" \
  -F "access_token={TOKEN}"

# Upload via file
curl -X POST "https://graph.facebook.com/v25.0/act_{AD_ACCOUNT_ID}/adimages" \
  -F "filename=@/path/to/image.jpg" \
  -F "access_token={TOKEN}"
```

**Response:**
```json
{
  "images": {
    "image.jpg": {
      "hash": "02bee5277ec507b6fd0f9b9ff2f22d9c",
      "url": "https://scontent.xx.fbcdn.net/...",
      "width": 1200,
      "height": 628
    }
  }
}
```

**Copy image between ad accounts:**
```bash
curl -X POST "https://graph.facebook.com/v25.0/act_{DEST_ACCOUNT_ID}/adimages" \
  -F 'copy_from={"source_account_id":"{SOURCE_ACCOUNT_ID}","hash":"02bee5277ec507b6fd0f9b9ff2f22d9c"}' \
  -F "access_token={TOKEN}"
```

### Video Upload

**Endpoint:** `POST /act_{AD_ACCOUNT_ID}/advideos`

```bash
# Simple upload (< 1GB)
curl -X POST "https://graph.facebook.com/v25.0/act_{AD_ACCOUNT_ID}/advideos" \
  -F "source=@/path/to/video.mp4" \
  -F "title=My Ad Video" \
  -F "access_token={TOKEN}"
```

**Chunked upload for large files:**
```bash
# Step 1: Start upload session
curl -X POST "https://graph.facebook.com/v25.0/act_{AD_ACCOUNT_ID}/advideos" \
  -F "upload_phase=start" \
  -F "file_size={SIZE_IN_BYTES}" \
  -F "access_token={TOKEN}"
# Returns: upload_session_id, video_id, start_offset, end_offset

# Step 2: Upload chunks
curl -X POST "https://graph.facebook.com/v25.0/act_{AD_ACCOUNT_ID}/advideos" \
  -F "upload_phase=transfer" \
  -F "upload_session_id={SESSION_ID}" \
  -F "start_offset={START}" \
  -F "video_file_chunk=@chunk.mp4" \
  -F "access_token={TOKEN}"

# Step 3: Finish upload
curl -X POST "https://graph.facebook.com/v25.0/act_{AD_ACCOUNT_ID}/advideos" \
  -F "upload_phase=finish" \
  -F "upload_session_id={SESSION_ID}" \
  -F "title=My Video" \
  -F "access_token={TOKEN}"
```

### Ad Format Specs

#### Single Image Ads

| Spec | Requirement |
|------|-------------|
| **Recommended dimensions** | 1080 x 1080 px (1:1) or 1200 x 628 px (1.91:1) |
| **Minimum width** | 600 px |
| **Aspect ratios** | 1:1 (square), 1.91:1 (landscape), 4:5 (vertical — FB/IG feed) |
| **Aspect ratio tolerance** | 3% |
| **Max file size** | 30 MB |
| **File formats** | JPG, PNG |
| **Primary text** | 125 characters recommended (up to 150 visible) |
| **Headline** | 27 characters recommended (up to 40) |
| **Description** | 27 characters recommended |
| **CTA button** | See CTA options below |

#### Video Ads

| Spec | Requirement |
|------|-------------|
| **Resolution** | 1080 x 1080 px minimum recommended |
| **Duration** | 1 second to 241 minutes (Feed); 1-120 seconds (Stories/Reels) |
| **Max file size** | 4 GB |
| **File formats** | MP4, MOV (H.264 codec recommended) |
| **Aspect ratios** | 1:1, 4:5, 16:9, 9:16 (Stories/Reels) |
| **Thumbnail** | Auto-generated or custom upload; same aspect ratio as video |
| **Captions** | SRT file upload supported |

#### Carousel Ads

| Spec | Requirement |
|------|-------------|
| **Cards** | 2-10 cards per carousel |
| **Image per card** | 1080 x 1080 px recommended (1:1 aspect ratio) |
| **Max image size** | 30 MB per card |
| **Video per card** | Up to 240 minutes; 4 GB max |
| **Headline per card** | 32 characters recommended |
| **Description per card** | 18 characters recommended |
| **Link per card** | Each card can have its own destination URL |

#### Collection Ads

- Cover image or video + product set from catalog
- Requires a **product catalog** connected to the ad account
- Opens an **Instant Experience** (full-screen mobile) on tap
- Cover: same specs as single image/video
- Product set: minimum 4 products

#### CTA Button Options

`APPLY_NOW`, `BOOK_TRAVEL`, `CALL_NOW`, `CONTACT_US`, `DOWNLOAD`, `GET_DIRECTIONS`, `GET_OFFER`, `GET_QUOTE`, `INSTALL_MOBILE_APP`, `LEARN_MORE`, `LIKE_PAGE`, `LISTEN_MUSIC`, `MESSAGE_PAGE`, `NO_BUTTON`, `OPEN_LINK`, `ORDER_NOW`, `PLAY_GAME`, `SHOP_NOW`, `SIGN_UP`, `SUBSCRIBE`, `WATCH_MORE`, `WHATSAPP_MESSAGE`

### Creating Ad Creatives

**Endpoint:** `POST /act_{AD_ACCOUNT_ID}/adcreatives`

**Single image ad creative:**
```bash
curl -X POST "https://graph.facebook.com/v25.0/act_{AD_ACCOUNT_ID}/adcreatives" \
  -F "name=Image Ad Creative" \
  -F 'object_story_spec={
    "page_id": "{PAGE_ID}",
    "link_data": {
      "image_hash": "{IMAGE_HASH}",
      "link": "https://yoursite.com/landing",
      "message": "Check out our latest B2B solution",
      "name": "Headline Text",
      "description": "Link description",
      "call_to_action": {
        "type": "LEARN_MORE",
        "value": {"link": "https://yoursite.com/landing"}
      }
    }
  }' \
  -F "access_token={TOKEN}"
```

**Video ad creative:**
```bash
curl -X POST "https://graph.facebook.com/v25.0/act_{AD_ACCOUNT_ID}/adcreatives" \
  -F "name=Video Ad Creative" \
  -F 'object_story_spec={
    "page_id": "{PAGE_ID}",
    "video_data": {
      "video_id": "{VIDEO_ID}",
      "image_hash": "{THUMBNAIL_HASH}",
      "title": "Video Headline",
      "message": "Primary text for the video ad",
      "call_to_action": {
        "type": "SIGN_UP",
        "value": {"link": "https://yoursite.com/signup"}
      }
    }
  }' \
  -F "access_token={TOKEN}"
```

**Carousel ad creative:**
```bash
curl -X POST "https://graph.facebook.com/v25.0/act_{AD_ACCOUNT_ID}/adcreatives" \
  -F "name=Carousel Ad Creative" \
  -F 'object_story_spec={
    "page_id": "{PAGE_ID}",
    "link_data": {
      "message": "Explore our product suite",
      "link": "https://yoursite.com",
      "child_attachments": [
        {
          "link": "https://yoursite.com/product-1",
          "image_hash": "{IMAGE_HASH_1}",
          "name": "Product 1",
          "description": "Description 1",
          "call_to_action": {"type": "LEARN_MORE", "value": {"link": "https://yoursite.com/product-1"}}
        },
        {
          "link": "https://yoursite.com/product-2",
          "image_hash": "{IMAGE_HASH_2}",
          "name": "Product 2",
          "description": "Description 2",
          "call_to_action": {"type": "LEARN_MORE", "value": {"link": "https://yoursite.com/product-2"}}
        }
      ]
    }
  }' \
  -F "access_token={TOKEN}"
```

### Creating the Ad

**Endpoint:** `POST /act_{AD_ACCOUNT_ID}/ads`

```bash
curl -X POST "https://graph.facebook.com/v25.0/act_{AD_ACCOUNT_ID}/ads" \
  -F "name=My Ad" \
  -F "adset_id={AD_SET_ID}" \
  -F "creative={\"creative_id\":\"{CREATIVE_ID}\"}" \
  -F "status=PAUSED" \
  -F "access_token={TOKEN}"
```

**Python:**
```python
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adaccount import AdAccount

account = AdAccount('act_{AD_ACCOUNT_ID}')
ad = account.create_ad(params={
    'name': 'My Ad',
    'adset_id': '{AD_SET_ID}',
    'creative': {'creative_id': '{CREATIVE_ID}'},
    'status': 'PAUSED',
})
print(ad['id'])
```

---

## 5. Campaign Management

### Creating Campaigns

**Endpoint:** `POST /act_{AD_ACCOUNT_ID}/campaigns`

```bash
curl -X POST "https://graph.facebook.com/v25.0/act_{AD_ACCOUNT_ID}/campaigns" \
  -F "name=Q1 2026 - Lead Gen Campaign" \
  -F "objective=OUTCOME_LEADS" \
  -F "status=PAUSED" \
  -F "special_ad_categories=[]" \
  -F "access_token={TOKEN}"
```

**Python:**
```python
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adaccount import AdAccount

account = AdAccount('act_{AD_ACCOUNT_ID}')
campaign = account.create_campaign(params={
    'name': 'Q1 2026 - Lead Gen Campaign',
    'objective': 'OUTCOME_LEADS',
    'status': Campaign.Status.paused,
    'special_ad_categories': [],
})
print(campaign['id'])
```

### Campaign Objectives

| Objective | Purpose |
|-----------|---------|
| `OUTCOME_AWARENESS` | Brand awareness and reach |
| `OUTCOME_TRAFFIC` | Drive traffic to website/app |
| `OUTCOME_ENGAGEMENT` | Post engagement, page likes, event responses |
| `OUTCOME_LEADS` | Lead generation forms and conversions |
| `OUTCOME_APP_PROMOTION` | App installs and engagement |
| `OUTCOME_SALES` | Conversions, catalog sales, store traffic |

> **Note:** Legacy objectives like `LINK_CLICKS`, `CONVERSIONS`, `LEAD_GENERATION` still work but `OUTCOME_*` objectives are the current standard.

### Campaign Budget Optimization (CBO)

CBO sets the budget at the campaign level and distributes it across ad sets automatically.

```bash
curl -X POST "https://graph.facebook.com/v25.0/act_{AD_ACCOUNT_ID}/campaigns" \
  -F "name=CBO Campaign" \
  -F "objective=OUTCOME_LEADS" \
  -F "status=PAUSED" \
  -F "daily_budget=5000" \
  -F "bid_strategy=LOWEST_COST_WITHOUT_CAP" \
  -F "access_token={TOKEN}"
```

**When to use CBO vs ad set budgets:**
- **CBO:** When you want Meta to optimize spend across ad sets; best for testing multiple audiences
- **Ad set budgets:** When you need precise control over spend per audience segment

### Creating Ad Sets

**Endpoint:** `POST /act_{AD_ACCOUNT_ID}/adsets`

```bash
curl -X POST "https://graph.facebook.com/v25.0/act_{AD_ACCOUNT_ID}/adsets" \
  -F "name=US Decision Makers - Lead Gen" \
  -F "campaign_id={CAMPAIGN_ID}" \
  -F "daily_budget=2000" \
  -F "billing_event=IMPRESSIONS" \
  -F "optimization_goal=LEAD_GENERATION" \
  -F "bid_strategy=LOWEST_COST_WITHOUT_CAP" \
  -F "start_time=2026-04-01T00:00:00-0700" \
  -F "end_time=2026-04-30T23:59:59-0700" \
  -F 'targeting={
    "geo_locations": {"countries": ["US"]},
    "age_min": 25,
    "age_max": 55,
    "genders": [0],
    "publisher_platforms": ["facebook", "instagram"],
    "facebook_positions": ["feed", "right_hand_column"],
    "instagram_positions": ["stream", "story", "reels"],
    "custom_audiences": [{"id": "{CUSTOM_AUDIENCE_ID}"}],
    "flexible_spec": [
      {
        "interests": [{"id": "6003139266461", "name": "Marketing"}],
        "behaviors": [{"id": "6002714895372", "name": "Small business owners"}]
      }
    ],
    "targeting_expansion": {"expansion": true}
  }' \
  -F "status=PAUSED" \
  -F "access_token={TOKEN}"
```

**Python:**
```python
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.targeting import Targeting

account = AdAccount('act_{AD_ACCOUNT_ID}')
adset = account.create_ad_set(params={
    'name': 'US Decision Makers - Lead Gen',
    'campaign_id': '{CAMPAIGN_ID}',
    'daily_budget': 2000,  # in cents
    'billing_event': AdSet.BillingEvent.impressions,
    'optimization_goal': AdSet.OptimizationGoal.lead_generation,
    'bid_strategy': AdSet.BidStrategy.lowest_cost_without_cap,
    'start_time': '2026-04-01T00:00:00-0700',
    'end_time': '2026-04-30T23:59:59-0700',
    'targeting': {
        'geo_locations': {'countries': ['US']},
        'age_min': 25,
        'age_max': 55,
        'custom_audiences': [{'id': '{CUSTOM_AUDIENCE_ID}'}],
        'flexible_spec': [
            {'interests': [{'id': '6003139266461', 'name': 'Marketing'}]}
        ],
    },
    'status': AdSet.Status.paused,
})
```

### Targeting Options

**Demographics:**
- `age_min` / `age_max`: 18-65+
- `genders`: `[0]` (all), `[1]` (male), `[2]` (female)
- `locales`: Language codes (e.g., `[6]` for English US)

**Location:**
- `countries`: Array of ISO country codes
- `regions`: State/province targeting
- `cities`: City-level with radius
- `zips`: Zip/postal code targeting
- `geo_markets`: DMA targeting (US)

**Interests & Behaviors:**
- Use `flexible_spec` for OR logic within groups and AND logic between groups
- `exclusions` to exclude specific targeting
- `targeting_expansion`: Set `true` for Advantage Detailed Targeting (lets Meta expand beyond your spec)

**Custom/Lookalike Audiences:**
- `custom_audiences`: Array of `{"id": "AUDIENCE_ID"}`
- `excluded_custom_audiences`: Array to exclude

### Placement Options

```json
{
  "publisher_platforms": ["facebook", "instagram", "audience_network", "messenger"],
  "facebook_positions": ["feed", "right_hand_column", "instant_article", "marketplace", "video_feeds", "story", "search", "reels"],
  "instagram_positions": ["stream", "story", "explore", "reels", "profile_feed", "search"],
  "audience_network_positions": ["classic", "rewarded_video"],
  "messenger_positions": ["messenger_home", "sponsored_messages", "story"]
}
```

Omit placement fields entirely for **Advantage+ placements** (Meta auto-optimizes).

### Bid Strategies

| Strategy | Description | When to Use |
|----------|-------------|-------------|
| `LOWEST_COST_WITHOUT_CAP` | Get most results for budget | Default; best for most campaigns |
| `COST_CAP` | Target a specific cost per result | Maintain profitability targets |
| `BID_CAP` | Set max bid per auction | Strict cost control |
| `MINIMUM_ROAS` | Target minimum return on ad spend | E-commerce / value optimization |

### Scheduling

**Daily budget + continuous:**
```json
{"daily_budget": 2000, "start_time": "2026-04-01T00:00:00-0700"}
```

**Lifetime budget + date range:**
```json
{"lifetime_budget": 50000, "start_time": "2026-04-01", "end_time": "2026-04-30"}
```

**Dayparting** (requires lifetime budget):
```json
{
  "lifetime_budget": 50000,
  "pacing_type": ["day_parting"],
  "adset_schedule": [
    {"start_minute": 480, "end_minute": 1020, "days": [1,2,3,4,5], "timezone_type": "USER"}
  ]
}
```

### Campaign Status Lifecycle

```
PAUSED → ACTIVE → PAUSED (toggle)
ACTIVE → ARCHIVED (soft delete, keeps data)
ARCHIVED → ACTIVE (can reactivate)
DELETED (permanent, via DELETE endpoint)
```

**Update status:**
```bash
curl -X POST "https://graph.facebook.com/v25.0/{CAMPAIGN_ID}" \
  -F "status=ACTIVE" \
  -F "access_token={TOKEN}"
```

**Delivery status** (read-only, reflects actual ad delivery):
`ACTIVE`, `INACTIVE`, `PENDING_REVIEW`, `DISAPPROVED`, `CAMPAIGN_PAUSED`, `ADSET_PAUSED`, `NOT_DELIVERING`, `ACCOUNT_DISABLED`, etc.

---

## 6. Lead Ads

### Create Lead Gen Form

**Endpoint:** `POST /{PAGE_ID}/leadgen_forms`

```bash
curl -X POST "https://graph.facebook.com/v25.0/{PAGE_ID}/leadgen_forms" \
  -F "name=PaidEdge Demo Request Form" \
  -F 'questions=[
    {"type": "FULL_NAME"},
    {"type": "EMAIL"},
    {"type": "PHONE"},
    {"type": "COMPANY_NAME"},
    {"type": "JOB_TITLE"},
    {"type": "CUSTOM", "key": "company_size", "label": "Company Size", "options": [
      {"value": "1-50", "key": "1-50"},
      {"value": "51-200", "key": "51-200"},
      {"value": "201-1000", "key": "201-1000"},
      {"value": "1000+", "key": "1000+"}
    ]}
  ]' \
  -F 'privacy_policy={"url": "https://yoursite.com/privacy"}' \
  -F "is_optimized_for_quality=true" \
  -F 'tracking_parameters={"utm_source": "meta", "utm_medium": "lead_ad"}' \
  -F "access_token={TOKEN}"
```

### Standard Field Types

`FULL_NAME`, `FIRST_NAME`, `LAST_NAME`, `EMAIL`, `PHONE`, `COMPANY_NAME`, `JOB_TITLE`, `WORK_EMAIL`, `WORK_PHONE_NUMBER`, `CITY`, `STATE`, `ZIP`, `COUNTRY`, `POST_CODE`, `DATE_OF_BIRTH`, `GENDER`, `MILITARY_STATUS`, `RELATIONSHIP_STATUS`, `MARITAL_STATUS`

### Custom Questions

```json
{
  "type": "CUSTOM",
  "key": "budget_range",
  "label": "What is your monthly ad budget?",
  "options": [
    {"value": "Under $5k", "key": "under_5k"},
    {"value": "$5k-$25k", "key": "5k_25k"},
    {"value": "$25k+", "key": "over_25k"}
  ]
}
```

### Advanced Field Types

- **Appointment scheduling:** `type: "DATE_TIME"`
- **Store locator:** `type: "STORE_LOOKUP"` (requires Store Pages setup)
- **National ID:** Country-specific types (e.g., `ID_AR_DNI`, `ID_CPF`)

### Quality Optimization

Set `is_optimized_for_quality: true` to add a review/confirmation step before submission — reduces low-quality leads.

### Link Form to Ad Creative

```bash
curl -X POST "https://graph.facebook.com/v25.0/act_{AD_ACCOUNT_ID}/adcreatives" \
  -F "name=Lead Ad Creative" \
  -F 'object_story_spec={
    "page_id": "{PAGE_ID}",
    "link_data": {
      "message": "Get a free demo of our platform",
      "link": "https://fb.me/",
      "call_to_action": {
        "type": "SIGN_UP",
        "value": {"lead_gen_form_id": "{FORM_ID}"}
      }
    }
  }' \
  -F "access_token={TOKEN}"
```

> **Note:** The `link` field value must be `https://fb.me/` for lead ad creatives.

### Retrieving Lead Submissions

**By form:**
```bash
curl -X GET "https://graph.facebook.com/v25.0/{FORM_ID}/leads?\
  fields=created_time,id,ad_id,form_id,field_data&\
  access_token={TOKEN}"
```

**By ad:**
```bash
curl -X GET "https://graph.facebook.com/v25.0/{AD_ID}/leads?\
  access_token={TOKEN}"
```

**Individual lead:**
```bash
curl -X GET "https://graph.facebook.com/v25.0/{LEAD_ID}?\
  access_token={TOKEN}"
```

**Response format:**
```json
{
  "data": [
    {
      "created_time": "2026-03-20T08:49:14+0000",
      "id": "12345678",
      "ad_id": "87654321",
      "form_id": "11111111",
      "field_data": [
        {"name": "full_name", "values": ["Jane Smith"]},
        {"name": "email", "values": ["jane@company.com"]},
        {"name": "company_name", "values": ["Acme Corp"]},
        {"name": "job_title", "values": ["VP Marketing"]}
      ]
    }
  ]
}
```

**Filter by time:**
```bash
curl -X GET "https://graph.facebook.com/v25.0/{FORM_ID}/leads?\
  filtering=[{\"field\":\"time_created\",\"operator\":\"GREATER_THAN\",\"value\":1711000000}]&\
  access_token={TOKEN}"
```

**CSV export:**
```
GET https://www.facebook.com/ads/lead_gen/export_csv/?id={FORM_ID}&type=form&from_date={UNIX}&to_date={UNIX}
```

### Webhook Setup for Real-Time Leads

**Step 1 — Subscribe your app to the Page's leadgen field:**
```bash
curl -X POST "https://graph.facebook.com/v25.0/{PAGE_ID}/subscribed_apps" \
  -F "subscribed_fields=leadgen" \
  -F "access_token={PAGE_ACCESS_TOKEN}"
```

**Step 2 — Your webhook endpoint receives:**
```json
{
  "object": "page",
  "entry": [
    {
      "id": "153125381133",
      "time": 1438292065,
      "changes": [
        {
          "field": "leadgen",
          "value": {
            "leadgen_id": 123123123123,
            "page_id": 123123123,
            "form_id": 12312312312,
            "adgroup_id": 12312312312,
            "ad_id": 12312312312,
            "created_time": 1440120384
          }
        }
      ]
    }
  ]
}
```

**Step 3 — Use `leadgen_id` to fetch full lead data** via the retrieval endpoint above.

**Required permissions for webhooks:** `leads_retrieval`, `pages_manage_metadata`, `pages_show_list`, `pages_read_engagement`, `ads_management`

### Webhook vs Batch Retrieval

| Approach | Pros | Cons |
|----------|------|------|
| **Webhooks** | Real-time (minutes delay), push-based | Requires endpoint setup, can miss events |
| **Polling** | Simple, complete data | Latency, wastes API calls, rate limits |
| **Recommended** | Use webhooks primary + periodic polling as backup | — |

### Lead Data Retention

Leads are available via the API for **90 days** from creation. After 90 days, data is no longer accessible. Implement prompt retrieval and store leads in your own database.

---

## 7. Conversions API (CAPI)

### Overview

The Conversions API enables server-side event tracking, creating a direct connection between your server and Meta. This is essential for:
- Accurate conversion tracking post-iOS 14.5+
- Improved match quality over browser-only Pixel
- Offline event tracking
- Server-to-server attribution

### Sending Events

**Endpoint:** `POST /{PIXEL_ID}/events`

```bash
curl -X POST "https://graph.facebook.com/v25.0/{PIXEL_ID}/events" \
  -F 'data=[{
    "event_name": "Lead",
    "event_time": 1711900000,
    "action_source": "website",
    "event_source_url": "https://client-site.com/demo-request",
    "event_id": "evt_abc123",
    "user_data": {
      "em": ["309a0a5c3e211326ae75ca18196d301a9bdbd1a882a4d2569511033da23f0abd"],
      "ph": ["254aa248acb47dd654ca3ea53f48c2c26d641d23d7e2e93a1ec56258df7674c4"],
      "client_ip_address": "123.45.67.89",
      "client_user_agent": "Mozilla/5.0...",
      "fbc": "fb.1.1554763741205.AbCdEfGhIjKlMnOpQrStUvWxYz1234567890",
      "fbp": "fb.1.1558571054389.1098115397"
    },
    "custom_data": {
      "currency": "USD",
      "value": 0
    }
  }]' \
  -F "access_token={TOKEN}"
```

**Python:**
```python
from facebook_business.adobjects.serverside.event import Event
from facebook_business.adobjects.serverside.event_request import EventRequest
from facebook_business.adobjects.serverside.user_data import UserData
from facebook_business.adobjects.serverside.custom_data import CustomData
from facebook_business.api import FacebookAdsApi
import time, hashlib

FacebookAdsApi.init(app_id='{APP_ID}', app_secret='{APP_SECRET}', access_token='{TOKEN}')

user_data = UserData(
    emails=[hashlib.sha256('jane@company.com'.encode()).hexdigest()],
    phones=[hashlib.sha256('15551234567'.encode()).hexdigest()],
    client_ip_address='123.45.67.89',
    client_user_agent='Mozilla/5.0...',
    fbc='fb.1.1554763741205.AbCdEf...',
    fbp='fb.1.1558571054389.1098115397',
)

custom_data = CustomData(currency='USD', value=0)

event = Event(
    event_name='Lead',
    event_time=int(time.time()),
    event_id='evt_abc123',
    event_source_url='https://client-site.com/demo-request',
    action_source='website',
    user_data=user_data,
    custom_data=custom_data,
)

event_request = EventRequest(pixel_id='{PIXEL_ID}', events=[event])
response = event_request.execute()
print(response)
```

### Event Types

**Standard events:**

| Event Name | When to Fire |
|------------|-------------|
| `Purchase` | Completed purchase |
| `Lead` | Lead form submitted |
| `CompleteRegistration` | Registration completed |
| `ViewContent` | Key page viewed |
| `AddToCart` | Item added to cart |
| `InitiateCheckout` | Checkout started |
| `Subscribe` | Subscription started |
| `Contact` | Contact form submitted |
| `FindLocation` | Store locator used |
| `Schedule` | Appointment scheduled |
| `StartTrial` | Free trial started |
| `SubmitApplication` | Application submitted |
| `Search` | Search performed |

Custom events: any string (e.g., `DemoRequested`, `PricingPageViewed`).

### Required Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `event_name` | string | Standard or custom event name |
| `event_time` | integer | Unix timestamp (seconds). Max 7 days in past (62 days for `physical_store`) |
| `action_source` | string | `website`, `app`, `email`, `phone_call`, `chat`, `physical_store`, `system_generated`, `business_messaging`, `other` |
| `user_data` | object | Customer info for matching (see below) |

### User Data Fields

| Field | Key | Hashing | Description |
|-------|-----|---------|-------------|
| Email | `em` | SHA-256 | Hashed email(s) |
| Phone | `ph` | SHA-256 | Hashed phone(s) with country code |
| First Name | `fn` | SHA-256 | Hashed first name |
| Last Name | `ln` | SHA-256 | Hashed last name |
| Date of Birth | `db` | SHA-256 | YYYYMMDD format |
| Gender | `ge` | SHA-256 | `m` or `f` |
| City | `ct` | SHA-256 | Lowercase, no special chars |
| State | `st` | SHA-256 | 2-char code |
| Zip | `zp` | SHA-256 | 5-digit US |
| Country | `country` | SHA-256 | ISO 3166-1 alpha-2 |
| Client IP | `client_ip_address` | **No** | Raw IP |
| User Agent | `client_user_agent` | **No** | Raw UA string |
| Click ID | `fbc` | **No** | From `_fbc` cookie |
| Browser ID | `fbp` | **No** | From `_fbp` cookie |
| External ID | `external_id` | SHA-256 | Your user ID |

> **Tip:** The Meta Business SDK handles hashing automatically when you use its classes.

### Event Match Quality (EMQ)

EMQ scores range from 0-10 and measure how well your event data matches Meta users. Improve EMQ by:
- Sending more user data fields (email + phone + name = much better than email alone)
- Including `fbc` and `fbp` cookies from the browser
- Sending `client_ip_address` and `client_user_agent`
- Ensuring proper normalization before hashing

### Deduplication with Meta Pixel

When sending the same event from both Pixel (browser) and CAPI (server):
1. Set an identical `event_id` in both the Pixel `fbq('track', 'Lead', {}, {eventID: 'evt_abc123'})` and the CAPI request
2. Meta deduplicates within a **48-hour window** based on matching `event_name` + `event_id`
3. Without deduplication, the event counts double

### Test Events Mode

Add `test_event_code` to validate without affecting production data:

```bash
curl -X POST "https://graph.facebook.com/v25.0/{PIXEL_ID}/events" \
  -F 'data=[{
    "event_name": "Lead",
    "event_time": 1711900000,
    "action_source": "website",
    "user_data": {"client_ip_address": "1.2.3.4", "client_user_agent": "test ua"}
  }]' \
  -F "test_event_code=TEST12345" \
  -F "access_token={TOKEN}"
```

Find your test event code in Events Manager → Test Events tab.

> **Warning:** Events sent with `test_event_code` are NOT dropped — they flow into Events Manager and are used for targeting/measurement. Use a test pixel for true sandboxing.

### Conversions API Gateway vs Direct API

| Approach | Pros | Cons |
|----------|------|------|
| **Direct API** | Full control, flexible | You manage infrastructure |
| **CAPI Gateway** | Managed by Meta, easy setup | Less customization, hosted on your cloud |

For PaidEdge, **direct API** is recommended — you need per-tenant pixel routing and custom event logic.

### Data Processing Options (CCPA/Privacy)

```json
{
  "data_processing_options": ["LDU"],
  "data_processing_options_country": 1,
  "data_processing_options_state": 1000
}
```

Pass empty array to explicitly opt out of LDU: `"data_processing_options": []`

### Batch Sending

- Up to **1,000 events per request**
- Send events as soon as they occur, ideally within 1 hour
- Events older than 7 days are rejected (except `physical_store`: 62 days)

---

## 8. Reporting & Insights

### Insights API Endpoints

The Insights edge is available at every level of the hierarchy:

```
GET /act_{AD_ACCOUNT_ID}/insights  — account level
GET /{CAMPAIGN_ID}/insights        — campaign level
GET /{ADSET_ID}/insights           — ad set level
GET /{AD_ID}/insights              — ad level
```

### Basic Query

```bash
curl -G "https://graph.facebook.com/v25.0/{CAMPAIGN_ID}/insights" \
  -d "fields=impressions,reach,clicks,spend,actions,cost_per_action_type,cpc,cpm,ctr" \
  -d "time_range={\"since\":\"2026-03-01\",\"until\":\"2026-03-24\"}" \
  -d "access_token={TOKEN}"
```

**Python:**
```python
from facebook_business.adobjects.campaign import Campaign

campaign = Campaign('{CAMPAIGN_ID}')
insights = campaign.get_insights(
    fields=[
        'impressions', 'reach', 'clicks', 'spend',
        'actions', 'cost_per_action_type',
        'cpc', 'cpm', 'ctr', 'frequency',
    ],
    params={
        'time_range': {'since': '2026-03-01', 'until': '2026-03-24'},
        'time_increment': 1,  # daily breakdown
    }
)
for row in insights:
    print(row)
```

### Available Metrics

| Metric | Description |
|--------|-------------|
| `impressions` | Number of times ad was shown |
| `reach` | Number of unique people who saw the ad |
| `clicks` | Total clicks (all types) |
| `spend` | Total amount spent |
| `actions` | Array of action objects (conversions, leads, etc.) |
| `cost_per_action_type` | Cost breakdown per action type |
| `cpc` | Cost per click |
| `cpm` | Cost per 1,000 impressions |
| `ctr` | Click-through rate |
| `frequency` | Average times each person saw the ad |
| `video_p25_watched_actions` | 25% video views |
| `video_p50_watched_actions` | 50% video views |
| `video_p75_watched_actions` | 75% video views |
| `video_p100_watched_actions` | 100% video views |
| `video_avg_time_watched_actions` | Average video watch time |
| `inline_link_clicks` | Clicks on links in the ad |
| `inline_link_click_ctr` | Link click-through rate |
| `cost_per_inline_link_click` | Cost per link click |
| `conversions` | Total conversions |
| `conversion_values` | Total conversion value |
| `cost_per_conversion` | Cost per conversion |

### Breakdowns

**Demographic breakdowns:**
```bash
-d "breakdowns=age,gender"
```

**Platform/placement breakdowns:**
```bash
-d "breakdowns=publisher_platform,platform_position"
```

**Device breakdowns:**
```bash
-d "breakdowns=device_platform"
```

**Country breakdown:**
```bash
-d "breakdowns=country"
```

**Time breakdowns** (via `time_increment`):
```bash
-d "time_increment=1"     # daily
-d "time_increment=7"     # weekly
-d "time_increment=monthly"
```

### Action Breakdowns

```bash
-d "action_breakdowns=action_type"
```

The `actions` field returns an array like:
```json
{
  "actions": [
    {"action_type": "link_click", "value": "150"},
    {"action_type": "lead", "value": "23"},
    {"action_type": "landing_page_view", "value": "120"},
    {"action_type": "page_engagement", "value": "45"}
  ]
}
```

### Date Range Handling

**Preset ranges:**
```bash
-d "date_preset=last_7d"    # last_7d, last_14d, last_28d, last_30d, last_90d
-d "date_preset=today"       # today, yesterday, this_month, last_month
```

**Custom range:**
```bash
-d 'time_range={"since":"2026-03-01","until":"2026-03-24"}'
```

**Time increment** (breaks results into periods):
```bash
-d "time_increment=1"  # 1 row per day
```

### Async Reports (Large Queries)

For large result sets, use async to avoid timeouts:

```bash
# Step 1: Create async job
curl -X POST "https://graph.facebook.com/v25.0/act_{AD_ACCOUNT_ID}/insights" \
  -F "fields=impressions,reach,clicks,spend,actions" \
  -F "level=ad" \
  -F "breakdowns=age,gender" \
  -F 'time_range={"since":"2026-01-01","until":"2026-03-24"}' \
  -F "access_token={TOKEN}"
# Returns: {"report_run_id": "12345"}

# Step 2: Poll for completion
curl -X GET "https://graph.facebook.com/v25.0/{REPORT_RUN_ID}?\
  fields=async_status,async_percent_completion&\
  access_token={TOKEN}"
# Wait until async_status = "Job Completed"

# Step 3: Fetch results
curl -X GET "https://graph.facebook.com/v25.0/{REPORT_RUN_ID}/insights?\
  access_token={TOKEN}"
```

**Python async:**
```python
from facebook_business.adobjects.adaccount import AdAccount

account = AdAccount('act_{AD_ACCOUNT_ID}')
async_job = account.get_insights_async(
    fields=['impressions', 'reach', 'clicks', 'spend', 'actions'],
    params={
        'level': 'ad',
        'breakdowns': ['age', 'gender'],
        'time_range': {'since': '2026-01-01', 'until': '2026-03-24'},
    }
)

# Poll until complete
async_job.api_get()
while async_job['async_status'] != 'Job Completed':
    import time
    time.sleep(10)
    async_job.api_get()

# Fetch results
results = async_job.get_result()
for row in results:
    print(row)
```

### Attribution Settings

```bash
-d "action_attribution_windows=['1d_click','7d_click','1d_view']"
```

| Window | Meaning |
|--------|---------|
| `1d_click` | Conversion within 1 day of ad click |
| `7d_click` | Conversion within 7 days of ad click (default) |
| `1d_view` | Conversion within 1 day of ad view |
| `28d_click` | **Deprecated** — no longer available post-iOS 14.5 |

> **Important (June 2025+):** Attribution values default to ad-set-level attribution settings, and actions report using `action_report_time=mixed`.

### Filtering

```bash
-d 'filtering=[{"field":"ad.effective_status","operator":"IN","value":["ACTIVE","PAUSED"]}]'
```

**Sorting:**
```bash
-d "sort=reach_descending"
-d "sort=spend_descending"
```

**Level parameter** (aggregate at a specific hierarchy level):
```bash
-d "level=ad"       # one row per ad
-d "level=adset"    # one row per ad set
-d "level=campaign" # one row per campaign
```

### Notes

- Requires `ads_read` permission
- Unique metrics (reach, frequency) are resource-intensive — query separately from non-unique metrics
- Deleted/archived objects appear in parent-level queries but are excluded from filtered searches by default
- **No specific query timeout threshold** — if a query times out, break it into smaller time ranges or fewer breakdowns

---

## 9. Rate Limits

### Scoring System

Each Marketing API call receives a score:
- **Read operations:** 1 point
- **Write operations:** 3 points

Your total score is the sum of all calls. When you exceed the threshold, subsequent calls return errors.

### Access Tiers

| Tier | Max Score | Decay Period | Block Duration |
|------|-----------|-------------|----------------|
| **Development** | 60 | 300 seconds | 300 seconds |
| **Standard** | 9,000 | 300 seconds | 60 seconds |

### Business Use Case (BUC) Rate Limits

Per ad account, per hour:

| Use Case | Standard Tier | Development Tier |
|----------|--------------|------------------|
| `ads_management` | 100,000 + 40 × active ads | 300 + 40 × active ads |
| `custom_audience` | Min 190,000, max 700,000 | 5,000 + 40 × active audiences |
| `ads_insights` | 190,000 + 400 × active ads - user errors | 600 + 400 × active ads |
| Catalog Management | 20,000 + 20,000 × log₂(unique users) | — |

### Mutation QPS Limits

**100 requests per second** per app + ad account combination for create/edit on:
- `POST /act_{ID}/ads`
- `POST /act_{ID}/adsets`
- `POST /act_{ID}/campaigns`

### Budget Change Limits

- **Ad account spend changes:** 10 per day
- **Ad set budget changes:** 4 per hour per ad set

### Rate Limit Headers

**X-Ad-Account-Usage:**
```json
{"acc_id_util_pct": 45.5, "reset_time_duration": 300, "ads_api_access_tier": "standard_access"}
```

**X-Business-Use-Case-Usage:**
```json
{
  "{BUC}": [
    {"call_count": 150, "total_cputime": 30, "total_time": 45, "estimated_time_to_regain_access": 0}
  ]
}
```

**X-FB-Ads-Insights-Throttle:**
```json
{"app_id_util_pct": 12.5, "acc_id_util_pct": 8.2, "ads_api_access_tier": "standard_access"}
```

### Error Codes

| Code | Subcode | Meaning |
|------|---------|---------|
| 4 | — | App-level request limit reached |
| 4 | 1504022/1504039 | App-level Insights throttling |
| 17 | 2446079 | Account-level API limit reached |
| 613 | 1487742 | Too many calls from ad account |
| 613 | 5044001 | QPS mutation limit exceeded |
| 80000-80014 | — | BUC rate limit exceeded |

### Backoff Strategies

1. **Exponential backoff:** Start at 1s, double on each retry (1s → 2s → 4s → 8s), max 5 retries
2. **Check headers first:** Read `X-Ad-Account-Usage` before retrying — if `acc_id_util_pct` > 75%, slow down proactively
3. **Distribute requests:** Spread API calls evenly; avoid bursts
4. **Use async:** For Insights, always use async jobs for large queries

### Batch Requests

Combine multiple operations into a single HTTP request:

```bash
curl -X POST "https://graph.facebook.com/v25.0/" \
  -F 'batch=[
    {"method":"GET","relative_url":"act_{AD_ACCOUNT_ID}/campaigns?fields=name,status&limit=10"},
    {"method":"GET","relative_url":"act_{AD_ACCOUNT_ID}/adsets?fields=name,status&limit=10"}
  ]' \
  -F "access_token={TOKEN}"
```

- Max **50 requests per batch**
- Each sub-request still counts toward rate limits individually
- Useful for reducing HTTP overhead and connection setup costs

---

## 10. Python SDK

### Installation

```bash
pip install facebook_business
```

### Initialization

```python
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount

# Initialize globally
FacebookAdsApi.init(
    app_id='{APP_ID}',
    app_secret='{APP_SECRET}',
    access_token='{ACCESS_TOKEN}',
)

# Or initialize per-request (multi-tenant)
api = FacebookAdsApi.init(
    app_id='{APP_ID}',
    app_secret='{APP_SECRET}',
    access_token='{CLIENT_SPECIFIC_TOKEN}',
)
account = AdAccount('act_{AD_ACCOUNT_ID}', api=api)
```

For multi-tenant PaidEdge, use **per-request initialization** — create a new `FacebookAdsApi` instance per client token.

### Common Patterns

**Create a campaign:**
```python
from facebook_business.adobjects.campaign import Campaign

account = AdAccount('act_{AD_ACCOUNT_ID}')
campaign = account.create_campaign(params={
    Campaign.Field.name: 'My Campaign',
    Campaign.Field.objective: 'OUTCOME_LEADS',
    Campaign.Field.status: Campaign.Status.paused,
    Campaign.Field.special_ad_categories: [],
})
print(f"Campaign ID: {campaign['id']}")
```

**Read insights:**
```python
from facebook_business.adobjects.adaccount import AdAccount

account = AdAccount('act_{AD_ACCOUNT_ID}')
insights = account.get_insights(
    fields=['impressions', 'reach', 'clicks', 'spend', 'actions'],
    params={
        'time_range': {'since': '2026-03-01', 'until': '2026-03-24'},
        'level': 'campaign',
    }
)
for row in insights:
    print(row.export_all_data())
```

**Manage audiences:**
```python
from facebook_business.adobjects.customaudience import CustomAudience

# Create
audience = account.create_custom_audience(params={
    CustomAudience.Field.name: 'Email List Audience',
    CustomAudience.Field.subtype: CustomAudience.Subtype.custom,
    CustomAudience.Field.customer_file_source: 'USER_PROVIDED_ONLY',
})

# Add users
audience = CustomAudience('{AUDIENCE_ID}')
audience.add_users(
    schema=['EMAIL'],
    users=[['hashed_email_1'], ['hashed_email_2']],
    is_hashed=True,
)
```

### Pagination (Cursor-Based)

The SDK returns `Cursor` objects that handle pagination automatically:

```python
# Auto-pagination via iteration
campaigns = account.get_campaigns(
    fields=['name', 'status', 'objective'],
    params={'limit': 25}
)
for campaign in campaigns:  # automatically fetches next pages
    print(campaign['name'])

# Manual pagination
campaigns = account.get_campaigns(fields=['name'], params={'limit': 10})
while True:
    for campaign in campaigns:
        print(campaign['name'])
    if campaigns.load_next_page():
        continue
    break
```

### Error Handling

```python
from facebook_business.exceptions import FacebookRequestError

try:
    campaign = account.create_campaign(params={...})
except FacebookRequestError as e:
    print(f"Error code: {e.api_error_code()}")
    print(f"Error subcode: {e.api_error_subcode()}")
    print(f"Error message: {e.api_error_message()}")
    print(f"Error type: {e.api_error_type()}")
    print(f"HTTP status: {e.http_status()}")
    print(f"Blame field: {e.body().get('error', {}).get('error_data', {}).get('blame_field_specs', [])}")

    # Handle specific errors
    if e.api_error_code() == 190:
        # Invalid/expired token — refresh and retry
        pass
    elif e.api_error_code() in (4, 17):
        # Rate limited — backoff and retry
        import time
        time.sleep(60)
    elif e.api_error_code() == 100:
        # Invalid parameter — check blame_field_specs
        pass
```

**Common exception types:**
- `FacebookRequestError` — API returned an error
- `FacebookBadObjectError` — Invalid object state
- `FacebookBadParameterError` — Invalid parameter passed

### Retry Pattern for Rate Limits

```python
import time
from facebook_business.exceptions import FacebookRequestError

def api_call_with_retry(func, max_retries=5, base_delay=1):
    """Execute an API call with exponential backoff on rate limits."""
    for attempt in range(max_retries):
        try:
            return func()
        except FacebookRequestError as e:
            if e.api_error_code() in (4, 17, 80000, 80001, 80002, 80003, 80004):
                delay = base_delay * (2 ** attempt)
                print(f"Rate limited. Retrying in {delay}s...")
                time.sleep(delay)
            else:
                raise
    raise Exception(f"Max retries ({max_retries}) exceeded")
```

---

## 11. Common Gotchas

### App Review Rejection Reasons

1. **Insufficient screencasts:** Must show the entire user flow end-to-end, including how data is used
2. **Vague use case:** "We manage ads" isn't enough — explain the specific flow for each permission
3. **Requesting unnecessary permissions:** Only request what you actually need
4. **Missing privacy policy / TOS:** Must be live URLs, not localhost
5. **Development-mode app:** Ensure the app is in Live mode before submission
6. **No test account:** Provide test credentials for reviewers

**Tips to pass:**
- Record a 3-5 minute video for each permission showing exactly how it's used
- Explain the user benefit, not just the technical capability
- Include both admin and end-user flows

### Special Ad Categories

Campaigns for **housing, credit, or employment** must declare `special_ad_categories`:

```python
campaign = account.create_campaign(params={
    'name': 'Housing Campaign',
    'objective': 'OUTCOME_LEADS',
    'special_ad_categories': ['HOUSING'],  # or CREDIT, EMPLOYMENT
    'status': 'PAUSED',
})
```

**Restrictions under special categories:**
- No age targeting (all ages 18+)
- No gender targeting
- No zip code targeting
- No interest-based exclusions related to protected classes
- Location targeting minimum radius: 15 miles
- No lookalike audiences (replaced by "Special Ad Audiences")

### Policy Restrictions for B2B Advertising

- Cannot target by specific companies or employees (this is LinkedIn, not Meta)
- Job title targeting is limited and often inaccurate on Meta
- B2B interest/behavior targeting works better than demographic targeting on Meta
- Custom audiences from CRM data + lookalikes are the strongest B2B play

### Pixel Domain Verification

Required when:
- You want to track conversions on a domain you don't own
- Multiple businesses share a domain
- You're configuring Aggregated Event Measurement (AEM) for iOS 14.5+

Verify via DNS TXT record or meta tag in the domain's `<head>`.

### Business Manager Trust Tiers

| Tier | Ad Account Limit | Daily Spend Limit |
|------|-----------------|-------------------|
| Unverified | Low (typically 1-5) | Low |
| Verified | Higher (50+) | Higher |
| Approved for Standard Access | Highest | Highest |

Verify your business by submitting legal business documents in Business Settings → Business Info.

### iOS 14.5+ Impact

Since iOS 14.5 and App Tracking Transparency (ATT):
- **Attribution window reduced:** 28-day click deprecated; default is now 7-day click, 1-day view
- **Delayed reporting:** Conversions may take up to 3 days to report
- **Aggregated Event Measurement (AEM):** Limited to 8 conversion events per domain
- **Estimated results:** Some metrics are modeled/estimated rather than exact
- **Pixel signals reduced:** Server-side CAPI becomes essential for accuracy
- **Demographic breakdowns limited:** Some breakdowns may show partial data

### Creative Rejection Reasons

- Text overlay exceeding 20% of image (guideline, not hard rule anymore)
- Before/after imagery (especially for health/wellness)
- Misleading claims or exaggerated promises
- Personal attributes ("Are you...?") — cannot call out user characteristics
- Profanity or shocking content
- Broken landing page or URL mismatch

**Appeal process:** Ad Manager → Account Quality → select rejected ad → Request Review

### Custom Audience Terms of Service

Before creating or editing custom audiences, the business must accept CA Terms. Without acceptance, API calls return:
- **Error 200, subcode 1870090:** "Must agree to Custom Audience terms"

Accept via: Business Manager → Business Settings → Data Sources → Custom Audiences → Terms

### Token Expiry Surprises

| Token Type | Actual Behavior |
|------------|----------------|
| System user (non-expiring) | Truly never expires, but can be revoked |
| System user (60-day) | Expires exactly 60 days from generation or refresh |
| Long-lived user token | ~60 days, NOT auto-refreshed |
| Page token (from long-lived user) | Never expires, but invalidated if user changes password or revokes app |
| Short-lived user token | 1-2 hours, no refresh path — must re-authenticate |

**Common surprise:** Long-lived user tokens are NOT automatically refreshed. You must build refresh logic or use system user tokens instead.

### Other Important Notes

- **Conversions API rate limits:** CAPI calls count as Marketing API calls but have no separate rate limit
- **Event Manager attribution:** As of June 2025, attributed values default to ad-set-level attribution settings
- **Audience integrity (Sept 2025+):** Audiences suggesting restricted topics get `operation_status: 471`
- **API versioning:** Meta deprecates API versions ~2 years after release. Always target a recent version and plan upgrades.
- **Sandbox/test mode:** Use the Graph API Explorer with development app for testing; use `test_event_code` for CAPI testing

---

*Last updated: March 2026. Verify all specifications against the [official Meta Marketing API docs](https://developers.facebook.com/docs/marketing-api) before production implementation.*
