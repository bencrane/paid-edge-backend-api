# Google Ads API Reference — PaidEdge Integration Guide

> **Purpose:** Comprehensive reference for building a Google Ads integration for PaidEdge, a multi-tenant B2B SaaS platform that manages paid advertising campaigns on behalf of multiple client organizations. Each client connects their own Google Ads account via OAuth through a Manager Account (MCC).
>
> **API Version:** v18 (google-ads Python SDK v25.x)
>
> **Last Updated:** 2026-03-25

---

## Table of Contents

1. [Authentication & OAuth 2.0](#1-authentication--oauth-20)
2. [Account Structure](#2-account-structure)
3. [Customer Match / Custom Audiences](#3-customer-match--custom-audiences)
4. [Programmatic Ad Creation & Upload](#4-programmatic-ad-creation--upload)
5. [Campaign Management](#5-campaign-management)
6. [Lead Form Extensions](#6-lead-form-extensions)
7. [Conversion Tracking](#7-conversion-tracking)
8. [Reporting — GAQL](#8-reporting--gaql-google-ads-query-language)
9. [Rate Limits](#9-rate-limits)
10. [Python SDK](#10-python-sdk)
11. [Common Gotchas](#11-common-gotchas)

---

## 1. Authentication & OAuth 2.0

### 1.1 Overview

Every Google Ads API request requires three credentials:

1. **Developer Token** — identifies your application
2. **OAuth 2.0 credentials** — per-tenant authorization to manage their Google Ads accounts
3. **`login-customer-id` header** — routes requests through your MCC

### 1.2 Developer Token

A developer token is a unique string tied to your MCC (Manager Account), required in every API request via the `developer-token` header.

**How to apply:**

1. Sign into your Google Ads MCC at ads.google.com
2. Navigate to **Tools & Settings > Setup > API Center**
3. Fill out the application form
4. You receive a **test developer token** (basic access) immediately

**Access levels:**

| Level | Rate Limits | Requirements |
|-------|-------------|--------------|
| **Test (Basic Access)** | 15,000 operations/day | Automatic on creation |
| **Standard Access** | 500,000+ operations/day | Must apply and pass review |

**Approval timeline:** Standard access review takes **1–2 weeks** (sometimes longer). Google may request a product demo. You must have a verified OAuth consent screen (published, not in test mode).

> **Security:** Treat your developer token as a secret. Never expose it in client-side code.

### 1.3 OAuth 2.0 Authorization Code Flow

For a third-party web app like PaidEdge, use the **Authorization Code flow** (3-legged OAuth).

#### Step 1: Create OAuth 2.0 Credentials

1. Go to Google Cloud Console
2. Create or select a project
3. Navigate to **APIs & Services > Credentials**
4. Click **Create Credentials > OAuth client ID**
5. Select **Web application**
6. Add redirect URI: `https://app.paidedge.com/oauth/google-ads/callback`
7. Enable the **Google Ads API** in APIs & Services > Library

#### Step 2: Build the Authorization URL

```python
from urllib.parse import urlencode

GOOGLE_AUTH_ENDPOINT = "https://accounts.google.com/o/oauth2/v2/auth"

def build_authorization_url(state: str) -> str:
    params = {
        "client_id": "YOUR_CLIENT_ID.apps.googleusercontent.com",
        "redirect_uri": "https://app.paidedge.com/oauth/google-ads/callback",
        "response_type": "code",
        "scope": "https://www.googleapis.com/auth/adwords",
        "access_type": "offline",       # Required to get a refresh token
        "prompt": "consent",            # Force consent to always get refresh token
        "state": state,                 # CSRF protection + tenant context
        "include_granted_scopes": "true",
    }
    return f"{GOOGLE_AUTH_ENDPOINT}?{urlencode(params)}"
```

#### Step 3: Handle the Callback — Exchange Code for Tokens

```python
import httpx
from fastapi import APIRouter, Request, HTTPException

router = APIRouter()
GOOGLE_TOKEN_ENDPOINT = "https://oauth2.googleapis.com/token"

@router.get("/oauth/google-ads/callback")
async def google_ads_callback(request: Request):
    code = request.query_params.get("code")
    state = request.query_params.get("state")
    error = request.query_params.get("error")

    if error:
        raise HTTPException(status_code=400, detail=f"OAuth error: {error}")

    state_data = decode_and_validate_state(state)
    org_id = state_data["org_id"]

    async with httpx.AsyncClient() as client:
        response = await client.post(
            GOOGLE_TOKEN_ENDPOINT,
            data={
                "code": code,
                "client_id": "YOUR_CLIENT_ID.apps.googleusercontent.com",
                "client_secret": "YOUR_CLIENT_SECRET",
                "redirect_uri": "https://app.paidedge.com/oauth/google-ads/callback",
                "grant_type": "authorization_code",
            },
        )

    if response.status_code != 200:
        raise HTTPException(status_code=400, detail="Token exchange failed")

    token_data = response.json()
    # token_data = {
    #   "access_token": "ya29.xxx...",
    #   "expires_in": 3600,
    #   "refresh_token": "1//0xxx...",  <-- Store this securely!
    #   "scope": "https://www.googleapis.com/auth/adwords",
    #   "token_type": "Bearer"
    # }

    await store_tenant_credentials(
        org_id=org_id,
        refresh_token=token_data["refresh_token"],
    )

    return RedirectResponse("/dashboard?connected=google-ads")
```

**curl equivalent for token exchange:**

```bash
curl -X POST https://oauth2.googleapis.com/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "code=4/0AX4XfWh..." \
  -d "client_id=YOUR_CLIENT_ID.apps.googleusercontent.com" \
  -d "client_secret=YOUR_CLIENT_SECRET" \
  -d "redirect_uri=https://app.paidedge.com/oauth/google-ads/callback" \
  -d "grant_type=authorization_code"
```

#### Step 4: Refresh Access Tokens

```python
async def get_access_token(refresh_token: str) -> str:
    async with httpx.AsyncClient() as client:
        response = await client.post(
            GOOGLE_TOKEN_ENDPOINT,
            data={
                "client_id": "YOUR_CLIENT_ID.apps.googleusercontent.com",
                "client_secret": "YOUR_CLIENT_SECRET",
                "refresh_token": refresh_token,
                "grant_type": "refresh_token",
            },
        )
    if response.status_code != 200:
        raise Exception(f"Token refresh failed: {response.text}")
    return response.json()["access_token"]
```

### 1.4 Refresh Tokens — Storage, Rotation, Expiry

Google OAuth 2.0 refresh tokens **do not expire** unless:

1. **User revokes access** via Google Account Permissions
2. **Token is unused for 6 months**
3. **OAuth consent screen is in "Testing" mode** — tokens expire after 7 days
4. **100 refresh token limit per client ID per Google Account** — oldest is invalidated

**Storage best practice:** Encrypt refresh tokens at rest using Fernet or similar:

```python
from cryptography.fernet import Fernet

ENCRYPTION_KEY = os.environ["REFRESH_TOKEN_ENCRYPTION_KEY"]
fernet = Fernet(ENCRYPTION_KEY)

async def store_tenant_credentials(org_id: str, refresh_token: str):
    encrypted_token = fernet.encrypt(refresh_token.encode()).decode()
    await db.execute(
        """
        INSERT INTO google_ads_credentials (org_id, encrypted_refresh_token, connected_at)
        VALUES (:org_id, :token, now())
        ON CONFLICT (org_id)
        DO UPDATE SET encrypted_refresh_token = :token, connected_at = now()
        """,
        {"org_id": org_id, "token": encrypted_token},
    )

async def get_tenant_refresh_token(org_id: str) -> str:
    row = await db.fetch_one(
        "SELECT encrypted_refresh_token FROM google_ads_credentials WHERE org_id = :org_id",
        {"org_id": org_id},
    )
    if not row:
        raise ValueError(f"No Google Ads credentials for org {org_id}")
    return fernet.decrypt(row["encrypted_refresh_token"].encode()).decode()
```

### 1.5 The `login-customer-id` Header

Specifies **which MCC you are authenticating through** when making API requests.

```
API Request
├── Header: login-customer-id = 1234567890    ← Your MCC (constant)
├── Header: developer-token = AbCdEf...       ← Your app's developer token
├── Header: Authorization = Bearer ya29...     ← OAuth token (per-tenant)
└── URL: /customers/9876543210/campaigns       ← Target client account (per-tenant)
```

- **`login-customer-id`**: Always your MCC's ID (no hyphens). Constant across all tenants.
- **Customer ID in URL**: The specific client account. Varies per tenant.

### 1.6 Per-Tenant Token Management

**Database schema:**

```sql
CREATE TABLE google_ads_credentials (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id UUID NOT NULL REFERENCES organizations(id),
    encrypted_refresh_token TEXT NOT NULL,
    google_ads_customer_id VARCHAR(10),
    connected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    is_active BOOLEAN NOT NULL DEFAULT true,
    UNIQUE(org_id)
);
```

**Client factory pattern:**

```python
class GoogleAdsClientFactory:
    def __init__(self, db, encryption_key: str):
        self.db = db
        self.fernet = Fernet(encryption_key)
        self.developer_token = os.environ["GOOGLE_ADS_DEVELOPER_TOKEN"]
        self.client_id = os.environ["GOOGLE_ADS_CLIENT_ID"]
        self.client_secret = os.environ["GOOGLE_ADS_CLIENT_SECRET"]
        self.mcc_id = os.environ["GOOGLE_ADS_MCC_ID"]

    async def get_client(self, org_id: str) -> GoogleAdsClient:
        creds = await self.db.fetch_one(
            "SELECT encrypted_refresh_token FROM google_ads_credentials WHERE org_id = :org_id AND is_active = true",
            {"org_id": org_id},
        )
        if not creds:
            raise ValueError(f"No active Google Ads connection for org {org_id}")

        refresh_token = self.fernet.decrypt(creds["encrypted_refresh_token"].encode()).decode()

        return GoogleAdsClient.load_from_dict({
            "developer_token": self.developer_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": refresh_token,
            "login_customer_id": self.mcc_id,
            "use_proto_plus": True,
        })
```

### 1.7 Service Accounts vs OAuth

| Approach | Use Case | For PaidEdge? |
|---|---|---|
| **OAuth 2.0 (Authorization Code)** | Third-party apps managing ads on behalf of users | **Yes** |
| **Service Accounts** | Internal tools within a single Google Workspace domain | **No** |

Service accounts require domain-wide delegation, which is not practical for multi-tenant SaaS. **Always use OAuth 2.0 Authorization Code flow.**

### 1.8 Linking Client Accounts to MCC Programmatically

```python
def create_manager_link(client, manager_customer_id, client_customer_id):
    """Send a link invitation from your MCC to a client account."""
    service = client.get_service("CustomerManagerLinkService")
    operation = client.get_type("CustomerManagerLinkOperation")
    manager_link = operation.create
    manager_link.manager_customer = (
        client.get_service("GoogleAdsService").customer_path(manager_customer_id)
    )
    manager_link.status = client.enums.ManagerLinkStatusEnum.PENDING

    response = service.mutate_customer_manager_links(
        customer_id=manager_customer_id,
        operations=[operation],
    )
    return response.results[0].resource_name
```

---

## 2. Account Structure

### 2.1 Hierarchy

```
Manager Account (MCC)  ← PaidEdge's top-level account
└── Client Account (Customer)  ← One per client organization
    ├── Campaign
    │   ├── Ad Group
    │   │   ├── Ad (Responsive Search Ad, etc.)
    │   │   ├── Ad Group Criterion (Keywords, Audiences)
    │   │   └── Ad Group Ad
    │   ├── Campaign Criterion (Location targeting, etc.)
    │   └── Campaign Budget
    ├── Conversion Actions
    ├── User Lists (Audiences)
    └── Assets (Sitelinks, Callouts, Images, etc.)
```

| Entity | Description |
|---|---|
| **Manager Account (MCC)** | Meta-account managing multiple client accounts |
| **Client Account (Customer)** | Individual Google Ads account with own billing/campaigns |
| **Campaign** | Collection of ad groups sharing budget/bidding/targeting |
| **Ad Group** | Set of ads and keywords/targeting within a campaign |
| **Ad** | The actual creative shown to users |
| **Assets** | Reusable components (sitelinks, callouts, images, logos). Replaced legacy "Extensions" |

### 2.2 Discovering Linked Accounts — `listAccessibleCustomers`

After OAuth, discover which accounts the user has access to:

```python
def list_accessible_customers(client):
    """List all Google Ads accounts accessible to the authenticated user."""
    customer_service = client.get_service("CustomerService")
    response = customer_service.list_accessible_customers()

    accounts = []
    for resource_name in response.resource_names:
        customer_id = resource_name.split("/")[1]
        accounts.append({"resource_name": resource_name, "customer_id": customer_id})
    return accounts


def get_account_details(client, customer_id):
    """Get detailed info about a specific account."""
    ga_service = client.get_service("GoogleAdsService")
    query = """
        SELECT customer.id, customer.descriptive_name, customer.currency_code,
               customer.time_zone, customer.manager, customer.test_account, customer.status
        FROM customer LIMIT 1
    """
    response = ga_service.search_stream(customer_id=customer_id, query=query)
    for batch in response:
        for row in batch.results:
            return {
                "id": row.customer.id,
                "name": row.customer.descriptive_name,
                "currency": row.customer.currency_code,
                "timezone": row.customer.time_zone,
                "is_manager": row.customer.manager,
                "is_test": row.customer.test_account,
            }
```

**curl:**

```bash
curl -X GET "https://googleads.googleapis.com/v18/customers:listAccessibleCustomers" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  -H "developer-token: ${DEVELOPER_TOKEN}"
# Response: { "resourceNames": ["customers/1234567890", "customers/9876543210"] }
```

### 2.3 Listing All Clients Under MCC

```python
def list_all_clients_under_mcc(client, mcc_customer_id):
    ga_service = client.get_service("GoogleAdsService")
    query = """
        SELECT customer_client.id, customer_client.descriptive_name,
               customer_client.level, customer_client.manager,
               customer_client.status, customer_client.currency_code
        FROM customer_client
        WHERE customer_client.status = 'ENABLED'
    """
    response = ga_service.search_stream(customer_id=mcc_customer_id, query=query)
    accounts = []
    for batch in response:
        for row in batch.results:
            if not row.customer_client.manager:  # Filter to leaf accounts only
                accounts.append({
                    "id": row.customer_client.id,
                    "name": row.customer_client.descriptive_name,
                    "level": row.customer_client.level,
                })
    return accounts
```

### 2.4 Permission Levels

| Access Level | Read | Edit | Manage Budget | Link/Unlink |
|---|---|---|---|---|
| **Read Only** | Yes | No | No | No |
| **Standard** | Yes | Yes | Yes | No |
| **Administrative** | Yes | Yes | Yes | Yes |

PaidEdge typically needs **Standard** or **Administrative** access.

---

## 3. Customer Match / Custom Audiences

### 3.1 Overview

Customer Match uploads first-party customer data (email, phone, address, mobile IDs) to Google Ads. Google matches against signed-in users to create audience segments.

**Prerequisites:**
- Good policy compliance history
- Good payment history
- At least 90 days Google Ads history
- More than $50,000 USD total lifetime spend (for targeting)
- Standard Access developer token

### 3.2 Creating a Customer Match User List

```python
def create_customer_match_user_list(client, customer_id, list_name, membership_lifespan_days=30):
    user_list_service = client.get_service("UserListService")
    operation = client.get_type("UserListOperation")
    user_list = operation.create

    user_list.name = list_name
    user_list.membership_life_span = membership_lifespan_days  # Max 540, use 10000 for no expiry
    user_list.crm_based_user_list.upload_key_type = (
        client.enums.CustomerMatchUploadKeyTypeEnum.CONTACT_INFO
    )
    user_list.crm_based_user_list.data_source_type = (
        client.enums.UserListCrmDataSourceTypeEnum.FIRST_PARTY
    )

    response = user_list_service.mutate_user_lists(
        customer_id=customer_id, operations=[operation]
    )
    return response.results[0].resource_name
```

**curl:**

```bash
curl -X POST "https://googleads.googleapis.com/v18/customers/9876543210/userLists:mutate" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  -H "developer-token: ${DEVELOPER_TOKEN}" \
  -H "login-customer-id: 1234567890" \
  -H "Content-Type: application/json" \
  -d '{
    "operations": [{
      "create": {
        "name": "PaidEdge - High Value Customers Q1 2026",
        "membershipLifeSpan": 90,
        "crmBasedUserList": {
          "uploadKeyType": "CONTACT_INFO",
          "dataSourceType": "FIRST_PARTY"
        }
      }
    }]
  }'
```

### 3.3 Hashing Requirements

All PII must be hashed with **SHA-256** before upload. Never send plaintext PII.

**Normalization rules:**

| Data Type | Steps | Example |
|---|---|---|
| **Email** | Trim, lowercase, remove dots from Gmail username | `" John.Doe@Gmail.com "` → `"johndoe@gmail.com"` |
| **Phone** | E.164 format (country code, no spaces/dashes) | `"(555) 123-4567"` → `"+15551234567"` |
| **First/Last Name** | Trim, lowercase | `" John "` → `"john"` |
| **Country** | 2-letter ISO, uppercase | `"US"`, `"GB"` |
| **Zip** | Trim, US: first 5 digits only | `"90210-1234"` → `"90210"` |

```python
import hashlib
import re

def normalize_and_hash_email(email: str) -> str:
    email = email.strip().lower()
    parts = email.split("@")
    if len(parts) == 2 and parts[1] in ("gmail.com", "googlemail.com"):
        parts[0] = parts[0].replace(".", "")
    return hashlib.sha256("@".join(parts).encode("utf-8")).hexdigest()

def normalize_and_hash_phone(phone: str) -> str:
    cleaned = re.sub(r"[^\d+]", "", phone)
    if not cleaned.startswith("+"):
        if cleaned.startswith("1") and len(cleaned) == 11:
            cleaned = "+" + cleaned
        elif len(cleaned) == 10:
            cleaned = "+1" + cleaned
    return hashlib.sha256(cleaned.encode("utf-8")).hexdigest()

def normalize_and_hash(value: str) -> str:
    return hashlib.sha256(value.strip().lower().encode("utf-8")).hexdigest()
```

### 3.4 Uploading Data via `OfflineUserDataJobService`

The upload lifecycle: **Create Job → Add Operations → Run Job → Poll for Completion**

```python
def upload_customer_match_data(client, customer_id, user_list_resource_name, customer_data):
    offline_service = client.get_service("OfflineUserDataJobService")

    # Step 1: Create the job
    job = client.get_type("OfflineUserDataJob")
    job.type_ = client.enums.OfflineUserDataJobTypeEnum.CUSTOMER_MATCH_USER_LIST
    job.customer_match_user_list_metadata.user_list = user_list_resource_name

    create_response = offline_service.create_offline_user_data_job(
        customer_id=customer_id, job=job
    )
    job_resource_name = create_response.resource_name

    # Step 2: Build operations
    operations = []
    for record in customer_data:
        operation = client.get_type("OfflineUserDataJobOperation")
        user_data = operation.create

        if record.get("email"):
            uid = client.get_type("UserIdentifier")
            uid.hashed_email = normalize_and_hash_email(record["email"])
            user_data.user_identifiers.append(uid)

        if record.get("phone"):
            uid = client.get_type("UserIdentifier")
            uid.hashed_phone_number = normalize_and_hash_phone(record["phone"])
            user_data.user_identifiers.append(uid)

        if record.get("first_name") and record.get("last_name"):
            uid = client.get_type("UserIdentifier")
            uid.address_info.hashed_first_name = normalize_and_hash(record["first_name"])
            uid.address_info.hashed_last_name = normalize_and_hash(record["last_name"])
            if record.get("country_code"):
                uid.address_info.country_code = record["country_code"]
            if record.get("postal_code"):
                uid.address_info.postal_code = record["postal_code"].strip()[:5]
            user_data.user_identifiers.append(uid)

        operations.append(operation)

    # Step 3: Add operations in batches of 10,000
    BATCH_SIZE = 10_000
    for i in range(0, len(operations), BATCH_SIZE):
        batch = operations[i:i + BATCH_SIZE]
        request = client.get_type("AddOfflineUserDataJobOperationsRequest")
        request.resource_name = job_resource_name
        request.operations = batch
        request.enable_partial_failure = True
        offline_service.add_offline_user_data_job_operations(request=request)

    # Step 4: Run the job
    lro = offline_service.run_offline_user_data_job(resource_name=job_resource_name)

    # Step 5: Wait for completion
    lro.result(timeout=300)
    print(f"Upload complete: {len(operations)} records processed")
    return job_resource_name
```

### 3.5 Match Rates

| Data Combination | Typical Match Rate |
|---|---|
| Email only | 29–62% |
| Phone only | 20–40% |
| Email + Phone | 40–75% |
| Email + Phone + Name + Address | 50–80%+ |

**Improve match rates by:** providing multiple identifiers per user, normalizing carefully, using primary email addresses, including phone country codes, keeping data fresh, and meeting the minimum 1,000 members for targeting.

### 3.6 Membership Lifespan

| Parameter | Default | Max | No Expiry |
|---|---|---|---|
| `membership_life_span` | 30 days | 540 days | 10000 |

Re-uploading a user resets their membership timer.

### 3.7 Removing Users from a List

Use `operation.remove` instead of `operation.create`:

```python
def remove_users_from_list(client, customer_id, user_list_resource_name, emails_to_remove):
    offline_service = client.get_service("OfflineUserDataJobService")

    job = client.get_type("OfflineUserDataJob")
    job.type_ = client.enums.OfflineUserDataJobTypeEnum.CUSTOMER_MATCH_USER_LIST
    job.customer_match_user_list_metadata.user_list = user_list_resource_name
    create_response = offline_service.create_offline_user_data_job(customer_id=customer_id, job=job)
    job_resource_name = create_response.resource_name

    operations = []
    for email in emails_to_remove:
        op = client.get_type("OfflineUserDataJobOperation")
        uid = client.get_type("UserIdentifier")
        uid.hashed_email = normalize_and_hash_email(email)
        op.remove.user_identifiers.append(uid)  # .remove instead of .create
        operations.append(op)

    request = client.get_type("AddOfflineUserDataJobOperationsRequest")
    request.resource_name = job_resource_name
    request.operations = operations
    request.enable_partial_failure = True
    offline_service.add_offline_user_data_job_operations(request=request)

    lro = offline_service.run_offline_user_data_job(resource_name=job_resource_name)
    lro.result(timeout=300)
```

### 3.8 Similar Audiences — DEPRECATED

> **Similar Audiences were deprecated in August 2023 and are no longer available.** Google has fully sunset this feature. Existing Similar Audiences have stopped being populated.

**Replacement options:**
- **Optimized Targeting** — auto-expands beyond selected audiences
- **Audience Expansion** — broadens reach for Display/Video
- **Smart Bidding with first-party data signals** — Customer Match lists as audience signals
- **Performance Max campaigns** — audience signals guide AI targeting

### 3.9 Combined Audiences

Combined Audiences use AND/OR/NOT logic across multiple segments. Created in the Google Ads UI, but targetable and queryable via API:

```python
def list_combined_audiences(client, customer_id):
    ga_service = client.get_service("GoogleAdsService")
    query = """
        SELECT combined_audience.id, combined_audience.name,
               combined_audience.status, combined_audience.resource_name
        FROM combined_audience
        WHERE combined_audience.status = 'ENABLED'
    """
    response = ga_service.search_stream(customer_id=customer_id, query=query)
    audiences = []
    for batch in response:
        for row in batch.results:
            audiences.append({
                "id": row.combined_audience.id,
                "name": row.combined_audience.name,
                "resource_name": row.combined_audience.resource_name,
            })
    return audiences
```

### 3.10 Deleting User Lists

User lists cannot be permanently deleted — set status to `CLOSED` (irreversible):

```python
def close_user_list(client, customer_id, user_list_resource_name):
    user_list_service = client.get_service("UserListService")
    operation = client.get_type("UserListOperation")
    user_list = operation.update
    user_list.resource_name = user_list_resource_name
    user_list.membership_status = client.enums.UserListMembershipStatusEnum.CLOSED

    field_mask = client.get_type("FieldMask")
    field_mask.paths.append("membership_status")
    client.copy_from(operation.update_mask, field_mask)

    user_list_service.mutate_user_lists(customer_id=customer_id, operations=[operation])
```

---

## 4. Programmatic Ad Creation & Upload

### 4.1 Responsive Search Ads (RSA)

RSAs are the standard text ad format for Search campaigns. Google dynamically tests combinations of your headlines and descriptions.

**Requirements:**
- **Headlines:** Min 3, max 15. Each ≤ 30 characters.
- **Descriptions:** Min 2, max 4. Each ≤ 90 characters.
- **Final URL:** Required.
- **Pinning:** Optional — pin specific headlines/descriptions to positions.
- **Ad Strength:** Google rates your ad (POOR, AVERAGE, GOOD, EXCELLENT).

```python
def create_responsive_search_ad(client, customer_id, ad_group_id):
    ad_group_ad_service = client.get_service("AdGroupAdService")
    operation = client.get_type("AdGroupAdOperation")
    ad_group_ad = operation.create

    ad_group_ad.status = client.enums.AdGroupAdStatusEnum.PAUSED
    ad_group_ad.ad_group = client.get_service("AdGroupService").ad_group_path(
        customer_id, ad_group_id
    )

    ad = ad_group_ad.ad
    ad.final_urls.append("https://www.example.com")

    ad.responsive_search_ad.headlines.extend([
        create_ad_text_asset(client, "Best Running Shoes"),         # Position unspecified
        create_ad_text_asset(client, "Free Shipping Today"),        # Position unspecified
        create_ad_text_asset(client, "Shop Our Top Brands", 1),    # Pinned to position 1
    ])

    ad.responsive_search_ad.descriptions.extend([
        create_ad_text_asset(client, "Wide selection of premium running shoes. Shop now and save big on top brands."),
        create_ad_text_asset(client, "Free returns. 30-day money-back guarantee. Order today."),
    ])

    response = ad_group_ad_service.mutate_ad_group_ads(
        customer_id=customer_id, operations=[operation]
    )
    return response.results[0].resource_name


def create_ad_text_asset(client, text, pinned_position=None):
    ad_text_asset = client.get_type("AdTextAsset")
    ad_text_asset.text = text
    if pinned_position:
        ad_text_asset.pinned_field = client.enums.ServedAssetFieldTypeEnum.Value(
            f"HEADLINE_{pinned_position}" if pinned_position <= 3 else f"DESCRIPTION_{pinned_position - 3}"
        )
    return ad_text_asset
```

**curl:**

```bash
curl -X POST "https://googleads.googleapis.com/v18/customers/9876543210/adGroupAds:mutate" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  -H "developer-token: ${DEVELOPER_TOKEN}" \
  -H "login-customer-id: 1234567890" \
  -H "Content-Type: application/json" \
  -d '{
    "operations": [{
      "create": {
        "status": "PAUSED",
        "adGroup": "customers/9876543210/adGroups/111111111",
        "ad": {
          "finalUrls": ["https://www.example.com"],
          "responsiveSearchAd": {
            "headlines": [
              {"text": "Best Running Shoes"},
              {"text": "Free Shipping Today"},
              {"text": "Shop Our Top Brands"}
            ],
            "descriptions": [
              {"text": "Wide selection of premium running shoes. Shop now and save big."},
              {"text": "Free returns. 30-day money-back guarantee. Order today."}
            ]
          }
        }
      }
    }]
  }'
```

### 4.2 Responsive Display Ads

**Requirements:**
- **Marketing images (landscape):** 1200×628 min, at least 1 required (max 15)
- **Square marketing images:** 1200×1200 min, at least 1 required (max 15)
- **Logo (landscape):** 1200×1200 recommended (max 5)
- **Short headlines:** ≤ 30 chars, at least 1 (max 5)
- **Long headline:** ≤ 90 chars, exactly 1
- **Descriptions:** ≤ 90 chars, at least 1 (max 5)
- **Business name:** Required

```python
def create_responsive_display_ad(client, customer_id, ad_group_id,
                                  landscape_image_asset, square_image_asset, logo_asset):
    service = client.get_service("AdGroupAdService")
    operation = client.get_type("AdGroupAdOperation")
    ad_group_ad = operation.create

    ad_group_ad.status = client.enums.AdGroupAdStatusEnum.PAUSED
    ad_group_ad.ad_group = client.get_service("AdGroupService").ad_group_path(
        customer_id, ad_group_id
    )

    ad = ad_group_ad.ad
    ad.final_urls.append("https://www.example.com")
    rda = ad.responsive_display_ad

    # Images
    img = client.get_type("AdImageAsset")
    img.asset = landscape_image_asset  # Resource name from AssetService
    rda.marketing_images.append(img)

    sq_img = client.get_type("AdImageAsset")
    sq_img.asset = square_image_asset
    rda.square_marketing_images.append(sq_img)

    logo = client.get_type("AdImageAsset")
    logo.asset = logo_asset
    rda.logo_images.append(logo)

    # Text
    rda.headlines.append(create_ad_text_asset(client, "Best Running Shoes"))
    rda.long_headline.text = "Shop the Best Running Shoes at Unbeatable Prices"
    rda.descriptions.append(create_ad_text_asset(client, "Free shipping. Top brands. Order today."))
    rda.business_name = "Example Store"

    response = service.mutate_ad_group_ads(customer_id=customer_id, operations=[operation])
    return response.results[0].resource_name
```

### 4.3 Media/Asset Upload

Upload images and other media via `AssetService`:

```python
def upload_image_asset(client, customer_id, image_data: bytes, asset_name: str):
    """Upload an image and return the asset resource name."""
    asset_service = client.get_service("AssetService")
    operation = client.get_type("AssetOperation")
    asset = operation.create

    asset.name = asset_name
    asset.type_ = client.enums.AssetTypeEnum.IMAGE
    asset.image_asset.data = image_data
    # image_asset.file_size and mime_type are auto-detected

    response = asset_service.mutate_assets(customer_id=customer_id, operations=[operation])
    return response.results[0].resource_name
```

**curl:**

```bash
# Upload an image asset (base64-encoded)
curl -X POST "https://googleads.googleapis.com/v18/customers/9876543210/assets:mutate" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  -H "developer-token: ${DEVELOPER_TOKEN}" \
  -H "login-customer-id: 1234567890" \
  -H "Content-Type: application/json" \
  -d '{
    "operations": [{
      "create": {
        "name": "hero-image-2026",
        "type": "IMAGE",
        "imageAsset": {
          "data": "BASE64_ENCODED_IMAGE_DATA"
        }
      }
    }]
  }'
```

**Asset types:** `IMAGE`, `MEDIA_BUNDLE`, `YOUTUBE_VIDEO`, `TEXT`, `LEAD_FORM`, `BOOK_ON_GOOGLE`, `CALL`, `CALLOUT`, `SITELINK`, `STRUCTURED_SNIPPET`, `PROMOTION`, `PRICE`

For YouTube videos, reference them by video ID (upload to YouTube first):

```python
def create_youtube_video_asset(client, customer_id, youtube_video_id, asset_name):
    asset_service = client.get_service("AssetService")
    operation = client.get_type("AssetOperation")
    asset = operation.create
    asset.name = asset_name
    asset.type_ = client.enums.AssetTypeEnum.YOUTUBE_VIDEO
    asset.youtube_video_asset.youtube_video_id = youtube_video_id

    response = asset_service.mutate_assets(customer_id=customer_id, operations=[operation])
    return response.results[0].resource_name
```

### 4.4 Performance Max Campaigns

PMax campaigns use **Asset Groups** (not Ad Groups) and let Google automate targeting across all channels (Search, Display, YouTube, Gmail, Discover, Maps).

**Key differences from standard campaigns:**
- No manual keyword targeting — Google controls targeting
- Uses **Audience Signals** (suggestions, not restrictions)
- Assets are organized into **Asset Groups**, not Ad Groups
- Limited reporting granularity
- Requires a **Final URL** per asset group

```python
def create_pmax_campaign(client, customer_id, budget_id):
    campaign_service = client.get_service("CampaignService")
    operation = client.get_type("CampaignOperation")
    campaign = operation.create

    campaign.name = "PMax Campaign - Q1 2026"
    campaign.status = client.enums.CampaignStatusEnum.PAUSED
    campaign.advertising_channel_type = client.enums.AdvertisingChannelTypeEnum.PERFORMANCE_MAX
    campaign.campaign_budget = f"customers/{customer_id}/campaignBudgets/{budget_id}"

    # PMax requires a bidding strategy
    campaign.maximize_conversions.target_cpa_micros = 15_000_000  # $15 target CPA
    # OR: campaign.maximize_conversion_value.target_roas = 3.0

    campaign.start_date = "2026-04-01"
    campaign.url_expansion_opt_out = False  # Allow Google to expand URL targeting

    response = campaign_service.mutate_campaigns(customer_id=customer_id, operations=[operation])
    campaign_resource = response.results[0].resource_name

    # Then create an Asset Group for this campaign
    create_asset_group(client, customer_id, campaign_resource)
    return campaign_resource


def create_asset_group(client, customer_id, campaign_resource):
    asset_group_service = client.get_service("AssetGroupService")
    operation = client.get_type("AssetGroupOperation")
    asset_group = operation.create

    asset_group.name = "Main Asset Group"
    asset_group.campaign = campaign_resource
    asset_group.status = client.enums.AssetGroupStatusEnum.PAUSED

    asset_group.final_urls.append("https://www.example.com")
    asset_group.final_mobile_urls.append("https://m.example.com")

    response = asset_group_service.mutate_asset_groups(
        customer_id=customer_id, operations=[operation]
    )
    return response.results[0].resource_name
```

**Link assets to an Asset Group:**

```python
def link_asset_to_asset_group(client, customer_id, asset_group_resource, asset_resource, field_type):
    """field_type: HEADLINE, DESCRIPTION, LONG_HEADLINE, MARKETING_IMAGE, SQUARE_MARKETING_IMAGE, LOGO, YOUTUBE_VIDEO, etc."""
    service = client.get_service("AssetGroupAssetService")
    operation = client.get_type("AssetGroupAssetOperation")
    aga = operation.create

    aga.asset_group = asset_group_resource
    aga.asset = asset_resource
    aga.field_type = client.enums.AssetFieldTypeEnum.Value(field_type)

    response = service.mutate_asset_group_assets(customer_id=customer_id, operations=[operation])
    return response.results[0].resource_name
```

**PMax asset requirements:**
- At least 1 text asset for HEADLINE (≤30 chars, max 15)
- At least 1 LONG_HEADLINE (≤90 chars, max 5)
- At least 1 DESCRIPTION (≤90 chars, max 5)
- At least 1 MARKETING_IMAGE (landscape, 1200×628)
- At least 1 SQUARE_MARKETING_IMAGE (1200×1200)
- At least 1 LOGO (1200×1200)
- Optional: YOUTUBE_VIDEO, BUSINESS_NAME, CALL_TO_ACTION_SELECTION

### 4.5 Video Ads (YouTube)

Videos must be uploaded to YouTube first, then referenced via `youtube_video_id`.

**Formats:**

| Format | Min Length | Max Length | Skippable | Placement |
|--------|-----------|-----------|-----------|-----------|
| **In-stream (skippable)** | 12s | No max (15s-3min recommended) | After 5s | Before/during/after videos |
| **In-stream (non-skippable)** | 6s | 15s | No | Before/during/after videos |
| **Bumper** | — | 6s | No | Before/during/after videos |
| **In-feed (discovery)** | Any | Any | N/A | YouTube search, related, home feed |
| **Shorts** | — | 60s | Varies | YouTube Shorts feed |

```python
def create_video_ad(client, customer_id, ad_group_id, youtube_video_id):
    service = client.get_service("AdGroupAdService")
    operation = client.get_type("AdGroupAdOperation")
    ad_group_ad = operation.create

    ad_group_ad.status = client.enums.AdGroupAdStatusEnum.PAUSED
    ad_group_ad.ad_group = client.get_service("AdGroupService").ad_group_path(
        customer_id, ad_group_id
    )

    ad = ad_group_ad.ad
    ad.final_urls.append("https://www.example.com")

    # In-stream skippable
    video_ad = ad.video_ad
    video_ad.video.asset = f"customers/{customer_id}/assets/{youtube_video_id}"

    in_stream = video_ad.in_stream
    in_stream.action_button_label = "Learn More"
    in_stream.action_headline = "Premium Running Shoes"

    response = service.mutate_ad_group_ads(customer_id=customer_id, operations=[operation])
    return response.results[0].resource_name
```

### 4.6 Demand Gen Campaigns (formerly Discovery)

Demand Gen campaigns serve across YouTube, Gmail, and Discover feed. Three creative formats:
- **Single image ads** — one image + text
- **Carousel ads** — multiple cards (2–10), each with image + headline + URL
- **Video ads** — YouTube video reference

```python
def create_demand_gen_campaign(client, customer_id, budget_id):
    service = client.get_service("CampaignService")
    operation = client.get_type("CampaignOperation")
    campaign = operation.create

    campaign.name = "Demand Gen - Q1 2026"
    campaign.status = client.enums.CampaignStatusEnum.PAUSED
    campaign.advertising_channel_type = client.enums.AdvertisingChannelTypeEnum.DEMAND_GEN
    campaign.campaign_budget = f"customers/{customer_id}/campaignBudgets/{budget_id}"
    campaign.maximize_conversions.target_cpa_micros = 20_000_000  # $20 CPA

    response = service.mutate_campaigns(customer_id=customer_id, operations=[operation])
    return response.results[0].resource_name
```

**Image specs for Demand Gen:**
- Landscape: 1200×628 (min 600×314)
- Square: 1200×1200 (min 300×300)
- Portrait: 960×1200 (min 480×600) — optional, recommended
- Max file size: 5MB
- Formats: JPG, PNG

### 4.7 Creating the Full Chain — Campaign → Ad Group → Ad

```python
import time

def create_full_search_campaign(client, customer_id):
    """Create a complete Search campaign with budget, ad group, keywords, and ad."""

    # 1. Create Campaign Budget
    budget_service = client.get_service("CampaignBudgetService")
    budget_op = client.get_type("CampaignBudgetOperation")
    budget = budget_op.create
    budget.name = f"Budget {int(time.time())}"
    budget.amount_micros = 50_000_000  # $50/day
    budget.delivery_method = client.enums.BudgetDeliveryMethodEnum.STANDARD

    budget_response = budget_service.mutate_campaign_budgets(
        customer_id=customer_id, operations=[budget_op]
    )
    budget_resource = budget_response.results[0].resource_name

    # 2. Create Campaign
    campaign_service = client.get_service("CampaignService")
    campaign_op = client.get_type("CampaignOperation")
    campaign = campaign_op.create
    campaign.name = f"Search Campaign {int(time.time())}"
    campaign.status = client.enums.CampaignStatusEnum.PAUSED
    campaign.advertising_channel_type = client.enums.AdvertisingChannelTypeEnum.SEARCH
    campaign.campaign_budget = budget_resource
    campaign.maximize_clicks.cpc_bid_ceiling_micros = 5_000_000  # $5 max CPC
    campaign.network_settings.target_google_search = True
    campaign.network_settings.target_search_network = False

    campaign_response = campaign_service.mutate_campaigns(
        customer_id=customer_id, operations=[campaign_op]
    )
    campaign_resource = campaign_response.results[0].resource_name

    # 3. Create Ad Group
    ad_group_service = client.get_service("AdGroupService")
    ag_op = client.get_type("AdGroupOperation")
    ag = ag_op.create
    ag.name = f"Ad Group {int(time.time())}"
    ag.campaign = campaign_resource
    ag.status = client.enums.AdGroupStatusEnum.ENABLED
    ag.type_ = client.enums.AdGroupTypeEnum.SEARCH_STANDARD
    ag.cpc_bid_micros = 1_000_000  # $1 default bid

    ag_response = ad_group_service.mutate_ad_groups(
        customer_id=customer_id, operations=[ag_op]
    )
    ad_group_resource = ag_response.results[0].resource_name
    ad_group_id = ad_group_resource.split("/")[-1]

    # 4. Add Keywords
    criterion_service = client.get_service("AdGroupCriterionService")
    keywords = [
        ("running shoes", "EXACT"),
        ("best running shoes", "PHRASE"),
        ("buy running shoes online", "BROAD"),
    ]
    kw_ops = []
    for text, match_type in keywords:
        kw_op = client.get_type("AdGroupCriterionOperation")
        criterion = kw_op.create
        criterion.ad_group = ad_group_resource
        criterion.status = client.enums.AdGroupCriterionStatusEnum.ENABLED
        criterion.keyword.text = text
        criterion.keyword.match_type = client.enums.KeywordMatchTypeEnum.Value(match_type)
        kw_ops.append(kw_op)

    criterion_service.mutate_ad_group_criteria(
        customer_id=customer_id, operations=kw_ops
    )

    # 5. Create RSA
    create_responsive_search_ad(client, customer_id, ad_group_id)

    return campaign_resource
```

---

## 5. Campaign Management

### 5.1 Campaign Types

| Type | Enum Value | Description |
|------|-----------|-------------|
| **Search** | `SEARCH` | Text ads on Google Search and search partners |
| **Display** | `DISPLAY` | Image/responsive ads across Google Display Network |
| **Video** | `VIDEO` | Video ads on YouTube and video partners |
| **Performance Max** | `PERFORMANCE_MAX` | AI-driven across all Google channels |
| **Demand Gen** | `DEMAND_GEN` | YouTube, Gmail, Discover feed (formerly Discovery) |
| **Shopping** | `SHOPPING` | Product listing ads (requires Merchant Center) |
| **Local** | `LOCAL` | Drive visits to physical locations |
| **Smart** | `SMART` | Automated campaigns for small businesses |

### 5.2 Bidding Strategies

| Strategy | Works With | Description |
|----------|-----------|-------------|
| **Manual CPC** | Search, Display | Set bids manually per keyword/placement |
| **Enhanced CPC** | Search, Display | Manual CPC + Google adjusts bids for likely converters |
| **Maximize Clicks** | Search, Display | Auto-bid to get most clicks within budget |
| **Maximize Conversions** | Search, Display, PMax, Demand Gen | Auto-bid for most conversions |
| **Target CPA** | Search, Display, PMax, Demand Gen | Auto-bid targeting a cost per conversion |
| **Target ROAS** | Search, Display, Shopping, PMax | Auto-bid targeting a return on ad spend |
| **Maximize Conversion Value** | Search, PMax | Auto-bid for highest total conversion value |

```python
def create_campaign_with_bidding(client, customer_id, budget_resource, strategy="maximize_conversions"):
    service = client.get_service("CampaignService")
    operation = client.get_type("CampaignOperation")
    campaign = operation.create

    campaign.name = "My Campaign"
    campaign.status = client.enums.CampaignStatusEnum.PAUSED
    campaign.advertising_channel_type = client.enums.AdvertisingChannelTypeEnum.SEARCH
    campaign.campaign_budget = budget_resource

    if strategy == "manual_cpc":
        campaign.manual_cpc.enhanced_cpc_enabled = True
    elif strategy == "maximize_clicks":
        campaign.maximize_clicks.cpc_bid_ceiling_micros = 3_000_000  # $3 cap
    elif strategy == "maximize_conversions":
        campaign.maximize_conversions.target_cpa_micros = 0  # No target, just maximize
    elif strategy == "target_cpa":
        campaign.maximize_conversions.target_cpa_micros = 15_000_000  # $15 target CPA
    elif strategy == "target_roas":
        campaign.maximize_conversion_value.target_roas = 4.0  # 400% ROAS target

    response = service.mutate_campaigns(customer_id=customer_id, operations=[operation])
    return response.results[0].resource_name
```

### 5.3 Budget Settings

```python
def create_campaign_budget(client, customer_id, daily_budget_dollars, shared=False):
    service = client.get_service("CampaignBudgetService")
    operation = client.get_type("CampaignBudgetOperation")
    budget = operation.create

    budget.name = f"Budget ${daily_budget_dollars}/day"
    budget.amount_micros = int(daily_budget_dollars * 1_000_000)
    budget.delivery_method = client.enums.BudgetDeliveryMethodEnum.STANDARD
    budget.explicitly_shared = shared  # True = can be shared across campaigns

    response = service.mutate_campaign_budgets(customer_id=customer_id, operations=[operation])
    return response.results[0].resource_name
```

### 5.4 Targeting

#### Geographic Targeting

```python
def add_location_targeting(client, customer_id, campaign_id, geo_target_constant_id):
    """geo_target_constant_id: e.g., 2840 for United States, 1014221 for San Francisco"""
    service = client.get_service("CampaignCriterionService")
    operation = client.get_type("CampaignCriterionOperation")
    criterion = operation.create

    criterion.campaign = f"customers/{customer_id}/campaigns/{campaign_id}"
    criterion.location.geo_target_constant = (
        f"geoTargetConstants/{geo_target_constant_id}"
    )

    response = service.mutate_campaign_criteria(customer_id=customer_id, operations=[operation])
    return response.results[0].resource_name
```

#### Keyword Targeting (Ad Group Level)

Match types: `BROAD`, `PHRASE`, `EXACT`

```python
def add_keywords(client, customer_id, ad_group_resource, keywords):
    """keywords: list of (text, match_type) tuples"""
    service = client.get_service("AdGroupCriterionService")
    operations = []
    for text, match_type in keywords:
        op = client.get_type("AdGroupCriterionOperation")
        criterion = op.create
        criterion.ad_group = ad_group_resource
        criterion.status = client.enums.AdGroupCriterionStatusEnum.ENABLED
        criterion.keyword.text = text
        criterion.keyword.match_type = client.enums.KeywordMatchTypeEnum.Value(match_type)
        operations.append(op)

    response = service.mutate_ad_group_criteria(
        customer_id=customer_id, operations=operations, partial_failure=True
    )
    return response
```

#### Negative Keywords

```python
def add_negative_keyword(client, customer_id, campaign_id, keyword_text):
    service = client.get_service("CampaignCriterionService")
    operation = client.get_type("CampaignCriterionOperation")
    criterion = operation.create

    criterion.campaign = f"customers/{customer_id}/campaigns/{campaign_id}"
    criterion.negative = True
    criterion.keyword.text = keyword_text
    criterion.keyword.match_type = client.enums.KeywordMatchTypeEnum.BROAD

    response = service.mutate_campaign_criteria(customer_id=customer_id, operations=[operation])
    return response.results[0].resource_name
```

### 5.5 Ad Schedule (Dayparting)

```python
def add_ad_schedule(client, customer_id, campaign_id, day_of_week, start_hour, end_hour):
    """Schedule ads to run only during specific hours on specific days."""
    service = client.get_service("CampaignCriterionService")
    operation = client.get_type("CampaignCriterionOperation")
    criterion = operation.create

    criterion.campaign = f"customers/{customer_id}/campaigns/{campaign_id}"
    criterion.ad_schedule.day_of_week = client.enums.DayOfWeekEnum.Value(day_of_week)
    criterion.ad_schedule.start_hour = start_hour
    criterion.ad_schedule.start_minute = client.enums.MinuteOfHourEnum.ZERO
    criterion.ad_schedule.end_hour = end_hour
    criterion.ad_schedule.end_minute = client.enums.MinuteOfHourEnum.ZERO

    response = service.mutate_campaign_criteria(customer_id=customer_id, operations=[operation])
    return response.results[0].resource_name
```

### 5.6 Enable / Pause / Remove

```python
def update_campaign_status(client, customer_id, campaign_id, new_status):
    """new_status: 'ENABLED', 'PAUSED', or 'REMOVED'"""
    service = client.get_service("CampaignService")
    operation = client.get_type("CampaignOperation")
    campaign = operation.update

    campaign.resource_name = f"customers/{customer_id}/campaigns/{campaign_id}"
    campaign.status = client.enums.CampaignStatusEnum.Value(new_status)

    field_mask = client.get_type("FieldMask")
    field_mask.paths.append("status")
    client.copy_from(operation.update_mask, field_mask)

    response = service.mutate_campaigns(customer_id=customer_id, operations=[operation])
    return response.results[0].resource_name
```

**Status lifecycle:** `ENABLED` ↔ `PAUSED` → `REMOVED` (irreversible). Removed campaigns are never truly deleted — filter with `status != 'REMOVED'` in queries.

### 5.7 Experiments and Drafts

Create A/B tests by creating a campaign experiment:

```python
def create_experiment(client, customer_id, base_campaign_id, experiment_name, traffic_split_percent=50):
    experiment_service = client.get_service("ExperimentService")
    operation = client.get_type("ExperimentOperation")
    experiment = operation.create

    experiment.name = experiment_name
    experiment.type_ = client.enums.ExperimentTypeEnum.SEARCH_CUSTOM
    experiment.status = client.enums.ExperimentStatusEnum.SETUP
    experiment.suffix = "[Experiment]"

    # Create experiment arm for the control
    arm_service = client.get_service("ExperimentArmService")
    control_op = client.get_type("ExperimentArmOperation")
    control = control_op.create
    control.trial = experiment.resource_name
    control.name = "Control"
    control.control = True
    control.traffic_split = 100 - traffic_split_percent
    control.campaigns.append(f"customers/{customer_id}/campaigns/{base_campaign_id}")

    response = experiment_service.mutate_experiments(
        customer_id=customer_id, operations=[operation]
    )
    return response.results[0].resource_name
```

---

## 6. Lead Form Extensions

### 6.1 Creating Lead Form Assets

```python
def create_lead_form_asset(client, customer_id):
    asset_service = client.get_service("AssetService")
    operation = client.get_type("AssetOperation")
    asset = operation.create

    asset.name = "Contact Us Lead Form"
    lead_form = asset.lead_form_asset

    lead_form.call_to_action_type = client.enums.LeadFormCallToActionTypeEnum.LEARN_MORE
    lead_form.call_to_action_description = "Get a free consultation"
    lead_form.business_name = "PaidEdge"
    lead_form.headline = "Request a Free Demo"
    lead_form.description = "Fill out the form and our team will contact you within 24 hours."
    lead_form.privacy_policy_url = "https://www.paidedge.com/privacy"

    # Standard fields
    fields = [
        client.enums.LeadFormFieldUserInputTypeEnum.FULL_NAME,
        client.enums.LeadFormFieldUserInputTypeEnum.EMAIL,
        client.enums.LeadFormFieldUserInputTypeEnum.PHONE_NUMBER,
        client.enums.LeadFormFieldUserInputTypeEnum.COMPANY_NAME,
        client.enums.LeadFormFieldUserInputTypeEnum.JOB_TITLE,
    ]
    for field_type in fields:
        field = client.get_type("LeadFormField")
        field.input_type = field_type
        lead_form.fields.append(field)

    # Custom question (optional)
    custom_q = client.get_type("LeadFormCustomQuestionField")
    custom_q.custom_question_text = "What is your monthly ad spend?"
    custom_q.single_choice_answers.answers.extend([
        "Less than $5,000",
        "$5,000 - $25,000",
        "$25,000 - $100,000",
        "More than $100,000",
    ])
    lead_form.custom_question_fields.append(custom_q)

    # Post-submit content
    lead_form.post_submit_headline = "Thank you!"
    lead_form.post_submit_description = "We'll be in touch within 24 hours."
    lead_form.post_submit_call_to_action_type = client.enums.LeadFormPostSubmitCallToActionTypeEnum.VISIT_SITE

    # Delivery: webhook URL for real-time notifications
    delivery = client.get_type("LeadFormDeliveryMethod")
    delivery.webhook.advertiser_webhook_url = "https://api.paidedge.com/webhooks/google-ads/leads"
    delivery.webhook.google_secret = "your_webhook_secret"
    lead_form.delivery_methods.append(delivery)

    response = asset_service.mutate_assets(customer_id=customer_id, operations=[operation])
    return response.results[0].resource_name
```

### 6.2 Linking Lead Form to a Campaign

```python
def link_lead_form_to_campaign(client, customer_id, campaign_id, lead_form_asset_resource):
    service = client.get_service("CampaignAssetService")
    operation = client.get_type("CampaignAssetOperation")
    campaign_asset = operation.create

    campaign_asset.campaign = f"customers/{customer_id}/campaigns/{campaign_id}"
    campaign_asset.asset = lead_form_asset_resource
    campaign_asset.field_type = client.enums.AssetFieldTypeEnum.LEAD_FORM

    response = service.mutate_campaign_assets(customer_id=customer_id, operations=[operation])
    return response.results[0].resource_name
```

### 6.3 Pulling Submissions

**Via GAQL:**

```sql
SELECT
  lead_form_submission_data.id,
  lead_form_submission_data.asset,
  lead_form_submission_data.campaign,
  lead_form_submission_data.ad_group,
  lead_form_submission_data.gclid,
  lead_form_submission_data.submission_date_time,
  lead_form_submission_data.lead_form_submission_fields
FROM lead_form_submission_data
WHERE lead_form_submission_data.submission_date_time >= '2026-03-01'
ORDER BY lead_form_submission_data.submission_date_time DESC
```

**Via Webhook:** Configure a webhook URL in the lead form asset (shown above). Google sends POST requests with submission data in near-real-time.

**Submission data retention:** Google retains lead form submission data for **30 days**. Download submissions regularly or use webhooks for real-time delivery.

---

## 7. Conversion Tracking

### 7.1 Conversion Actions

```python
def create_conversion_action(client, customer_id):
    service = client.get_service("ConversionActionService")
    operation = client.get_type("ConversionActionOperation")
    action = operation.create

    action.name = "My Purchase Conversion"
    action.type_ = client.enums.ConversionActionTypeEnum.UPLOAD_CLICKS
    action.category = client.enums.ConversionActionCategoryEnum.PURCHASE
    action.status = client.enums.ConversionActionStatusEnum.ENABLED

    # Attribution model
    action.attribution_model_settings.attribution_model = (
        client.enums.AttributionModelEnum.GOOGLE_ADS_LAST_CLICK
    )

    # Counting: ONE_PER_CLICK for leads, MANY_PER_CLICK for purchases
    action.counting_type = client.enums.ConversionActionCountingTypeEnum.ONE_PER_CLICK
    action.click_through_lookback_window_days = 30
    action.view_through_lookback_window_days = 1
    action.value_settings.default_value = 50.0
    action.value_settings.always_use_default_value = False

    response = service.mutate_conversion_actions(customer_id=customer_id, operations=[operation])
    return response.results[0].resource_name
```

**curl:**

```bash
curl -X POST "https://googleads.googleapis.com/v18/customers/1234567890/conversionActions:mutate" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  -H "developer-token: ${DEVELOPER_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "operations": [{
      "create": {
        "name": "My Purchase Conversion",
        "type": "UPLOAD_CLICKS",
        "category": "PURCHASE",
        "status": "ENABLED",
        "countingType": "ONE_PER_CLICK",
        "clickThroughLookbackWindowDays": 30,
        "attributionModelSettings": {"attributionModel": "GOOGLE_ADS_LAST_CLICK"},
        "valueSettings": {"defaultValue": 50.0, "alwaysUseDefaultValue": false}
      }
    }]
  }'
```

### 7.2 Attribution Models

| Model | Status |
|-------|--------|
| **Data-Driven** | Recommended, default for new actions |
| **Last Click** | Available |
| **First Click** | **Deprecated/Removed** |
| **Linear** | **Deprecated/Removed** |
| **Time Decay** | **Deprecated/Removed** |
| **Position-Based** | **Deprecated/Removed** |

> Rule-based models were deprecated and migrated to data-driven. Use `DATA_DRIVEN` or `GOOGLE_ADS_LAST_CLICK`.

### 7.3 Offline Conversion Import (Click Conversions)

```python
def upload_click_conversions(client, customer_id, conversion_action_id,
                              gclid, conversion_date_time, conversion_value):
    service = client.get_service("ConversionUploadService")
    click_conversion = client.get_type("ClickConversion")

    click_conversion.conversion_action = (
        f"customers/{customer_id}/conversionActions/{conversion_action_id}"
    )
    click_conversion.gclid = gclid
    click_conversion.conversion_date_time = conversion_date_time  # "yyyy-mm-dd hh:mm:ss+|-hh:mm"
    click_conversion.conversion_value = conversion_value
    click_conversion.currency_code = "USD"

    response = service.upload_click_conversions(
        customer_id=customer_id,
        conversions=[click_conversion],
        partial_failure=True,
    )

    if response.partial_failure_error:
        print(f"Partial failure: {response.partial_failure_error.message}")
    else:
        for result in response.results:
            print(f"Uploaded conversion for gclid={result.gclid}")
```

**curl:**

```bash
curl -X POST \
  "https://googleads.googleapis.com/v18/customers/1234567890:uploadClickConversions" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  -H "developer-token: ${DEVELOPER_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "conversions": [{
      "gclid": "CjwKCAjw...",
      "conversionAction": "customers/1234567890/conversionActions/987654321",
      "conversionDateTime": "2026-03-20 14:30:00-05:00",
      "conversionValue": 125.50,
      "currencyCode": "USD"
    }],
    "partialFailure": true
  }'
```

### 7.4 Call Conversions

```python
def upload_call_conversion(client, customer_id, conversion_action_id,
                            caller_id, call_start_date_time,
                            conversion_date_time, conversion_value):
    service = client.get_service("ConversionUploadService")
    call_conversion = client.get_type("CallConversion")

    call_conversion.conversion_action = f"customers/{customer_id}/conversionActions/{conversion_action_id}"
    call_conversion.caller_id = caller_id  # E.164 format: "+14155551234"
    call_conversion.call_start_date_time = call_start_date_time
    call_conversion.conversion_date_time = conversion_date_time
    call_conversion.conversion_value = conversion_value
    call_conversion.currency_code = "USD"

    response = service.upload_call_conversions(
        customer_id=customer_id, conversions=[call_conversion], partial_failure=True
    )
    return response
```

### 7.5 Enhanced Conversions

Improves attribution by matching hashed first-party data to Google users:

```python
import hashlib

def normalize_and_hash(value):
    return hashlib.sha256(value.strip().lower().encode("utf-8")).hexdigest()

def upload_enhanced_conversion(client, customer_id, conversion_action_id,
                                 order_id, conversion_date_time,
                                 conversion_value, user_email, user_phone=None):
    service = client.get_service("ConversionUploadService")
    click_conversion = client.get_type("ClickConversion")

    click_conversion.conversion_action = f"customers/{customer_id}/conversionActions/{conversion_action_id}"
    click_conversion.conversion_date_time = conversion_date_time
    click_conversion.conversion_value = conversion_value
    click_conversion.currency_code = "USD"
    click_conversion.order_id = order_id

    uid_email = client.get_type("UserIdentifier")
    uid_email.hashed_email = normalize_and_hash(user_email)
    uid_email.user_identifier_source = client.enums.UserIdentifierSourceEnum.FIRST_PARTY
    click_conversion.user_identifiers.append(uid_email)

    if user_phone:
        uid_phone = client.get_type("UserIdentifier")
        uid_phone.hashed_phone_number = normalize_and_hash(user_phone)
        uid_phone.user_identifier_source = client.enums.UserIdentifierSourceEnum.FIRST_PARTY
        click_conversion.user_identifiers.append(uid_phone)

    response = service.upload_click_conversions(
        customer_id=customer_id, conversions=[click_conversion], partial_failure=True
    )
    return response
```

### 7.6 Consent Mode

```python
# Set consent on each conversion upload
click_conversion.consent.ad_user_data = client.enums.ConsentStatusEnum.GRANTED
click_conversion.consent.ad_personalization = client.enums.ConsentStatusEnum.GRANTED
```

| Consent Field | Effect When `DENIED` |
|---------------|---------------------|
| `ad_user_data` | Data not linked to Google user accounts |
| `ad_personalization` | Data not used for remarketing |

> **EEA/UK requirement:** Accurate consent signals are mandatory. `DENIED` triggers conversion modeling instead of direct attribution.

### 7.7 Server-Side vs Client-Side Tagging

| Approach | When to Use |
|----------|------------|
| **Client-side (gtag.js / GTM)** | Standard web tracking, captures gclid automatically |
| **Server-side (sGTM)** | Data control, reduced client JS, first-party cookies |
| **API-only (ConversionUploadService)** | Offline conversions, CRM events, call center outcomes |

For PaidEdge: capture `gclid` client-side, upload conversions server-side via API when the offline event occurs.

---

## 8. Reporting — GAQL (Google Ads Query Language)

### 8.1 Syntax

```sql
SELECT field1, field2, metric1, metric2
FROM resource
WHERE condition1 AND condition2
ORDER BY metric1 DESC
LIMIT 100
PARAMETERS include_drafts=true
```

**Rules:**
- `SELECT` and `FROM` required. Exactly one resource.
- `WHERE`, `ORDER BY`, `LIMIT`, `PARAMETERS` optional.
- No JOINs. Each query reads one resource.
- Cannot mix incompatible segments and metrics.

### 8.2 Key Resources

| Resource | Description |
|----------|------------|
| `campaign` | Campaign-level settings and metrics |
| `ad_group` | Ad group-level settings and metrics |
| `ad_group_ad` | Individual ads and their performance |
| `keyword_view` | Keyword-level performance metrics |
| `search_term_view` | Actual search terms that triggered ads |
| `geographic_view` | Performance by location |
| `campaign_audience_view` | Audience performance at campaign level |
| `landing_page_view` | Performance by landing page |
| `change_status` | Change history |

### 8.3 Common Metrics

| Metric | Description |
|--------|------------|
| `metrics.impressions` | Times ad was shown |
| `metrics.clicks` | Clicks |
| `metrics.cost_micros` | Cost in micros (**divide by 1,000,000**) |
| `metrics.conversions` | Conversions |
| `metrics.conversions_value` | Total conversion value |
| `metrics.all_conversions` | All conversions including cross-device |
| `metrics.ctr` | Click-through rate |
| `metrics.average_cpc` | Average CPC (in micros) |
| `metrics.average_cpm` | Average CPM (in micros) |
| `metrics.cost_per_conversion` | Cost per conversion (in micros) |
| `metrics.video_views` | Video views |
| `metrics.search_impression_share` | Search impression share |

> **Critical:** `cost_micros` of `5230000` = **$5.23**. Always divide by 1,000,000.

### 8.4 Segments

| Segment | Description |
|---------|------------|
| `segments.date` | Day-level breakout |
| `segments.device` | DESKTOP, MOBILE, TABLET |
| `segments.ad_network_type` | SEARCH, CONTENT, YOUTUBE_WATCH, etc. |
| `segments.conversion_action` | By specific conversion action |
| `segments.day_of_week` | MONDAY through SUNDAY |
| `segments.hour` | Hour of day (0–23) |

### 8.5 Date Filtering

```sql
-- Specific range
WHERE segments.date BETWEEN '2026-03-01' AND '2026-03-25'

-- Predefined ranges
WHERE segments.date DURING LAST_30_DAYS
WHERE segments.date DURING LAST_7_DAYS
WHERE segments.date DURING THIS_MONTH
WHERE segments.date DURING LAST_MONTH
WHERE segments.date DURING TODAY
WHERE segments.date DURING YESTERDAY
```

> When using `segments.date` in WHERE, you must also SELECT it.

### 8.6 Common Report Queries

#### Campaign Performance (Last 30 Days)

```sql
SELECT campaign.id, campaign.name, campaign.status,
       metrics.impressions, metrics.clicks, metrics.ctr,
       metrics.cost_micros, metrics.conversions, metrics.cost_per_conversion
FROM campaign
WHERE segments.date DURING LAST_30_DAYS AND campaign.status != 'REMOVED'
ORDER BY metrics.cost_micros DESC
LIMIT 50
```

#### Daily Spend by Campaign

```sql
SELECT segments.date, campaign.id, campaign.name,
       metrics.cost_micros, metrics.impressions, metrics.clicks, metrics.conversions
FROM campaign
WHERE segments.date BETWEEN '2026-03-01' AND '2026-03-25' AND campaign.status = 'ENABLED'
ORDER BY segments.date DESC, metrics.cost_micros DESC
```

#### Keyword Performance

```sql
SELECT ad_group.name, ad_group_criterion.keyword.text,
       ad_group_criterion.keyword.match_type,
       metrics.impressions, metrics.clicks, metrics.cost_micros,
       metrics.conversions, metrics.average_cpc
FROM keyword_view
WHERE segments.date BETWEEN '2026-03-01' AND '2026-03-25'
ORDER BY metrics.impressions DESC
LIMIT 100
```

#### Search Terms Report

```sql
SELECT campaign.name, ad_group.name, search_term_view.search_term,
       search_term_view.status, metrics.impressions, metrics.clicks,
       metrics.cost_micros, metrics.conversions
FROM search_term_view
WHERE segments.date DURING LAST_30_DAYS AND metrics.impressions > 10
ORDER BY metrics.cost_micros DESC
LIMIT 200
```

#### Device Performance

```sql
SELECT campaign.name, segments.device,
       metrics.impressions, metrics.clicks, metrics.ctr,
       metrics.cost_micros, metrics.conversions
FROM campaign
WHERE segments.date DURING LAST_7_DAYS AND campaign.status = 'ENABLED'
ORDER BY metrics.cost_micros DESC
```

#### Geographic Performance

```sql
SELECT campaign.name, geographic_view.country_criterion_id,
       geographic_view.location_type, metrics.impressions, metrics.clicks,
       metrics.cost_micros, metrics.conversions
FROM geographic_view
WHERE segments.date DURING LAST_30_DAYS
ORDER BY metrics.impressions DESC
LIMIT 100
```

### 8.7 `search` vs `search_stream`

| Method | Behavior | Best For |
|--------|----------|---------|
| `GoogleAdsService.Search` | Pages of up to 10,000 rows | Small queries, specific page needed |
| `GoogleAdsService.SearchStream` | Streams all results in one request | **Large result sets — use this for almost everything** |

```python
# Streaming (recommended)
def report_streaming(client, customer_id, query):
    ga_service = client.get_service("GoogleAdsService")
    stream = ga_service.search_stream(customer_id=customer_id, query=query)
    results = []
    for batch in stream:
        for row in batch.results:
            results.append(row)
    return results

# Paginated
def report_paginated(client, customer_id, query):
    ga_service = client.get_service("GoogleAdsService")
    response = ga_service.search(customer_id=customer_id, query=query)
    return list(response)  # auto-paginates
```

**curl (streaming):**

```bash
curl -X POST \
  "https://googleads.googleapis.com/v18/customers/1234567890/googleAds:searchStream" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  -H "developer-token: ${DEVELOPER_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT campaign.id, campaign.name, metrics.cost_micros FROM campaign WHERE segments.date DURING LAST_30_DAYS"}'
```

---

## 9. Rate Limits

### 9.1 Access Levels

| Limit | Basic Access | Standard Access |
|-------|-------------|-----------------|
| **Mutate operations/day** | 15,000 | 500,000+ |
| **Report requests/day** | 1,000 | 15,000+ |
| **Max operations per mutate request** | 5,000 | 5,000 |
| **Max rows per search page** | 10,000 | 10,000 |

> **Basic access is a hard wall for multi-tenant SaaS.** 15K ops/day across 10 accounts is only 1,500 ops per account. Apply for standard access early.

### 9.2 Quota Reset

- Resets at **midnight Pacific Time** daily
- Per **developer token**, not per OAuth client or customer ID
- All MCC accounts under the same dev token share quota

### 9.3 Applying for Standard Access

1. App must comply with Google Ads API ToS
2. Submit application via Google API Console
3. Review takes **1–2 weeks**
4. Must demonstrate a production app

### 9.4 Retry Strategy

```python
import time
import random
from google.ads.googleads.errors import GoogleAdsException

def execute_with_retry(func, max_retries=5, base_delay=1.0):
    for attempt in range(max_retries):
        try:
            return func()
        except GoogleAdsException as ex:
            is_rate_limit = any(
                error.error_code.HasField("quota_error")
                for error in ex.failure.errors
            )
            if not is_rate_limit or attempt == max_retries - 1:
                raise
            delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
            print(f"Rate limited. Retrying in {delay:.1f}s (attempt {attempt + 1})")
            time.sleep(delay)
```

### 9.5 Best Practices

1. **Batch operations** — up to 5,000 per request
2. **Use `search_stream`** — one request vs many paginated
3. **Cache results** — don't re-query unchanged data
4. **Use change events** — `ChangeStatusService` to detect changes vs polling
5. **Use `BatchJobService`** — for tens of thousands of mutations
6. **Off-peak scheduling** — run bulk ops during off-peak hours

---

## 10. Python SDK

### 10.1 Installation

```bash
pip install google-ads==25.1.0  # Pin version in production
# google-ads 25.x → API v18
```

### 10.2 YAML Configuration

```yaml
developer_token: "YOUR_DEVELOPER_TOKEN"
client_id: "YOUR_CLIENT_ID.apps.googleusercontent.com"
client_secret: "YOUR_CLIENT_SECRET"
refresh_token: "YOUR_REFRESH_TOKEN"
login_customer_id: "1234567890"
use_proto_plus: true
```

### 10.3 Client Initialization

```python
from google.ads.googleads.client import GoogleAdsClient

# From YAML
client = GoogleAdsClient.load_from_storage("google-ads.yaml")

# From dict (best for multi-tenant)
client = GoogleAdsClient.load_from_dict({
    "developer_token": "...",
    "client_id": "...",
    "client_secret": "...",
    "refresh_token": "...",
    "login_customer_id": "1234567890",
    "use_proto_plus": True,
})

# From environment variables
# GOOGLE_ADS_DEVELOPER_TOKEN, GOOGLE_ADS_CLIENT_ID, etc.
client = GoogleAdsClient.load_from_env()
```

### 10.4 Field Masks for Updates

When updating, you must specify which fields are changing:

```python
from google.api_core import protobuf_helpers

def update_campaign_name(client, customer_id, campaign_id, new_name):
    service = client.get_service("CampaignService")
    operation = client.get_type("CampaignOperation")
    campaign = operation.update

    campaign.resource_name = f"customers/{customer_id}/campaigns/{campaign_id}"
    campaign.name = new_name

    client.copy_from(
        operation.update_mask,
        protobuf_helpers.field_mask(None, campaign._pb),
    )

    response = service.mutate_campaigns(customer_id=customer_id, operations=[operation])
    return response.results[0].resource_name
```

### 10.5 Error Handling

```python
from google.ads.googleads.errors import GoogleAdsException

try:
    response = ga_service.search_stream(customer_id=customer_id, query=query)
    for batch in response:
        for row in batch.results:
            print(row.campaign.name)
except GoogleAdsException as ex:
    print(f"Request failed: {ex.error.code().name}")
    print(f"Request ID: {ex.request_id}")
    for error in ex.failure.errors:
        print(f"  Error: {error.error_code}")
        print(f"  Message: {error.message}")
        if error.location:
            for path in error.location.field_path_elements:
                print(f"    Field: {path.field_name}, Index: {path.index}")
```

### 10.6 Partial Failures

```python
# Always set partial_failure=True for batch operations
response = service.mutate_campaigns(
    customer_id=customer_id, operations=operations, partial_failure=True
)

if response.partial_failure_error:
    print(f"Some operations failed: {response.partial_failure_error.message}")
    for i, result in enumerate(response.results):
        if not result.resource_name:
            print(f"  Operation {i} FAILED")
        else:
            print(f"  Operation {i} OK: {result.resource_name}")
```

---

## 11. Common Gotchas

### 11.1 Developer Token Approval Takes Time

- Basic access is instant. Standard access takes **1–2 weeks**.
- Plan your timeline accordingly. Build with basic access, don't launch without standard.

### 11.2 Basic Access — The 15K Wall

15,000 operations/day is shared across your entire developer token. For multi-tenant SaaS, this is exhausted almost immediately. **Apply for standard access as early as possible.**

### 11.3 Performance Max Opacity

- No keyword-level reporting
- No ad group control (uses asset groups)
- Limited search term visibility
- Cannot see which individual asset drove conversions
- Limited placement/audience exclusions

Set expectations with stakeholders that PMax data is less granular.

### 11.4 Policy Review Delays

New ads go through policy review (hours to 1 business day). `ad_group_ad.policy_summary.approval_status = 'UNDER_REVIEW'` is normal — don't treat it as an error.

### 11.5 Disapproval Reasons

```sql
SELECT ad_group_ad.ad.id, ad_group_ad.policy_summary.approval_status,
       ad_group_ad.policy_summary.policy_topic_entries
FROM ad_group_ad
WHERE ad_group_ad.policy_summary.approval_status = 'DISAPPROVED'
```

Appeals are via the Google Ads UI, not the API.

### 11.6 Keyword Match Type Changes

Broad match now matches related searches, synonyms, and intent variations aggressively. `running shoes` can match `best sneakers for jogging`. Monitor search terms reports.

### 11.7 Currency — Everything is Micros

| Micros | Actual |
|--------|--------|
| `5000000` | $5.00 |
| `1500000` | $1.50 |
| `250000` | $0.25 |

```python
cost_dollars = row.metrics.cost_micros / 1_000_000
target_cpa_micros = int(desired_cpa * 1_000_000)
```

> If your dashboard shows a CPA of $2,340,000, you forgot to divide.

### 11.8 Resource Name Format

```
customers/{customer_id}/campaigns/{campaign_id}
customers/{customer_id}/adGroups/{ad_group_id}
customers/{customer_id}/adGroupAds/{ad_group_id}~{ad_id}       # ~ separator for composites
customers/{customer_id}/adGroupCriteria/{ad_group_id}~{criterion_id}
```

**Common mistakes:**
- Using `~` vs `/` incorrectly for composite resources
- Including dashes in customer IDs (`123-456-7890` → must be `1234567890`)

```python
# Use helper methods to build resource names
service = client.get_service("GoogleAdsService")
campaign_resource = service.campaign_path("1234567890", "111111111")
# → "customers/1234567890/campaigns/111111111"
```

### 11.9 Non-Atomic Mutates

- **Without `partial_failure`:** One bad operation fails the entire request
- **With `partial_failure=True`:** Valid operations succeed, bad ones fail individually

> **Always use `partial_failure=True` for batch operations.** Without it, 1 bad operation out of 100 kills all 100.

### 11.10 Search Terms ≠ Keywords

| Report | Resource | Shows |
|--------|----------|-------|
| **Keyword Report** | `keyword_view` | Your bidded keywords and their metrics |
| **Search Terms Report** | `search_term_view` | Actual user queries that triggered your ads |

One keyword triggers many search terms. The search terms report is essential for negative keyword discovery and understanding user intent.

### 11.11 Additional Gotchas

- **`login-customer-id` confusion:** Set to MCC ID, not client ID. Getting these backwards causes `PERMISSION_DENIED`.
- **Removed entities persist:** Campaigns/ads set to `REMOVED` are never truly deleted. Always filter `status != 'REMOVED'`.
- **Zero-impression rows:** GAQL queries don't return rows with zero impressions by default.
- **Enum values in GAQL:** Status filters use quoted strings: `WHERE campaign.status = 'ENABLED'`

---

## Quick Reference

### Required Headers (Every REST Request)

```
Authorization: Bearer {access_token}
developer-token: {developer_token}
login-customer-id: {mcc_customer_id}    (no hyphens)
Content-Type: application/json
```

### Key Endpoints

| Component | Endpoint |
|---|---|
| OAuth Authorization | `https://accounts.google.com/o/oauth2/v2/auth` |
| Token Exchange | `https://oauth2.googleapis.com/token` |
| Google Ads API Base | `https://googleads.googleapis.com/v18/` |

### Python SDK

```bash
pip install google-ads>=25.0.0
```

---

*This reference covers the complete Google Ads API surface needed for PaidEdge's multi-tenant integration. Always check the [Google Ads API release notes](https://developers.google.com/google-ads/api/docs/release-notes) for the latest version and breaking changes.*
