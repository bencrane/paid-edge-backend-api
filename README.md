# PaidEdge Backend API

FastAPI backend for PaidEdge — auth, tenant resolution, and CRUD operations backed by Supabase and ClickHouse.

## Stack

- **FastAPI** (Python 3.12+) — async API framework
- **Supabase** — Postgres 17 for auth + CRUD (via `supabase-py`)
- **ClickHouse Cloud** — analytics queries (via `clickhouse-connect`)
- **Doppler** — secrets management (injected at runtime)
- **Railway** — deployment

## Quick Start

```bash
# Create virtual environment
python -m venv .venv && source .venv/bin/activate

# Install dependencies
pip install -e ".[dev]"

# Copy env vars
cp .env.example .env
# Fill in your Supabase + ClickHouse credentials

# Run dev server
uvicorn app.main:app --reload
```

## Project Structure

```
app/
├── main.py            # FastAPI app, middleware, CORS
├── config.py          # pydantic-settings config
├── dependencies.py    # DI: get_current_user, get_tenant, get_supabase, get_clickhouse
├── auth/              # Auth endpoints (signup, login, logout, me, refresh)
│   ├── router.py
│   ├── middleware.py   # JWT validation via PyJWT
│   └── models.py
├── tenants/           # Org CRUD, member management, provider configs
│   ├── router.py
│   ├── models.py
│   └── service.py     # Tenant resolution
├── db/
│   ├── supabase.py    # Supabase service-role client
│   └── clickhouse.py  # ClickHouse client
└── shared/
    ├── models.py      # Base Pydantic models
    ├── errors.py      # Custom HTTP exceptions
    └── pagination.py  # Pagination helpers
```

## API Endpoints

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/health` | No | Health check |
| POST | `/auth/signup` | No | Sign up via Supabase Auth |
| POST | `/auth/login` | No | Login, returns JWT tokens |
| POST | `/auth/logout` | Yes | Logout |
| GET | `/auth/me` | Yes | Current user + memberships |
| POST | `/auth/refresh` | No | Refresh JWT |
| GET | `/orgs` | Yes | List user's orgs |
| POST | `/orgs` | Yes | Create org |
| GET | `/orgs/{id}` | Yes | Get org details |
| PATCH | `/orgs/{id}` | Yes | Update org (admin) |
| POST | `/orgs/{id}/members` | Yes | Invite member (admin) |
| DELETE | `/orgs/{id}/members/{uid}` | Yes | Remove member (admin) |
| GET | `/orgs/{id}/providers` | Yes | List provider configs |
| PUT | `/orgs/{id}/providers/{p}` | Yes | Upsert provider config (admin) |
| DELETE | `/orgs/{id}/providers/{p}` | Yes | Remove provider config (admin) |

## Auth Flow

1. JWT extracted from `Authorization: Bearer <token>` header
2. Validated locally via PyJWT against `SUPABASE_JWT_SECRET`
3. Tenant resolved from `X-Organization-Id` header (or default org)
4. User + tenant injected into route handlers via FastAPI dependencies

## Deployment

Deployed on Railway via Dockerfile. Doppler injects secrets at runtime via `DOPPLER_TOKEN_BACKEND_API`.
