import logging
import time

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse

from app.analytics.router import router as analytics_router
from app.assets.generation_router import router as generation_router
from app.assets.router import router as assets_router
from app.attribution.router import router as attribution_router
from app.audiences.router import router as audiences_router
from app.auth.linkedin import router as linkedin_auth_router
from app.auth.meta import router as meta_auth_router
from app.auth.middleware import JWTAuthMiddleware
from app.auth.rate_limit import RateLimitMiddleware
from app.auth.router import router as auth_router
from app.campaigns.router import router as campaigns_router
from app.config import settings
from app.db.supabase import get_supabase_client
from app.landing_pages.router import router as landing_pages_router
from app.shared.error_handlers import register_error_handlers
from app.shared.logging_config import configure_logging
from app.shared.models import CheckResult, HealthResponse, ReadinessResponse
from app.shared.request_id import RequestIDMiddleware
from app.tenants.router import router as tenants_router
from app.usage.router import router as usage_router

# --- Structured logging ---

configure_logging()
logger = logging.getLogger(__name__)

app = FastAPI(
    title="PaidEdge API",
    version="0.7.0.0",
    description="Multi-tenant B2B paid advertising platform API.",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_tags=[
        {"name": "auth", "description": "Authentication and user management"},
        {"name": "assets", "description": "Creative asset generation, rendering, and management"},
        {"name": "campaigns", "description": "Campaign management"},
        {"name": "audiences", "description": "Audience segments and platform sync"},
        {"name": "analytics", "description": "Performance analytics and reporting"},
        {"name": "attribution", "description": "Marketing attribution and funnel analysis"},
        {"name": "landing_pages", "description": "Landing page hosting and form submission"},
        {"name": "organizations", "description": "Organization and provider management"},
        {"name": "usage", "description": "API usage tracking and reporting"},
        {"name": "health", "description": "System health and readiness checks"},
    ],
)

# --- Error handlers ---

register_error_handlers(app)

# --- Middleware (order matters: last added = first executed) ---
# Execution order on request: RequestID → CORS → JWT → RateLimit → route handler

app.add_middleware(RateLimitMiddleware, rpm=settings.RATE_LIMIT_RPM)
app.add_middleware(JWTAuthMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(RequestIDMiddleware)

# --- Routers ---

app.include_router(auth_router)
app.include_router(linkedin_auth_router)
app.include_router(meta_auth_router)
app.include_router(assets_router)
app.include_router(audiences_router)
app.include_router(generation_router)
app.include_router(campaigns_router)
app.include_router(analytics_router)
app.include_router(attribution_router)
app.include_router(landing_pages_router)
app.include_router(tenants_router)
app.include_router(usage_router)


# --- Health checks ---


@app.get("/health", response_model=HealthResponse, tags=["health"], summary="Basic health check")
async def health():
    """Returns OK if the API process is running."""
    return HealthResponse()


@app.get("/health/live", response_model=HealthResponse, tags=["health"], summary="Liveness probe")
async def health_live():
    """Liveness probe — always returns 200 if the process is running."""
    return HealthResponse()


@app.get("/health/ready", tags=["health"], summary="Readiness probe")
async def health_ready():
    """Readiness probe — checks database and Claude API connectivity.

    Returns 200 with check details if all dependencies are healthy,
    or 503 with degraded status if any check fails.
    """
    checks: dict[str, CheckResult] = {}
    all_ok = True

    # Check database
    t0 = time.monotonic()
    try:
        supabase = get_supabase_client()
        supabase.table("organizations").select("id").limit(1).execute()
        latency = int((time.monotonic() - t0) * 1000)
        checks["database"] = CheckResult(status="ok", latency_ms=latency)
    except Exception:
        latency = int((time.monotonic() - t0) * 1000)
        checks["database"] = CheckResult(
            status="error", latency_ms=latency, error="database unreachable"
        )
        all_ok = False

    # Check Claude API reachability
    t0 = time.monotonic()
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.get("https://api.anthropic.com/v1/messages")
            # Even 401 means the API is reachable
            latency = int((time.monotonic() - t0) * 1000)
            checks["claude_api"] = CheckResult(status="ok", latency_ms=latency)
    except Exception:
        latency = int((time.monotonic() - t0) * 1000)
        checks["claude_api"] = CheckResult(
            status="error", latency_ms=latency, error="claude api unreachable"
        )
        all_ok = False

    status_value = "ok" if all_ok else "degraded"
    status_code = 200 if all_ok else 503
    response = ReadinessResponse(status=status_value, checks=checks)

    return JSONResponse(status_code=status_code, content=response.model_dump())
