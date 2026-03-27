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
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
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


@app.get("/health", response_model=HealthResponse)
async def health():
    return HealthResponse()


@app.get("/health/live", response_model=HealthResponse)
async def health_live():
    """Liveness probe — always returns 200 if the process is running."""
    return HealthResponse()


@app.get("/health/ready")
async def health_ready():
    """Readiness probe — checks database and Claude API connectivity."""
    checks: dict[str, CheckResult] = {}
    all_ok = True

    # Check database
    t0 = time.monotonic()
    try:
        supabase = get_supabase_client()
        supabase.table("organizations").select("id").limit(1).execute()
        latency = int((time.monotonic() - t0) * 1000)
        checks["database"] = CheckResult(status="ok", latency_ms=latency)
    except Exception as exc:
        latency = int((time.monotonic() - t0) * 1000)
        checks["database"] = CheckResult(status="error", latency_ms=latency, error=str(exc))
        all_ok = False

    # Check Claude API reachability
    t0 = time.monotonic()
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get("https://api.anthropic.com/v1/messages")
            # Even 401 means the API is reachable
            latency = int((time.monotonic() - t0) * 1000)
            checks["claude_api"] = CheckResult(status="ok", latency_ms=latency)
    except Exception as exc:
        latency = int((time.monotonic() - t0) * 1000)
        checks["claude_api"] = CheckResult(status="error", latency_ms=latency, error=str(exc))
        all_ok = False

    status_value = "ok" if all_ok else "degraded"
    status_code = 200 if all_ok else 503
    response = ReadinessResponse(status=status_value, checks=checks)

    return JSONResponse(status_code=status_code, content=response.model_dump())
