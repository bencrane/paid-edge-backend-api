from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.assets.generation_router import router as generation_router
from app.assets.router import router as assets_router
from app.audiences.router import router as audiences_router
from app.auth.linkedin import router as linkedin_auth_router
from app.auth.google_ads import router as google_ads_auth_router
from app.auth.meta import router as meta_auth_router
from app.auth.middleware import JWTAuthMiddleware
from app.auth.router import router as auth_router
from app.campaigns.router import router as campaigns_router
from app.config import settings
from app.landing_pages.router import router as landing_pages_router
from app.shared.models import HealthResponse
from app.tenants.router import router as tenants_router

app = FastAPI(
    title="PaidEdge API",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# --- Middleware (order matters: last added = first executed) ---

app.add_middleware(JWTAuthMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Routers ---

app.include_router(auth_router)
app.include_router(linkedin_auth_router)
app.include_router(meta_auth_router)
app.include_router(google_ads_auth_router)
app.include_router(assets_router)
app.include_router(audiences_router)
app.include_router(generation_router)
app.include_router(campaigns_router)
app.include_router(landing_pages_router)
app.include_router(tenants_router)


# --- Health check ---


@app.get("/health", response_model=HealthResponse)
async def health():
    return HealthResponse()
