from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.auth.middleware import JWTAuthMiddleware
from app.auth.router import router as auth_router
from app.config import settings
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
app.include_router(tenants_router)


# --- Health check ---


@app.get("/health", response_model=HealthResponse)
async def health():
    return HealthResponse()
