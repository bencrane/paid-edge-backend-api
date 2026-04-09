import jwt
from fastapi import Request
from jwt import PyJWKClient
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import JSONResponse, Response

# Better Auth JWKS endpoint (EdDSA)
_jwks_client = PyJWKClient(
    "https://api.authengine.dev/api/auth/jwks",
    cache_jwk_set=True,
    lifespan=300,
)

# Paths that don't require authentication
PUBLIC_PATHS = frozenset(
    ["/health", "/health/live", "/health/ready", "/docs", "/openapi.json", "/redoc"]
)
PUBLIC_PREFIXES = (
    "/auth/signup",
    "/auth/login",
    "/auth/refresh",
    "/auth/linkedin/callback",
    "/auth/meta/callback",
    "/auth/google-ads/callback",
    "/lp/",
)


class JWTAuthMiddleware(BaseHTTPMiddleware):
    """Validate JWT from Authorization header and inject user context into request state.

    Uses Better Auth (EdDSA via JWKS) only.
    """

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        path = request.url.path

        # Skip auth for public paths
        if path in PUBLIC_PATHS or path.startswith(PUBLIC_PREFIXES):
            return await call_next(request)

        # OPTIONS requests pass through (CORS preflight)
        if request.method == "OPTIONS":
            return await call_next(request)

        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            return JSONResponse(
                status_code=401,
                content={"detail": "Missing or invalid Authorization header"},
            )

        token = auth_header.removeprefix("Bearer ")

        # Verify Better Auth (EdDSA)
        payload = self._try_better_auth(token)

        if payload is None:
            return JSONResponse(status_code=401, content={"detail": "Invalid token"})

        # Set on request.state for downstream use
        request.state.user_id = payload["sub"]
        request.state.org_id = payload.get("org_id")
        request.state.role = payload.get("role", "member")
        request.state.token_type = payload.get("type", "session")
        request.state.jwt_payload = payload

        return await call_next(request)

    @staticmethod
    def _try_better_auth(token: str) -> dict | None:
        """Decode a Better Auth EdDSA JWT via JWKS."""
        try:
            signing_key = _jwks_client.get_signing_key_from_jwt(token)
            return jwt.decode(
                token,
                signing_key,
                algorithms=["EdDSA"],
                issuer="https://api.authengine.dev",
                audience="https://api.authengine.dev",
                options={"require": ["exp", "sub", "org_id", "role", "type"]},
            )
        except jwt.PyJWTError:
            return None
