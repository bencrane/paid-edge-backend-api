import jwt
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import JSONResponse, Response

from app.config import settings

# Paths that don't require authentication
PUBLIC_PATHS = {"/health", "/docs", "/openapi.json", "/redoc"}
PUBLIC_PREFIXES = ("/auth/signup", "/auth/login", "/auth/refresh")


class JWTAuthMiddleware(BaseHTTPMiddleware):
    """Validate JWT from Authorization header and inject user_id into request state."""

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
        try:
            payload = jwt.decode(
                token,
                settings.SUPABASE_JWT_SECRET,
                algorithms=["HS256"],
                audience="authenticated",
            )
            request.state.user_id = payload["sub"]
            request.state.jwt_payload = payload
        except jwt.ExpiredSignatureError:
            return JSONResponse(status_code=401, content={"detail": "Token has expired"})
        except jwt.InvalidTokenError:
            return JSONResponse(status_code=401, content={"detail": "Invalid token"})

        return await call_next(request)
