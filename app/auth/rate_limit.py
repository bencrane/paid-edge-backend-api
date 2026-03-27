"""Per-user sliding-window rate limiting middleware (CEX-39)."""

from __future__ import annotations

import time

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import JSONResponse, Response

from app.auth.middleware import PUBLIC_PATHS, PUBLIC_PREFIXES


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Sliding-window rate limiter keyed on authenticated user_id.

    Tracks request timestamps per user in memory. Suitable for
    single-instance deployments (swap for Redis for multi-instance).
    """

    def __init__(self, app, rpm: int = 60):
        super().__init__(app)
        self.rpm = rpm
        self._window = 60.0  # seconds
        self._counters: dict[str, list[float]] = {}

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        path = request.url.path

        # Skip public / unauthenticated paths
        if path in PUBLIC_PATHS or path.startswith(PUBLIC_PREFIXES):
            return await call_next(request)
        if request.method == "OPTIONS":
            return await call_next(request)

        # user_id is set by JWTAuthMiddleware (which runs before this)
        user_id: str | None = getattr(request.state, "user_id", None)
        if not user_id:
            # Not authenticated — JWT middleware will reject downstream
            return await call_next(request)

        now = time.monotonic()
        timestamps = self._counters.setdefault(user_id, [])

        # Prune timestamps outside the sliding window
        cutoff = now - self._window
        timestamps[:] = [t for t in timestamps if t > cutoff]

        remaining = max(0, self.rpm - len(timestamps))
        reset_at = int(time.time()) + int(self._window)

        if len(timestamps) >= self.rpm:
            # Oldest timestamp determines when space opens
            oldest = min(timestamps)
            retry_after = int((oldest + self._window) - now) + 1
            return JSONResponse(
                status_code=429,
                content={
                    "error": {
                        "type": "rate_limit_exceeded",
                        "message": f"Rate limit exceeded. Try again in {retry_after} seconds.",
                        "details": None,
                        "request_id": getattr(request.state, "request_id", None),
                    }
                },
                headers={
                    "Retry-After": str(retry_after),
                    "X-RateLimit-Limit": str(self.rpm),
                    "X-RateLimit-Remaining": "0",
                    "X-RateLimit-Reset": str(reset_at),
                },
            )

        # Record this request
        timestamps.append(now)
        remaining = max(0, self.rpm - len(timestamps))

        response = await call_next(request)

        # Attach rate-limit headers to every authenticated response
        response.headers["X-RateLimit-Limit"] = str(self.rpm)
        response.headers["X-RateLimit-Remaining"] = str(remaining)
        response.headers["X-RateLimit-Reset"] = str(reset_at)

        return response
