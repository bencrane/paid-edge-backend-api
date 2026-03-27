"""Structured error types for the API (CEX-42).

All errors carry an `error_type` string used in the structured JSON response:
  {"error": {"type": "...", "message": "...", "details": ..., "request_id": "..."}}
"""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException, status


class _StructuredHTTPException(HTTPException):
    """Base for errors that include an error_type for structured responses."""

    error_type: str = "internal_error"

    def __init__(
        self,
        status_code: int,
        detail: str,
        *,
        error_type: str | None = None,
        details: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ):
        super().__init__(status_code=status_code, detail=detail, headers=headers)
        if error_type:
            self.error_type = error_type
        self.details = details


# --- Existing error types (now structured) ---


class NotFoundError(_StructuredHTTPException):
    error_type = "not_found"

    def __init__(self, detail: str = "Resource not found", **kwargs: Any):
        super().__init__(status_code=status.HTTP_404_NOT_FOUND, detail=detail, **kwargs)


class ForbiddenError(_StructuredHTTPException):
    error_type = "forbidden"

    def __init__(self, detail: str = "Forbidden", **kwargs: Any):
        super().__init__(status_code=status.HTTP_403_FORBIDDEN, detail=detail, **kwargs)


class UnauthorizedError(_StructuredHTTPException):
    error_type = "auth_error"

    def __init__(self, detail: str = "Not authenticated", **kwargs: Any):
        super().__init__(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=detail,
            headers={"WWW-Authenticate": "Bearer"},
            **kwargs,
        )


class BadRequestError(_StructuredHTTPException):
    error_type = "bad_request"

    def __init__(self, detail: str = "Bad request", **kwargs: Any):
        super().__init__(status_code=status.HTTP_400_BAD_REQUEST, detail=detail, **kwargs)


class ConflictError(_StructuredHTTPException):
    error_type = "conflict"

    def __init__(self, detail: str = "Conflict", **kwargs: Any):
        super().__init__(status_code=status.HTTP_409_CONFLICT, detail=detail, **kwargs)


# --- New Phase 7 error types ---


class ValidationError(_StructuredHTTPException):
    error_type = "validation_error"

    def __init__(self, detail: str = "Validation error", **kwargs: Any):
        super().__init__(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=detail, **kwargs
        )


class RateLimitError(_StructuredHTTPException):
    error_type = "rate_limit_exceeded"

    def __init__(self, detail: str = "Rate limit exceeded", **kwargs: Any):
        super().__init__(status_code=429, detail=detail, **kwargs)


class GenerationError(_StructuredHTTPException):
    error_type = "generation_error"

    def __init__(self, detail: str = "Generation failed", **kwargs: Any):
        super().__init__(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=detail, **kwargs
        )


class RenderError(_StructuredHTTPException):
    error_type = "render_error"

    def __init__(self, detail: str = "Rendering failed", **kwargs: Any):
        super().__init__(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=detail, **kwargs
        )


class ProviderError(_StructuredHTTPException):
    error_type = "provider_error"

    def __init__(self, detail: str = "Provider error", **kwargs: Any):
        super().__init__(status_code=status.HTTP_502_BAD_GATEWAY, detail=detail, **kwargs)
