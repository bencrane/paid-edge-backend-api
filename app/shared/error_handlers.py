"""Structured error response handlers (CEX-42).

Registers exception handlers on the FastAPI app so all errors
return a consistent JSON shape with request_id.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.responses import JSONResponse

from app.shared.errors import _StructuredHTTPException

logger = logging.getLogger(__name__)


def _get_request_id(request: Request) -> str | None:
    return getattr(request.state, "request_id", None)


def _error_response(
    status_code: int,
    error_type: str,
    message: str,
    request_id: str | None,
    details: Any = None,
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={
            "error": {
                "type": error_type,
                "message": message,
                "details": details,
                "request_id": request_id,
            }
        },
    )


def register_error_handlers(app: FastAPI) -> None:
    """Register exception handlers for structured error responses."""

    @app.exception_handler(_StructuredHTTPException)
    async def structured_http_handler(
        request: Request, exc: _StructuredHTTPException
    ) -> JSONResponse:
        return _error_response(
            status_code=exc.status_code,
            error_type=exc.error_type,
            message=str(exc.detail),
            request_id=_get_request_id(request),
            details=getattr(exc, "details", None),
        )

    @app.exception_handler(RequestValidationError)
    async def validation_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        # Convert Pydantic validation errors to structured format
        field_errors = []
        for err in exc.errors():
            loc = " → ".join(str(part) for part in err.get("loc", []))
            field_errors.append({"field": loc, "message": err.get("msg", "")})

        return _error_response(
            status_code=422,
            error_type="validation_error",
            message="Request validation failed",
            request_id=_get_request_id(request),
            details={"errors": field_errors},
        )

    @app.exception_handler(StarletteHTTPException)
    async def starlette_http_handler(
        request: Request, exc: StarletteHTTPException
    ) -> JSONResponse:
        # Catch Starlette's native HTTPExceptions (404 from routing, etc.)
        error_type = {
            400: "bad_request",
            401: "auth_error",
            403: "forbidden",
            404: "not_found",
            405: "method_not_allowed",
            429: "rate_limit_exceeded",
        }.get(exc.status_code, "http_error")

        return _error_response(
            status_code=exc.status_code,
            error_type=error_type,
            message=str(exc.detail),
            request_id=_get_request_id(request),
        )

    @app.exception_handler(Exception)
    async def generic_handler(request: Request, exc: Exception) -> JSONResponse:
        # Never leak internal details
        logger.exception("Unhandled exception: %s", exc)
        return _error_response(
            status_code=500,
            error_type="internal_error",
            message="An unexpected error occurred",
            request_id=_get_request_id(request),
        )
