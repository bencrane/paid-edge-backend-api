from datetime import datetime

from pydantic import BaseModel, ConfigDict


class TimestampMixin(BaseModel):
    created_at: datetime
    updated_at: datetime | None = None


class BaseSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class HealthResponse(BaseModel):
    status: str = "ok"
    version: str = "0.1.0"


class CheckResult(BaseModel):
    status: str  # "ok" | "error"
    latency_ms: int
    error: str | None = None


class ReadinessResponse(BaseModel):
    status: str  # "ok" | "degraded"
    checks: dict[str, CheckResult]
