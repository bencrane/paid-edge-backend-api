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
