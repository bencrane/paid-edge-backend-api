from pydantic import BaseModel


class PaginationParams(BaseModel):
    limit: int = 50
    offset: int = 0


class PaginatedResponse(BaseModel):
    data: list
    total: int
    limit: int
    offset: int
