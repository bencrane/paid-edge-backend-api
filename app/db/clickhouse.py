import clickhouse_connect
from clickhouse_connect.driver import Client as CHClient

from app.config import settings

_client: CHClient | None = None


def get_clickhouse_client() -> CHClient:
    """Return a ClickHouse client. Tenant filtering is done at query time."""
    global _client  # noqa: PLW0603
    if _client is None:
        _client = clickhouse_connect.get_client(
            host=settings.CLICKHOUSE_HOST,
            port=settings.CLICKHOUSE_PORT,
            username=settings.CLICKHOUSE_USER,
            password=settings.CLICKHOUSE_PASSWORD,
            database=settings.CLICKHOUSE_DATABASE,
            secure=True,
        )
    return _client
