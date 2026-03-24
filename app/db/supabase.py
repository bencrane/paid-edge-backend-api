from supabase import Client, create_client

from app.config import settings

_client: Client | None = None


def get_supabase_client() -> Client:
    """Return a Supabase client using the service role key (bypasses RLS)."""
    global _client  # noqa: PLW0603
    if _client is None:
        _client = create_client(
            settings.SUPABASE_URL,
            settings.SUPABASE_SERVICE_ROLE_KEY,
        )
    return _client
