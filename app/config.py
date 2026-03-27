from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Supabase
    SUPABASE_URL: str
    SUPABASE_ANON_KEY: str
    SUPABASE_SERVICE_ROLE_KEY: str
    SUPABASE_JWT_SECRET: str

    # ClickHouse
    CLICKHOUSE_HOST: str = "gf9xtjjqyl.us-east-1.aws.clickhouse.cloud"
    CLICKHOUSE_PORT: int = 8443
    CLICKHOUSE_USER: str = "default"
    CLICKHOUSE_PASSWORD: str = ""
    CLICKHOUSE_DATABASE: str = "paid_engine_x_api"

    # LinkedIn OAuth
    LINKEDIN_CLIENT_ID: str = ""
    LINKEDIN_CLIENT_SECRET: str = ""
    LINKEDIN_REDIRECT_URI: str = ""
    LINKEDIN_API_VERSION: str = "202603"

    # Meta OAuth
    META_APP_ID: str = ""
    META_APP_SECRET: str = ""
    META_REDIRECT_URI: str = ""
    META_API_VERSION: str = "v25.0"
    META_SYSTEM_USER_ID: str = ""

    # data-engine-x (injected by Doppler via `doppler run`)
    DATA_ENGINE_X_BASE_URL: str = ""
    DATA_ENGINE_X_API_TOKEN: str = ""

    # hubspot-engine-x (injected by Doppler via `doppler run`)
    HUBSPOT_ENGINE_X_BASE_URL: str = ""
    HUBSPOT_ENGINE_X_API_TOKEN: str = ""

    # sfdc-engine-x (injected by Doppler via `doppler run`)
    SFDC_ENGINE_X_BASE_URL: str = ""
    SFDC_ENGINE_X_API_TOKEN: str = ""

    # Anthropic (injected by Doppler via `doppler run`)
    ANTHROPIC_API_KEY: str = ""

    # RudderStack (server-side identify/track for landing page forms)
    RUDDERSTACK_DATA_PLANE_URL: str = ""
    RUDDERSTACK_WRITE_KEY: str = ""

    # dub.co (tracked short links)
    DUBCO_API_KEY: str = ""

    # Rate limiting
    RATE_LIMIT_RPM: int = 60

    # Webhooks
    WEBHOOK_SIGNING_SECRET: str = ""

    # App
    APP_ENV: str = "development"
    APP_URL: str = "http://localhost:8000"
    FRONTEND_URL: str = "http://localhost:3000"
    CORS_ORIGINS: list[str] = ["http://localhost:3000"]

    model_config = SettingsConfigDict(env_file=".env")


settings = Settings()  # type: ignore[call-arg]
