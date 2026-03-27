-- CEX-40: Usage events table for per-request metrics tracking

CREATE TABLE IF NOT EXISTS usage_events (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    org_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    asset_type TEXT NOT NULL,
    status TEXT NOT NULL,
    duration_ms INTEGER,
    claude_tokens_input INTEGER,
    claude_tokens_output INTEGER,
    provider_costs JSONB,
    request_id TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_usage_events_org_date ON usage_events (org_id, created_at);
CREATE INDEX idx_usage_events_user_date ON usage_events (user_id, created_at);
