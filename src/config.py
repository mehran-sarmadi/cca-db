# PostgreSQL table configurations
POSTGRES_TABLES = {
    "subscribers": {
        "id": "BIGSERIAL PRIMARY KEY",
        "created_at": "TIMESTAMPTZ NOT NULL DEFAULT NOW()",
        "updated_at": "TIMESTAMPTZ NOT NULL DEFAULT NOW()",
    },
    "experts": {
        "id": "BIGSERIAL PRIMARY KEY",
        "name": "VARCHAR(100) NOT NULL UNIQUE",
        "created_at": "TIMESTAMPTZ NOT NULL DEFAULT NOW()",
        "updated_at": "TIMESTAMPTZ NOT NULL DEFAULT NOW()",
    },
    "calls_details": {
        "id": "BIGSERIAL PRIMARY KEY",
        # JSONB columns with safe defaults to avoid NULLs
        "sentiment_analysis": "JSONB NOT NULL DEFAULT '{}'::jsonb",
        "bad_words": "JSONB NOT NULL DEFAULT '[]'::jsonb",
        "start_greeting": "JSONB NOT NULL DEFAULT '{}'::jsonb",
        "end_greeting": "JSONB NOT NULL DEFAULT '{}'::jsonb",
        "summary": "JSONB NOT NULL DEFAULT '{}'::jsonb",
        "calls_texts": "JSONB NOT NULL DEFAULT '[]'::jsonb",
        "created_at": "TIMESTAMPTZ NOT NULL DEFAULT NOW()",
        "updated_at": "TIMESTAMPTZ NOT NULL DEFAULT NOW()",
    },
}

# ClickHouse table configurations
CLICKHOUSE_TABLES = {
    "selected_calls_details": {
        "id": "Int64",
        # JSON-encoded strings for flexible ingestion; extract with JSON* functions
        "sentiment_analysis": "String",
        "bad_words": "String",
        "start_greeting": "String",
        "end_greeting": "String",
        "summary": "String",
        "calls_texts": "String",
        "created_at": "DateTime",
    }
}
