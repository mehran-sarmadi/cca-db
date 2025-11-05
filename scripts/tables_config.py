# PostgreSQL table configurations
POSTGRES_TABLES = {
    "subscribers": {
        "id": "SERIAL PRIMARY KEY",
    },
    "experts": {
        "id": "SERIAL PRIMARY KEY",
        "name": "VARCHAR(100) NOT NULL"
    },
    "calls_texts": {
        "id": "SERIAL PRIMARY KEY",
        "call_id": "INTEGER REFERENCES calls_analysis(id)",
        "text": "JSONB"
    },
    "calls_details": {
        "id": "SERIAL PRIMARY KEY",
        "sentiment_analysis": "JSONB",
        "bad_words": "JSONB",
        "start_greeting": "JSONB",
        "end_greeting": "JSONB",
        "summary": "JSONB",
    }
}

# ClickHouse table configurations
CLICKHOUSE_TABLES = {
    "selected_calls_details": {
        "call_id": "Int32",
        "subscriber_id": "Int32",
        "expert_id": "Int32",
        "call_timestamp": "DateTime",
        "duration_seconds": "Int32",
        "features": "JSON"
    }
}
