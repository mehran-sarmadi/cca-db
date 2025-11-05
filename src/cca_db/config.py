# PostgreSQL table configurations
POSTGRES_TABLES = {
    "subscribers": {
        "id": "SERIAL PRIMARY KEY",
    },
    "experts": {
        "id": "SERIAL PRIMARY KEY",
        "name": "VARCHAR(100) NOT NULL",
    },
    "calls_details": {
        "id": "SERIAL PRIMARY KEY",
        "sentiment_analysis": "JSONB",
        "bad_words": "JSONB",
        "start_greeting": "JSONB",
        "end_greeting": "JSONB",
        "summary": "JSONB",
        "calls_texts": "JSONB",
    },
}

# ClickHouse table configurations
CLICKHOUSE_TABLES = {
    "selected_calls_details": {
        "id": "Int32",
        "sentiment_analysis": "String",
        "bad_words": "String",
        "start_greeting": "String",
        "end_greeting": "String",
        "summary": "String",
        "calls_texts": "String",
    }
}
