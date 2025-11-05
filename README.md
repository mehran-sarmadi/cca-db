# cca-db

Compact helpers for working with PostgreSQL and ClickHouse.

## Structure

- `src/cca_db/`
  - `clients.py`: `PostgresClient`, `ClickHouseClient` low-level wrappers
  - `admin.py`: `PostgresAdmin`, `ClickHouseAdmin` higher-level DDL/DML/analytics helpers
  - `config.py`: `POSTGRES_TABLES`, `CLICKHOUSE_TABLES` configuration maps
  - `__init__.py`: public exports

Legacy shims remain in `src/dbs.py`, `src/dbs_admin.py`, and `src/tables_config.py` but are deprecated and will be removed in a future version.

## Usage

```python
from cca_db import PostgresClient, ClickHouseClient, PostgresAdmin, ClickHouseAdmin
from cca_db import POSTGRES_TABLES, CLICKHOUSE_TABLES

pg = PostgresClient()
pg_admin = PostgresAdmin(pg)

# Create a table if not exists
pg_admin.create_table_if_not_exists("experts", POSTGRES_TABLES["experts"]) 

ch = ClickHouseClient()
ch_admin = ClickHouseAdmin(ch)
ch_admin.create_database_if_not_exists("default")
ch_admin.create_table_if_not_exists(
    "default",
    "selected_calls_details",
    CLICKHOUSE_TABLES["selected_calls_details"],
    order_by=["call_timestamp"],
)
```

## Migration notes

- Import from `cca_db.*` instead of `dbs`, `dbs_admin`, and `tables_config`.
- Class names changed:
  - `Postgres` -> `PostgresClient`
  - `ClickHouse` -> `ClickHouseClient`
- Method renames and fixes:
  - `create_table` -> `create_table_if_not_exists`
  - `select` -> `select_rows`
  - `filter` -> `filter_rows`
  - `remove_table` -> `drop_table_if_exists`
  - `show_tables` -> `list_tables`
  - `add_call_detail` fixed and renamed to `insert_call_detail` to match the `calls_details` schema
- ClickHouse schema in `config.py` now includes `subscriber_id` and `expert_id` to match insert usage.

## Environment

Expects the following environment variables (e.g., via `.env`):

- PostgreSQL: `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_HOST`, `POSTGRES_PORT`
- ClickHouse: `CLICKHOUSE_DB`, `CLICKHOUSE_HOST`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`