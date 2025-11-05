CCA-DB quickstart
=================

This repo provides tiny admin/client wrappers around Postgres and ClickHouse plus a runnable sample that loads mock call data and demonstrates:

- Creating tables (idempotent)
- Inserting JSON-rich rows
- Simple SELECT with JSON filters
- Aggregations using helper builders (WHERE, GROUP BY, HAVING, ORDER BY)
- Time-bucketing metrics in ClickHouse

Prereqs
-------

- Python 3.12+
- Running PostgreSQL and ClickHouse instances
- Environment variables (e.g. in a .env file):

```
POSTGRES_DB=...
POSTGRES_USER=...
POSTGRES_PASSWORD=...
POSTGRES_HOST=localhost
POSTGRES_PORT=5432

CLICKHOUSE_DB=zaal
CLICKHOUSE_HOST=localhost
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
```

Install deps
------------

Using uv (recommended):

```
uv sync
```

Or pip:

```
python -m venv .venv && source .venv/bin/activate
pip install -e .
```

Run the sample
--------------

```
python sample_usage.py
```

What you’ll see
---------------

- Postgres and ClickHouse tables are (re)created
- Data from data/ is inserted (ClickHouse rows include a created_at timestamp)
- Results are printed in a readable format
- An aggregation by sentiment class and a time-bucketed summary

Code pointers
-------------

- src/admin.py: helper builders: _build_where_clause, _build_group_by_clause, _build_having_clause, _build_order_by_clause
- sample_usage.py: end-to-end example including aggregation and time bucket
