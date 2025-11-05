"""
cca_db: Database clients and admin helpers for PostgreSQL and ClickHouse.

Public API:
- PostgresClient, ClickHouseClient: low-level clients (connections + execute helpers)
- PostgresAdmin, ClickHouseAdmin: convenience wrappers for common DDL/DML/analytics
- POSTGRES_TABLES, CLICKHOUSE_TABLES: table configuration maps
"""

from .clients import PostgresClient, ClickHouseClient
from .admin import PostgresAdmin, ClickHouseAdmin
from .config import POSTGRES_TABLES, CLICKHOUSE_TABLES

__all__ = [
    "PostgresClient",
    "ClickHouseClient",
    "PostgresAdmin",
    "ClickHouseAdmin",
    "POSTGRES_TABLES",
    "CLICKHOUSE_TABLES",
]
