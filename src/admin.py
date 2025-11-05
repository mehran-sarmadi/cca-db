from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .clients import PostgresClient, ClickHouseClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# SQL helper builders
def _build_where_clause(where: Optional[List[str]]) -> str:
    return f" WHERE {' AND '.join(where)}" if where else ""


def _build_group_by_clause(group_by: Optional[List[str]]) -> str:
    return f" GROUP BY {', '.join(group_by)}" if group_by else ""


def _build_having_clause(having: Optional[List[str]]) -> str:
    return f" HAVING {' AND '.join(having)}" if having else ""


def _build_order_by_clause(order_by: Optional[List[str]]) -> str:
    return f" ORDER BY {', '.join(order_by)}" if order_by else ""



class PostgresAdmin:
    def __init__(self, pg_client: PostgresClient):
        self.client = pg_client 

    def create_table_if_not_exists(self, table_name: str, columns: Dict[str, str]) -> None:
        cols_def = ", ".join([f"{col} {dtype}" for col, dtype in columns.items()])
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({cols_def});"
        self.client.execute_query(sql)
        self.client.commit()

    def select_rows(self, table_name: str, columns: str = "*", where: Optional[List[str]] = None):
        sql = f"SELECT {columns} FROM {table_name}" + _build_where_clause(where)
        return self.client.execute_query(sql)

    def insert_row(self, table: str, row: Dict[str, Any]) -> None:
        cols = list(row.keys())
        vals = []
        for c in cols:
            val = row[c]
            # Convert dict/list to JSON string for PostgreSQL compatibility
            if isinstance(val, (dict, list)):
                vals.append(json.dumps(val))
            else:
                vals.append(val)
        placeholders = ", ".join(["%s"] * len(cols))
        sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})"
        self.client.execute_query(sql, tuple(vals))
        self.client.commit()

    def drop_table_if_exists(self, table_name: str) -> None:
        self.client.execute_query(f"DROP TABLE IF EXISTS {table_name};")
        self.client.commit()

    def list_tables(self) -> List[Tuple[Any, ...]]:
        sql = (
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            """
        )
        return self.client.execute_query(sql)


class ClickHouseAdmin:
    def __init__(self, ch_client: ClickHouseClient):
        self.client = ch_client

    def select_rows(self, table_name: str, columns: str = "*", where: Optional[List[str]] = None):
        sql = f"SELECT {columns} FROM {table_name}" + _build_where_clause(where)
        return self.client.execute_query(sql)

    def create_table_if_not_exists(
        self,
        database_name: str,
        table_name: str,
        columns: Dict[str, str],
        order_by: Optional[List[str]] = None,
        partition_by: Optional[str] = None,
        engine: str = "MergeTree()",
        ttl: Optional[str] = None,
        settings: Optional[Dict[str, Any]] = None,
    ) -> None:
        cols_def = ", ".join([f"{col} {dtype}" for col, dtype in columns.items()])
        clauses: List[str] = [f"ENGINE = {engine}"]
        if partition_by:
            clauses.append(f"PARTITION BY {partition_by}")
        if order_by:
            clauses.append("ORDER BY (" + ", ".join(order_by) + ")")
        else:
            clauses.append("ORDER BY tuple()")
        if ttl:
            clauses.append(f"TTL {ttl}")
        engine_clause = " ".join(clauses)
        sql = f"CREATE TABLE IF NOT EXISTS {database_name}.{table_name} ({cols_def}) {engine_clause}"
        self.client.execute_query(sql)

    def insert_row(self, table: str, row: Dict[str, Any]) -> None:
        cols = list(row.keys())
        vals = []
        for c in cols:
            val = row[c]
            # Convert values to ClickHouse-compatible format
            if isinstance(val, bool):
                vals.append("1" if val else "0")
            elif isinstance(val, str):
                # Escape single quotes in strings
                escaped = val.replace("'", "\\'")
                vals.append(f"'{escaped}'")
            elif val is None:
                vals.append("NULL")
            elif isinstance(val, (dict, list)):
                vals.append(f"'{json.dumps(val)}'")
            else:
                vals.append(str(val))
        sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join(vals)})"
        self.client.execute_query(sql)

    def aggregate(
        self,
        table: str,
        group_by: List[str],
        aggregates: Dict[str, str],
        where: Optional[List[str]] = None,
        having: Optional[List[str]] = None,
        order_by: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ):
        select_parts = group_by + [f"{expr} AS {alias}" for alias, expr in aggregates.items()]
        sql = f"SELECT {', '.join(select_parts)} FROM {table}"
        sql += (
            _build_where_clause(where)
            + _build_group_by_clause(group_by)
            + _build_having_clause(having)
            + _build_order_by_clause(order_by)
        )
        if limit:
            sql += f" LIMIT {limit}"
        return self.client.execute_query(sql)

    def time_bucket(
        self,
        table: str,
        timestamp_col: str,
        granularity: str,
        value_exprs: Dict[str, str],
        where: Optional[List[str]] = None,
        order_by_bucket: bool = True,
        limit: Optional[int] = None,
    ):
        func_map = {
            "hour": "toStartOfHour",
            "day": "toStartOfDay",
            "week": "toStartOfWeek",
            "month": "toStartOfMonth",
        }
        if granularity not in func_map:
            raise ValueError("granularity must be one of: " + ", ".join(func_map))
        bucket = f"{func_map[granularity]}({timestamp_col}) AS bucket"
        metrics = [f"{expr} AS {alias}" for alias, expr in value_exprs.items()]
        sql = f"SELECT {bucket}, {', '.join(metrics)} FROM {table}"
        sql += _build_where_clause(where) + " GROUP BY bucket"
        if order_by_bucket:
            sql += " ORDER BY bucket"
        if limit:
            sql += f" LIMIT {limit}"
        return self.client.execute_query(sql)

    def drop_table_if_exists(self, table_name: str) -> None:
        self.client.execute_query(f"DROP TABLE IF EXISTS {table_name};")

    def list_tables(self, database: Optional[str] = None) -> List[Tuple[Any, ...]]:
        sql = f"SHOW TABLES FROM {database}" if database else "SHOW TABLES"
        return self.client.execute_query(sql)


if __name__ == "__main__":
    from .clients import PostgresClient, ClickHouseClient

    pg_admin = PostgresAdmin(PostgresClient())
    ch_admin = ClickHouseAdmin(ClickHouseClient())

    print("postgres tables:")
    for t in pg_admin.list_tables():
        print(t[0])
        print(pg_admin.select_rows(t[0]))
        print("-----")

    print("clickhouse tables:")
    for t in ch_admin.list_tables():
        print(t[0])
        print(ch_admin.select_rows(t[0]))
        print("-----")