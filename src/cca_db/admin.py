from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .clients import PostgresClient, ClickHouseClient

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


def _build_settings_clause(settings: Optional[Dict[str, Any]]) -> str:
    if not settings:
        return ""
    parts: List[str] = []
    for k, v in settings.items():
        if isinstance(v, bool):
            val = "1" if v else "0"
        elif isinstance(v, str):
            val = f"'{v}'"
        else:
            val = str(v)
        parts.append(f"{k}={val}")
    return " SETTINGS " + ", ".join(parts)


def _append_window_over_clause(
    expr: str, partition_by: Optional[List[str]], order_by: Optional[List[str]]
) -> str:
    parts: List[str] = []
    if partition_by:
        parts.append("PARTITION BY " + ", ".join(partition_by))
    if order_by:
        parts.append("ORDER BY " + ", ".join(order_by))
    if not parts:
        return expr
    return f"{expr} OVER ({' '.join(parts)})"


class PostgresAdmin:
    def __init__(self, pg: PostgresClient):
        self.pg = pg

    def create_table_if_not_exists(self, table_name: str, columns: Dict[str, str]) -> None:
        cols_def = ", ".join([f"{col} {dtype}" for col, dtype in columns.items()])
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({cols_def});"
        self.pg.execute_query(sql)
        self.pg.commit()

    def select_rows(self, table_name: str, columns: str = "*", where: Optional[List[str]] = None):
        sql = f"SELECT {columns} FROM {table_name}" + _build_where_clause(where)
        return self.pg.execute_query(sql)

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
        self.pg.execute_query(sql, tuple(vals))
        self.pg.commit()

    def insert_many_rows(self, table: str, columns: List[str], rows: List[Tuple[Any, ...]]):
        placeholders = "(" + ", ".join(["%s"] * len(columns)) + ")"
        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s"
        # Use execute_many helper for performance
        self.pg.execute_many(sql, rows)
        self.pg.commit()

    def upsert_row(
        self,
        table: str,
        row: Dict[str, Any],
        conflict_cols: List[str],
        update_cols: Optional[List[str]] = None,
    ) -> None:
        cols = list(row.keys())
        vals = [row[c] for c in cols]
        placeholders = ", ".join(["%s"] * len(cols))
        updates_cols = update_cols or [c for c in cols if c not in conflict_cols]
        set_clause = ", ".join([f"{c}=EXCLUDED.{c}" for c in updates_cols]) or "NOTHING"
        sql = (
            f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders}) "
            f"ON CONFLICT ({', '.join(conflict_cols)}) "
            + (f"DO UPDATE SET {set_clause}" if set_clause != "NOTHING" else "DO NOTHING")
        )
        self.pg.execute_query(sql, tuple(vals))
        self.pg.commit()

    def drop_table_if_exists(self, table_name: str) -> None:
        self.pg.execute_query(f"DROP TABLE IF EXISTS {table_name};")
        self.pg.commit()

    def list_tables(self) -> List[Tuple[Any, ...]]:
        sql = (
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            """
        )
        return self.pg.execute_query(sql)


class ClickHouseAdmin:
    def __init__(self, ch: ClickHouseClient):
        self.ch = ch

    def select_rows(self, table_name: str, columns: str = "*", where: Optional[List[str]] = None):
        sql = f"SELECT {columns} FROM {table_name}" + _build_where_clause(where)
        return self.ch.execute_query(sql)

    def create_database_if_not_exists(self, database_name: str):
        self.ch.execute_query(f"CREATE DATABASE IF NOT EXISTS {database_name}")

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
        sql += _build_settings_clause(settings)
        self.ch.execute_query(sql)

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
            else:
                vals.append(str(val))
        sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join(vals)})"
        self.ch.execute_query(sql)

    def insert_many_rows(self, table: str, columns: List[str], rows: List[Tuple[Any, ...]]):
        cols = ", ".join(columns)
        sql = f"INSERT INTO {table} ({cols}) VALUES"
        self.ch.execute_query(sql, rows)

    # Analytics helpers
    def aggregate(
        self,
        table: str,
        group_by: List[str],
        aggregates: Dict[str, str],
        where: Optional[List[str]] = None,
        having: Optional[List[str]] = None,
        order_by: Optional[List[str]] = None,
        limit: Optional[int] = None,
        settings: Optional[Dict[str, Any]] = None,
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
        sql += _build_settings_clause(settings)
        return self.ch.execute_query(sql)

    def window(
        self,
        table: str,
        base_columns: List[str],
        window_exprs: List[str],
        where: Optional[List[str]] = None,
        partition_by: Optional[List[str]] = None,
        order_by: Optional[List[str]] = None,
        settings: Optional[Dict[str, Any]] = None,
    ):
        resolved_exprs = [
            _append_window_over_clause(expr, partition_by, order_by) for expr in window_exprs
        ]
        select_list = base_columns + resolved_exprs
        sql = f"SELECT {', '.join(select_list)} FROM {table}"
        sql += _build_where_clause(where)
        sql += _build_settings_clause(settings)
        return self.ch.execute_query(sql)

    def time_bucket(
        self,
        table: str,
        timestamp_col: str,
        granularity: str,
        value_exprs: Dict[str, str],
        where: Optional[List[str]] = None,
        order_by_bucket: bool = True,
        limit: Optional[int] = None,
        settings: Optional[Dict[str, Any]] = None,
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
        sql += _build_settings_clause(settings)
        return self.ch.execute_query(sql)

    def filter_rows(
        self,
        table: str,
        where: Optional[List[str]] = None,
        columns: str = "*",
        limit: Optional[int] = None,
        order_by: Optional[List[str]] = None,
        settings: Optional[Dict[str, Any]] = None,
    ):
        sql = f"SELECT {columns} FROM {table}"
        sql += _build_where_clause(where) + _build_order_by_clause(order_by)
        if limit:
            sql += f" LIMIT {limit}"
        sql += _build_settings_clause(settings)
        return self.ch.execute_query(sql)

    def create_materialized_view(
        self,
        view_name: str,
        select_sql: str,
        engine: str = "AggregatingMergeTree()",
        to_table: Optional[str] = None,
    ):
        if to_table:
            sql = f"CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name} TO {to_table} AS {select_sql}"
        else:
            sql = (
                f"CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name} ENGINE = {engine} AS {select_sql}"
            )
        return self.ch.execute_query(sql)

    def grouping_sets(
        self,
        table: str,
        groupings: List[List[str]],
        aggregates: Dict[str, str],
        where: Optional[List[str]] = None,
        order_by: Optional[List[str]] = None,
        settings: Optional[Dict[str, Any]] = None,
    ):
        all_dims: List[str] = []
        for g in groupings:
            for c in g:
                if c not in all_dims:
                    all_dims.append(c)
        grouping_sql = ", ".join([f"({', '.join(g)})" if g else "()" for g in groupings])
        select_parts = all_dims + [f"{expr} AS {alias}" for alias, expr in aggregates.items()]
        sql = f"SELECT {', '.join(select_parts)} FROM {table}"
        sql += (
            _build_where_clause(where)
            + f" GROUP BY GROUPING SETS ({grouping_sql})"
            + _build_order_by_clause(order_by)
            + _build_settings_clause(settings)
        )
        return self.ch.execute_query(sql)

    def drop_table_if_exists(self, table_name: str) -> None:
        self.ch.execute_query(f"DROP TABLE IF EXISTS {table_name};")

    def list_tables(self, database: Optional[str] = None) -> List[Tuple[Any, ...]]:
        sql = f"SHOW TABLES FROM {database}" if database else "SHOW TABLES"
        return self.ch.execute_query(sql)


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
