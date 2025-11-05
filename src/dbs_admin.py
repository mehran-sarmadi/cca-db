from dbs import Postgres, ClickHouse
from typing import Dict, List, Tuple, Optional, Any
import logging
# Avoid linter warnings for potentially unused configs
from tables_config import POSTGRES_TABLES as _POSTGRES_TABLES, CLICKHOUSE_TABLES as _CLICKHOUSE_TABLES

postgres = Postgres()
clickhouse = ClickHouse()
logger = logging.getLogger(__name__)

# Small SQL helpers to keep query composition consistent
def _sql_where(where: Optional[List[str]]) -> str:
    return f" WHERE {' AND '.join(where)}" if where else ""

def _sql_group_by(group_by: Optional[List[str]]) -> str:
    return f" GROUP BY {', '.join(group_by)}" if group_by else ""

def _sql_having(having: Optional[List[str]]) -> str:
    return f" HAVING {' AND '.join(having)}" if having else ""

def _sql_order_by(order_by: Optional[List[str]]) -> str:
    return f" ORDER BY {', '.join(order_by)}" if order_by else ""

def _sql_settings(settings: Optional[Dict[str, Any]]) -> str:
    """
    Build a ClickHouse SETTINGS clause from a dict, returning an empty string if no settings.
    Booleans are converted to 1/0, strings are quoted.
    """
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

def _append_window_over(expr: str, partition_by: Optional[List[str]], order_by: Optional[List[str]]) -> str:
    """
    Append an OVER(...) clause to a window expression if partition_by or order_by are provided.
    If neither partition_by nor order_by is provided, returns the original expression unchanged.
    """
    parts: List[str] = []
    if partition_by:
        parts.append("PARTITION BY " + ", ".join(partition_by))
    if order_by:
        parts.append("ORDER BY " + ", ".join(order_by))
    if not parts:
        return expr
    return f"{expr} OVER ({' '.join(parts)})"


class PostgresAdmin:
    def __init__(self, pg: Postgres):
        self.pg = pg

    def create_table(self, table_name: str, columns: Dict[str, str]):
        cols_def = ", ".join([f"{col} {dtype}" for col, dtype in columns.items()])
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({cols_def});"
        self.pg.execute_query(create_table_query)
        self.pg.conn.commit()

    def select(self, table_name: str, columns: str = "*", where_clause: Optional[List[str]] = None):
        query = f"SELECT {columns} FROM {table_name}" + _sql_where(where_clause)
        return self.pg.execute_query(query)

    def add_call_detail(self, details: Dict):
        insert_query = """
        INSERT INTO calls_analysis (sentiment_analysis, bad_words, start_greeting, end_greeting, summary)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id;
        """
        params = (
            details.get("sentiment_analysis"),
            details.get("bad_words"),
            details.get("start_greeting"),
            details.get("end_greeting"),
            details.get("summary"),
        )
        result = self.pg.execute_query(insert_query, params)
        self.pg.conn.commit()
        return result[0][0]

    def add_call_texts(self, call_id, call_text_json):
        insert_query = """
        INSERT INTO calls_texts (call_id, text)
        VALUES (%s, %s);
        """
        self.pg.execute_query(insert_query, (call_id, call_text_json))
        self.pg.conn.commit()

    def insert_many(self, table: str, columns: List[str], rows: List[Tuple[Any, ...]]):
        placeholders = "(" + ", ".join(["%s"] * len(columns)) + ")"
        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES {placeholders}"
        self.pg.execute_query(sql, rows)
        self.pg.conn.commit()

    def upsert(self, table: str, row: Dict[str, Any], conflict_cols: List[str], update_cols: Optional[List[str]] = None):
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
        self.pg.conn.commit()

    def explain(self, sql: str, params: Optional[Tuple[Any, ...]] = None):
        return self.pg.execute_query("EXPLAIN " + sql, params)

    def set_statement_timeout(self, ms: int):
        self.pg.execute_query(f"SET statement_timeout = {int(ms)}")

    def remove_table(self, table_name: str):
        drop_query = f"DROP TABLE IF EXISTS {table_name};"
        self.pg.execute_query(drop_query)
        self.pg.conn.commit()

    def show_tables(self) -> List[Tuple]:
        """
        List tables in the current Postgres database.
        """
        query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        """
        return self.pg.execute_query(query)


class ClickHouseAdmin:
    def __init__(self, ch: ClickHouse):
        self.ch = ch

    def select(self, table_name: str, columns: str = "*", where_clause: Optional[List[str]] = None):
        query = f"SELECT {columns} FROM {table_name}" + _sql_where(where_clause)
        return self.ch.execute_query(query)

    def _table_sampling_supported(self, table: str) -> bool:
        """
        Check ClickHouse metadata to determine if a table supports the SAMPLE clause.
        Returns True when the table has a non-empty sampling_key in system.tables.
        """
        # Determine database and table name
        if "." in table:
            db_name, tbl_name = table.split(".", 1)
        else:
            # Fallback to current database if not qualified
            try:
                db_res = self.ch.execute_query("SELECT currentDatabase()")
                db_name = db_res[0][0] if db_res else None
            except Exception:
                db_name = None
            tbl_name = table

        if not db_name:
            # If we cannot determine DB name, be conservative and say no sampling
            return False

        try:
            res = self.ch.execute_query(
                """
                SELECT sampling_key != '' AS supports
                FROM system.tables
                WHERE database = %(db)s AND name = %(tbl)s
                LIMIT 1
                """,
                {"db": db_name, "tbl": tbl_name},
            )
            return bool(res and res[0][0])
        except Exception as e:
            # On any metadata lookup issue, assume not supported to avoid server exceptions
            logger.debug(f"Failed to check sampling support for {table}: {e}")
            return False

    def create_database_if_not_exists(self, database_name: str):
        self.ch.execute_query(f"CREATE DATABASE IF NOT EXISTS {database_name}")

    def create_table(
        self,
        database_name: str,
        table_name: str,
        columns: Dict[str, str],
        order_by: Optional[List[str]] = None,
        partition_by: Optional[str] = None,
        engine: str = "MergeTree()",
        ttl: Optional[str] = None,
        settings: Optional[Dict[str, Any]] = None,
    ):
        cols_def = ", ".join([f"{col} {dtype}" for col, dtype in columns.items()])
        clauses = [f"ENGINE = {engine}"]
        if partition_by:
            clauses.append(f"PARTITION BY {partition_by}")
        if order_by:
            # ORDER BY must be a single expression; use parentheses for multiple columns
            clauses.append("ORDER BY (" + ", ".join(order_by) + ")")
        else:
            clauses.append("ORDER BY tuple()")
        if ttl:
            clauses.append(f"TTL {ttl}")
        engine_clause = " ".join(clauses)
        create_table_query = f"CREATE TABLE IF NOT EXISTS {database_name}.{table_name} ({cols_def}) {engine_clause}"
        create_table_query += _sql_settings(settings)
        self.ch.execute_query(create_table_query)

    def add_selected_call_detail(self, call_detail: Dict):
        insert_query = """
        INSERT INTO selected_calls_details (call_id, subscriber_id, expert_id, call_timestamp, duration_seconds, features)
        VALUES (%(call_id)s, %(subscriber_id)s, %(expert_id)s, %(call_timestamp)s, %(duration_seconds)s, %(features)s);
        """
        self.ch.execute_query(insert_query, call_detail)

    def insert_many(self, table: str, columns: List[str], rows: List[Tuple[Any, ...]]):
        cols = ", ".join(columns)
        # ClickHouse driver expects data passed as the second arg; no %s placeholders.
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
        query = f"SELECT {', '.join(select_parts)} FROM {table}"
        query += _sql_where(where) + _sql_group_by(group_by) + _sql_having(having) + _sql_order_by(order_by)
        if limit:
            query += f" LIMIT {limit}"
        query += _sql_settings(settings)
        return self.ch.execute_query(query)

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
        resolved_exprs = [_append_window_over(expr, partition_by, order_by) for expr in window_exprs]
        select_list = base_columns + resolved_exprs
        query = f"SELECT {', '.join(select_list)} FROM {table}"
        query += _sql_where(where)
        query += _sql_settings(settings)
        return self.ch.execute_query(query)

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
        func_map = {"hour": "toStartOfHour", "day": "toStartOfDay", "week": "toStartOfWeek", "month": "toStartOfMonth"}
        if granularity not in func_map:
            raise ValueError("granularity must be one of: " + ", ".join(func_map))
        bucket = f"{func_map[granularity]}({timestamp_col}) AS bucket"
        metrics = [f"{expr} AS {alias}" for alias, expr in value_exprs.items()]
        query = f"SELECT {bucket}, {', '.join(metrics)} FROM {table}"
        query += _sql_where(where) + " GROUP BY bucket"
        if order_by_bucket:
            query += " ORDER BY bucket"
        if limit:
            query += f" LIMIT {limit}"
        query += _sql_settings(settings)
        return self.ch.execute_query(query)

    def filter(
        self,
        table: str,
        where: Optional[List[str]] = None,
        columns: str = "*",
        limit: Optional[int] = None,
        order_by: Optional[List[str]] = None,
        settings: Optional[Dict[str, Any]] = None,
    ):
        query = f"SELECT {columns} FROM {table}"
        query += _sql_where(where) + _sql_order_by(order_by)
        if limit:
            query += f" LIMIT {limit}"
        query += _sql_settings(settings)
        return self.ch.execute_query(query)

    def join(
        self,
        left_table: str,
        right_table: str,
        join_type: str,
        on_condition: str,
        select_exprs: List[str],
        where: Optional[List[str]] = None,
        group_by: Optional[List[str]] = None,
        order_by: Optional[List[str]] = None,
        settings: Optional[Dict[str, Any]] = None,
    ):
        query = f"SELECT {', '.join(select_exprs)} FROM {left_table} {join_type} JOIN {right_table} ON {on_condition}"
        query += _sql_where(where) + _sql_group_by(group_by) + _sql_order_by(order_by) + _sql_settings(settings)
        return self.ch.execute_query(query)

    def create_materialized_view(self, view_name: str, select_sql: str, engine: str = "AggregatingMergeTree()", to_table: Optional[str] = None):
        if to_table:
            query = f"CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name} TO {to_table} AS {select_sql}"
        else:
            query = f"CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name} ENGINE = {engine} AS {select_sql}"
        return self.ch.execute_query(query)

    def sample(self, table: str, ratio: float, columns: str = "*", where: Optional[List[str]] = None, limit: Optional[int] = None):
        """
        Return a random sample of rows from a table.

        Uses ClickHouse's SAMPLE clause when supported by the table; otherwise falls back to
        probabilistic filtering with randCanonical().
        """
        if not (0 < ratio <= 1):
            raise ValueError("ratio must be in (0, 1].")

        use_sample_clause = self._table_sampling_supported(table)
        if use_sample_clause:
            query = f"SELECT {columns} FROM {table} SAMPLE {ratio}"
            if where:
                query += " WHERE " + " AND ".join(where)
        else:
            # Fallback: probabilistic sampling. Note this scans the table; it's a safe default.
            logger.info(
                f"Table {table} doesn't support SAMPLE; falling back to WHERE randCanonical() < {ratio}."
            )
            filters: List[str] = []
            if where:
                filters.append("(" + " AND ".join(where) + ")")
            filters.append(f"randCanonical() < {ratio}")
            query = f"SELECT {columns} FROM {table} WHERE " + " AND ".join(filters)

        if limit:
            query += f" LIMIT {limit}"

        return self.ch.execute_query(query)

    def json_extract(
        self,
        table: str,
        json_col: str,
        extracts: Dict[str, Tuple[str, str]],
        where: Optional[List[str]] = None,
        limit: Optional[int] = None,
        settings: Optional[Dict[str, Any]] = None,
    ):
        func_map = {
            "String": "JSONExtractString",
            "Int": "JSONExtractInt",
            "Int64": "JSONExtractInt",
            "UInt64": "JSONExtractUInt",
            "Float64": "JSONExtractFloat",
            "Float32": "JSONExtractFloat",
            "Bool": "JSONExtractBool",
        }
        selects = []
        for alias, (key, ch_type) in extracts.items():
            func = func_map.get(ch_type)
            if not func:
                raise ValueError(f"Unsupported ch_type '{ch_type}' for alias '{alias}'.")
            selects.append(f"{func}({json_col}, '{key}') AS {alias}")
        query = f"SELECT {', '.join(selects)} FROM {table}"
        if where:
            query += " WHERE " + " AND ".join(where)
        if limit:
            query += f" LIMIT {limit}"
        query += _sql_settings(settings)
        return self.ch.execute_query(query)

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
        query = f"SELECT {', '.join(select_parts)} FROM {table}"
        query += _sql_where(where) + f" GROUP BY GROUPING SETS ({grouping_sql})" + _sql_order_by(order_by) + _sql_settings(settings)
        return self.ch.execute_query(query)

    def optimize_table(self, table: str):
        return self.ch.execute_query(f"OPTIMIZE TABLE {table} FINAL")

    def explain(self, sql: str, params: Optional[Tuple[Any, ...]] = None):
        return self.ch.execute_query("EXPLAIN " + sql, params)
    
    def remove_table(self, table_name: str):
        drop_query = f"DROP TABLE IF EXISTS {table_name};"
        self.ch.execute_query(drop_query)

    def show_tables(self, database: Optional[str] = None) -> List[Tuple]:
        """
        List tables in the specified database. If no database is provided, lists tables in the current database.
        """
        if database:
            query = "SHOW TABLES FROM " + database
        else:
            query = "SHOW TABLES"
        return self.ch.execute_query(query)
    

if __name__ == "__main__":

    pg_admin = PostgresAdmin(postgres)
    ch_admin = ClickHouseAdmin(clickhouse)

    # see tables names
    print("postgres tables:")
    pg_tables = pg_admin.show_tables()
    # pg_tables.reverse()
    for table in pg_tables:
        # pg_admin.remove_table(table[0])
        print(table[0])
        print()
        print(pg_admin.select(table[0]))
        print("-----")

    print("clickhouse tables:")
    ch_tables = ch_admin.show_tables()
    for table in ch_tables:
        # ch_admin.remove_table(table[0])
        print(table[0])
        print()
        print(ch_admin.select(table[0]))
        print("-----")

    # # see all tables and rows
    # print("postgres tables and rows:")
    # print(pg_admin.select(""))
    # print("clickhouse tables and rows:")
    # print(ch_admin.select(""))