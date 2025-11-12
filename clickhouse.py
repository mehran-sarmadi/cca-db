from __future__ import annotations

import re
from typing import List, Dict, Optional, Any, Tuple
import json
from datetime import datetime, timedelta
from clickhouse_driver import Client as CHClient
import os
import pandas as pd
import random
from tqdm import tqdm
import time

class ClickHouseDBOps():

    # 1. Constructor
    def __init__ (self, config) -> None:
        self.ch_client = CHClient(
            database=config['database'],
            host=config['host'],
            user=config['user'],
            password=config['password'],
        )

    # 2. Connection management
    def close(self):
        self.ch_client.disconnect()

    # 3. Table/structure management
    def create_table_if_not_exists(
            self,
            database_name: str,
            table_name: str,
            columns: Dict[str, str],
            order_by: Optional[List[str]] = None,
            partition_by: Optional[str] = None,
            engine: str = "MergeTree()",
            ttl: Optional[str] = None,
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
            self.ch_client.execute(sql)

    def show_tables(self, database_name: Optional[str] = None) -> List[str]:
        """
        Returns a list of all table names in the specified database.
        If database_name is not provided, uses the connected database.
        """
        if database_name:
            query = f"SHOW TABLES FROM {database_name}"
        else:
            query = "SHOW TABLES"
        data = self.ch_client.execute(query)
        tables = [row[0] for row in data]
        return tables

    # 4. Data insertion methods
    def insert_row(self, table: str, row: Dict[str, Any]) -> None:
        cols = list(row.keys())
        def _convert_value(v: Any):
            if isinstance(v, bool):
                return 1 if v else 0
            if v is None:
                return None
            # For Map columns in ClickHouse, pass dicts through as-is
            if isinstance(v, dict):
                return v
            if isinstance(v, (list, tuple)):
                return list(v)
            return v
        vals = [ _convert_value(row[c]) for c in cols ]
        sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES"
        self.ch_client.execute(sql, [tuple(vals)])

    def insert_batch(self, table: str, rows: list[Dict[str, Any]], batch_size: int = 1000) -> None:
        if not rows:
            return
        for batch_start in tqdm(range(0, len(rows), batch_size), total=(len(rows) + batch_size - 1) // batch_size):
            batch_end = min(batch_start + batch_size, len(rows))
            batch = rows[batch_start:batch_end]
            cols = list(batch[0].keys())
            def _convert_value(v: Any):
                if isinstance(v, bool):
                    return 1 if v else 0
                if v is None:
                    return None
                # For Map columns in ClickHouse, pass dicts through as-is
                if isinstance(v, dict):
                    return v
                if isinstance(v, (list, tuple)):
                    return list(v)
                return v
            rows_to_insert = []
            for row in batch:
                vals = [ _convert_value(row[c]) for c in cols ]
                rows_to_insert.append(tuple(vals))
            sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES"
            self.ch_client.execute(sql, rows_to_insert)

    def counts_per_timestep(self, table: str, columns: list[str], from_time_before: int | str | datetime, freq: str, loc_id: Optional[int] = None, end_date: Optional[datetime] = None) -> Dict[str, pd.DataFrame]:
        """Return dictionary of pivot DataFrames aggregated per time step."""

        if not columns:
            return {}

        # Resolve start/end time
        if isinstance(from_time_before, datetime):
            start_date = from_time_before
            end_date = end_date or datetime.now()
        else:
            if isinstance(from_time_before, int):
                from_time_before = f"{from_time_before}d"
            value, unit = self.parse_interval(str(from_time_before))
            delta_map = {
                "MINUTE": timedelta(minutes=value),
                "HOUR": timedelta(hours=value),
                "DAY": timedelta(days=value),
            }
            end_date = end_date or datetime.now()
            start_date = end_date - delta_map[unit]

        result = {
            "categories": self.get_category_counts_pivot(table, "category_subcategory_dict", start_date, end_date, freq, loc_id),
            "subcategories": self.get_subcategory_counts_pivot(table, "category_subcategory_dict", start_date, end_date, freq, loc_id),
            "all": self.get_row_counts_per_timestep(table, start_date, end_date, freq, loc_id),
        }

        return result

    def get_category_counts_pivot(self, table, column, start_date, end_date, freq, loc_id: Optional[int] = None):
        interval_value, interval_unit = self.parse_interval(freq)

        def fmt(dt):
            if isinstance(dt, datetime):
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            return str(dt).replace("T", " ").split(".")[0]

        start_date, end_date = fmt(start_date), fmt(end_date)
        loc_filter = f"AND location_id = {loc_id}" if loc_id is not None else ""

        query = f"""
            SELECT
                toStartOfInterval(created_at, INTERVAL {interval_value} {interval_unit}) AS time_group,
                arrayJoin(mapKeys({column})) AS category,
                count() AS cnt
            FROM {table}
            WHERE created_at >= toDateTime('{start_date}')
            AND created_at < toDateTime('{end_date}')
            {loc_filter}
            GROUP BY time_group, category
            ORDER BY time_group, category
        """
        data = self.ch_client.execute(query)
        df = pd.DataFrame(data, columns=["time_group", "category", "count"])
        if df.empty:
            return pd.DataFrame()
        df_pivot = df.pivot_table(index="category", columns="time_group", values="count", fill_value=0)
        df_pivot = df_pivot.reindex(sorted(df_pivot.columns), axis=1)
        df_pivot.reset_index(inplace=True)
        return df_pivot


    def get_subcategory_counts_pivot(self, table, column, start_date, end_date, freq, loc_id: Optional[int] = None):
        interval_value, interval_unit = self.parse_interval(freq)
        def fmt(dt):
            if isinstance(dt, datetime):
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            return str(dt).replace("T", " ").split(".")[0]
        start_date = fmt(start_date)
        end_date = fmt(end_date)
        loc_filter = f"AND location_id = {loc_id}" if loc_id is not None else ""
        query = f"""
            SELECT
                toStartOfInterval(created_at, INTERVAL {interval_value} {interval_unit}) AS time_group,
                (k, toString(subcat)) AS kv_pair,
                count() AS cnt
            FROM (
                SELECT created_at, k, arrayJoin({column}[k]) AS subcat
                FROM (
                    SELECT created_at, arrayJoin(mapKeys({column})) AS k, {column}
                    FROM {table}
                    WHERE created_at >= toDateTime('{start_date}')
                    AND created_at < toDateTime('{end_date}')
                    {loc_filter}
                )
            )
            GROUP BY time_group, kv_pair
            ORDER BY time_group, kv_pair
        """
        # t1 = time.time()
        data = self.ch_client.execute(query)
        # t2 = time.time()
        # print(f"Time taken for subcategory query: {t2 - t1} seconds")
        df = pd.DataFrame(data, columns=["time_group", "kv_pair", "count"])
        if df.empty:
            return pd.DataFrame()
        kv_df = pd.DataFrame(df["kv_pair"].tolist(), columns=["category", "subcategory"])
        df[["category", "subcategory"]] = kv_df
        df_pivot = df.pivot_table(index=["category", "subcategory"], columns="time_group", values="count", fill_value=0)
        df_pivot = df_pivot.reindex(sorted(df_pivot.columns), axis=1)
        df_pivot.reset_index(inplace=True)
        return df_pivot
    
    def get_row_counts_per_timestep(self, table: str, start_date: datetime, end_date: datetime, freq: str, loc_id: Optional[int] = None) -> pd.DataFrame:
        """Returns a DataFrame with row counts aggregated per time step."""
        interval_value, interval_unit = self.parse_interval(freq)
        def fmt(dt):
            if isinstance(dt, datetime):
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            return str(dt).replace("T", " ").split(".")[0]
        start_date = fmt(start_date)
        end_date = fmt(end_date)
        loc_filter = f"AND location_id = {loc_id}" if loc_id is not None else ""
        query = f"""
            SELECT
                toStartOfInterval(created_at, INTERVAL {interval_value} {interval_unit}) AS time_group,
                count() AS cnt
            FROM {table}
            WHERE created_at >= toDateTime('{start_date}')
            AND created_at < toDateTime('{end_date}')
            {loc_filter}
            GROUP BY time_group
            ORDER BY time_group
        """
        # t1 = time.time()
        data = self.ch_client.execute(query)
        # t2 = time.time()
        # print(f"Time taken for row counts query: {t2 - t1} seconds")
        df = pd.DataFrame(data, columns=["time_group", "count"])
        return df


    # 6. Utility/helper methods
    @staticmethod
    def parse_interval(interval_str: str) -> Tuple[int, str]:
        """Parse interval like '5m', '10min', '4h', '2d', or single-letter 'H','D','M' into (value, unit).

        Returns:
            (value, unit) where unit is one of 'MINUTE', 'HOUR', 'DAY'.
        """
        if not isinstance(interval_str, str):
            raise ValueError(f"Interval must be a string, got {type(interval_str)}")
        s = interval_str.strip()
        # allow optional number (defaults to 1) and units like m|min|minute(s), h|hour|hours|d|day|days
        match = re.match(r"(?:(\d+)\s*)?(m|min|minute|minutes|h|hour|hours|d|day|days)$", s, re.I)
        if not match:
            raise ValueError(f"Invalid interval format: {interval_str}")
        value_str = match.group(1)
        value = int(value_str) if value_str is not None else 1
        unit_raw = match.group(2).lower()
        if unit_raw.startswith("m"):
            return value, "MINUTE"
        if unit_raw.startswith("h"):
            return value, "HOUR"
        if unit_raw.startswith("d"):
            return value, "DAY"
        raise ValueError(f"Unsupported interval unit: {unit_raw}")
    

    def counts_per_timestep_all_locs(
        self,
        table: str,
        columns: list[str],
        from_time_before: int | str | datetime,
        freq: str,
        end_date: Optional[datetime] = None
    ) -> Dict[int, Dict[str, pd.DataFrame]]:
        """Return dictionary of pivot DataFrames aggregated per time step for all location_ids."""

        if not columns:
            return {}

        # Resolve start/end time
        if isinstance(from_time_before, datetime):
            start_date = from_time_before
            end_date = end_date or datetime.now()
        else:
            if isinstance(from_time_before, int):
                from_time_before = f"{from_time_before}d"
            value, unit = self.parse_interval(str(from_time_before))
            delta_map = {
                "MINUTE": timedelta(minutes=value),
                "HOUR": timedelta(hours=value),
                "DAY": timedelta(days=value),
            }
            end_date = end_date or datetime.now()
            start_date = end_date - delta_map[unit]

        result = {
            "categories": self.get_category_counts_pivot_all_locs(table, "category_subcategory_dict", start_date, end_date, freq),
            "subcategories": self.get_subcategory_counts_pivot_all_locs(table, "category_subcategory_dict", start_date, end_date, freq),
            "all": self.get_row_counts_per_timestep_all_locs(table, start_date, end_date, freq)
        }

        return result



    def get_category_counts_pivot_all_locs(self, table, column, start_date, end_date, freq):
        interval_value, interval_unit = self.parse_interval(freq)

        def fmt(dt):
            if isinstance(dt, datetime):
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            return str(dt).replace("T", " ").split(".")[0]

        start_date, end_date = fmt(start_date), fmt(end_date)

        query = f"""
            SELECT
                toStartOfInterval(created_at, INTERVAL {interval_value} {interval_unit}) AS time_group,
                arrayJoin(mapKeys({column})) AS category,
                location_id,
                count() AS cnt
            FROM {table}
            WHERE created_at >= toDateTime('{start_date}')
            AND created_at < toDateTime('{end_date}')
            GROUP BY time_group, category, location_id
            ORDER BY time_group, category, location_id
        """
        data = self.ch_client.execute(query)
        df = pd.DataFrame(data, columns=["time_group", "category", "location_id", "count"])
        if df.empty:
            return pd.DataFrame()
        df_pivot = df.pivot_table(index=["location_id", "category"], columns="time_group", values="count", fill_value=0)
        df_pivot = df_pivot.reindex(sorted(df_pivot.columns), axis=1)
        df_pivot.reset_index(inplace=True)
        return df_pivot


    def get_subcategory_counts_pivot_all_locs(self, table, column, start_date, end_date, freq):
        interval_value, interval_unit = self.parse_interval(freq)
        def fmt(dt):
            return dt.strftime("%Y-%m-%d %H:%M:%S") if isinstance(dt, datetime) else str(dt).replace("T", " ").split(".")[0]
        start_date, end_date = fmt(start_date), fmt(end_date)

        query = f"""
            SELECT
                toStartOfInterval(created_at, INTERVAL {interval_value} {interval_unit}) AS time_group,
                (k, toString(subcat)) AS kv_pair,
                location_id,
                count() AS cnt
            FROM (
                SELECT created_at, k, arrayJoin({column}[k]) AS subcat, location_id
                FROM (
                    SELECT created_at, arrayJoin(mapKeys({column})) AS k, {column}, location_id
                    FROM {table}
                    WHERE created_at >= toDateTime('{start_date}')
                    AND created_at < toDateTime('{end_date}')
                )
            )
            GROUP BY time_group, kv_pair, location_id
            ORDER BY time_group, kv_pair, location_id
        """
        data = self.ch_client.execute(query)
        df = pd.DataFrame(data, columns=["time_group", "kv_pair", "location_id", "count"])
        if df.empty:
            return pd.DataFrame()
        kv_df = pd.DataFrame(df["kv_pair"].tolist(), columns=["category", "subcategory"])
        df[["category", "subcategory"]] = kv_df
        df_pivot = df.pivot_table(index=["location_id", "category", "subcategory"], columns="time_group", values="count", fill_value=0)
        df_pivot = df_pivot.reindex(sorted(df_pivot.columns), axis=1)
        df_pivot.reset_index(inplace=True)
        return df_pivot

    def get_row_counts_per_timestep_all_locs(self, table, start_date, end_date, freq):
        interval_value, interval_unit = self.parse_interval(freq)
        def fmt(dt):
            return dt.strftime("%Y-%m-%d %H:%M:%S") if isinstance(dt, datetime) else str(dt).replace("T", " ").split(".")[0]
        start_date, end_date = fmt(start_date), fmt(end_date)

        query = f"""
            SELECT
                toStartOfInterval(created_at, INTERVAL {interval_value} {interval_unit}) AS time_group,
                location_id,
                count() AS cnt
            FROM {table}
            WHERE created_at >= toDateTime('{start_date}')
            AND created_at < toDateTime('{end_date}')
            GROUP BY time_group, location_id
            ORDER BY time_group, location_id
        """
        data = self.ch_client.execute(query)
        return pd.DataFrame(data, columns=["time_group", "location_id", "count"])
