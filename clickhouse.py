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

    def show_tables(self) -> List[str]:
        """Return list of table names in the current database."""
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
            if isinstance(v, dict):
                return json.dumps(v)
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
                if isinstance(v, dict):
                    return json.dumps(v)
                if isinstance(v, (list, tuple)):
                    return list(v)
                return v
            rows_to_insert = []
            for row in batch:
                vals = [ _convert_value(row[c]) for c in cols ]
                rows_to_insert.append(tuple(vals))
            sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES"
            self.ch_client.execute(sql, rows_to_insert)

    # 5. Data retrieval/query methods
    def get_category_counts(self, table, column, start_date=None, end_date=None, loc_id: Optional[int] = None):
        """
        Returns a DataFrame with each category and its count, sorted by count descending.
        """
        date_filter = ""
        if start_date:
            date_filter += f" AND created_at >= toDateTime('{start_date}')"
        if end_date:
            date_filter += f" AND created_at < toDateTime('{end_date}')"
        loc_filter = f" AND location_id = {loc_id}" if loc_id is not None else ""
        query = f"""
            SELECT
                arrayJoin({column}) AS category,
                count() AS cnt
            FROM {table}
            WHERE 1=1
            {date_filter}
            {loc_filter}
            GROUP BY category
            ORDER BY cnt DESC
        """
        data = self.ch_client.execute(query)
        df = pd.DataFrame(data, columns=["category", "count"])
        return df

    def get_subcategory_counts(self, table, column, start_date=None, end_date=None, loc_id: Optional[int] = None):
        """
        Returns a DataFrame with each (category, subcategory) and its count, sorted by count descending.
        Assumes column is a JSON string mapping categories to subcategory lists.
        """
        date_filter = ""
        if start_date:
            date_filter += f" AND created_at >= toDateTime('{start_date}')"
        if end_date:
            date_filter += f" AND created_at < toDateTime('{end_date}')"
        loc_filter = f" AND location_id = {loc_id}" if loc_id is not None else ""
        query = f"""
            SELECT
                kv_pair.1 AS category,
                kv_pair.2 AS subcategory,
                count() AS cnt
            FROM (
                SELECT arrayJoin(
                    arrayFlatten(
                        arrayMap(
                            key -> arrayMap(
                                subcat -> (key, toString(subcat)),
                                JSONExtractArrayRaw(JSONExtractRaw({column}), key)
                            ),
                            JSONExtractKeys(JSONExtractRaw({column}))
                        )
                    )
                ) AS kv_pair
                FROM {table}
                WHERE 1=1
                {date_filter}
                {loc_filter}
            )
            GROUP BY category, subcategory
            ORDER BY cnt DESC
        """
        data = self.ch_client.execute(query)
        df = pd.DataFrame(data, columns=["category", "subcategory", "count"])
        return df

    def get_counts_pivot(self, table, column, start_date, end_date, freq, loc_id: Optional[int] = None):
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
                arrayJoin({column}) AS category,
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
                arrayJoin(
                    arrayFlatten(
                        arrayMap(
                            key -> arrayMap(
                                subcat -> (key, toString(subcat)),
                                JSONExtractArrayRaw(JSONExtractRaw({column}), key)
                            ),
                            JSONExtractKeys(JSONExtractRaw({column}))
                        )
                    )
                ) AS kv_pair,
                count() AS cnt
            FROM {table}
            WHERE created_at >= toDateTime('{start_date}')
            AND created_at < toDateTime('{end_date}')
            {loc_filter}
            GROUP BY time_group, kv_pair
            ORDER BY time_group, kv_pair
        """
        data = self.ch_client.execute(query)
        df = pd.DataFrame(data, columns=["time_group", "kv_pair", "count"])
        if df.empty:
            return pd.DataFrame()
        kv_df = pd.DataFrame(df["kv_pair"].tolist(), columns=["category", "subcategory"])
        df[["category", "subcategory"]] = kv_df
        df_pivot = df.pivot_table(index=["category", "subcategory"], columns="time_group", values="count", fill_value=0)
        df_pivot = df_pivot.reindex(sorted(df_pivot.columns), axis=1)
        return df_pivot


    def counts_per_timestep(self, table: str, columns: list, from_time_before: str, freq, loc_id: Optional[int] = None) -> Dict[str, pd.DataFrame]:
        """Return dictionary of pivot DataFrames aggregated per time step.

        from_time_before can be a flexible interval string like '100m', '4h', '10d'.
        For backward compatibility, callers may still pass an int (treated as days).
        """
        # Backwards-compatible: if caller passed an int positional (old API), convert it to days string
        if isinstance(from_time_before, int):
            from_time_before = f"{from_time_before}d"

        now = datetime.now()
        value, unit = self.parse_interval(from_time_before)
        if unit == "MINUTE":
            start_date = now - timedelta(minutes=value)
        elif unit == "HOUR":
            start_date = now - timedelta(hours=value)
        elif unit == "DAY":
            start_date = now - timedelta(days=value)
        else:
            raise ValueError(f"Unsupported time unit: {unit}")

        end_date = now
        # Return mapping for the new schema names by default
        dfs_dic: Dict[str, Optional[pd.DataFrame]] = {col: None for col in columns}

        # helper to map common singular names to actual column names in the schema
        col_aliases = {
            "category": "categories",
            "categories": "categories",
            "subcategory": "subcategories",
            "subcategories": "subcategories",
            # keep 'all' mapped to row counts
            "all": "all",
        }

        for column in columns:
            mapped = col_aliases.get(column, column)
            if mapped == "all":
                df = self.get_row_counts_per_timestep(table, start_date, end_date, freq, loc_id)
            elif mapped == "subcategories":
                # For subcategory breakdown we use the JSON-dict column to produce (category, subcategory) pairs
                df = self.get_subcategory_counts_pivot(table, "category_subcategory_dict", start_date, end_date, freq, loc_id)
            elif mapped == "categories":
                df = self.get_counts_pivot(table, "categories", start_date, end_date, freq, loc_id)
            else:
                # fallback: treat provided column as an array-like column name and pass through
                df = self.get_counts_pivot(table, mapped, start_date, end_date, freq, loc_id)

            dfs_dic[column] = df

        return dfs_dic

    def get_row_counts_per_timestep(self, table: str, start_date, end_date, freq: str, loc_id: Optional[int] = None):
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
                count(*) AS row_count
            FROM {table}
            WHERE created_at >= toDateTime('{start_date}')
            AND created_at < toDateTime('{end_date}')
            {loc_filter}
            GROUP BY time_group
            ORDER BY time_group
        """

        data = self.ch_client.execute(query)
        df = pd.DataFrame(data, columns=["time_group", "row_count"])
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
        # allow optional number (defaults to 1) and units like m|min|minute(s), h|hour(s), d|day(s)
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

    def get_counts_pivot(self, table, column, start_date, end_date, freq, loc_id: Optional[int] = None):
        interval_value, interval_unit = self.parse_interval(freq)

        def fmt(dt):
            if isinstance(dt, datetime):
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            return str(dt).replace("T", " ").split(".")[0]

        start_date = fmt(start_date)
        end_date = fmt(end_date)

        # new schema uses `location_id` for location filtering
        loc_filter = f"AND location_id = {loc_id}" if loc_id is not None else ""

        # For array columns (e.g., categories or subcategories), use arrayJoin
        query = f"""
            SELECT
                toStartOfInterval(created_at, INTERVAL {interval_value} {interval_unit}) AS time_group,
                arrayJoin({column}) AS category,
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
        return df_pivot

    def get_subcategory_counts_pivot(self, table, column, start_date, end_date, freq, loc_id: Optional[int] = None):
        interval_value, interval_unit = self.parse_interval(freq)

        def fmt(dt):
            if isinstance(dt, datetime):
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            return str(dt).replace("T", " ").split(".")[0]

        start_date = fmt(start_date)
        end_date = fmt(end_date)

        # new schema uses `location_id` for location filtering
        loc_filter = f"AND location_id = {loc_id}" if loc_id is not None else ""

        # column is expected to be a String containing JSON that maps categories -> [subcategories]
        # Parse the JSON and extract (category, subcategory) pairs
        query = f"""
            SELECT
                toStartOfInterval(created_at, INTERVAL {interval_value} {interval_unit}) AS time_group,
                arrayJoin(
                    arrayFlatten(
                        arrayMap(
                            key -> arrayMap(
                                subcat -> (key, toString(subcat)),
                                JSONExtractArrayRaw(JSONExtractRaw({column}), key)
                            ),
                            JSONExtractKeys(JSONExtractRaw({column}))
                        )
                    )
                ) AS kv_pair,
                count() AS cnt
            FROM {table}
            WHERE created_at >= toDateTime('{start_date}')
            AND created_at < toDateTime('{end_date}')
            {loc_filter}
            GROUP BY time_group, kv_pair
            ORDER BY time_group, kv_pair
        """

        data = self.ch_client.execute(query)
        df = pd.DataFrame(data, columns=["time_group", "kv_pair", "count"])
        if df.empty:
            return pd.DataFrame()

        # Safely unpack kv_pair tuples into category and subcategory columns
        kv_df = pd.DataFrame(df["kv_pair"].tolist(), columns=["category", "subcategory"])
        df[["category", "subcategory"]] = kv_df
        df_pivot = df.pivot_table(index=["category", "subcategory"], columns="time_group", values="count", fill_value=0)
        df_pivot = df_pivot.reindex(sorted(df_pivot.columns), axis=1)
        return df_pivot



    def counts_per_timestep(self, table: str, columns: list, from_time_before: str, freq, loc_id: Optional[int] = None) -> Dict[str, pd.DataFrame]:
        """Return dictionary of pivot DataFrames aggregated per time step.

        from_time_before can be a flexible interval string like '100m', '4h', '10d'.
        For backward compatibility, callers may still pass an int (treated as days).
        """
        # Backwards-compatible: if caller passed an int positional (old API), convert it to days string
        if isinstance(from_time_before, int):
            from_time_before = f"{from_time_before}d"

        now = datetime.now()
        value, unit = self.parse_interval(from_time_before)
        if unit == "MINUTE":
            start_date = now - timedelta(minutes=value)
        elif unit == "HOUR":
            start_date = now - timedelta(hours=value)
        elif unit == "DAY":
            start_date = now - timedelta(days=value)
        else:
            raise ValueError(f"Unsupported time unit: {unit}")

        end_date = now
        # Return mapping for the new schema names by default
        dfs_dic: Dict[str, Optional[pd.DataFrame]] = {col: None for col in columns}

        # helper to map common singular names to actual column names in the schema
        col_aliases = {
            "category": "categories",
            "categories": "categories",
            "subcategory": "subcategories",
            "subcategories": "subcategories",
            # keep 'all' mapped to row counts
            "all": "all",
        }

        for column in columns:
            mapped = col_aliases.get(column, column)
            if mapped == "all":
                df = self.get_row_counts_per_timestep(table, start_date, end_date, freq, loc_id)
            elif mapped == "subcategories":
                # For subcategory breakdown we use the JSON-dict column to produce (category, subcategory) pairs
                df = self.get_subcategory_counts_pivot(table, "category_subcategory_dict", start_date, end_date, freq, loc_id)
            elif mapped == "categories":
                df = self.get_counts_pivot(table, "categories", start_date, end_date, freq, loc_id)
            else:
                # fallback: treat provided column as an array-like column name and pass through
                df = self.get_counts_pivot(table, mapped, start_date, end_date, freq, loc_id)

            dfs_dic[column] = df

        return dfs_dic

    def get_row_counts_per_timestep(self, table: str, start_date, end_date, freq: str, loc_id: Optional[int] = None):
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
                count(*) AS row_count
            FROM {table}
            WHERE created_at >= toDateTime('{start_date}')
            AND created_at < toDateTime('{end_date}')
            {loc_filter}
            GROUP BY time_group
            ORDER BY time_group
        """

        data = self.ch_client.execute(query)
        df = pd.DataFrame(data, columns=["time_group", "row_count"])
        return df
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

    @staticmethod
    def parse_interval(interval_str: str) -> Tuple[int, str]:
        """Parse interval like '5m', '10min', '4h', '2d', or single-letter 'H','D','M' into (value, unit).

        Returns:
            (value, unit) where unit is one of 'MINUTE', 'HOUR', 'DAY'.
        """
        if not isinstance(interval_str, str):
            raise ValueError(f"Interval must be a string, got {type(interval_str)}")
        s = interval_str.strip()
        # allow optional number (defaults to 1) and units like m|min|minute(s), h|hour(s), d|day(s)
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
    
    def show_tables(self) -> List[str]:
        """Return list of table names in the current database."""
        query = "SHOW TABLES"
        data = self.ch_client.execute(query)
        tables = [row[0] for row in data]
        return tables