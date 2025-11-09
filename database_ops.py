from __future__ import annotations

from ast import List
from typing import Dict, Optional, Any
import json
from datetime import datetime, timedelta
from clickhouse_driver import Client as CHClient
import os
import pandas as pd
import random
from tqdm import tqdm

class DataBaseOps():
    def __init__ (self, config) -> None:
        self.ch_client = CHClient(
            database=config['database'],
            host=config['host'],
            user=config['user'],
            password=config['password'],
        )

    def close(self):
        self.ch_client.disconnect()


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
            self.ch_client.execute(sql)

    def insert_batch(self, table: str, rows: list[Dict[str, Any]], batch_size: int = 1000) -> None:
        """
        Insert multiple rows in batches for better performance.
        
        Args:
            table: The table name to insert into
            rows: List of dictionaries representing rows to insert
            batch_size: Number of rows to insert per batch (default 1000)
        """
        if not rows:
            return
        
        # Process rows in batches
        for batch_start in tqdm(range(0, len(rows), batch_size), total=(len(rows) + batch_size - 1) // batch_size):
            batch_end = min(batch_start + batch_size, len(rows))
            batch = rows[batch_start:batch_end]
            
            # Get columns from first row
            cols = list(batch[0].keys())
            
            # Build VALUES clause for all rows in batch
            values_clauses = []
            for row in batch:
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
                values_clauses.append(f"({', '.join(vals)})")
            
            sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES {', '.join(values_clauses)}"
            self.ch_client.execute(sql)

    def get_counts_pivot(self, table, column, start_date, end_date, freq, loc_id: Optional[int] = None):
        interval_unit = 'HOUR' if freq == 'H' else 'DAY'

        def fmt(dt):
            if isinstance(dt, datetime):
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            return str(dt).replace("T", " ").split(".")[0]

        start_date = fmt(start_date)
        end_date = fmt(end_date)

        loc_filter = f"AND loc_id = {loc_id}" if loc_id is not None else ""

        query = f"""
            SELECT
                toStartOfInterval(created_at, INTERVAL 1 {interval_unit}) AS time_group,
                arrayJoin(JSONExtractKeys({column})) AS category_key,
                count() AS cnt
            FROM {table}
            WHERE created_at >= toDateTime('{start_date}')
            AND created_at < toDateTime('{end_date}')
            {loc_filter}
            GROUP BY time_group, category_key
            ORDER BY time_group, category_key
        """

        data = self.ch_client.execute(query)

        df = pd.DataFrame(data, columns=["time_group", "category", "count"])
        df_pivot = df.pivot_table(index="category", columns="time_group", values="count", fill_value=0)
        df_pivot = df_pivot.reindex(sorted(df_pivot.columns), axis=1)
        return df_pivot

    def get_subcategory_counts_pivot(self, table, column, start_date, end_date, freq, loc_id: Optional[int] = None):
        interval_unit = 'HOUR' if freq == 'H' else 'DAY'

        def fmt(dt):
            if isinstance(dt, datetime):
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            return str(dt).replace("T", " ").split(".")[0]

        start_date = fmt(start_date)
        end_date = fmt(end_date)

        loc_filter = f"AND loc_id = {loc_id}" if loc_id is not None else ""

        query = f"""
            SELECT
                toStartOfInterval(created_at, INTERVAL 1 {interval_unit}) AS time_group,
                arrayJoin(
                    arrayFlatten(
                        arrayMap(
                            key ->
                                CASE
                                    WHEN JSONType(JSONExtractRaw({column}, key)) = 'Array' THEN
                                        arrayMap(x -> (key, x), JSONExtractArrayRaw({column}, key))
                                    WHEN JSONType(JSONExtractRaw({column}, key)) = 'Object' THEN
                                        arrayMap(x -> (key, x), JSONExtractKeys(JSONExtractRaw({column}, key)))
                                    ELSE
                                        [(key, key)]
                                END,
                            JSONExtractKeys({column})
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
        # Safely unpack kv_pair tuples into category and subcategory columns
        kv_df = pd.DataFrame(df["kv_pair"].tolist(), columns=["category", "subcategory"])
        df[["category", "subcategory"]] = kv_df
        df_pivot = df.pivot_table(index=["category", "subcategory"], columns="time_group", values="count", fill_value=0)
        df_pivot = df_pivot.reindex(sorted(df_pivot.columns), axis=1)
        return df_pivot



    def counts_per_timestep(self, table: str, columns: list, from_days_before: int, freq, loc_id: Optional[int] = None) -> Dict[str, pd.DataFrame]:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=from_days_before)
        dfs_dic = {"all": None, "category": None, "subcategory": None, "campaign": None, "service": None}
        for column in columns:
            if column == "all":
                df = self.get_row_counts_per_timestep(
                    table,
                    start_date,
                    end_date,
                    freq,
                    loc_id
                )
            elif column == "subcategory":
                df = self.get_subcategory_counts_pivot(
                    table,
                    "category",
                    start_date,
                    end_date,
                    freq,
                    loc_id
                )
            else:
                df = self.get_counts_pivot(
                    table,
                    column,
                    start_date,
                    end_date,
                    freq,
                    loc_id
                )
            dfs_dic[column] = df
        return dfs_dic

    def get_row_counts_per_timestep(self, table: str, start_date, end_date, freq: str, loc_id: Optional[int] = None):
        interval_unit = 'HOUR' if freq == 'H' else 'DAY'

        def fmt(dt):
            if isinstance(dt, datetime):
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            return str(dt).replace("T", " ").split(".")[0]

        start_date = fmt(start_date)
        end_date = fmt(end_date)

        loc_filter = f"AND loc_id = {loc_id}" if loc_id is not None else ""

        query = f"""
            SELECT
                toStartOfInterval(created_at, INTERVAL 1 {interval_unit}) AS time_group,
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


if __name__ == "__main__":

    from dotenv import load_dotenv
    load_dotenv()

    def build_mock_data(num_data=700,
                        num_categories=15,
                        num_campaigns=5,
                        num_services=5,
                        max_random_sub=5):
        now = datetime.now()
        start_time = now - timedelta(days=10)
        all_data = []
        for _ in tqdm(range(num_data), total=num_data):
            data = {"loc_id": None,"category": {}, "campaign": {}, "service": {}}

            # categories: c1..cN each with specified number of subcategories (or random)
            for _ in range(random.randint(1, 3)):
                i = random.randint(1, num_categories)
                key = f"c{i}"
                n = random.randint(0, max_random_sub)
                subs = [f"c{i}_{j}" for j in range(1, n + 1)]
                data["category"][key] = subs

            # campaigns: cmp1..cmpN, optional sub-items (or random)
            for _ in range(random.randint(1, 3)):
                i = random.randint(1, num_campaigns)
                key = f"cmp{i}"
                n = random.randint(0, max_random_sub)
                subs = [f"cmp{i}_{j}" for j in range(1, n + 1)]
                data["campaign"][key] = subs

            for _ in range(random.randint(1, 3)):
                i = random.randint(1, num_services)
                key = f"srv{i}"
                n = random.randint(0, max_random_sub)
                subs = [f"srv{i}_{j}" for j in range(1, n + 1)]
                data["service"][key] = subs
            
            data["loc_id"] = 1000 + random.randint(1, 10)


            created_at = start_time + timedelta(minutes=random.randint(0, 14400))
            data["created_at"] = created_at.strftime("%Y-%m-%d %H:%M:%S")

            all_data.append(data)
        return all_data


    CLICKHOUSE_TABLES = {
    "superset_test": {
        "id": "Int64",
        "loc_id": "Int64",
        "category": "String",
        "campaign": "String",
        "service": "String",
        "created_at": "DateTime",
    }
    }



    ch_client_config = {
        "database": os.getenv("CLICKHOUSE_DB"),
        "host": os.getenv("CLICKHOUSE_HOST"),
        "user": os.getenv("CLICKHOUSE_USER"),
        "password": os.getenv("CLICKHOUSE_PASSWORD"),
    }

    database_operator = DataBaseOps(ch_client_config)

    
    database = "zaal"
    table_name = "superset_test"

    database_operator.ch_client.execute(f"DROP TABLE IF EXISTS {table_name};")
    database_operator.create_table_if_not_exists(database, table_name, CLICKHOUSE_TABLES[table_name])


    # Start time: 10 days ago from now
    all_data = build_mock_data(400000)

    database_operator.insert_batch(table_name, all_data)

    dfs_dic = database_operator.counts_per_timestep(
        table_name,
        ["all", "category", "subcategory", "campaign", "service"],
        from_days_before=8,
        freq='D',
        # loc_id=1003
    )


    for key, df in dfs_dic.items():
        print(f"\n\nPivot table for {key}:\n")
        print(df)