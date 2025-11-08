from __future__ import annotations

from ast import List
from typing import Dict, Optional, Any
import json
from datetime import datetime, timedelta
from clickhouse_driver import Client as CHClient
import os
import pandas as pd
import random


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



    def get_counts_pivot(self, table, column, start_date, end_date, freq):
        interval_unit = 'HOUR' if freq == 'H' else 'DAY'

        def fmt(dt):
            if isinstance(dt, datetime):
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            return str(dt).replace("T", " ").split(".")[0]

        start_date = fmt(start_date)
        end_date = fmt(end_date)

        # --- Step 1: Query ClickHouse ---
        query = f"""
            SELECT
                toStartOfInterval(created_at, INTERVAL 1 {interval_unit}) AS time_group,
                arrayJoin(JSONExtractKeys({column})) AS category_key,
                count() AS cnt
            FROM {table}
            WHERE created_at >= toDateTime('{start_date}')
            AND created_at < toDateTime('{end_date}')
            GROUP BY time_group, category_key
            ORDER BY time_group, category_key
        """

        data = self.ch_client.execute(query)

        df = pd.DataFrame(data, columns=["time_group", "category", "count"])
        df_pivot = df.pivot_table(
            index="category",
            columns="time_group",
            values="count",
            fill_value=0,
        )
        df_pivot = df_pivot.reindex(sorted(df_pivot.columns), axis=1)
        return df_pivot

    def get_subcategory_counts_pivot(self, table, column, start_date, end_date, freq):
        """
        Returns a pivoted DataFrame where:
        - rows = subcategories (e.g. c3_1, c3_2, c4_1)
        - includes a 'category' column showing the parent key (e.g. c3, c4)
        - columns = time intervals
        - values = counts
        """

        interval_unit = 'HOUR' if freq == 'H' else 'DAY'

        def fmt(dt):
            if isinstance(dt, datetime):
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            return str(dt).replace("T", " ").split(".")[0]

        start_date = fmt(start_date)
        end_date = fmt(end_date)

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
            GROUP BY time_group, kv_pair
            ORDER BY time_group, kv_pair
        """

        data = self.ch_client.execute(query)

        # Each kv_pair is a tuple (category, subcategory)
        df = pd.DataFrame(data, columns=["time_group", "kv_pair", "count"])

        # Split kv_pair into separate columns
        df[["category", "subcategory"]] = pd.DataFrame(df["kv_pair"].tolist(), index=df.index)

        # Pivot so rows = subcategory, columns = time intervals
        df_pivot = df.pivot_table(
            index=["category", "subcategory"],
            columns="time_group",
            values="count",
            fill_value=0,
        )

        df_pivot = df_pivot.reindex(sorted(df_pivot.columns), axis=1)
        return df_pivot



    def counts_per_timestep(self, table: str, columns: list, from_days_before: int, freq):
        end_date = datetime.now()
        start_date = end_date - timedelta(days=from_days_before)
        dfs_dic = {"all": None, "category": None, "subcategory": None, "campaign": None, "service": None}
        for column in columns:
            if column == "all":
                df = self.get_row_counts_per_timestep(
                    table,
                    start_date,
                    end_date,
                    freq
                )
            elif column == "subcategory":
                df = self.get_subcategory_counts_pivot(
                    table,
                    "category",
                    start_date,
                    end_date,
                    freq
                )
            else:
                df = self.get_counts_pivot(
                    table,
                    column,
                    start_date,
                    end_date,
                    freq
                )
            dfs_dic[column] = df
        return dfs_dic
    
    def get_row_counts_per_timestep(self, table: str, start_date, end_date, freq: str):
        """
        Returns a DataFrame with total row counts per time step.
        Example:
            time_group              count
            2025-11-01 00:00:00     120
            2025-11-01 01:00:00     95
        """

        interval_unit = 'HOUR' if freq == 'H' else 'DAY'

        def fmt(dt):
            if isinstance(dt, datetime):
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            return str(dt).replace("T", " ").split(".")[0]

        start_date = fmt(start_date)
        end_date = fmt(end_date)

        query = f"""
            SELECT
                toStartOfInterval(created_at, INTERVAL 1 {interval_unit}) AS time_group,
                count(*) AS row_count
            FROM {table}
            WHERE created_at >= toDateTime('{start_date}')
            AND created_at < toDateTime('{end_date}')
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
                        num_categories=3,
                        num_campaigns=3,
                        num_services=3,
                        max_random_sub=5):
        all_data = []
        for _ in range(num_data):
            data = {"category": {}, "campaign": {}, "service": {}}

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

            all_data.append(data)
        return all_data


    CLICKHOUSE_TABLES = {
    "morteza_test": {
        "id": "Int64",
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
    table_name = "morteza_test"

    database_operator.ch_client.execute(f"DROP TABLE IF EXISTS {table_name};")
    database_operator.create_table_if_not_exists(database, table_name, CLICKHOUSE_TABLES[table_name])


    # Start time: 7 days ago from now
    all_data = build_mock_data(6)
    now = datetime.now()
    start_time = now - timedelta(days=7)

    for idx, base_row in enumerate(all_data):
        print(f"Inserting row {idx + 1} into ClickHouse…")
        row = dict(base_row)

        # Each subsequent row is 15 minutes apart
        created_at = start_time + timedelta(minutes=15 * idx)
        row["created_at"] = created_at.strftime("%Y-%m-%d %H:%M:%S")
        print(base_row)
        print(created_at)
        database_operator.insert_row(table_name, row)


    dfs_dic = database_operator.counts_per_timestep(
        table_name,
        ["all", "category", "subcategory", "campaign", "service"],
        from_days_before=8,
        freq='D'
    )


    for key, df in dfs_dic.items():
        print(f"\n\nPivot table for {key}:\n")
        print(df)