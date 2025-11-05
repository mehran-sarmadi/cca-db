"""
Mock demo: run all major methods with mocked clients to see generated SQL and calls.

This is a simple script for visual inspection. It doesn't require live databases.
"""
from unittest.mock import MagicMock

from cca_db.admin import PostgresAdmin, ClickHouseAdmin


def main():
    # Mock Postgres client
    pg = MagicMock(name="PostgresClient")
    pg.execute_query.return_value = [(123,)]
    pg_admin = PostgresAdmin(pg)

    print("-- PostgresAdmin demo --")
    pg_admin.create_table_if_not_exists("experts", {"id": "SERIAL PRIMARY KEY"})
    print(pg.execute_query.call_args)

    pg_admin.select_rows("experts", where=["id > 10"])
    print(pg.execute_query.call_args)

    new_id = pg_admin.insert_call_detail(
        {
            "sentiment_analysis": {"score": 0.9},
            "bad_words": [],
            "start_greeting": {},
            "end_greeting": {},
            "summary": {"text": "ok"},
            "calls_texts": ["hello"],
        }
    )
    print("inserted id:", new_id)
    print(pg.execute_query.call_args)

    # Mock ClickHouse client
    ch = MagicMock(name="ClickHouseClient")
    ch_admin = ClickHouseAdmin(ch)
    print("\n-- ClickHouseAdmin demo --")
    ch_admin.create_database_if_not_exists("db1")
    print(ch.execute_query.call_args)

    ch_admin.create_table_if_not_exists(
        "db1", "t", {"a": "Int32"}, order_by=["a"], partition_by="toDate(now())"
    )
    print(ch.execute_query.call_args)

    ch_admin.aggregate("t", ["a"], {"cnt": "count()"}, where=["a>0"], limit=10)
    print(ch.execute_query.call_args)

    ch_admin.window("t", ["a"], ["sum(b) AS sb"], order_by=["a"]) 
    print(ch.execute_query.call_args)

    ch_admin.time_bucket("t", "ts", "hour", {"cnt": "count()"})
    print(ch.execute_query.call_args)

    ch_admin.create_materialized_view("mv1", "SELECT 1")
    print(ch.execute_query.call_args)

    ch_admin.grouping_sets("t", [["a"], ["b"]], {"cnt": "count()"})
    print(ch.execute_query.call_args)


if __name__ == "__main__":
    main()
