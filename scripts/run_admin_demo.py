import os
import sys
from datetime import datetime

# Make 'src' importable
HERE = os.path.dirname(__file__)
ROOT = os.path.abspath(os.path.join(HERE, ".."))
if ROOT not in sys.path:
    sys.path.append(ROOT)

from src.dbs_admin import PostgresAdmin, ClickHouseAdmin, postgres, clickhouse  # type: ignore
from scripts.generate_mock_data import (
    pg_calls_analysis_rows,
    pg_call_text_json,
    ch_selected_calls_details_rows,
    ch_experts_rows,
)

def print_section(title: str):
    print(f"\n=== {title} ===")

def main():
    pg_admin = PostgresAdmin(postgres)
    ch_admin = ClickHouseAdmin(clickhouse)

    # ---------- Postgres ----------
    print_section("Postgres: create tables")
    pg_admin.create_table(
        "calls_analysis",
        {
            "id": "SERIAL PRIMARY KEY",
            "sentiment_analysis": "TEXT",
            "bad_words": "TEXT",
            "start_greeting": "BOOLEAN",
            "end_greeting": "BOOLEAN",
            "summary": "TEXT",
        },
    )
    pg_admin.create_table(
        "calls_texts",
        {
            "id": "SERIAL PRIMARY KEY",
            "call_id": "INTEGER REFERENCES calls_analysis(id)",
            "text": "TEXT",
        },
    )
    # Small table for upsert demo
    pg_admin.create_table(
        "kv_store",
        {"k": "TEXT PRIMARY KEY", "v": "INTEGER"},
    )

    print_section("Postgres: insert mock data")
    call_ids = []
    for row in pg_calls_analysis_rows():
        call_id = pg_admin.add_call_detail(row)
        call_ids.append(call_id)
        pg_admin.add_call_texts(call_id, pg_call_text_json(call_id))

    print("Inserted call IDs:", call_ids)

    print_section("Postgres: select")
    rows = pg_admin.select("calls_analysis", columns="id, sentiment_analysis, summary")
    for r in rows:
        print(r)

    print_section("Postgres: upsert")
    pg_admin.upsert(
        "kv_store",
        row={"k": "alpha", "v": 1},
        conflict_cols=["k"],
    )
    pg_admin.upsert(
        "kv_store",
        row={"k": "alpha", "v": 2},
        conflict_cols=["k"],
        update_cols=["v"],
    )
    print(pg_admin.select("kv_store"))

    print_section("Postgres: EXPLAIN and statement timeout")
    plan = pg_admin.explain("SELECT * FROM calls_analysis WHERE id = %s", (call_ids[0],))
    print("Explain:", plan)
    pg_admin.set_statement_timeout(10_000)
    print("Statement timeout set.")

    # ---------- ClickHouse ----------
    db = "zaal"
    scd = f"{db}.selected_calls_details"
    experts_tbl = f"{db}.experts"
    mv_target = f"{db}.expert_calls_mv_target"
    mv_name = f"{db}.mv_expert_calls"

    print_section("ClickHouse: create tables")
    ch_admin.create_database_if_not_exists(db)
    ch_admin.create_table(
        db,
        "selected_calls_details",
        {
            "call_id": "UInt64",
            "subscriber_id": "UInt64",
            "expert_id": "UInt64",
            "call_timestamp": "DateTime",
            "duration_seconds": "UInt32",
            "features": "String",
        },
        order_by=["expert_id", "call_timestamp"],
        engine="MergeTree()",
    )
    ch_admin.create_table(
        db,
        "experts",
        {"expert_id": "UInt64", "name": "String"},
        order_by=["expert_id"],
        engine="MergeTree()",
    )

    print_section("ClickHouse: load mock data")
    start = datetime(2025, 1, 1, 9, 0, 0)
    for row in ch_selected_calls_details_rows(start, n=6):
        ch_admin.add_selected_call_detail(row)
    # experts
    cols = ["expert_id", "name"]
    expert_rows = [(r["expert_id"], r["name"]) for r in ch_experts_rows()]
    ch_admin.insert_many(experts_tbl, cols, expert_rows)
    print("Inserted selected_calls_details and experts.")

    print_section("ClickHouse: aggregate")
    agg = ch_admin.aggregate(
        scd,
        group_by=["expert_id"],
        aggregates={"total_calls": "count()", "avg_duration": "avg(duration_seconds)"},
        order_by=["expert_id"],
    )
    for r in agg:
        print(r)

    print_section("ClickHouse: window")
    win = ch_admin.window(
        scd,
        base_columns=["call_id", "expert_id", "duration_seconds", "call_timestamp"],
        window_exprs=["row_number()", "sum(duration_seconds)"],
        partition_by=["expert_id"],
        order_by=["call_timestamp"],
    )
    for r in win[:5]:
        print(r)

    print_section("ClickHouse: time_bucket (day)")
    tb = ch_admin.time_bucket(
        scd,
        timestamp_col="call_timestamp",
        granularity="day",
        value_exprs={"calls": "count()", "dur_sum": "sum(duration_seconds)"},
        order_by_bucket=True,
    )
    for r in tb:
        print(r)

    print_section("ClickHouse: filter")
    flt = ch_admin.filter(
        scd,
        where=["duration_seconds >= 180"],
        columns="expert_id, call_id, duration_seconds",
        order_by=["duration_seconds DESC"],
        limit=5,
    )
    for r in flt:
        print(r)

    print_section("ClickHouse: join with experts")
    j = ch_admin.join(
        scd,
        experts_tbl,
        join_type="LEFT",
        on_condition=f"{scd}.expert_id = {experts_tbl}.expert_id",
        select_exprs=[f"{scd}.expert_id", f"{experts_tbl}.name", "count() AS c"],
        group_by=[f"{scd}.expert_id", f"{experts_tbl}.name"],
        order_by=[f"{scd}.expert_id"],
        settings={"join_algorithm": "auto"},
    )
    for r in j:
        print(r)

    print_section("ClickHouse: materialized view -> target table")
    # target table that MV will write into
    ch_admin.create_table(
        db,
        "expert_calls_mv_target",
        {"expert_id": "UInt64", "cnt": "UInt64"},
        order_by=["expert_id"],
        engine="MergeTree()",
    )
    # MV that pipes grouped counts into target
    select_sql = f"SELECT expert_id, count() AS cnt FROM {scd} GROUP BY expert_id"
    ch_admin.create_materialized_view(mv_name, select_sql, to_table=mv_target)
    # trigger MV by inserting one more row
    ch_admin.add_selected_call_detail(
        {
            "call_id": 9999,
            "subscriber_id": 299,
            "expert_id": 10,
            "call_timestamp": datetime(2025, 1, 2, 10, 0, 0).strftime("%Y-%m-%d %H:%M:%S"),
            "duration_seconds": 120,
            "features": '{"topic":"general","score":0.9,"flag":false}',
        }
    )
    mv_out = ch_admin.filter(mv_target, order_by=["expert_id"])
    for r in mv_out:
        print(r)

    print_section("ClickHouse: sample")
    smp = ch_admin.sample(scd, ratio=0.5, columns="expert_id, call_id, duration_seconds", limit=3)
    for r in smp:
        print(r)

    print_section("ClickHouse: JSON extract")
    js = ch_admin.json_extract(
        scd,
        json_col="features",
        extracts={"topic": ("topic", "String"), "score": ("score", "Float64"), "flag": ("flag", "Bool")},
        limit=5,
    )
    for r in js:
        print(r)

    print_section("ClickHouse: grouping sets")
    gs = ch_admin.grouping_sets(
        scd,
        groupings=[[], ["expert_id"]],
        aggregates={"cnt": "count()", "dur_sum": "sum(duration_seconds)"},
        order_by=["expert_id"],
        settings={"max_bytes_before_external_group_by": 0},
    )
    for r in gs:
        print(r)

    print_section("ClickHouse: optimize and explain")
    print(ch_admin.optimize_table(scd))
    print(ch_admin.explain(f"SELECT count() FROM {scd} WHERE duration_seconds > 0"))

    print_section("Demo completed")

if __name__ == "__main__":
    main()
