from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

from src.clients import ClickHouseClient, PostgresClient
from src.admin import (
    ClickHouseAdmin,
    PostgresAdmin,
    _build_group_by_clause,
    _build_having_clause,
    _build_order_by_clause,
    _build_where_clause,
)
from src.config import CLICKHOUSE_TABLES, POSTGRES_TABLES


def _print_title(title: str) -> None:
    print("\n" + "=" * len(title))
    print(title)
    print("=" * len(title))


def _looks_like_json(s: str) -> bool:
    s = s.strip()
    return (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]"))


def _truncate_for_display(obj: object, limit: int = 2) -> object:
    """Limit long lists for display to the first `limit` items.

    - For top-level lists (e.g., calls_texts column), only show the first `limit` and append
      an indicator with how many more items remain.
    - For dicts, specifically truncate the `calls_texts` key if present.
    Other fields are left as-is for readability.
    """
    if isinstance(obj, list):
        if len(obj) <= limit:
            return obj
        shown = obj[:limit]
        remaining = len(obj) - limit
        return shown + [{"…": f"+{remaining} more"}]
    if isinstance(obj, dict) and "calls_texts" in obj and isinstance(obj["calls_texts"], list):
        new_obj = dict(obj)
        new_obj["calls_texts"] = _truncate_for_display(obj["calls_texts"], limit)  # type: ignore[index]
        return new_obj
    return obj


def print_rows_pretty(rows: Sequence[Sequence[object]]) -> None:
    """Pretty-print a sequence of rows, JSON-decoding items when applicable.

    Long lists (like calls_texts) are truncated to 2 items to keep output concise.
    """
    print(f"rows: {len(rows) if rows is not None else 0}")
    if not rows:
        return
    for i, row in enumerate(rows, start=1):
        print(f"-- row {i} --")
        for j, item in enumerate(row, start=1):
            if isinstance(item, str) and _looks_like_json(item):
                try:
                    item = json.loads(item)
                except json.JSONDecodeError:
                    pass
            if isinstance(item, (dict, list)):
                item = _truncate_for_display(item, limit=2)
                print(f"[{j}]\n" + json.dumps(item, indent=2, ensure_ascii=False))
            else:
                print(f"[{j}] {item}")
        print()


def print_table(rows: List[Sequence[object]], headers: Optional[List[str]] = None) -> None:
    if not rows:
        print("(no rows)")
        return
    str_rows = [["" if c is None else str(c) for c in r] for r in rows]
    if headers:
        str_rows = [headers] + str_rows
    widths = [max(len(r[col]) for r in str_rows) for col in range(len(str_rows[0]))]
    if headers:
        head = " | ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
        print(head)
        print("-+-".join("-" * w for w in widths))
    for r in (str_rows[1:] if headers else str_rows):
        print(" | ".join(r[i].ljust(widths[i]) for i in range(len(r))))


def load_mock_data() -> dict:
    """Load JSONL mock data from the repo's data/ folder."""
    root = Path(__file__).parent
    data_dir = root / "data"
    conversations_dir = data_dir / "conversations"
    conv_out_dir = data_dir / "conversations_output"

    sample = {"subscribers": [], "experts": [], "calls_details": []}
    for id in range(1, 10):
        with open(conv_out_dir / f"conversation_{id}.jsonl", "r", encoding="utf-8") as f:
            call_details = f.read()
            data = json.loads(call_details)
        row = {
            "id": id,
            "sentiment_analysis": data["sentiment_analysis"],
            "bad_words": data["bad_words"],
            "start_greeting": data["start_greeting"],
            "end_greeting": data["end_greeting"],
            "summary": data["summary"],
            "calls_texts": [],
        }
        with open(conversations_dir / f"conversation_{id}.jsonl", "r", encoding="utf-8") as f:
            for line in f:
                call_text = json.loads(line)
                row["calls_texts"].append(call_text)

        sample["calls_details"].append(row)
        sample["subscribers"].append({"id": id + 100})
        sample["experts"].append({"id": id + 100, "name": f"Expert {id}"})

    return sample


# --- Small, readable steps -------------------------------------------------

def ensure_postgres_schema(pg: PostgresAdmin) -> None:
    """Drop and recreate Postgres tables used in the sample."""
    pg.drop_table_if_exists("experts")
    pg.drop_table_if_exists("subscribers")
    pg.drop_table_if_exists("calls_details")

    pg.create_table_if_not_exists("experts", POSTGRES_TABLES["experts"])
    pg.create_table_if_not_exists("subscribers", POSTGRES_TABLES["subscribers"])
    pg.create_table_if_not_exists("calls_details", POSTGRES_TABLES["calls_details"])


def ensure_clickhouse_schema(ch: ClickHouseAdmin, database: str = "zaal") -> None:
    """Drop and recreate ClickHouse table used in the sample."""
    ch.drop_table_if_exists("selected_calls_details")
    ch.create_table_if_not_exists(database, "selected_calls_details", CLICKHOUSE_TABLES["selected_calls_details"]) 


def insert_postgres_data(pg: PostgresAdmin, sample: dict) -> None:
    """Insert experts, subscribers, and calls_details rows into Postgres."""
    for idx, _ in enumerate(sample["calls_details"]):
        print(f"Inserting row {idx + 1} into Postgres…")
        pg.insert_row("experts", sample["experts"][idx])
        pg.insert_row("subscribers", sample["subscribers"][idx])
        pg.insert_row("calls_details", sample["calls_details"][idx])


def insert_clickhouse_data(ch: ClickHouseAdmin, sample: dict) -> None:
    """Insert rows into ClickHouse with a generated created_at timestamp."""
    now = datetime.now()
    for idx, base_row in enumerate(sample["calls_details"]):
        print(f"Inserting row {idx + 1} into ClickHouse…")
        row = dict(base_row)
        row["created_at"] = (now - timedelta(hours=idx)).strftime("%Y-%m-%d %H:%M:%S")
        ch.insert_row("selected_calls_details", row)


def demo_postgres_select(pg: PostgresAdmin) -> None:
    _print_title("Postgres: Angry subscriber sentiment")
    rows = pg.select_rows(
        "calls_details",
        where=["(sentiment_analysis->'subscriber'->>'class') = 'ANGRY'"],
    )
    if rows:
        for r in rows:
            _id = r[0]
            # r[1] is the sentiment_analysis JSONB
            sub_class = None
            try:
                sub_class = r[1]["subscriber"]["class"]
            except Exception:
                pass
            print(f"id={_id} | subscriber.class={sub_class}")
    else:
        print("(no rows)")


def demo_clickhouse_select(ch: ClickHouseAdmin) -> None:
    _print_title("ClickHouse: Angry subscriber sentiment")
    rows = ch.select_rows(
        "selected_calls_details",
        where=["JSONExtractString(sentiment_analysis, 'subscriber', 'class') = 'ANGRY'"],
    )
    print_rows_pretty(rows or [])


def demo_clickhouse_aggregation(ch: ClickHouseAdmin) -> None:
    _print_title("ClickHouse aggregation: count by subscriber sentiment class")
    group_by = ["JSONExtractString(sentiment_analysis, 'subscriber', 'class')"]
    aggregates = {"cnt": "count()"}
    rows = ch.aggregate(
        table="selected_calls_details",
        group_by=group_by,
        aggregates=aggregates,
        where=None,
        having=["count() > 0"],
        order_by=["cnt DESC"],
        limit=10,
    )
    print_table(rows or [], headers=["subscriber_class", "cnt"])  # type: ignore[arg-type]


def demo_builders_sql() -> str:
    """Show how the SQL helper builders combine into one readable query string."""
    group_by = ["JSONExtractString(sentiment_analysis, 'subscriber', 'class')"]
    gb_expr = group_by[0]
    sql = (
        f"SELECT {gb_expr}, count() AS cnt FROM selected_calls_details"
        + _build_where_clause(None)
        + _build_group_by_clause(group_by)
        + _build_having_clause(["count() > 0"]) 
        + _build_order_by_clause(["cnt DESC"]) 
        + " LIMIT 10"
    )
    return sql


def demo_clickhouse_time_bucket(ch: ClickHouseAdmin) -> None:
    _print_title("ClickHouse time bucket (hour): totals and angry calls")
    rows = ch.time_bucket(
        table="selected_calls_details",
        timestamp_col="created_at",
        granularity="hour",
        value_exprs={
            "total_calls": "count()",
            "angry_calls": "countIf(JSONExtractString(sentiment_analysis, 'subscriber', 'class') = 'ANGRY')",
        },
        where=None,
        order_by_bucket=True,
        limit=24,
    )
    print_table(rows or [], headers=["bucket", "total_calls", "angry_calls"])  # type: ignore[arg-type]


def main() -> None:
    # Prepare data in-memory
    sample = load_mock_data()

    # Clients and Admins
    pg_admin = PostgresAdmin(PostgresClient())
    ch_admin = ClickHouseAdmin(ClickHouseClient())

    # Schemas
    ensure_postgres_schema(pg_admin)
    ensure_clickhouse_schema(ch_admin, database="zaal")

    # Inserts
    insert_postgres_data(pg_admin, sample)
    insert_clickhouse_data(ch_admin, sample)

    # Demos
    demo_postgres_select(pg_admin)
    demo_clickhouse_select(ch_admin)
    demo_clickhouse_aggregation(ch_admin)

    # Optional: show how helper builders compose SQL
    sql_demo = demo_builders_sql()
    print("\n[Built SQL]", sql_demo)

    # Time bucketing
    demo_clickhouse_time_bucket(ch_admin)


if __name__ == "__main__":
    main()

