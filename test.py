import json
data_sample = {
    "subscribers": [],
    "experts": [],
    "calls_details": [],
}

for id in range(1, 10):
    with open(f"/home/mehran/Mehran/work/mcinext/cca-db/data/conversations_output/conversation_{id}.jsonl") as f:
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
    with open(f"/home/mehran/Mehran/work/mcinext/cca-db/data/conversations/conversation_{id}.jsonl") as f:
        for line in f:
            call_text = json.loads(line)
            row["calls_texts"].append(call_text)

    data_sample["calls_details"].append(row)

    data_sample["subscribers"].append({
        "id": id + 100
    })

    data_sample["experts"].append({
        "id": id + 100,
        "name": f"Expert {id}"
    })




# with open("/home/mehran/Mehran/work/mcinext/cca-db/data/sample_data.jsonl", "w") as f:
#     f.write(json.dumps(data_sample, indent=4))

from src.cca_db.clients import PostgresClient, ClickHouseClient
from src.cca_db.admin import PostgresAdmin, ClickHouseAdmin
from src.cca_db.config import POSTGRES_TABLES, CLICKHOUSE_TABLES


pg_admin = PostgresAdmin(PostgresClient())
ch_admin = ClickHouseAdmin(ClickHouseClient())

pg_admin.drop_table_if_exists("experts")
pg_admin.drop_table_if_exists("subscribers")
pg_admin.drop_table_if_exists("calls_details")

pg_admin.create_table_if_not_exists("experts", POSTGRES_TABLES["experts"])
pg_admin.create_table_if_not_exists("subscribers", POSTGRES_TABLES["subscribers"])
pg_admin.create_table_if_not_exists("calls_details", POSTGRES_TABLES["calls_details"])



for idx in range(0, 9):
    print(f"Inserting row {idx+1}...")
    pg_admin.insert_row("experts", data_sample["experts"][idx])
    pg_admin.insert_row("subscribers", data_sample["subscribers"][idx])
    pg_admin.insert_row("calls_details", data_sample["calls_details"][idx])

ch_admin.drop_table_if_exists("selected_calls_details")
ch_admin.create_table_if_not_exists("zaal", "selected_calls_details", CLICKHOUSE_TABLES["selected_calls_details"])
for idx in range(0, 9):
    print(f"Inserting row {idx+1}...")
    row = {
        "id": data_sample["calls_details"][idx]["id"],
        "sentiment_analysis": json.dumps(data_sample["calls_details"][idx]["sentiment_analysis"]),
        "bad_words": json.dumps(data_sample["calls_details"][idx]["bad_words"]),
        "start_greeting": json.dumps(data_sample["calls_details"][idx]["start_greeting"]),
        "end_greeting": json.dumps(data_sample["calls_details"][idx]["end_greeting"]),
        "summary": json.dumps(data_sample["calls_details"][idx]["summary"]),
        "calls_texts": json.dumps(data_sample["calls_details"][idx]["calls_texts"]),
    }
    ch_admin.insert_row("selected_calls_details", row)

# Display PostgreSQL tables
print("\n" + "="*50)
print("POSTGRESQL TABLES")
print("="*50)

for table_name in ["experts", "subscribers", "calls_details"]:
    print(f"\n--- Table: {table_name} ---")
    result = pg_admin.pg.execute_query(f"SELECT * FROM {table_name}")
    for row in result:
        print(row)

# Display ClickHouse table
print("\n" + "="*50)
print("CLICKHOUSE TABLES")
print("="*50)
print("\n--- Table: selected_calls_details ---")
result = ch_admin.ch.execute_query("SELECT * FROM zaal.selected_calls_details")
for row in result:
    # Format ClickHouse output for readability
    row_dict = {
        "id": row[0],
        "sentiment_analysis": json.loads(row[1]) if isinstance(row[1], str) else row[1],
        "bad_words": json.loads(row[2]) if isinstance(row[2], str) else row[2],
        "start_greeting": json.loads(row[3]) if isinstance(row[3], str) else row[3],
        "end_greeting": json.loads(row[4]) if isinstance(row[4], str) else row[4],
        "summary": json.loads(row[5]) if isinstance(row[5], str) else row[5],
        "calls_texts": json.loads(row[6]) if isinstance(row[6], str) else row[6],
    }
    print(json.dumps(row_dict, indent=2, ensure_ascii=False))
    print("-" * 50)


