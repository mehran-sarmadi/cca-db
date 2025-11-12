from ast import Dict
import random
from dotenv import load_dotenv
from datetime import datetime, time, timedelta
import os
from clickhouse import ClickHouseDBOps
from tqdm import tqdm
load_dotenv()
import random
from datetime import datetime, timedelta
from tqdm import tqdm
import time

import random

def build_mock_data(num_data, num_categories=10, num_subcategories=3):
    sentiments = ["POSITIVE", "NEGATIVE", "NEUTRAL", "NORMAL"]
    satisfaction_classes = ["satisfied", "unsatisfied", "neutral"]

    call_categories = [f"category_{i}" for i in range(1, num_categories + 1)]
    subcategories = {cat: [f"subcategory_{i}" for i in range(1, num_subcategories + 1)] for cat in call_categories}

    greetings = [
        "سلام وقت بخیر",
        "روزتون بخیر",
        "ممنون از تماستون خدانگهدار",
        "خداحافظ روز خوبی داشته باشید"
    ]

    summaries = [
        {
            "call_reason_summary": "سرعت اینترنت در ساعات شب کاهش می‌یابد.",
            "dialogue_summary": "مشترک درباره کاهش سرعت اینترنت در ساعات شب شکایت کرد. کارشناس توضیح داد که به دلیل مصرف بالای بسته نامحدود، سرعت در شب کاهش می‌یابد. مکالمه با خداحافظی پایان یافت."
        },
        {
            "call_reason_summary": "مشترک از قطعی اینترنت خانگی شکایت دارد.",
            "dialogue_summary": "مشترک از قطعی مکرر اینترنت خانگی در روزهای اخیر شکایت داشت. کارشناس تنظیمات مودم را بررسی کرد اما مشکل حل نشد. تماس با خداحافظی پایان یافت."
        },
        {
            "call_reason_summary": "پرداخت قبض انجام نشده است.",
            "dialogue_summary": "مشترک اعلام کرد که مبلغ قبض را پرداخت کرده ولی در سیستم ثبت نشده است. کارشناس اعلام کرد بررسی بیشتری لازم است."
        }
    ]
    now = datetime.now()
    start_time = now - timedelta(days=90)
    all_data = []

    for _ in tqdm(range(num_data), total=num_data):
        created_at = start_time + timedelta(minutes=random.randint(0, 14400))

        summary = random.choice(summaries)
        cat = random.choice(call_categories)
        subcat = random.choice(subcategories[cat])
        expert_sentiment = random.choice(sentiments)
        subscriber_sentiment = random.choice(sentiments)
        satisfaction = random.choice(satisfaction_classes)
        farewell_text = random.choice([g for g in greetings if "خداحافظ" in g or "خدانگهدار" in g])

        data = {
            "sentiment_analysis": {
                "expert": {
                    "class": expert_sentiment,
                    "where": []
                },
                "subscriber": {
                    "class": subscriber_sentiment,
                    "where": []
                }
            },
            "bad_words": {
                "expert": {
                    "class": random.choice([True, False]),
                    "where": []
                },
                "subscriber": {
                    "class": random.choice([True, False]),
                    "where": []
                }
            },
            "start_greeting": {
                "expert": {
                    "class": random.choice([True, False]),
                    "where": [random.choice(greetings)] if random.random() < 0.5 else []
                }
            },
            "end_greeting": {
                "farewell": {
                    "class": True,
                    "where": [farewell_text]
                },
                "reference": {
                    "class": random.choice([True, False]),
                    "where": []
                }
            },
            "summary": summary,
            "sugestions": "",
            "satisfaction": {
                "class": satisfaction
            },
            "call_category_existing": [cat],
            "call_category_new": [],
            "call_subcategory_existing": [
                {"subcategory": subcat, "category": cat}
            ],
            "call_subcategory_new": [],
            "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S"),
            "loc_id": 1000 + random.randint(1, 10)
        }

        all_data.append(data)

    return all_data

import json


def transform_to_clickhouse(mock_data):
    """
    Transform mock_data (list of dicts) into ClickHouse schema-compatible dicts.
    """

    transformed = []

    for record in tqdm(mock_data):
        # Sentiment info
        user_sentiment = record["sentiment_analysis"]["subscriber"]["class"]
        agent_sentiment = record["sentiment_analysis"]["expert"]["class"]
        user_sentiment_meta = record["sentiment_analysis"]["subscriber"]["where"]
        agent_sentiment_meta = record["sentiment_analysis"]["expert"]["where"]

        # Bad words
        user_bad_words = "True" if record["bad_words"]["subscriber"]["class"] else "False"
        agent_bad_words = "True" if record["bad_words"]["expert"]["class"] else "False"
        user_bad_words_meta = record["bad_words"]["subscriber"]["where"]
        agent_bad_words_meta = record["bad_words"]["expert"]["where"]

        # Greetings
        agent_start_greeting = "True" if record["start_greeting"]["expert"]["class"] else "False"
        agent_start_greeting_meta = record["start_greeting"]["expert"]["where"]

        agent_farewell = "True" if record["end_greeting"]["farewell"]["class"] else "False"
        agent_farewell_meta = record["end_greeting"]["farewell"]["where"]

        agent_reference = "True" if record["end_greeting"]["reference"]["class"] else "False"
        agent_reference_meta = record["end_greeting"]["reference"]["where"]

        # Satisfaction
        user_satisfaction = record["satisfaction"]["class"]

        # Categories & subcategories
        # categories = record["call_category_existing"] + record["call_category_new"]
        # subcategories = [s["subcategory"] for s in record["call_subcategory_existing"]] + [
        #     s["subcategory"] for s in record["call_subcategory_new"]
        # ]

        # Dict of category → array of subcategories (for JSON extraction in ClickHouse)
        # Build a dict where each category maps to an array of its subcategories
        category_subcategory_dict = {}
        for s in record["call_subcategory_existing"] + record["call_subcategory_new"]:
            cat = s["category"]
            subcat = s["subcategory"]
            if cat not in category_subcategory_dict:
                category_subcategory_dict[cat] = []
            category_subcategory_dict[cat].append(subcat)

        # Summary info
        call_reason = record["summary"]["call_reason_summary"]
        call_summary = record["summary"]["dialogue_summary"]
        suggestions = record.get("sugestions", "")

        # Other metadata
        location_id = record["loc_id"]
        user_id = "user_" + str(random.randint(1000, 9999))
        agent_id = "agent_" + str(random.randint(1000, 9999))
        # Convert string back to datetime for ClickHouse DateTime64 column
        created_at = datetime.strptime(record["created_at"], "%Y-%m-%d %H:%M:%S")

        transformed.append({
            "user_sentiment": user_sentiment,
            "agent_sentiment": agent_sentiment,
            "user_sentiment_meta": user_sentiment_meta,
            "agent_sentiment_meta": agent_sentiment_meta,
            "user_bad_words": user_bad_words,
            "agent_bad_words": agent_bad_words,
            "user_bad_words_meta": user_bad_words_meta,
            "agent_bad_words_meta": agent_bad_words_meta,
            "agent_start_greeting": agent_start_greeting,
            "agent_start_greeting_meta": agent_start_greeting_meta,
            "agent_farewell": agent_farewell,
            "agent_farewell_meta": agent_farewell_meta,
            "agent_reference": agent_reference,
            "agent_reference_meta": agent_reference_meta,
            "user_satisfaction": user_satisfaction,
            # "categories": categories,
            # "subcategories": subcategories,
            # Store as a native dict so it maps to ClickHouse Map(String, Array(String))
            "category_subcategory_dict": category_subcategory_dict,
            "call_reason": call_reason,
            "call_summary": call_summary,
            "suggestions": suggestions,
            "location_id": location_id,
            "user_id": user_id,
            "agent_id": agent_id,
            "created_at": created_at,
        })

    return transformed


CLICKHOUSE_TABLES = {
"superset_test_final_schema": {
    "user_sentiment": "LowCardinality(String)",
    "agent_sentiment": "LowCardinality(String)",
    "user_sentiment_meta": "Array(String)",
    "agent_sentiment_meta": "Array(String)",
    "user_bad_words": "LowCardinality(String)",
    "agent_bad_words": "LowCardinality(String)",
    "user_bad_words_meta": "Array(String)",
    "agent_bad_words_meta": "Array(String)",
    "agent_start_greeting": "LowCardinality(String)",
    "agent_start_greeting_meta": "Array(String)",
    "agent_farewell": "LowCardinality(String)",
    "agent_farewell_meta": "Array(String)",
    "agent_reference": "LowCardinality(String)",
    "agent_reference_meta": "Array(String)",
    "user_satisfaction": "LowCardinality(String)",
    # "categories": "Array(String)",
    # "subcategories": "Array(String)",
    "category_subcategory_dict": "Map(String, Array(String))",
    "call_reason": "String",
    "call_summary": "String",
    "suggestions": "String",
    "location_id": "Int64",
    "user_id": "String",
    "agent_id": "String",
    "created_at": "DateTime64"
}
}



ch_client_config = {
"database": os.getenv("CLICKHOUSE_DB"),
"host": os.getenv("CLICKHOUSE_HOST"),
"user": os.getenv("CLICKHOUSE_USER"),
"password": os.getenv("CLICKHOUSE_PASSWORD"),
}

database_operator = ClickHouseDBOps(ch_client_config)


database = "zaal"
table_name = "superset_test_final_schema"

# database_operator.ch_client.execute(f"DROP TABLE IF EXISTS {table_name};")
# database_operator.create_table_if_not_exists(database, table_name, CLICKHOUSE_TABLES[table_name])


# Start time: 10 days ago from now
all_data = build_mock_data(3000000)
transformed_all_data = transform_to_clickhouse(all_data)

# print("Print transformed data:")
# for data in transformed_all_data:
#     print("\n----\n")
#     for key, value in data.items():
#         print(f"{key}")
#         print(f"{value}")



database_operator.insert_batch(table_name, transformed_all_data, batch_size=10000)




t1 = time.time()
dfs_dic = database_operator.counts_per_timestep(
    table_name,
    ["all", "categories", "subcategories"],
    from_time_before="4d",
    freq='d',
    # loc_id="1003"
)
t2 = time.time()
print(f"Time taken for counts_per_timestep: {t2 - t1} seconds")



for key, df in dfs_dic.items():
    print(f"\n\nPivot table for {key}:\n")
    print(df)


t1 = time.time()
dfs_dic = database_operator.counts_per_timestep_all_locs(
    table_name,
    ["all", "categories", "subcategories"],
    from_time_before="4d",
    freq='d',
)
t2 = time.time()
print(f"Time taken for counts_per_timestep: {t2 - t1} seconds")

for key, df in dfs_dic.items():
    print(f"\n\nPivot table for {key}:\n")
    print(df)


# # Access per-location breakdowns easily:
# print(result["all"].head())               # total counts by location_id and time
# print(result["categories"].head())        # category counts per location
# print(result["subcategories"].head())     # subcategory counts per location

# print("data")
# print(transformed_all_data)

# for key, df in dfs_dic.items():
#     print(f"\n\nPivot table for {key}:\n")
#     print(df)

# print("Show tables:")
# tables = database_operator.show_tables()
# for table in tables:
#     print(f" - {table}")

#     # print first 5 row of each
#     print(f"\nFirst 5 rows of table {table}:\n")
#     data = database_operator.ch_client.execute(f"SELECT * FROM {table} LIMIT 5")
#     for row in data:
#         print(row)

database_operator.close()