from ast import Dict
import random
from dotenv import load_dotenv
from datetime import datetime, time, timedelta
import os
from postgres import PostgresDBOps
from tqdm import tqdm
import time
import pandas as pd
import json
from openai import OpenAI


load_dotenv()

client = OpenAI(
    api_key="not-needed",
    base_url="http://172.23.0.3:8002/v1"
)

print("Defining Postgres table schemas...")
POSTGRES_TABLES = {
    "categories": {
        "category": "TEXT PRIMARY KEY",
        "count": "INT",
        "embedding": "FLOAT8[]",
    },
    "subcategories": {
        "category": "TEXT",
        "subcategory": "TEXT",
        "count": "INT",
        "embedding": "FLOAT8[]",
    },
}


pg_client_config = {
				"database": os.getenv("POSTGRES_DB"),
				"user": os.getenv("POSTGRES_USER"),
				"password": os.getenv("POSTGRES_PASSWORD"),
				"host": os.getenv("POSTGRES_HOST"),
				"port": os.getenv("POSTGRES_PORT"),
			}

print("Connecting to Postgres...")
database_operator = PostgresDBOps(config=pg_client_config)


# print("Creating tables if not exist...")
table_name = "categories"

database_operator.cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
database_operator.create_table_if_not_exists(table_name, POSTGRES_TABLES[table_name])
category_table = database_operator.get_table_as_dataframe("categories")
print(f"Existing categories in DB: {len(category_table)}")

# print("Creating tables if not exist...")
table_name = "subcategories"

database_operator.cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
database_operator.create_table_if_not_exists(table_name, POSTGRES_TABLES[table_name])
database_operator.cursor.execute(f"ALTER TABLE {table_name} ADD PRIMARY KEY (category, subcategory);")
subcategory_table = database_operator.get_table_as_dataframe("subcategories")
print(f"Existing subcategories in DB: {len(subcategory_table)}")

print("Inserting test data...")
with open("/home/mehran/Mehran/work/mcinext/cca/cca-db/test_subcategory_handcraft.json", "r") as f:
    test_subcategory = json.load(f)


print("Inserting categories...")
for category in tqdm(test_subcategory.keys(), total=len(test_subcategory.keys())):
    resp = client.embeddings.create(
                                model="hakim-unsup",
                                input=[category]
                            )
    embedding = resp.data[0].embedding
    database_operator.add_or_increment("categories", {
        "category": category,
        "embedding": embedding
    })

print("Inserting subcategories...")
for key, value in tqdm(test_subcategory.items(), total=len(test_subcategory)):
    for v in value:
        resp = client.embeddings.create(
                                model="hakim-unsup",
                                input=[v]
                            )
        embedding = resp.data[0].embedding
        database_operator.add_or_increment("subcategories", {
            "subcategory": v,
            "category": key,
            "embedding": embedding
        })


print(category_table.columns)
print(subcategory_table.columns)


category_table = database_operator.get_table_as_dataframe("categories")
print(f"Existing categories in DB: {len(category_table)}")

subcategory_table = database_operator.get_table_as_dataframe("subcategories")
print(f"Existing subcategories in DB: {len(subcategory_table)}")


print(category_table.head())
print(subcategory_table.head())








