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
load_dotenv()

print("Defining Postgres table schemas...")
POSTGRES_TABLES = {
    "categories": {
        "category": "TEXT PRIMARY KEY",
        "count": "INT",
        "embeddings": "FLOAT8[]",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP"
    },
    "subcategories": {
        "category": "TEXT",
        "subcategory": "TEXT",
        "count": "INT",
        "embeddings": "FLOAT8[]",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
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
    database_operator.add_or_increment("categories", {
        "category": category,
        "embeddings": [random.random() for _ in range(768)],  # Assuming 768-dimensional embeddings
    })

print("Inserting subcategories...")
for key, value in tqdm(test_subcategory.items(), total=len(test_subcategory)):
    for v in value:
        database_operator.add_or_increment("subcategories", {
            "subcategory": v,
            "category": key,
            "embeddings": [random.random() for _ in range(768)],  # Assuming 768-dimensional embeddings
        })


print(category_table.columns)
print(subcategory_table.columns)
