from __future__ import annotations

import os
from typing import List, Dict, Optional, Any
from tqdm import tqdm
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
import pandas as pd

# Load environment variables from .env
load_dotenv()


class PostgresDBOps:
	def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
		"""
		Initialize Postgres client using config dict or .env variables.
		"""
		if config is None:
			config = {
				"database": os.getenv("POSTGRES_DB"),
				"user": os.getenv("POSTGRES_USER"),
				"password": os.getenv("POSTGRES_PASSWORD"),
				"host": os.getenv("POSTGRES_HOST"),
				"port": os.getenv("POSTGRES_PORT"),
			}

		# Create a connection and cursor
		self.conn = psycopg2.connect(**config)
		self.conn.autocommit = True
		self.cursor = self.conn.cursor()

	def close(self) -> None:
		"""Close cursor and connection."""
		try:
			if self.cursor is not None:
				self.cursor.close()
		finally:
			if self.conn is not None:
				self.conn.close()

	def insert_row(self, table: str, row: Dict[str, Any]) -> None:
		"""
		Insert a single row into the specified table.
		"""
		if not row:
			return

		cols = list(row.keys())
		vals = [row[c] for c in cols]
		placeholders = ", ".join(["%s"] * len(vals))
		sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})"
		self.cursor.execute(sql, vals)

	def insert_batch(self, table: str, rows: List[Dict[str, Any]], batch_size: int = 1000) -> None:
		"""
		Insert multiple rows in batches for better performance.
		Uses psycopg2.extras.execute_values for efficient bulk insert.
		"""
		if not rows:
			return

		cols = list(rows[0].keys())
		records = [[row.get(c) for c in cols] for row in rows]

		total_batches = (len(records) + batch_size - 1) // batch_size
		for batch_start in tqdm(range(0, len(records), batch_size), total=total_batches):
			batch_end = min(batch_start + batch_size, len(records))
			batch = records[batch_start:batch_end]
			insert_query = f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s"
			psycopg2.extras.execute_values(self.cursor, insert_query, batch, template=None, page_size=batch_size)

	def create_table_if_not_exists(
		self,
		table_name: str,
		columns: Dict[str, str],
		primary_key: Optional[List[str]] = None,
		indexes: Optional[List[str]] = None,
		if_not_exists: bool = True,
	) -> None:
		"""
		Create a table if it does not exist, with optional primary key and indexes.
		columns: mapping of column_name -> SQL_TYPE (e.g. "id SERIAL", "created_at TIMESTAMP DEFAULT NOW()")
		primary_key: list of column names to include in PRIMARY KEY clause
		indexes: list of column names to create a simple index on
		"""
		cols_def = ", ".join([f"{col} {dtype}" for col, dtype in columns.items()])
		pk_clause = f", PRIMARY KEY ({', '.join(primary_key)})" if primary_key else ""
		sql = f"CREATE TABLE {'IF NOT EXISTS ' if if_not_exists else ''}{table_name} ({cols_def}{pk_clause});"
		self.cursor.execute(sql)

		# Add indexes if provided
		if indexes:
			for idx_col in indexes:
				idx_name = f"{table_name}_{idx_col}_idx"
				# PostgreSQL supports IF NOT EXISTS for CREATE INDEX starting in newer versions
				try:
					self.cursor.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table_name} ({idx_col});")
				except psycopg2.DatabaseError:
					# Fallback for older PG versions: ignore index creation errors
					pass

	def add_or_increment(self, table: str, data_dict: Dict[str, Any], count_col: str = "count"):
		"""
		Args:
			table: Name of the table to update
			columns: The column values to add or increment
		"""
		if "subcategory" in data_dict:
			columns = ["category", "subcategory"]
		else:
			columns = ["category"]

		where_clause = f"{columns[0]} = %s"
		values = (data_dict[columns[0]],)

		if "subcategory" in data_dict:
			where_clause += f" AND {columns[1]} = %s"
			values += (data_dict[columns[1]],)

		check_query = f"SELECT {count_col} FROM {table} WHERE {where_clause}"
		self.cursor.execute(check_query, values)
		result = self.cursor.fetchone()
		
		if result:
			# Record exists, increment count by 1
			update_query = f"UPDATE {table} SET {count_col} = {count_col} + 1 WHERE {where_clause}"
			self.cursor.execute(update_query, values)
		else:
			# Record doesn't exist, insert new row with count 1
			cols = list(data_dict.keys()) + [count_col]
			vals = [data_dict[c] for c in cols[:-1]] + [1]
			placeholders = ", ".join(["%s"] * len(vals))
			insert_query = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})"
			self.cursor.execute(insert_query, vals)

	def get_table_as_dataframe(self, table: str) -> pd.DataFrame:
		query = f"SELECT * FROM {table}"
		self.cursor.execute(query)
		rows = self.cursor.fetchall()  # list of tuples
		cols = [desc[0] for desc in self.cursor.description]
		df = pd.DataFrame(rows, columns=cols)
		return df