from clickhouse_driver import Client
from dotenv import load_dotenv
import os
import psycopg2
from psycopg2.extras import execute_values
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()
# sudo ip route add 172.17.0.0/24 via 172.18.160.1

class Postgres:
    def __init__(self):
        self.conn = self.connect()

    def connect(self):
        logger.info("Connecting to PostgreSQL database...")
        conn = psycopg2.connect(
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT")
        ) 
        return conn

    def disconnect(self):
        logger.info("Disconnecting from PostgreSQL database...")
        self.conn.close()

    def execute_query(self, query, params=None):
        logger.info(f"Executing query on PostgreSQL database: {query[:100]}...")
        cursor = self.conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        try:
            results = cursor.fetchall()
        except psycopg2.ProgrammingError:
            # No results to fetch (INSERT, UPDATE, DELETE, etc.)
            results = None
        cursor.close()
        return results

    def execute_many(self, query, values):
        """Execute query with multiple value sets using execute_values for better performance"""
        logger.info(f"Executing batch query on PostgreSQL database: {query[:100]}...")
        cursor = self.conn.cursor()
        execute_values(cursor, query, values)
        cursor.close()

    def execute_file(self, filepath):
        """Execute SQL commands from a file"""
        logger.info(f"Executing SQL file: {filepath}")
        with open(filepath, 'r') as f:
            sql = f.read()
        cursor = self.conn.cursor()
        cursor.execute(sql)
        cursor.close()

    def commit(self):
        """Commit the current transaction"""
        self.conn.commit()

    def rollback(self):
        """Rollback the current transaction"""
        self.conn.rollback()

    def cursor(self):
        """Get a cursor for manual control"""
        return self.conn.cursor()


class ClickHouse:
    def __init__(self):
        self.client = self.connect()

    def connect(self):
        logger.info("Connecting to ClickHouse database...")
        client = Client(
            database=os.getenv("CLICKHOUSE_DB"),
            host=os.getenv("CLICKHOUSE_HOST"),
            user=os.getenv("CLICKHOUSE_USER"),
            password=os.getenv("CLICKHOUSE_PASSWORD")
        )
        return client

    def disconnect(self):
        logger.info("Disconnecting from ClickHouse database...")
        self.client.disconnect()

    def execute_query(self, query, params=None):
        logger.info(f"Executing query on ClickHouse database: {query[:100]}...")
        print(query)
        print(params)
        if params:
            results = self.client.execute(query, params)
        else:
            results = self.client.execute(query)
        return results

    def execute_many(self, query, values):
        """Execute query with batch insert"""
        logger.info(f"Executing batch query on ClickHouse database: {query[:100]}...")
        self.client.execute(query, values)

    def execute_file(self, filepath):
        """Execute SQL commands from a file"""
        logger.info(f"Executing SQL file: {filepath}")
        with open(filepath, 'r') as f:
            sql = f.read()
        # ClickHouse client can handle multiple statements
        for statement in sql.split(';'):
            statement = statement.strip()
            if statement:
                self.client.execute(statement)


if __name__ == "__main__":
    # Example usage
    pg = Postgres()
    pg_results = pg.execute_query("SELECT NOW();")
    logger.info(f"PostgreSQL Results: {pg_results}")
    pg.disconnect()

    ch = ClickHouse()
    ch_results = ch.execute_query("SELECT now();")
    logger.info(f"ClickHouse Results: {ch_results}")
    ch.disconnect()