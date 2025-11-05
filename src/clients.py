import logging
import os
from typing import Any, Iterable, Optional, Sequence
from clickhouse_driver import Client as CHClient
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv


load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class PostgresClient:
    """Lightweight PostgreSQL client wrapper.

    Provides connection management and small helpers for queries and script execution.
    """

    def __init__(self) -> None:
        self.conn = self._connect()

    def _connect(self):
        logger.info("Connecting to PostgreSQL database...")
        return psycopg2.connect(
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
        )

    def disconnect(self) -> None:
        logger.info("Disconnecting from PostgreSQL database...")
        self.conn.close()

    def execute_query(self, query: str, params: Optional[Sequence[Any]] = None):
        logger.info(f"Executing query on PostgreSQL: {query[:100]}...")
        cur = self.conn.cursor()
        if params is not None:
            cur.execute(query, params)
        else:
            cur.execute(query)
        try:
            rows = cur.fetchall()
        except psycopg2.ProgrammingError:
            rows = None  # statements without result sets
        finally:
            cur.close()
        return rows

    def commit(self) -> None:
        self.conn.commit()




class ClickHouseClient:
    """Lightweight ClickHouse client wrapper."""

    def __init__(self) -> None:
        self.client = self._connect()

    def _connect(self):
        logger.info("Connecting to ClickHouse database...")
        return CHClient(
            database=os.getenv("CLICKHOUSE_DB"),
            host=os.getenv("CLICKHOUSE_HOST"),
            user=os.getenv("CLICKHOUSE_USER"),
            password=os.getenv("CLICKHOUSE_PASSWORD"),
        )

    def disconnect(self) -> None:
        logger.info("Disconnecting from ClickHouse database...")
        self.client.disconnect()

    def execute_query(self, query: str, params: Optional[Sequence[Any]] = None):
        logger.info(f"Executing query on ClickHouse: {query[:100]}...")
        if params is not None:
            return self.client.execute(query, params)
        return self.client.execute(query)


if __name__ == "__main__":
    pg = PostgresClient()
    logger.info(f"PostgreSQL NOW(): {pg.execute_query('SELECT NOW();')}")
    pg.disconnect()

    ch = ClickHouseClient()
    logger.info(f"ClickHouse now(): {ch.execute_query('SELECT now();')}")
    ch.disconnect()
