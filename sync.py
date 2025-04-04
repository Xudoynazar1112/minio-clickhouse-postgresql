import psycopg2
from clickhouse_driver import Client
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    pg_conn = psycopg2.connect(dbname="file_ingestion", user="postgres", password="password", host="localhost", port=5433)
    logger.info("Connected to PostgreSQL")
except psycopg2.Error as e:
    logger.error(f"Failed to connect to PostgreSQL: {e}")
    exit(1)

try:
    ch_client = Client(host="localhost", port=9009, user="default", password="")
    logger.info("Connected to ClickHouse")
except Exception as e:
    logger.error(f"Failed to connect to ClickHouse: {e}")
    exit(1)

def sync_to_clickhouse():
    while True:
        try:
            with pg_conn.cursor() as cur:
                cur.execute(
                    "SELECT file_id, user_id, upload_timestamp, size_bytes, status "
                    "FROM files WHERE upload_timestamp > NOW() - INTERVAL '1 hour' AND synced = FALSE"
                )
                rows = cur.fetchall()
                logger.info(f"Fetched {len(rows)} rows from PostgreSQL")
                if rows:
                    ch_client.execute(
                        "INSERT INTO file_stats (file_id, user_id, upload_timestamp, size_bytes, status) VALUES",
                        [(str(row[0]), row[1], row[2], row[3], row[4]) for row in rows]
                    )
                    # Mark rows as synced
                    file_ids = [str(row[0]) for row in rows]
                    cur.execute(
                        "UPDATE files SET synced = TRUE WHERE file_id IN %s",
                        (tuple(file_ids),)
                    )
                    logger.info(f"Synced {len(rows)} rows to ClickHouse")
            pg_conn.commit()
        except psycopg2.Error as e:
            logger.error(f"PostgreSQL error: {e}")
            pg_conn.rollback()
        except Exception as e:
            logger.error(f"ClickHouse error: {e}")
        time.sleep(10)

if __name__ == "__main__":
    try:
        sync_to_clickhouse()
    except KeyboardInterrupt:
        logger.info("Shutting down sync.py")
        pg_conn.close()
