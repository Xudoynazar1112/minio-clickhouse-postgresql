import psycopg2
import time
import logging
from clickhouse_driver import Client
from prometheus_client import Gauge, start_http_server

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

postgres_gauge = Gauge('postgres_rows_fetched', 'Number of rows fetched from PostgreSQL')
clickhouse_gauge = Gauge('clickhouse_rows_synced', 'Number of rows synced to ClickHouse')
start_http_server(8001)

pg_conn = psycopg2.connect(dbname="file_ingestion", user="postgres", password="password", host="localhost", port=5433)
clickhouse_client = Client(host='localhost', port=9009)

def sync_data():
    while True:
        try:
            with pg_conn.cursor() as cur:
                cur.execute(
                    "SELECT file_id, user_id, upload_timestamp, size_bytes, status, transcription, summary, category "
                    "FROM musics WHERE upload_timestamp > NOW() - INTERVAL '1 hour' AND synced = FALSE"
                )
                rows = cur.fetchall()
                postgres_gauge.set(len(rows))
                logger.info(f"Fetched {len(rows)} rows from PostgreSQL")

                if rows:
                    for row in rows:
                        file_id, user_id, upload_timestamp, size_bytes, status, transcription, summary, category = row
                        clickhouse_client.execute(
                            "INSERT INTO music_states (file_id, user_id, upload_timestamp, size_bytes, status, transcription, summary, category) VALUES",
                            [(file_id, user_id, upload_timestamp, size_bytes, status, transcription, summary, category)]
                        )
                    cur.execute(
                        "UPDATE musics SET synced = TRUE WHERE file_id IN %s",
                        (tuple(row[0] for row in rows),)
                    )
                    pg_conn.commit()
                    clickhouse_gauge.set(len(rows))
                    logger.info(f"Synced {len(rows)} rows to ClickHouse")
        except Exception as e:
            logger.error(f"Error syncing data: {e}")
        time.sleep(10)

if __name__ == "__main__":
    try:
        sync_data()
    except KeyboardInterrupt:
        logger.info("Shutting down sync.py")
        pg_conn.close()
