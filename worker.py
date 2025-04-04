from minio import Minio
import psycopg2
import redis
import time
import logging
from io import BytesIO

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    minio_client = Minio("localhost:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)
    logger.info("Connected to MinIO")
except Exception as e:
    logger.error(f"Failed to connect to MinIO: {e}")
    exit(1)

try:
    pg_conn = psycopg2.connect(dbname="file_ingestion", user="postgres", password="password", host="localhost", port=5433)
    logger.info("Connected to PostgreSQL")
except psycopg2.Error as e:
    logger.error(f"Failed to connect to PostgreSQL: {e}")
    exit(1)

try:
    redis_client = redis.Redis(host="localhost", port=6379, db=0)
    logger.info("Connected to Redis")
except redis.RedisError as e:
    logger.error(f"Failed to connect to Redis: {e}")
    exit(1)

def process_file():
    while True:
        try:
            file_id = redis_client.rpop("file_queue")
            if file_id:
                file_id = file_id.decode("utf-8")
                logger.info(f"Processing file_id: {file_id}")
                with pg_conn.cursor() as cur:
                    cur.execute("UPDATE files SET status = 'processing' WHERE file_id = %s", (file_id,))
                    pg_conn.commit()
                    logger.info(f"Updated status to 'processing' for file_id: {file_id}")
                    cur.execute("SELECT minio_raw_key, file_name FROM files WHERE file_id = %s", (file_id,))
                    result = cur.fetchone()
                    if result:
                        raw_key, file_name = result
                        logger.info(f"Found file: {file_name}, raw_key: {raw_key}")
                        processed_key = f"{file_id}/processed/{file_name}"
                        try:
                            # Download the object from raw-bucket
                            response = minio_client.get_object("raw-bucket", raw_key)
                            data = response.read()  # This returns bytes
                            response.close()
                            response.release_conn()
                            logger.info(f"Downloaded object {raw_key} from raw-bucket")
                            # Upload the object to processed-bucket using BytesIO
                            data_stream = BytesIO(data)
                            minio_client.put_object("processed-bucket", processed_key, data_stream, length=len(data))
                            logger.info(f"Copied to processed-bucket: {processed_key}")
                            cur.execute("UPDATE files SET status = 'completed', minio_processed_key = %s WHERE file_id = %s", (processed_key, file_id))
                            pg_conn.commit()
                            logger.info(f"Processed {file_id}")
                        except Exception as e:
                            logger.error(f"Object {raw_key} not found in raw-bucket or other error: {e}")
                            continue
                    else:
                        logger.warning(f"File {file_id} not found in database")
            else:
                logger.debug("No files in queue")
        except psycopg2.Error as e:
            logger.error(f"PostgreSQL error: {e}")
            pg_conn.rollback()
        except redis.RedisError as e:
            logger.error(f"Redis error: {e}")
        except Exception as e:
            logger.error(f"MinIO or other error: {e}")
        time.sleep(1)

if __name__ == "__main__":
    try:
        process_file()
    except KeyboardInterrupt:
        logger.info("Shutting down worker.py")
        pg_conn.close()
