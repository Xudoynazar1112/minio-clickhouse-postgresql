from minio import Minio
import psycopg2
import redis
import uuid
import os
import logging

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

def upload_file(file_path):
    try:
        if not os.path.exists(file_path):
            logger.error(f"File {file_path} does not exist")
            return
        file_id = str(uuid.uuid4())
        file_name = os.path.basename(file_path)
        raw_key = f"{file_id}/{file_name}"
        minio_client.fput_object("raw-bucket", raw_key, file_path)
        logger.info(f"Uploaded {file_name} to raw-bucket with key: {raw_key}")
        with pg_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO files (file_id, user_id, file_name, minio_raw_key, status, size_bytes) VALUES (%s, %s, %s, %s, %s, %s)",
                (file_id, 1, file_name, raw_key, "uploaded", os.path.getsize(file_path))
            )
            pg_conn.commit()
            logger.info(f"Inserted file_id {file_id} into PostgreSQL")
        redis_client.lpush("file_queue", file_id)
        logger.info(f"Pushed file_id {file_id} to Redis queue")
        print(f"Uploaded {file_name} with file_id: {file_id}")
    except Exception as e:
        logger.error(f"Failed to upload file: {e}")

if __name__ == "__main__":
    upload_file("example.txt")
