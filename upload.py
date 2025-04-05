import os
import uuid
import logging
from minio import Minio
import psycopg2
import redis
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MinIO, PostgreSQL, Redis connections
minio_client = Minio("localhost:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)
pg_conn = psycopg2.connect(dbname="file_ingestion", user="postgres", password="password", host="localhost", port=5433)
redis_client = redis.Redis(host="localhost", port=6379, db=0)

def upload_file(file_path):
    file_name = os.path.basename(file_path)
    file_id = str(uuid.uuid4())
    raw_key = f"{file_id}/{file_name}"

    # Upload to MinIO raw-bucket
    minio_client.fput_object("raw-bucket", raw_key, file_path)
    logger.info(f"Uploaded {file_name} to raw-bucket with key: {raw_key}")

    # Insert into PostgreSQL musics table
    with pg_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO musics (file_id, user_id, file_name, minio_raw_key, status, size_bytes, upload_timestamp) "
            "VALUES (%s, %s, %s, %s, %s, %s, NOW())",
            (file_id, 1, file_name, raw_key, "uploaded", os.path.getsize(file_path))
        )
    pg_conn.commit()
    logger.info(f"Inserted file_id {file_id} into PostgreSQL")

    # Push to Redis queue
    redis_client.lpush("file_queue", file_id)
    logger.info(f"Pushed file_id {file_id} to Redis queue")

if __name__ == "__main__":
    try:
        file_path = "Imagine_Dragons_x_J_I_D_Enemy_from_the_series_Arcane_League_of_Legends.mp3"
        if not os.path.exists(file_path):
            logger.error(f"File {file_path} does not exist")
        else:
            upload_file(file_path)
    except Exception as e:
        logger.error(f"Error uploading file: {e}")
    finally:
        pg_conn.close()
