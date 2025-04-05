from minio import Minio
import psycopg2
import redis
import time
import logging
from io import BytesIO
import os
import tempfile
import shutil
from spleeter.separator import Separator
import assemblyai as aai
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize, sent_tokenize
from textblob import TextBlob
from tenacity import retry, stop_after_attempt, wait_fixed
from prometheus_client import Counter, start_http_server
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Prometheus metrics
files_processed = Counter('files_processed_total', 'Total files processed')
files_failed = Counter('files_failed_total', 'Total files failed')
start_http_server(8000)

# MinIO, PostgreSQL, Redis connections
minio_client = Minio("localhost:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)
pg_conn = psycopg2.connect(dbname="file_ingestion", user="postgres", password="password", host="localhost", port=5433)
redis_client = redis.Redis(host="localhost", port=6379, db=0)

# Initialize AssemblyAI
aai.settings.api_key = os.getenv('ASSEMBLYAI_API_KEY')
transcriber = aai.Transcriber()

# Download NLTK data
nltk.download('punkt', quiet=True)
nltk.download('punkt_tab', quiet=True)
nltk.download('stopwords', quiet=True)

# Initialize Spleeter
separator = Separator('spleeter:2stems')

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def upload_to_minio(bucket, key, data_stream, length):
    minio_client.put_object(bucket, key, data_stream, length)

def summarize_lyrics(text):
    sentences = sent_tokenize(text)
    words = word_tokenize(text.lower())
    stop_words = set(stopwords.words("english"))
    filtered_words = [word for word in words if word not in stop_words]
    freq = nltk.FreqDist(filtered_words)
    sentence_scores = {}
    for sentence in sentences:
        for word, freq_score in freq.most_common(10):
            if word in sentence.lower():
                sentence_scores[sentence] = sentence_scores.get(sentence, 0) + freq_score
    summary_sentences = sorted(sentence_scores, key=sentence_scores.get, reverse=True)[:2]
    return " ".join(summary_sentences)

def categorize_lyrics(text):
    blob = TextBlob(text)
    polarity = blob.sentiment.polarity
    mood = "Positive/Uplifting" if polarity > 0.2 else "Negative/Sad" if polarity < -0.2 else "Neutral"
    text_lower = text.lower()
    if "love" in text_lower or "heart" in text_lower:
        genre, theme = "Pop/Romance", "Love"
    elif "king" in text_lower or "queen" in text_lower or "power" in text_lower:
        genre, theme = "Rock/Epic Pop", "Power/Leadership"
    elif "dance" in text_lower or "beat" in text_lower:
        genre, theme = "Dance/Pop", "Energy"
    else:
        genre, theme = "Unknown", "General"
    return f"Genre: {genre}, Mood: {mood}, Theme: {theme}"

def process_file():
    while True:
        try:
            file_id = redis_client.rpop("file_queue")
            if file_id:
                file_id = file_id.decode("utf-8")
                logger.info(f"Processing file_id: {file_id}")
                with pg_conn.cursor() as cur:
                    cur.execute("UPDATE musics SET status = 'processing' WHERE file_id = %s", (file_id,))
                    pg_conn.commit()
                    logger.info(f"Updated status to 'processing' for file_id: {file_id}")
                    cur.execute("SELECT minio_raw_key, file_name FROM musics WHERE file_id = %s", (file_id,))
                    result = cur.fetchone()
                    if result:
                        raw_key, file_name = result
                        logger.info(f"Found file: {file_name}, raw_key: {raw_key}")

                        # Download the music file from raw-bucket
                        with tempfile.NamedTemporaryFile(suffix='.mp3', delete=False) as temp_file:
                            minio_client.fget_object("raw-bucket", raw_key, temp_file.name)
                            temp_file_path = temp_file.name

                        # Create a temporary file with the original file name
                        temp_dir = tempfile.mkdtemp()
                        renamed_file_path = os.path.join(temp_dir, file_name)
                        shutil.copy(temp_file_path, renamed_file_path)

                        # Extract vocals using Spleeter
                        output_dir = tempfile.mkdtemp()
                        separator.separate_to_file(renamed_file_path, output_dir)
                        # The subdirectory will now be named after file_name (without extension)
                        base_name = os.path.splitext(file_name)[0]
                        vocal_path = os.path.join(output_dir, base_name, 'vocals.wav')
                        melody_path = os.path.join(output_dir, base_name, 'accompaniment.wav')

                        # Verify the files exist
                        if not os.path.exists(vocal_path):
                            raise FileNotFoundError(f"Vocal file not found at {vocal_path}")
                        if not os.path.exists(melody_path):
                            raise FileNotFoundError(f"Melody file not found at {melody_path}")

                        # Transcribe vocals using AssemblyAI
                        transcript = transcriber.transcribe(vocal_path)
                        if transcript.status == aai.TranscriptStatus.completed:
                            transcription = transcript.text
                            logger.info(f"Transcription: {transcription}")
                        else:
                            logger.error(f"Transcription failed: {transcript.error}")
                            cur.execute("UPDATE musics SET status = 'failed' WHERE file_id = %s", (file_id,))
                            pg_conn.commit()
                            files_failed.inc()
                            continue

                        # Summarize the transcription
                        summary = summarize_lyrics(transcription)
                        logger.info(f"Summary: {summary}")

                        # Categorize the music
                        category = categorize_lyrics(transcription)
                        logger.info(f"Category: {category}")

                        # Upload vocals and melody to processed-bucket
                        vocal_key = f"{file_id}/processed/vocals_{file_name.replace('.mp3', '.wav')}"
                        melody_key = f"{file_id}/processed/melody_{file_name.replace('.mp3', '.wav')}"
                        with open(vocal_path, 'rb') as vocal_file:
                            vocal_data = vocal_file.read()
                            upload_to_minio("processed-bucket", vocal_key, BytesIO(vocal_data), len(vocal_data))
                        with open(melody_path, 'rb') as melody_file:
                            melody_data = melody_file.read()
                            upload_to_minio("processed-bucket", melody_key, BytesIO(melody_data), len(melody_data))
                        logger.info(f"Uploaded vocals and melody to processed-bucket")

                        # Update PostgreSQL with metadata
                        cur.execute(
                            "UPDATE musics SET status = 'completed', minio_processed_key = %s, transcription = %s, summary = %s, category = %s WHERE file_id = %s",
                            (vocal_key, transcription, summary, category, file_id)
                        )
                        pg_conn.commit()
                        logger.info(f"Processed {file_id}")

                        # Clean up temporary files
                        os.remove(temp_file_path)
                        os.remove(renamed_file_path)
                        shutil.rmtree(temp_dir)
                        shutil.rmtree(output_dir)

                        files_processed.inc()
                    else:
                        logger.warning(f"File {file_id} not found in database")
                        files_failed.inc()
            else:
                logger.debug("No files in queue")
        except Exception as e:
            logger.error(f"Error processing file: {e}")
            files_failed.inc()
            time.sleep(1)

if __name__ == "__main__":
    try:
        process_file()
    except KeyboardInterrupt:
        logger.info("Shutting down worker.py")
        pg_conn.close()
