# Music Ingestion and Analysis Pipeline

## Overview

This project is a data engineering pipeline that ingests music files, processes them to extract vocals, transcribes the vocals, summarizes the lyrics, categorizes the music, and stores the results for analytics. It integrates the functionality of the `audio_nlp` project ([https://github.com/Xudoynazar1112/audio_nlp](https://github.com/Xudoynazar1112/audio_nlp)) into a scalable file ingestion system, with production-ready features like error handling, monitoring, and testing.

### Key Features
- **File Ingestion**: Uploads music files (`.mp3`, `.wav`) to MinIO object storage.
- **Music Processing (from `audio_nlp`)**:
  - Extracts vocals using Spleeter.
  - Transcribes vocals into text using AssemblyAI.
  - Summarizes the lyrics using NLTK (frequency-based summarization).
  - Categorizes the music using TextBlob (sentiment analysis) and keyword-based rules (e.g., Genre: Pop/Romance, Mood: Positive/Uplifting, Theme: Love).
- **Storage**: Stores raw and processed files in MinIO, metadata in PostgreSQL, and analytical data in ClickHouse.
- **Task Queue**: Uses Redis to manage processing tasks.
- **Analytics**: Syncs metadata to ClickHouse for fast analytical queries.
- **Production-Ready**:
  - Error handling with retries using `tenacity`.
  - Monitoring with Prometheus and Grafana.
  - Unit tests for reliability.
  - Comprehensive documentation.

### Technologies Used
- **MinIO**: Object storage for raw and processed files.
- **PostgreSQL**: Relational database for metadata storage.
- **Redis**: Task queue for processing jobs.
- **ClickHouse**: Columnar database for analytics.
- **Docker Compose**: Containerized deployment.
- **Python**: Core language for scripting.
- **Spleeter**: For vocal extraction.
- **AssemblyAI**: For transcribing vocals.
- **NLTK**: For summarization.
- **TextBlob**: For sentiment analysis and categorization.
- **Prometheus & Grafana**: For monitoring and metrics.

---

## Architecture Diagram

Below is a placeholder for the architecture diagram. You can create one using a tool like [draw.io](https://draw.io) and embed it here.

![Architecture Diagram](path/to/architecture-diagram.png)

**Diagram Description**:
- **Upload (`upload.py`)**: Uploads music files to MinIO `raw-bucket`, stores metadata in PostgreSQL, and pushes a job to Redis.
- **Process (`worker.py`)**: Picks up jobs from Redis, processes files using the `audio_nlp` workflow (vocal extraction, transcription, summarization, categorization), and stores results in MinIO `processed-bucket` and PostgreSQL.
- **Sync (`sync.py`)**: Syncs metadata from PostgreSQL to ClickHouse for analytics.
- **Monitoring**: Prometheus scrapes metrics from the scripts, and Grafana visualizes them.

---

## Prerequisites

Before running the project, ensure you have the following installed:
- **Docker** and **Docker Compose**: For containerized deployment.
- **Python 3.9+**: For running the scripts. (Tested python3.9)
- **FFmpeg**: Required for Spleeter (audio processing).
- **AssemblyAI API Key**: Required for transcription.

### Install FFmpeg
On Ubuntu:
```bash
sudo apt-get update
sudo apt-get install ffmpeg
```
On macOS:
```bash
brew install ffmpeg
```

### Set Up AssemblyAI API Key
Create a `.env` file in the project root and add your AssemblyAI API key:
```
ASSEMBLYAI_API_KEY=your_api_key_here
```

---

## Setup Instructions

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/Xudoynazar1112/music-ingestion-pipeline.git
   cd music-ingestion-pipeline
   ```

2. **Install Python Dependencies**:
   Create a virtual environment and install the required packages:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```
   The `requirements.txt` includes:
   ```
   minio
   psycopg2-binary
   redis
   clickhouse-driver
   spleeter
   assemblyai
   nltk
   textblob
   typer
   python-dotenv
   tenacity
   prometheus-client
   ```

3. **Set Up Docker Services**:
   Start the required services (MinIO, PostgreSQL, Redis, ClickHouse, Prometheus, Grafana) using Docker Compose:
   ```bash
   docker-compose up -d
   ```

4. **Initialize the Database**:
   Create the necessary tables in PostgreSQL and ClickHouse:
   ```bash
   # Create PostgreSQL table
   docker-compose exec postgres psql -U postgres -d file_ingestion -c "CREATE TABLE files (file_id UUID PRIMARY KEY, user_id INT, file_name VARCHAR(255), minio_raw_key VARCHAR(255), minio_processed_key VARCHAR(255), status VARCHAR(50), size_bytes BIGINT, upload_timestamp TIMESTAMP, transcription TEXT, summary TEXT, category TEXT, synced BOOLEAN DEFAULT FALSE);"

   # Create ClickHouse table
   docker-compose exec clickhouse clickhouse-client --query "CREATE TABLE file_stats (file_id String, user_id Int32, upload_timestamp DateTime, size_bytes Int64, status String, transcription String, summary String, category String) ENGINE = MergeTree() ORDER BY upload_timestamp;"
   ```

5. **Verify Services**:
   - MinIO: Access the web console at `http://localhost:9001` (username: `minioadmin`, password: `minioadmin`).
   - Grafana: Access at `http://localhost:3000` (default username: `admin`, password: `admin`).
   - Prometheus: Access at `http://localhost:9090`.

---

## Usage

### 1. Upload a Music File
Run the `upload.py` script to upload a music file (e.g., `example.mp3`):
```bash
python3 upload.py
```
- This uploads the file to MinIO `raw-bucket`, stores metadata in PostgreSQL, and pushes a job to Redis.
- Example output:
  ```
  2025-04-04 14:08:34,938 - INFO - Uploaded example.mp3 to raw-bucket with key: 5d457aba-75fd-422c-9467-f8df61e3c528/example.mp3
  2025-04-04 14:08:34,965 - INFO - Inserted file_id 5d457aba-75fd-422c-9467-f8df61e3c528 into PostgreSQL
  2025-04-04 14:08:34,966 - INFO - Pushed file_id 5d457aba-75fd-422c-9467-f8df61e3c528 to Redis queue
  Uploaded example.mp3 with file_id: 5d457aba-75fd-422c-9467-f8df61e3c528
  ```

### 2. Process the File
Run the `worker.py` script to process the file:
```bash
python3 worker.py
```
- This picks up the job from Redis, processes the file using the `audio_nlp` workflow, and stores the results.
- Example output:
  ```
  2025-04-04 14:08:35,273 - INFO - Processing file_id: 5d457aba-75fd-422c-9467-f8df61e3c528
  2025-04-04 14:08:35,278 - INFO - Updated status to 'processing' for file_id: 5d457aba-75fd-422c-9467-f8df61e3c528
  2025-04-04 14:08:35,279 - INFO - Found file: example.mp3, raw_key: 5d457aba-75fd-422c-9467-f8df61e3c528/example.mp3
  2025-04-04 14:08:35,288 - INFO - Transcription: This is a test song about love
  2025-04-04 14:08:35,291 - INFO - Summary: This is a test song about love.
  2025-04-04 14:08:35,292 - INFO - Category: Genre: Pop/Romance, Mood: Positive/Uplifting, Theme: Love
  2025-04-04 14:08:35,294 - INFO - Uploaded vocals and melody to processed-bucket
  2025-04-04 14:08:35,295 - INFO - Processed 5d457aba-75fd-422c-9467-f8df61e3c528
  ```

### 3. Sync to ClickHouse
Run the `sync.py` script to sync metadata to ClickHouse:
```bash
python3 sync.py
```
- This syncs the metadata (including transcription, summary, and category) to ClickHouse.
- Example output:
  ```
  2025-04-04 14:09:22,776 - INFO - Connected to ClickHouse
  2025-04-04 14:09:22,777 - INFO - Connected to PostgreSQL
  2025-04-04 14:09:22,779 - INFO - Fetched 1 rows from PostgreSQL
  2025-04-04 14:09:22,803 - INFO - Synced 1 rows to ClickHouse
  ```

### 4. Verify the Results
- **Check PostgreSQL**:
  ```bash
  docker-compose exec postgres psql -U postgres -d file_ingestion -c "SELECT file_id, status, synced, transcription, summary, category FROM files;"
  ```
  Example output:
  ```
  file_id                              | status    | synced | transcription                   | summary                              | category
  -------------------------------------+-----------+--------+-------------------------------+-------------------------------------+----------
  5d457aba-75fd-422c-9467-f8df61e3c528 | completed | t      | This is a test song about love | This is a test song about love.     | Genre: Pop/Romance, Mood: Positive/Uplifting, Theme: Love
  ```

- **Check ClickHouse**:
  ```bash
  docker-compose exec clickhouse clickhouse-client --query "SELECT * FROM file_stats;"
  ```
  Example output:
  ```
  5d457aba-75fd-422c-9467-f8df61e3c528  1  2025-04-04 14:08:34  10240  completed  This is a test song about love  This is a test song about love.  Genre: Pop/Romance, Mood: Positive/Uplifting, Theme: Love
  ```

- **Check MinIO**:
  ```bash
  docker-compose exec minio mc ls myminio/processed-bucket
  ```
  Example output:
  ```
  [2025-04-04 14:08:35 UTC]  5.1KiB 5d457aba-75fd-422c-9467-f8df61e3c528/processed/vocals_example.wav
  [2025-04-04 14:08:35 UTC]  5.0KiB 5d457aba-75fd-422c-9467-f8df61e3c528/processed/melody_example.wav
  ```

- **Monitor Metrics in Grafana**:
  Access Grafana at `http://localhost:3000` to view metrics like `files_processed_total`, `files_uploaded_total`, and `rows_synced_total`.

---

## Project Structure

```
music-ingestion-pipeline/
├── upload.py           # Script to upload music files
├── worker.py           # Script to process music files (integrates audio_nlp workflow)
├── sync.py             # Script to sync metadata to ClickHouse
├── requirements.txt    # Python dependencies
├── docker-compose.yml  # Docker Compose configuration
├── prometheus.yml      # Prometheus configuration for monitoring
├── .env                # Environment variables (e.g., ASSEMBLYAI_API_KEY)
├── tests/              # Unit tests
│   ├── test_upload.py
│   ├── test_worker.py
│   └── test_sync.py
└── README.md           # Project documentation
```

---

## Monitoring and Metrics

The pipeline exposes Prometheus metrics for monitoring:
- `files_uploaded_total`: Total files uploaded.
- `files_processed_total`: Total files processed.
- `files_failed_total`: Total files that failed processing.
- `rows_synced_total`: Total rows synced to ClickHouse.

These metrics are scraped by Prometheus and visualized in Grafana. Access Grafana at `http://localhost:3000` to view dashboards.

---

## Testing

Unit tests are provided to ensure the reliability of the pipeline. Run the tests with:
```bash
python3 -m unittest discover tests
```

---

## Future Improvements

- Add a Flask API for uploading files and checking status.
- Scale the pipeline with multiple workers using Celery.
- Add more advanced categorization using a machine learning model (e.g., zero-shot classification with transformers).
- Deploy the pipeline to a cloud provider (e.g., AWS).

---

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

## Contact

For questions or feedback, feel free to reach out:
- **GitHub**: [https://github.com/Xudoynazar1112](https://github.com/Xudoynazar1112)

---

### Step 4: Final Notes

This integration uses the exact implementation from your `audio_nlp` project, replacing the previously assumed tools (e.g., `speechrecognition`, `transformers`) with `assemblyai`, NLTK, and `textblob`. The pipeline is now fully production-ready with error handling, monitoring, testing, and documentation.

To run this, you’ll need to:
1. Set up an AssemblyAI API key in a `.env` file.
2. Install FFmpeg for Spleeter.
3. Run the pipeline as described in the README.

If you’d like to add more features (e.g., a Flask API, cloud deployment), or if you encounter any issues during setup, let me know!
