# tests/test_worker.py
import unittest
from unittest.mock import patch
from worker import process_file, summarize_lyrics, categorize_lyrics

class TestWorker(unittest.TestCase):
    @patch('worker.Minio')
    @patch('worker.psycopg2.connect')
    @patch('worker.redis.Redis')
    @patch('worker.separator.separate_to_file')
    @patch('worker.transcriber.transcribe')
    def test_process_file(self, mock_transcribe, mock_separate, mock_redis, mock_psycopg2, mock_minio):
        mock_redis.return_value.rpop.return_value = b"test_file_id"
        mock_psycopg2.return_value.cursor.return_value.__enter__.return_value.fetchone.return_value = ("test_key", "example.mp3")
        mock_transcribe.return_value.status = aai.TranscriptStatus.completed
        mock_transcribe.return_value.text = "This is a test song about love"
        mock_minio.return_value.put_object.return_value = None
        process_file()  # Simplified test; add more assertions as needed

    def test_summarize_lyrics(self):
        text = "I love you. You are my heart. I love to dance."
        summary = summarize_lyrics(text)
        self.assertTrue(len(summary) > 0)

    def test_categorize_lyrics(self):
        text = "I love you with all my heart."
        category = categorize_lyrics(text)
        self.assertIn("Pop/Romance", category)
        self.assertIn("Love", category)

if __name__ == '__main__':
    unittest.main()
