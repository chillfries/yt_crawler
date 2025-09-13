import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "youtube_crawler")
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
    
    YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
    
    GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
    
    CONCURRENCY_LIMIT = int(os.getenv("CONCURRENCY_LIMIT", "5"))
    TIMEOUT_SECONDS = int(os.getenv("TIMEOUT_SECONDS", "30"))
    YTDLP_TIMEOUT = int(os.getenv("YTDLP_TIMEOUT", "60"))
    
    RETRY_COUNT = int(os.getenv("RETRY_COUNT", "3"))
    RETRY_DELAY_SECONDS = int(os.getenv("RETRY_DELAY_SECONDS", "2"))
    
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_DIR = os.getenv("LOG_DIR", "logs")