import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
    GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    CONCURRENCY_LIMIT = int(os.getenv("CONCURRENCY_LIMIT", 5))  # yt-dlp는 더 느리므로 동시성 제한
    TIMEOUT_SECONDS = int(os.getenv("TIMEOUT_SECONDS", 300))  # yt-dlp용으로 타임아웃 증가
    RETRY_COUNT = int(os.getenv("RETRY_COUNT", 3))
    RETRY_DELAY_SECONDS = int(os.getenv("RETRY_DELAY_SECONDS", 5))
    
    # yt-dlp 관련 설정
    YTDLP_TIMEOUT = int(os.getenv("YTDLP_TIMEOUT", 300))  # yt-dlp 전용 타임아웃