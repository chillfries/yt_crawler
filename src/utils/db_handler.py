import psycopg2
from psycopg2 import extras
import json
from .config import Config
from .logger import Logger

class DBHandler:
    def __init__(self):
        self.logger = Logger("db_handler.log", Config.LOG_LEVEL).get_logger()
        self.conn = None
    
    def connect(self):
        try:
            self.conn = psycopg2.connect(
                host=Config.DB_HOST,
                port=Config.DB_PORT,
                dbname=Config.DB_NAME,
                user=Config.DB_USER,
                password=Config.DB_PASSWORD,
                cursor_factory=psycopg2.extras.DictCursor
            )
            self.conn.autocommit = True
            self.logger.info("Database connection established.")
            self.create_table_if_not_exists()
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {e}")
            self.conn = None
            raise

    def create_table_if_not_exists(self):
        if not self.conn:
            return
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS recipes (
                    video_id VARCHAR(255) PRIMARY KEY,
                    data JSONB
                );
            """)
        self.logger.info("Table 'recipes' checked/created.")

    def close(self):
        if self.conn:
            self.conn.close()
            self.logger.info("Database connection closed.")
            self.conn = None
            
    def insert_or_update_video(self, video_id, data):
        if not self.conn:
            self.logger.error("No database connection.")
            return
        
        data_json = json.dumps(data)
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO recipes (video_id, data) VALUES (%s, %s) ON CONFLICT (video_id) DO UPDATE SET data = EXCLUDED.data;",
                    (video_id, data_json)
                )
            self.logger.info(f"Video {video_id} data inserted/updated.")
            return True
        except Exception as e:
            self.logger.error(f"Failed to insert/update video {video_id}: {e}")
            return False

    def get_video_data(self, video_id=None):
        if not self.conn:
            self.logger.error("No database connection.")
            return []
        
        try:
            with self.conn.cursor() as cur:
                if video_id:
                    cur.execute("SELECT * FROM recipes WHERE video_id = %s;", (video_id,))
                else:
                    cur.execute("SELECT * FROM recipes;")
                
                return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            self.logger.error(f"Failed to fetch video data: {e}")
            return []