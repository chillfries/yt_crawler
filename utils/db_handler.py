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
            self.create_tables()
            self.create_indexes()
            self.logger.info("Database connection established")
        except Exception as e:
            self.logger.error(f"DB connection failed: {e}")
            raise

    def create_tables(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS recipes (
                    video_id VARCHAR(255) PRIMARY KEY,
                    data JSONB,
                    dish_name VARCHAR(255),
                    ingredients JSONB,
                    recipe JSONB
                );
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS skipped_videos (
                    video_id VARCHAR(255) PRIMARY KEY,
                    reason TEXT,
                    url VARCHAR(255),
                    skipped_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                );
            """)
        self.logger.info("Tables created/verified")

    def create_indexes(self):
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_recipes_video_id ON recipes(video_id);",
            "CREATE INDEX IF NOT EXISTS idx_skipped_videos_video_id ON skipped_videos(video_id);"
        ]
        
        with self.conn.cursor() as cur:
            for idx in indexes:
                cur.execute(idx)
        self.logger.info("Indexes created/verified")

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
            self.logger.info("Database connection closed")

    def insert_or_update_video(self, video_id, data):
        dish_name = data.get("dish_name")
        ingredients = json.dumps(data.get("ingredients", [])) if data.get("ingredients") else None
        recipe = json.dumps(data.get("recipe", [])) if data.get("recipe") else None
        data_json = json.dumps(data)

        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO recipes (video_id, data, dish_name, ingredients, recipe)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (video_id) DO UPDATE SET 
                        data = EXCLUDED.data,
                        dish_name = EXCLUDED.dish_name,
                        ingredients = EXCLUDED.ingredients,
                        recipe = EXCLUDED.recipe;
                """, (video_id, data_json, dish_name, ingredients, recipe))
            self.logger.info(f"Video {video_id} saved successfully")
            return True
        except Exception as e:
            self.logger.error(f"Insert failed for {video_id}: {e}")
            return False

    def get_video_data(self, video_id):
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1 FROM recipes WHERE video_id = %s LIMIT 1;", (video_id,))
                return cur.fetchone() is not None
        except Exception as e:
            self.logger.error(f"Query failed for {video_id}: {e}")
            return False

    def is_video_skipped(self, video_id):
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1 FROM skipped_videos WHERE video_id = %s LIMIT 1;", (video_id,))
                return cur.fetchone() is not None
        except Exception as e:
            self.logger.error(f"Skip check failed for {video_id}: {e}")
            return False

    def insert_skipped_video(self, video_id, reason, url):
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO skipped_videos (video_id, reason, url) 
                    VALUES (%s, %s, %s) 
                    ON CONFLICT (video_id) DO NOTHING;
                """, (video_id, reason, url))
            self.logger.info(f"Video {video_id} added to skip list: {reason}")
            return True
        except Exception as e:
            self.logger.error(f"Skip insert failed for {video_id}: {e}")
            return False

    def delete_video(self, video_id):
        try:
            with self.conn.cursor() as cur:
                cur.execute("DELETE FROM recipes WHERE video_id = %s;", (video_id,))
                deleted_rows = cur.rowcount
            
            if deleted_rows > 0:
                self.logger.info(f"Video {video_id} deleted successfully")
                return True
            else:
                self.logger.warning(f"Video {video_id} not found for deletion")
                return False
                
        except Exception as e:
            self.logger.error(f"Delete failed for {video_id}: {e}")
            return False

    def get_uncleaned_videos(self):
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT video_id, data FROM recipes 
                    WHERE NOT (data ? 'clean_description' AND data ? 'clean_captions')
                    ORDER BY video_id;
                """)
                results = []
                for row in cur.fetchall():
                    results.append({
                        'video_id': row[0],
                        'data': row[1]
                    })
                return results
        except Exception as e:
            self.logger.error(f"Failed to get uncleaned videos: {e}")
            return []

    def get_cleaned_videos(self):
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT video_id, data FROM recipes 
                    WHERE (data ? 'clean_description' AND data ? 'clean_captions')
                    AND NOT (data ? 'dish_name' AND data ? 'ingredients' AND data ? 'recipe')
                    ORDER BY video_id;
                """)
                results = []
                for row in cur.fetchall():
                    results.append({
                        'video_id': row[0],
                        'data': row[1]
                    })
                return results
        except Exception as e:
            self.logger.error(f"Failed to get cleaned videos: {e}")
            return []