import asyncio
from datetime import datetime, timezone
from utils.config import Config
from utils.db_handler import DBHandler
from utils.youtube_api import YoutubeAPI
from utils.text_utils import join_captions_from_ytdlp
from utils.logger import Logger

class YoutubeCrawler:
    def __init__(self, keyword):
        self.keyword = keyword
        self.logger = Logger("youtube_crawler.log", Config.LOG_LEVEL).get_logger()
        self.db = DBHandler()
        self.youtube_api = YoutubeAPI()
        self.semaphore = asyncio.Semaphore(Config.CONCURRENCY_LIMIT)

    async def _process_video(self, video_id):
        async with self.semaphore:
            self.logger.info(f"[{video_id}] Processing video")
            
            if self.db.get_video_data(video_id):
                self.logger.info(f"[{video_id}] Already exists, skipping")
                return False
            if self.db.is_video_skipped(video_id):
                self.logger.info(f"[{video_id}] In skip list, skipping")
                return False

            try:
                details = await self.youtube_api.get_video_details(video_id)
                if not details or (details.get("duration") and details.get("duration") < 60):
                    self.db.insert_skipped_video(video_id, "Short video or no details", f"https://www.youtube.com/watch?v={video_id}")
                    return False

                if self.is_promotional(details.get("title", ""), details.get("description", "")):
                    self.db.insert_skipped_video(video_id, "Promotional content", f"https://www.youtube.com/watch?v={video_id}")
                    return False

                captions = await self.youtube_api.get_captions_with_retry(video_id)
                raw_description = details.get("description", "")
                raw_captions = join_captions_from_ytdlp(captions) if captions else ""

                desc_len = len(raw_description.strip())
                captions_len = len(raw_captions.strip())
                self.logger.info(f"[{video_id}] Content length - description: {desc_len}, captions: {captions_len}")
                
                if desc_len < 100 and captions_len < 100:
                    self.db.insert_skipped_video(video_id, f"Insufficient content: desc={desc_len}chars, captions={captions_len}chars", f"https://www.youtube.com/watch?v={video_id}")
                    return False

                video_data = {
                    "video_id": video_id,
                    "title": details.get("title", ""),
                    "url": f"https://www.youtube.com/watch?v={video_id}",
                    "image_url": details.get("thumbnails", {}).get("high", {}).get("url", ""),
                    "raw_description": raw_description,
                    "raw_captions": raw_captions,
                    "captions_segments": captions,
                    "metadata": {
                        "collected_at": datetime.now(timezone.utc).isoformat(),
                        "caption_method": "yt-dlp"
                    }
                }
                
                success = self.db.insert_or_update_video(video_id, video_data)
                if success:
                    self.logger.info(f"[{video_id}] Successfully saved to database")
                else:
                    self.logger.error(f"[{video_id}] Failed to save to database")
                return success

            except Exception as e:
                self.logger.error(f"[{video_id}] Error: {e}")
                self.db.insert_skipped_video(video_id, f"Processing error: {str(e)}", f"https://www.youtube.com/watch?v={video_id}")
                return False

    async def run(self):
        print(f"시작: '{self.keyword}' 키워드로 일괄 수집")
        self.logger.info(f"Starting batch crawl for '{self.keyword}'")
        
        self.db.connect()
        if not self.db.conn:
            print("데이터베이스 연결 실패")
            return
        
        print("검색 중...")
        search_results, _ = await self.youtube_api.search_videos(self.keyword, 20, None)
        
        if not search_results:
            print("검색 결과가 없습니다")
            self.db.close()
            return
        
        print(f"{len(search_results)}개 비디오 발견, 처리 시작...")
        
        tasks = []
        for result in search_results:
            video_id = result.get("id", {}).get("videoId")
            if video_id:
                tasks.append(self._process_video(video_id))

        processed_results = await asyncio.gather(*tasks)
        collected_count = sum(processed_results)
        processed_count = len(tasks)
        
        self.db.close()
        print(f"완료! {collected_count}개 수집됨 (총 {processed_count}개 처리)")
        self.logger.info(f"Batch completed: {collected_count} collected out of {processed_count} processed for '{self.keyword}'")
        
        return collected_count

    def is_promotional(self, title: str, description: str):
        promotional_keywords = ['광고', '협찬', '유료광고', 'PPL', 'sponsor', 'sponsored']
        text = (title + ' ' + description).lower()
        return any(keyword.lower() in text for keyword in promotional_keywords)

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python youtube_crawler.py <keyword>")
        sys.exit(1)
        
    keyword = sys.argv[1]
    
    crawler = YoutubeCrawler(keyword)
    asyncio.run(crawler.run())