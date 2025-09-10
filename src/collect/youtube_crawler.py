import asyncio
from datetime import datetime
from ..utils.config import Config
from ..utils.db_handler import DBHandler
from ..utils.youtube_api import YoutubeAPI
from ..utils.text_utils import join_captions_from_ytdlp
from ..utils.logger import Logger

class YoutubeCrawler:
    def __init__(self, keyword, num_videos):
        self.keyword = keyword
        self.num_videos = num_videos
        self.logger = Logger("crawler_collect.log", Config.LOG_LEVEL).get_logger()
        self.db = DBHandler()
        self.youtube_api = YoutubeAPI()
        self.semaphore = asyncio.Semaphore(Config.CONCURRENCY_LIMIT)
        self.collected_count = 0

    async def _process_video(self, video_id):
        async with self.semaphore:
            self.logger.info(f"[{video_id}] Starting to process video.")
            try:
                # Check for existing video to prevent duplicates
                existing_data = self.db.get_video_data(video_id)
                if existing_data:
                    self.logger.info(f"[{video_id}] Video already exists. Skipping.")
                    return

                # Get video details
                details = await asyncio.wait_for(
                    self.youtube_api.get_video_details(video_id),
                    timeout=Config.TIMEOUT_SECONDS
                )
                if not details:
                    self.logger.warning(f"[{video_id}] Failed to get video details. Skipping.")
                    return

                # Get captions using yt-dlp
                self.logger.info(f"[{video_id}] Extracting captions with yt-dlp...")
                captions = await asyncio.wait_for(
                    self.youtube_api.get_captions(video_id),
                    timeout=Config.TIMEOUT_SECONDS * 2  # yt-dlp는 더 오래 걸릴 수 있음
                )
                
                raw_description = details.get("description", "")
                raw_captions = ""
                
                # yt-dlp 자막 형식을 문자열로 변환
                if captions:
                    raw_captions = join_captions_from_ytdlp(captions)
                    self.logger.info(f"[{video_id}] Successfully extracted captions: {len(raw_captions)} characters")
                else:
                    self.logger.warning(f"[{video_id}] No captions found")

                video_data = {
                    "video_id": video_id,
                    "title": details.get("title", ""),
                    "url": f"https://www.youtube.com/watch?v={video_id}",
                    "image_url": details.get("thumbnails", {}).get("high", {}).get("url", ""),
                    "raw_description": raw_description,
                    "raw_captions": raw_captions,
                    "captions_segments": captions,  # 원본 자막 세그먼트 정보도 저장
                    "metadata": {
                        "collected_at": datetime.now(datetime.UTC).isoformat(),
                        "caption_method": "yt-dlp",
                        "caption_segments_count": len(captions) if captions else 0
                    }
                }
                
                if self.db.insert_or_update_video(video_id, video_data):
                    self.collected_count += 1
                    self.logger.info(f"[{video_id}] Successfully collected. Total collected: {self.collected_count}")

            except asyncio.TimeoutError:
                self.logger.error(f"[{video_id}] Processing timed out after {Config.TIMEOUT_SECONDS * 2} seconds.")
            except Exception as e:
                self.logger.error(f"[{video_id}] An error occurred: {e}")

    async def run(self):
        self.db.connect()
        if not self.db.conn:
            return

        self.logger.info(f"Starting to crawl for keyword: '{self.keyword}'")
        search_results = await self.youtube_api.search_videos(self.keyword, self.num_videos)
        
        tasks = []
        for result in search_results:
            video_id = result.get("id", {}).get("videoId")
            if video_id:
                tasks.append(self._process_video(video_id))

        if not tasks:
            self.logger.info("No videos found to process.")
            self.db.close()
            return
            
        await asyncio.gather(*tasks)

        self.db.close()
        self.logger.info(f"Crawling finished. Collected {self.collected_count} videos out of {len(search_results)} search results.")

if __name__ == "__main__":
    import sys
    
    try:
        if len(sys.argv) < 3:
            print("Usage: python youtube_crawler.py <keyword> <num_videos>")
            sys.exit(1)
            
        keyword = sys.argv[1]
        num_videos = int(sys.argv[2])
        
        crawler = YoutubeCrawler(keyword, num_videos)
        
        # asyncio.run()을 try-except 블록으로 감싸서 예외를 포착합니다.
        try:
            asyncio.run(crawler.run())
        except Exception as e:
            print(f"An unhandled error occurred: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            sys.exit(1)
            
    except ValueError:
        print("Error: num_videos must be an integer.")
        sys.exit(1)