import asyncio
import yt_dlp
import tempfile
import os
import json
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from .config import Config
from .logger import Logger

class YoutubeAPI:
    def __init__(self):
        self.logger = Logger("youtube_api.log", Config.LOG_LEVEL).get_logger()
        self.youtube = build("youtube", "v3", developerKey=Config.YOUTUBE_API_KEY)

    async def search_videos(self, query, max_results, page_token=None):
        try:
            request = self.youtube.search().list(
                q=query,
                part="id,snippet",
                type="video",
                order="viewCount",
                maxResults=max_results,
                relevanceLanguage="ko",
                pageToken=page_token
            )
            response = await asyncio.to_thread(request.execute)
            return response.get("items", []), response.get("nextPageToken")
        except Exception as e:
            self.logger.error(f"Search failed: {e}")
            return [], None

    async def get_video_details(self, video_id):
        try:
            def _probe():
                ydl_opts = {"quiet": True, "no_warnings": True, "skip_download": True}
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(video_id, download=False)
                    return {
                        "title": info.get("title", ""),
                        "description": info.get("description", ""),
                        "thumbnails": {"high": {"url": info.get("thumbnail", "")}},
                        "duration": info.get("duration")
                    }
            return await asyncio.to_thread(_probe)
        except Exception as e:
            self.logger.error(f"Video details failed for {video_id}: {e}")
            return {}

    async def get_captions_with_retry(self, video_id):
        for attempt in range(3):
            try:
                return await self.get_captions(video_id)
            except Exception as e:
                if attempt == 2:
                    self.logger.error(f"Captions failed after 3 attempts for {video_id}: {e}")
                    return []
                await asyncio.sleep(2 ** attempt)

    async def get_captions(self, video_id):
        ydl_opts = {
            'writesubtitles': True,
            'writeautomaticsub': True,
            'subtitleslangs': ['ko', 'ko-orig'],
            'subtitlesformat': 'json3',
            'skip_download': True,
            'quiet': True,
            'no_warnings': True,
        }
        
        with tempfile.TemporaryDirectory() as temp_dir:
            ydl_opts['outtmpl'] = os.path.join(temp_dir, '%(id)s.%(ext)s')
            return await asyncio.to_thread(self._extract_subtitles, video_id, ydl_opts, temp_dir)

    def _extract_subtitles(self, video_id, ydl_opts, temp_dir):
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(video_id, download=False)
                
                for lang in ['ko', 'ko-orig']:
                    if lang in info.get('subtitles', {}):
                        return self._download_and_parse(ydl, video_id, lang, temp_dir)
                
                for lang in ['ko', 'ko-orig']:
                    if lang in info.get('automatic_captions', {}):
                        return self._download_and_parse(ydl, video_id, lang, temp_dir)
                
                return []
        except Exception as e:
            raise Exception(f"Subtitle extraction failed: {e}")

    def _download_and_parse(self, ydl, video_id, lang, temp_dir):
        subtitle_opts = {
            'writeautomaticsub': True,
            'subtitleslangs': [lang],
            'subtitlesformat': 'json3',
            'skip_download': True,
            'outtmpl': os.path.join(temp_dir, f'{video_id}.%(ext)s'),
            'quiet': True,
            'no_warnings': True,
        }
        
        with yt_dlp.YoutubeDL(subtitle_opts) as subtitle_ydl:
            subtitle_ydl.download([video_id])
        
        subtitle_file = os.path.join(temp_dir, f"{video_id}.{lang}.json3")
        if not os.path.exists(subtitle_file):
            return []
        
        with open(subtitle_file, 'r', encoding='utf-8') as f:
            subtitle_data = json.load(f)
        
        captions = []
        for event in subtitle_data.get('events', []):
            if 'segs' in event:
                text = ''.join(seg.get('utf8', '') for seg in event['segs']).strip()
                if text:
                    captions.append({
                        'text': text,
                        'start': event.get('tStartMs', 0) / 1000.0,
                        'duration': event.get('dDurationMs', 0) / 1000.0
                    })
        
        return captions