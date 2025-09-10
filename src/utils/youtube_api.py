import asyncio
import yt_dlp
import tempfile
import os
import re
import json
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import HttpError as GoogleApiHttpError
from .config import Config
from .logger import Logger

class YoutubeAPI:
    def __init__(self):
        self.logger = Logger("youtube_api.log", Config.LOG_LEVEL).get_logger()
        self.youtube = build("youtube", "v3", developerKey=Config.YOUTUBE_API_KEY)

    async def _retry_on_error(self, func, *args, **kwargs):
        for attempt in range(Config.RETRY_COUNT):
            try:
                return await func(*args, **kwargs)
            except GoogleApiHttpError as e:
                if e.resp.status == 429:
                    self.logger.warning(f"Quota exceeded (429), retrying in {Config.RETRY_DELAY_SECONDS} seconds... (Attempt {attempt + 1}/{Config.RETRY_COUNT})")
                    await asyncio.sleep(Config.RETRY_DELAY_SECONDS)
                else:
                    self.logger.error(f"HTTP error occurred: {e}")
                    raise
            except Exception as e:
                self.logger.error(f"An error occurred: {e}, retrying... (Attempt {attempt + 1}/{Config.RETRY_COUNT})")
                await asyncio.sleep(Config.RETRY_DELAY_SECONDS)
        raise Exception(f"Failed after {Config.RETRY_COUNT} attempts.")

    async def search_videos(self, query, max_results):
        try:
            request = self.youtube.search().list(
                q=query,
                part="id,snippet",
                type="video",
                order="viewCount",
                maxResults=max_results,
                relevanceLanguage="ko"
            )
            response = await asyncio.to_thread(request.execute)
            return response.get("items", [])
        except Exception as e:
            self.logger.error(f"YouTube search API call failed: {e}")
            return []

    async def get_video_details(self, video_id):
        try:
            request = self.youtube.videos().list(
                part="snippet",
                id=video_id
            )
            response = await asyncio.to_thread(request.execute)
            return response.get("items", [{}])[0].get("snippet", {})
        except Exception as e:
            self.logger.error(f"Failed to get video details for {video_id}: {e}")
            return {}

    async def get_captions(self, video_id):
        """yt-dlp를 사용하여 자막을 추출합니다."""
        try:
            video_url = f"https://www.youtube.com/watch?v={video_id}"
            
            # yt-dlp 옵션 설정
            ydl_opts = {
                'writesubtitles': True,
                'writeautomaticsub': True,
                'subtitleslangs': ['ko', 'ko-orig'],  # 한국어 자막 우선
                'subtitlesformat': 'json3',  # JSON 형식으로 받음
                'skip_download': True,  # 비디오는 다운로드하지 않음
                'quiet': True,  # 로그 최소화
                'no_warnings': True,
            }
            
            # 임시 디렉토리 생성
            with tempfile.TemporaryDirectory() as temp_dir:
                ydl_opts['outtmpl'] = os.path.join(temp_dir, '%(id)s.%(ext)s')
                
                # yt-dlp 실행을 별도 스레드에서 처리
                result = await asyncio.to_thread(self._extract_subtitles_with_ytdlp, video_url, ydl_opts, temp_dir, video_id)
                return result
                
        except Exception as e:
            self.logger.error(f"Failed to get captions for {video_id} with yt-dlp: {e}")
            return []

    def _extract_subtitles_with_ytdlp(self, video_url, ydl_opts, temp_dir, video_id):
        """yt-dlp를 사용하여 실제로 자막을 추출합니다."""
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                # 비디오 정보 추출
                info = ydl.extract_info(video_url, download=False)
                
                # 자막 정보 확인
                subtitles = info.get('subtitles', {})
                automatic_captions = info.get('automatic_captions', {})
                
                # 수동 한국어 자막 우선 시도
                for lang_code in ['ko', 'ko-orig']:
                    if lang_code in subtitles:
                        self.logger.info(f"[{video_id}] Found manual Korean subtitles: {lang_code}")
                        return self._download_and_parse_subtitle(ydl, video_url, lang_code, temp_dir, video_id, is_manual=True)
                
                # 자동 생성 한국어 자막 시도
                for lang_code in ['ko', 'ko-orig']:
                    if lang_code in automatic_captions:
                        self.logger.info(f"[{video_id}] Found automatic Korean subtitles: {lang_code}")
                        return self._download_and_parse_subtitle(ydl, video_url, lang_code, temp_dir, video_id, is_manual=False)
                
                self.logger.info(f"[{video_id}] No Korean subtitles found")
                return []
                
        except Exception as e:
            self.logger.error(f"Error extracting subtitles for {video_id}: {e}")
            return []

    def _download_and_parse_subtitle(self, ydl, video_url, lang_code, temp_dir, video_id, is_manual=True):
        """자막을 다운로드하고 파싱합니다."""
        try:
            # 자막 다운로드 옵션 설정
            subtitle_opts = {
                'writesubtitles': True,
                'writeautomaticsub': not is_manual,
                'subtitleslangs': [lang_code],
                'subtitlesformat': 'json3',
                'skip_download': True,
                'outtmpl': os.path.join(temp_dir, f'{video_id}.%(ext)s'),
                'quiet': True,
                'no_warnings': True,
            }
            
            with yt_dlp.YoutubeDL(subtitle_opts) as subtitle_ydl:
                subtitle_ydl.download([video_url])
            
            # 다운로드된 자막 파일 찾기
            subtitle_file = os.path.join(temp_dir, f"{video_id}.{lang_code}.json3")
            
            if not os.path.exists(subtitle_file):
                self.logger.warning(f"[{video_id}] Subtitle file not found: {subtitle_file}")
                return []
            
            # JSON3 자막 파일 파싱
            import json
            with open(subtitle_file, 'r', encoding='utf-8') as f:
                subtitle_data = json.load(f)
            
            # JSON3 형식에서 텍스트 추출
            captions = []
            if 'events' in subtitle_data:
                for event in subtitle_data['events']:
                    if 'segs' in event:
                        text_parts = []
                        for seg in event['segs']:
                            if 'utf8' in seg:
                                text_parts.append(seg['utf8'])
                        if text_parts:
                            full_text = ''.join(text_parts).strip()
                            if full_text:
                                captions.append({
                                    'text': full_text,
                                    'start': event.get('tStartMs', 0) / 1000.0,  # 밀리초를 초로 변환
                                    'duration': event.get('dDurationMs', 0) / 1000.0
                                })
            
            self.logger.info(f"[{video_id}] Successfully extracted {len(captions)} caption segments")
            return captions
            
        except Exception as e:
            self.logger.error(f"Error downloading/parsing subtitle for {video_id}: {e}")
            return []